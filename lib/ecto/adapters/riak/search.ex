defmodule Ecto.Adapters.Riak.Search do
  @moduledoc """

  Yokozuna shares the same query syntax as Apache Solr.
  For Solr query options
  see -- http://wiki.apache.org/solr/CommonQueryParameters
  """

  alias Ecto.Adapters.Riak.Migration
  alias Ecto.Adapters.Riak.Object
  alias Ecto.Query.Normalizer
  alias Ecto.Query.Query
  alias Ecto.Query.QueryExpr
  alias Ecto.Query.Util

  ## ----------------------------------------------------------------------
  ## Types
  ## ----------------------------------------------------------------------
  
  @type bucket        :: binary
  @type entity        :: Ecto.Entity.t
  @type expr_field    :: atom  ## entity field reference
  @type expr_var      :: {atom, list, list}
  @type expr_select   :: {}
  @type expr_order_by :: {:asc | :desc, expr_var, expr_field}
  @type join_type     :: :inner | :left | :right | :full
  @type join_query    :: binary
  @type literal       :: term
  @type query         :: Ecto.Query.t
  @type querystring   :: binary
  @type query_tuple   :: {querystring, [search_option]}
  @type name          :: {bucket        :: binary,
                          entity_module :: atom,
                          model_module  :: atom}

  ## riak search result
  ## see -- https://github.com/basho/riak-erlang-client/blob/master/include/riakc.hrl
  @type search_doc    :: {index :: binary,
                          attr  :: [{binary, binary}]}
  @type search_result :: {:search_result,
                          docs :: [search_doc],
                          max_score :: integer,
                          num_found :: integer}

  @type source        :: {{bucket :: binary, unique_name :: integer},
                          entity_module :: atom,
                          model_module  :: atom}
  
  @type source_tuple  :: {{queryable :: binary,  ## the `x` in queryable x do ... end
                           varname   :: binary}, ## unique query variable
                          entity     :: atom,    ## entity module
                          model      :: atom}    ## module module
  @type search_option :: {:index, binary}
                       | {:q, query :: binary}
                       | {:df, default_field :: binary}
                       | {:op, :and | :or}
                       | {:start, integer} ## used for paging
                       | {:rows, integer}  ## max number of results
                       | {:sort, fieldname :: binary}
                       | {:filter, filterquery :: binary}
                       | {:presort, :key | :score}

  ## post processing
  @type post_proc_fun      :: ((entity) -> term)
  @type post_proc_group_by :: (([entity]) -> HashDict.t)
  @type post_proc_having   :: (([entity] | HashDict.t) -> HashDict.t)
  @type predicate_fun      :: ((term) -> boolean)

  ## ----------------------------------------------------------------------
  ## Constants
  ## ----------------------------------------------------------------------

  @yz_bucket_key "_yz_rb"
  @yz_riak_key   "_yz_rk"
  @yz_id_key     "_yz_id"
  @yz_meta_keys  [@yz_bucket_key, @yz_riak_key, @yz_id_key]

  unary_ops = [ -: "-", +: "+" ]

  binary_ops =
    [ ==: "=", !=: "!=", <=: "<=", >=: ">=", <:  "<", >:  ">",
      and: "AND", or: "OR",
      +:  "+", -:  "-", *:  "*",
      <>: "||", ++: "||",
      pow: "^", div: "/", rem: "%",
      date_add: "+", date_sub: "-",
      ilike: "ILIKE", like: "LIKE" ]

  functions =
    [ { { :downcase, 1 }, "lower" }, { { :upcase, 1 }, "upper" } ]

  Enum.map(unary_ops, fn { op, str } ->
    defp translate_name(unquote(op), 1), do: { :unary_op, unquote(str) }
  end)

  Enum.map(binary_ops, fn { op, str } ->
    defp translate_name(unquote(op), 2), do: { :binary_op, unquote(str) }
  end)

  Enum.map(functions, fn { { fun, arity }, str } ->
    defp translate_name(unquote(fun), unquote(arity)), do: { :fun, unquote(str) }
  end)

  defp translate_name(fun, _arity), do: { :fun, atom_to_binary(fun) }

  @binary_ops Dict.keys(binary_ops)

  ## ----------------------------------------------------------------------
  ## API
  ## ----------------------------------------------------------------------
  """
  # Notes on query support

  Order of operations:

  1. form query from any LIMIT, OFFSET, ORDER_BY, and WHERE clauses

  2. for each JOIN clause, construct multiple search queries 
     to obtain the correct info.
 
  3. generate post-processing function 
     for dealing with GROUP_BY, HAVING, and SELECT clauses.
     The order of post-processing is:

     (a) form GROUP_BY groups
     (b) apply HAVING aggregate functions
     (c) perform SELECT transformations

  4. Results processing
     - perform sibling resolution
     - perform model migration if needed
     - if there are joins, merge results based on the join
     - apply the post-processing function from step (3)
  
  ----------------------------------------------------------------------
  Explanation of the query operation semantics follows ...
  ----------------------------------------------------------------------

  ## FROM

  Riak will only support a single from clause.


  ## LIMIT, OFFSET, ORDER BY

  These are supported by YZ, and can work just fine as a straightforward query.


  ## WHERE

  The constraints imposed in a where clause are always applied after 
 
  
  ## GROUP BY and HAVING

  These are going to require post-processing.
  


  ## JOINS are a complicated operation to support

  It is possible to model a join over riak mapreduce,
  but mapreduce is inefficient, with non-deterministic latency.

  Yokuzuna on the other hand, has no true notion of joins.
  It does support a join keyword, which only works as a RIGHT OUTER join.

  Essentially, we're left with having to manually manage joins.

  
  The way that we will approach the problem will be to request all 
  objects which have either of the models
  ie: the query 
  
    from p in Posts
      join: c in Comments, on: c.post_id == p.id
      left_join: lp in p.permalink
      left_join: lc in c.permalink
      select: { p.title, lp.link, c.text, lc.link }

  Would first retrieve all Posts (the model in the from clause),
  and then traverse down to it's associations and fetch objects which require.

  Hence, the query above would require a total of 3 search requests to Riak,
  collecting the appropriate information along the way.

  """

  @doc """
  Constructs a {querystring, [search_option]} tuple which can be
  sent to YZ as a search request, and then provides a function 
  post_proc_fun/1 which should then be called on all results of
  the search query in the execute/3 function below.
  """        
  @spec query(query) :: {{querystring, [search_option]}, post_proc_fun}
  def query(Query[] = query) do
    sources = create_names(query)  # :: [source]
    {_, varnames} = from(query, sources)

    ## joins should not be support
    join     = join(query)
    
    select   = select_query(query.select, sources)
    order_by = order_by(query.order_bys, sources)
    limit    = limit(query.limit)
    offset   = limit(query.offset)

    select_post_proc = select_post_proc(query.select, sources)
    post_proc = fn(x)->
                    select_post_proc.(x)
                end

    ## querystring is just the "q" part 
    ## of the arguments to Yokozuna
    querystring = ""
    options = List.flatten([order_by, limit, offset])
      |> Enum.filter(&(nil != &1))
    query_part = {querystring, options}
    
    {query_part, post_proc}
  end

  @doc """
  Executes a search query using a provided worker pid,
  returning a list of valid entities (or an empty list)
  """
  @spec execute(pid, {querystring, [search_option]}, post_proc_fun) :: [entity]
  def execute(worker, {querystring, opts}, post_proc_fun) do
    case Riak.search(worker, querystring, opts) do
      {:ok, search_result} ->
        ## see search_doc type
        search_docs = elem(search_result, 1)

        ## Reduce search_docs into a HashDict mapping
        ## the riak object key to a list of stateboxes which
        ## can be used for sibling resolution
        doc_dict = Enum.reduce(search_docs,
                               HashDict.new,
                               fn(x, acc)->
                                   {key, json} = parse_search_result(x)
                                   box = Object.resolve_json(json)
                                   fun = &([box | &1])
                                   HashDict.update(acc, key, [box], fun)
                               end)

        ## Use doc_dict to resolve any duplicates (riak siblings).
        ## The resulting list is a list of entity objects
        resolved = Enum.map(HashDict.to_list(doc_dict),
                            fn({_, box_list})->
                                :statebox_orddict.from_values(box_list)
                                |> Object.statebox_to_entity
                            end)

        ## Perform any migrations if needed
        migrated = Enum.map(resolved, &Migration.migrate/1)
          
        ## Apply post_proc_fun and we're done
        Enum.map(migrated, &post_proc_fun.(&1))
      _ ->
        []
    end
  end  

  @spec parse_search_result(search_doc) :: {riak_key :: binary, ListDict :: tuple}
  defp parse_search_result({_, doc}) do
    riak_key = Dict.get(doc, @yz_riak_key)

    ## ==============
    ## Construct JSON
    
    json =
      ## Filter out YZ keys
      Enum.filter(doc, fn(x)-> ! (x in @yz_meta_keys) end)
    
      ## Riak Search returns list values as multiple key-value tuples
      ## ie: { "a":[1,2] } gets returned as: [{"a",1}, {"a",2}]
      ## Any duplicate keys should put their values into a list
      |> Enum.reduce(HashDict.new(),
                     fn({k,v}, acc)->
                         fun = fn(existing)->
                                   if HashDict.has_key?(acc, k) do
                                     if is_list(existing) do
                                       [v | existing]
                                     else
                                       [v, existing]
                                     end
                                   else
                                     v
                                   end
                               end
                         HashDict.update(acc, k, v, fun)
                     end)
      
      ## Transform to list and filter out nil values
      |> HashDict.to_list
      |> Enum.filter(fn({k,v})-> v != nil end)

    ## Return
    {riak_key, json}
  end
  
  @doc """
  Constructs a portion of the query which narrows down
  search objects to be of a particular model,
  and returns the variable names used in the from query
  """
  @spec from(name, source_tuple) :: {bucket, [vars :: binary]}
  defp from(from, sources) do
    from_model = Util.model(from)
    source = tuple_to_list(sources)
             |> Enum.find(&(from_model == Util.model(&1)))
    {bucket, name} = Util.source(source)
    {"_model_s:#{from_model}", [name]}
  end

  @spec select_query(expr_select, [source]) :: {:fl, binary}
  defp select_query(expr, sources) do
    ## Selects various fields from the entity object by using
    ## the "fl" option to YZ
    ## doc -- http://wiki.apache.org/solr/CommonQueryParameters#fl
    QueryExpr[expr: expr] = Normalizer.normalize_select(expr)
  end
  
  @spec select_post_proc(expr_select, [source]) :: post_proc_fun
  defp select_post_proc(expr, sources) do
  end

  ##@spec join(query, term, term) :: {join_type, [join_query]}
  
  defp join(query) do
    case query.joins do
      [] ->
        nil
      _ ->
        raise Ecto.QueryError, reason: "Riak adapter does not support joins"
    end
  end
  
  defp join(query, sources, varnames) do
    sources_list = tuple_to_list(sources)
  end

  @spec where(QueryExpr.t, [source]) :: querystring
  defp where(wheres, sources) do
    Enum.map_join(wheres,
                  " ",
                  fn(QueryExpr[expr: expr])->
                      expr(expr, sources)
                  end)
  end

  @spec group_by(term, [source]) :: post_proc_group_by
  
  defp group_by([], _) do
    ## Empty group_by query should simply return 
    ## the entire list of entities
    fn(entities) -> entities end
  end

  defp group_by(group_bys, sources) do
    ## create a post-processing function
    ## that accumulates values in a HashDict.
    ## (intended to be called as a function to Enum.reduce/3)
    ## 
    ## The hashdict maps tuples of field values
    ## to a list of entities which have that value.
    ## eg: grouping by [category_id, title]
    ##     could result in example groups:
    ##    { {2, "fruits"}  => [ ... ],
    ##      {3, "veggies"} => [ ... ],
    ##      ... }
    ## 
    ## An entity should only appear once in this dict
    
    fields = Enum.map(group_bys,
                      fn(expr)->
                          Enum.map(expr.expr, fn({ var, field })-> field end)
                      end)
    fields = Enum.flatten(fields)

    fn(entities)->
        fun = fn(entity, dict)->
                  ## Get values using fields to create key tuple
                  model = elem(entity, 0)
                  fields = model.__entity__(:entity_kw, entity)
                  values = Enum.map(fields, fn(x)-> Dict.get(fields, x, nil) end)
                  key = list_to_tuple(values)
                  HashDict.update(dict, key, [entity], fn(e)-> [entity | e] end)
              end
        Enum.reduce(entities, HashDict.new, fun)
    end
  end
  
  @spec having([term], [source]) :: post_proc_having

  def having([], _) do
    fn(entities) -> entities end
  end

  def having(havings, sources) do
    ## construct predicate function which gets called
    ## with an entity argument to determine if that entity
    ## fits the criteria of all the havings clauses
    
    pred = fn(entity)->
               true
           end

    ## Construct post proc function
    fn(entities) ->
        if is_list(entities) do
          Enum.map(entities, &having_post_process(&1, pred))
        else
          HashDict.values(entities)
          |> Enum.map(fn(entity_list)->
                          Enum.map(entity_list, &having_post_process(&1, pred))
                      end)
        end
    end
  end

  @spec having_post_process(entity, predicate_fun) :: boolean
  defp having_post_process(entity, pred) do
    pred.(entity)
  end

  @spec order_by(expr_order_by, [source]) :: {:sort, binary}
  defp order_by(order_bys, sources) do
    ## constructs the "sort" option to Yokozuna
    ## docs -- http://wiki.apache.org/solr/CommonQueryParameters#sort
    querystring =
      Enum.map_join(order_bys, ", ", 
                    fn(expr)->
                        Enum.map_join(expr.expr, ", ", &order_by_expr(&1, sources))
                    end)
    {:sort, querystring}
  end

  defp order_by_expr({direction, expr_var, field}, sources) do
    ##{_, name} = Util.find_source(sources, expr_var) |> Util.source
    str = "#{field}"
    str <> case direction do
             :asc  -> " asc"
             :desc -> " desc"
           end
  end

  @spec limit(integer) :: search_option
  defp limit(num) when is_integer(num) do
    {:rows, num}
  end 

  defp offset(num) when is_integer(num) do
    {:start, num}
  end

  ## ----------------------------------------------------------------------
  ## Variable Handling
  ## ----------------------------------------------------------------------

  @spec create_names(query) :: source_tuple
  defp create_names(query) do
    ## Creates unique variable names for a query
    sources = query.sources |> tuple_to_list
    Enum.reduce(sources,
                [],
                fn({ queryable, entity, model }, acc)->
                    name = unique_name(acc, String.first(queryable), 0)
                    [{{queryable,name}, entity, model} | acc]
                end)
    |> Enum.reverse
    |> list_to_tuple
  end

  @spec unique_name(list, binary, integer) :: binary
  defp unique_name(names, name, counter) do
    counted_name = name <> integer_to_binary(counter)
    if Enum.any?(names, fn({ { _, n }, _, _ })-> n == counted_name end) do
      unique_name(names, name, counter+1)
    else
      counted_name
    end
  end

  ## ----------------------------------------------------------------------
  ## Function Expressions
  ## ----------------------------------------------------------------------

  ##@spec expr(literal, [source]) :: binary ## part of a query expression

  ## Field access
  def fexpr({:., _, [{{:&, _, [_]}=var, field}]}, sources) when is_atom(field) do
    quote do
      ##elem(entity, 0).__entity__(:entity_kw, entity) |> Dict.get(field)
    end
  end

  ## Negation
  def fexpr({:!, _, [expr]}, sources) do
    quote do
      not expr(unquote(expr), sources)
    end
  end  
  
  def fexpr({:&, _, [_]}=var, sources) do
    
  end

  def fexpr({:==, _, [nil, right]}, sources) do
    fn()-> right == nil end
  end

  def fexpr({:==, _, [left, nil]}, sources) do
    quote do
      (unquote(left) == nil)
    end
  end

  def fexpr({:!=, _, [nil, right]}, sources) do
    quote do
      (unquote(right) == nil)
    end
  end

  def fexpr({:!=, _, [left, nil]}, sources) do
    quote do
      (unquote(left) == nil)
    end
  end

  def fexpr(list, sources) when is_list(list) do
  end

  def fexpr(literal, sources) do
    quote do
      literal(unquote(literal))
    end
  end

  ## ----------------------------------------------------------------------
  ## Binary Expressions
  ## ----------------------------------------------------------------------
  
  def expr({:., _, [{:&, _, [_]}=var, field]}, sources) when is_atom(field) do
    source = Util.find_source(sources, var)
    entity = Util.entity(source)
    type = entity.__entity__(:field_type, field)
    yz_key(field, type)
  end

  def expr({:!, _, [expr]}, sources) do
    "-" <> expr(expr, sources)
  end

  def expr({:&, _, [_]}=var, sources) do
    ## TODO
  end

  def expr({:==, _, [nil, right]}, sources) do
    op_to_binary(right, sources)
  end

  def expr({:==, _, [left, nil]}, sources) do
    op_to_binary(left, sources)
  end

  def expr({:!=, _, [nil, right]}, sources) do
    op_to_binary(right, sources)
  end

  def expr({:!=, _, [left, nil]}, sources) do
    op_to_binary(left, sources)
  end

  ## element in range
  def expr({:in, _, [left, Range[first: first, last: last]]}, sources) do
    field = expr(left, sources)
    range_start = expr(first, sources)
    range_end = expr(last, sources)
    "#{field}:[#{range_start} TO #{range_end}]"
  end

  ## element in collection
  def expr({:in, _, [left, right]}, sources) do
    expr(left, sources) <> ":(" <> expr(right, sources) <> ")"
  end

  ## range handling
  def expr(Range[] = range, sources) do
    expr(Enum.to_list(range), sources)
  end

  ## range handling
  def expr({:.., _, [first, last]}, sources) do
    expr(Enum.to_list(first..last), sources)
  end
  
  ## division (arithmetic)
  ## use solr "div" function
  def expr({:/, _, [left, right]}, sources) do
    "div(#{left}, #{right})"
  end

  def expr({arg, _, []}, sources) when is_tuple(arg) do
    expr(arg, sources)
  end

  def expr({fun, _, args}, sources) when is_atom(fun) and is_list(args) do
    case translate_name(fun, length(args)) do
      { :unary_op, op } ->
        arg = expr(Enum.first(args), sources)
        op <> arg
      { :binary_op, op } ->
        [left, right] = args
        op_to_binary(left, sources) <> " #{op} " <> op_to_binary(right, sources)
      { :fun, "localtimestamp" } ->
        "localtimestamp"
      { :fun, fun } ->
        "#{fun}(" <> Enum.map_join(args, ", ", &expr(&1, sources)) <> ")"
    end
  end

  def expr(list, sources) when is_list(list) do
    Enum.map_join(list, " ", &expr(&1, sources))
  end

  def expr(literal, sources) do
    literal(literal)
  end

  ## --------------------
  ## Operations
  
  defp op_to_binary({op, _, [_, _]}=expr, sources) when op in @binary_ops do
    "(" <> expr(expr, sources) <> ")"
  end

  defp op_to_binary(expr, sources) do
    expr(expr, sources)
  end

  ## --------------------
  ## Handling of literals
  @spec literal(term) :: binary
  
  defp literal(nil), do: "null"

  defp literal(true), do: "true"
  
  defp literal(false), do: "false"

  defp literal(Ecto.DateTime[] = dt) do
    "#{dt.year}-#{dt.month}-#{dt.day}T#{dt.hour}:#{dt.min}:#{dt.sec}Z"
  end

  defp literal(Ecto.Interval[] = i) do
    "#{i.year}-#{i.month}-#{i.day}T#{i.hour}:#{i.min}:#{i.sec}Z"
  end

  defp literal(Ecto.Binary[value: binary]) do
    :base64.encode(binary)
  end

  defp literal(literal) when is_binary(literal) do
    literal  ## TODO: escaping
  end

  defp literal(literal) when is_number(literal) do
    to_string(literal)
  end

  ## ----------------------------------------------------------------------
  ## Key and Value De/Serialization
  ## ----------------------------------------------------------------------

  @yz_key_regex  %r"_(i|is|f|fs|b|bs|b64_s|b64_ss|s|ss|i_dt|i_dts|dt|dts)$"

  @doc """
  Removes the default YZ schema suffix from a key
  schema: https://github.com/basho/yokozuna/blob/develop/priv/default_schema.xml
  """
  @spec key_from_yz(binary) :: binary
  def key_from_yz(key) do
    Regex.replace(@yz_key_regex, to_string(key), "")
  end

  @doc """
  adds a YZ schema suffix to a key depending on its type
  """
  @spec yz_key(binary, atom | {:list, atom}) :: binary
  def yz_key(key, type) do
    to_string(key) <> "_" <>
      case type do
        :integer  -> "i"
        :float    -> "f"
        :binary   -> "b64_s"
        :string   -> "s"
        :boolean  -> "b"
        :datetime -> "dt"
        :interval -> "i_dt"
        {:list, list_type} ->
          case list_type do
            :integer  -> "is"
            :float    -> "fs"
            :binary   -> "b64_ss"
            :string   -> "ss"
            :boolean  -> "bs"
            :datetime -> "dts"
            :interval -> "i_dts"
          end
      end
  end

  def yz_key_atom(key, type) do
    yz_key(key, type) |> to_atom
  end

  @spec yz_key_type(binary) :: atom | {:list, atom}
  def yz_key_type(key) do
    [suffix] = Regex.run(@yz_key_regex, key)
      |> Enum.filter(&String.starts_with?(&1, "_"))
    case suffix do
      "i"      -> :integer
      "f"      -> :float
      "b64_s"  -> :binary
      "s"      -> :string
      "b"      -> :boolean
      "dt"     -> :datetime
      "i_dt"   -> :interval
      "is"     -> {:list, :integer}
      "fs"     -> {:list, :float}
      "b64_ss" -> {:list, :binary}
      "ss"     -> {:list, :string}
      "bs"     -> {:list, :boolean}
      "dts"    -> {:list, :datetime}
      "i_dts"  -> {:list, :interval}
    end
  end

  @doc """
  Returns true if the key has a YZ suffix that indicates
  a multi-value (list) type
  """
  def is_list_key?(key) when is_binary(key) do
    regex = %r"_[is|fs|bs|ss|b64_ss|dts]$"
    Regex.match?(regex, key)
  end

  defp to_atom(x) when is_atom(x), do: x

  defp to_atom(x) when is_binary(x) do
    try do
      binary_to_existing_atom(x)
    catch
      _,_ -> binary_to_atom(x)
    end
  end
  
end