defmodule Ecto.Adapters.Riak.Search do
  @moduledoc """

  Yokozuna shares the same query syntax as Apache Solr.
  For Solr query options
  see -- http://wiki.apache.org/solr/CommonQueryParameters
  """

  alias Ecto.Adapters.Riak.Datetime
  require Ecto.Adapters.Riak.Datetime
  alias Ecto.Adapters.Riak.Migration
  alias Ecto.Adapters.Riak.Object
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
  @type search_index  :: binary

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
  
  ## See -- https://github.com/elixir-lang/ecto/blob/master/lib/ecto/query/api.ex
  ## for api functions to support
  @where_unary_ops   [:-, :+, :now]
  @where_binary_ops  [:==, :!=, :<=, :>=, :<, :>, :and, :or, :like, :date_add, :date_sub]
  @select_funs       [:random, :now, :localtimestamp]
  @select_unary_ops  [:-, :+, :round,
                      :downcase, :upcase]
  @select_binary_ops [:pow, :rem, :/, :*, :+, :-,
                      :==, :!=, :<=, :<, :>=, :>,
                      :and, :or,
                      :<>, :++,
                      :date_add, :date_sub]
  @select_aggr_ops   [:avg, :count, :max, :min, :sum]

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
     (b) apply HAVING aggregate functions  (NOT YET IMPLEMENTED)
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
  
  For now, only the group_by clause is supported


  ## JOINS are not supported

  """

  @doc """
  Constructs a tuple {{querystring, [search_option]}, post_proc_fun}
  The querystring and search_option are sent to YZ as a search request,
  where post_proc_fun/1 should  be called on all results of
  the search query in the execute/3 function below.
  """        
  @spec query(query) :: {{search_index, querystring, [search_option]}, post_proc_fun}
  def query(Query[] = query) do    
    sources = create_names(query)  # :: [source]
    search_index = Util.model(query.from) |> to_string
    
    ## Check to see if join is specified
    ## and raise error if present
    join(query) 

    where    = where(query.wheres, sources)    
    order_by = order_by(query.order_bys)
    limit    = limit(query.limit)
    offset   = offset(query.offset)

    group_by_post_proc = group_by(query.group_bys)
    ##having   = having(query.havings, sources)    
    select_post_proc  = select_post_proc(query.select)
    post_proc = fn(entities)->
                    group_by_post_proc.(entities)
                    |> select_post_proc.()
                end

    ## querystring is just the "q" part 
    ## of the arguments to Yokozuna
    querystring = Enum.join([where])
    options = List.flatten([order_by, limit, offset])
      |> Enum.filter(&(nil != &1))
    query_part = {search_index, querystring, options}
    
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
      |> Enum.filter(fn({_,v})-> v != nil end)

    ## Return
    {riak_key, json}
  end
  
  """
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

  ## ----------------------------------------------------------------------
  ## SELECT
  ## ----------------------------------------------------------------------
  
  @spec select_post_proc(expr_select) :: post_proc_fun
  defp select_post_proc(select) do
    ## Returns a function that takes an entity,
    ## extracts the needed fields for the select expression
    ## and transform it to the appropriate datastructure
    ##
    ## Note that because joins are not suppoted in Riak
    ## you can only perform select transformstions on the 
    ## model referenced in the `from` clause
    fn(entities)->
        cond do
          entities == [] ->
            []
          hd(entities) |> is_list ->
            ## case where we have a group_by or having clause.
            ## entities :: [ [entity] ]
            Enum.map(entities, &select_aggregate_transform(select.expr, &1))
          true ->
            ## entities :: [entity]
            Enum.map(entities, &select_transform(select.expr, &1))
        end
    end
  end

  ## select_transform/3 takes an expr 
  @spec select_transform(tuple, entity)
    :: {transformed :: term, expr_acc :: tuple}

  defp select_transform({:{}, _, list}, entity) do
    select_transform(list, entity)
    |> list_to_tuple
  end

  defp select_transform({_, _}=tuple, entity) do
    select_transform(tuple_to_list(tuple), entity)
    |> list_to_tuple
  end

  defp select_transform(list, entity) when is_list(list) do
    Enum.map(list,
             fn(elem)->
                 select_transform(elem, entity)
             end)
  end

  defp select_transform({{:., _, [{:&, _, [_]}, field]}, _, _}, entity) when is_atom(field) do
    ## attribute accessor
    entity_keyword(entity)[field]
  end

  defp select_transform({fun, _, _}, _)
  when is_atom(fun) and fun in @select_funs do
    case fun do
      :random ->
        :random.uniform()
      :now ->
        ## GMT timestamp
        Datetime.now_ecto_datetime()
      :localtimestamp ->
        ## timestamp with respect to the current timezone
        Datetime.now_local_ecto_datetime()
      _ ->
        raise Ecto.QueryError, reason: "unsupported select function: #{fun}"
    end
  end

  defp select_transform({op, _, args}, entity)
  when is_atom(op) and length(args) == 1 and op in @select_unary_ops do
    arg = select_transform(Enum.first(args), entity)
    case op do
      :- ->
        -1 * arg
      :+ ->
        arg
      :round ->
        Kernel.round(arg)
      :downcase ->
        String.downcase(arg)
      :upcase ->
        String.upcase(arg)
      _ ->
        raise Ecto.QueryError, reason: "unsupported select unary op: #{op}"
    end
  end

  defp select_transform({op, _, [left, right]}, entity)
  when is_atom(op) and op in @select_binary_ops do
    left = select_transform(left, entity)
    right = select_transform(right, entity)
    case op do
      :pow ->
        :math.pow(left, right)
      :rem ->
        rem(left, right)
      :+ ->
        left + right
      :- ->
        left - right
      :/ ->
        left / right
      :* ->
        left * right      
      :== ->
        left == right
      :!= ->
        left != right
      :<= ->
        left <= right
      :< ->
        left < right
      :>= ->
        left >= right
      :> ->
        left > right
      :and ->
        left and right
      :or ->
        left or right
      :<> ->
        left <> right
      :++ ->
        left ++ right
      :date_add ->
        nil
      :date_sub ->
        nil
      _ ->
        raise Ecto.QueryError, reason: "unsupported select binary op: #{op}"
    end
  end

  defp select_transform({:&, _, [_]}, entity) do
    entity
  end

  defp select_transform({_, _, args}, entity) do
    select_transform(args, entity)
  end

  defp select_transform(x, _) do
    x
  end

  ## -- select aggregate transform --    

  defp select_aggregate_transform({op, _, args}, entities)
  when is_atom(op) and op in @select_aggr_ops do
    ## first argument of args must be a field accessor
    {{:., _, [{:&, _, _}, field]}, _, _} = hd(args)
    
    ## Extractor functions
    value_fn = fn(entity)-> entity_keyword(entity)[field] end
    value_type_fn = fn(entity)-> entity_field_type(entity, field) end
    
    ## Dispatch
    case op do
      :avg ->
        length = length(entities)
        sum = Enum.reduce(entities, 0, fn(entity, acc)-> acc + value_fn.(entity) end)
        sum / length
      :count ->
        length(entities)
      :max ->
        Enum.map(entities, value_fn)
        |> Enum.max
      :min ->
        Enum.map(entities, value_fn)
        |> Enum.min
      :sum ->
        Enum.reduce(entities, 0, fn(entity, acc)->
                                     value = value_fn.(entity)
                                     value = case value_type_fn.(entity) do
                                               :integer -> round(value)
                                               :float   -> value
                                             end
                                     acc + value
                                 end)
      _ ->
        raise Ecto.QueryError, reason: "unsupported select aggregate op: #{op}"
    end
  end

  ## ----------------------------------------------------------------------
  ## JOIN
  ## ----------------------------------------------------------------------
  
  defp join(query) do
    case query.joins do
      [] ->
        nil
      _ ->
        raise Ecto.QueryError, reason: "Riak adapter does not support joins"
    end
  end

  ## ----------------------------------------------------------------------
  ## WHERE
  ## ----------------------------------------------------------------------

  @spec where(QueryExpr.t, [source]) :: querystring
  defp where(wheres, sources) do
    Enum.map_join(wheres,
                  " ",
                  fn(QueryExpr[expr: expr])->
                      expr(expr, sources)
                  end)
  end

  ## ----------------------------------------------------------------------
  ## GROUP BY
  ## ----------------------------------------------------------------------

  @spec group_by(term) :: post_proc_group_by
  
  defp group_by([]) do
    ## Empty group_by query should simply return 
    ## the entire list of entities
    fn(entities) -> entities end
  end

  defp group_by(group_bys) do
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
                          Enum.map(expr.expr, fn({_, field})-> field end)
                      end)
    fields = List.flatten(fields)

    fn(entities)->
        fun = fn(entity, dict)->
                  ## Get values using fields to create key tuple
                  entity_kw = entity_keyword(entity)
                  values = Enum.map(fields, &(entity_kw[&1]))
                  key = list_to_tuple(values)
                  HashDict.update(dict, key, [entity], fn(e)-> [entity | e] end)
              end
        Enum.reduce(entities, HashDict.new, fun)
        |> HashDict.values
    end
  end

  ## ----------------------------------------------------------------------
  ## HAVING
  ## ----------------------------------------------------------------------
  
  @spec having([term], [source]) :: post_proc_having

  def having(havings, _) do
    case havings do
      [] ->
        nil
      _ ->
        raise Ecto.QueryError, reason: "Riak adapter does not support having clause"
    end
  end

  def having(havings, _) do
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

  ## ----------------------------------------------------------------------
  ## ORDER BY, LIMIT, and OFFSET
  ## ----------------------------------------------------------------------

  @spec order_by(expr_order_by) :: {:sort, binary}
  
  defp order_by([]), do: nil

  defp order_by(order_bys) do
    ## constructs the "sort" option to Yokozuna
    ## docs -- http://wiki.apache.org/solr/CommonQueryParameters#sort
    querystring =
      Enum.map_join(order_bys, ", ", 
                    fn(expr)->
                        Enum.map_join(expr.expr, ", ", &order_by_expr(&1))
                    end)
    {:sort, querystring}
  end

  defp order_by_expr({direction, _, field}) do
    str = "#{field}"
    str <> case direction do
             :asc  -> " asc"
             :desc -> " desc"
           end
  end

  @spec limit(integer) :: search_option
  defp limit(nil), do: nil
  defp limit(num) when is_integer(num) do
    {:rows, num}
  end 

  defp offset(nil), do: nil
  defp offset(num) when is_integer(num) do
    {:start, num}
  end

  ## ----------------------------------------------------------------------
  ## Variable Handling
  ## ----------------------------------------------------------------------

  @spec create_names(query) :: source_tuple
  def create_names(query) do
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
  # def fexpr({:., _, [{{:&, _, [_]}=var, field}]}, sources) when is_atom(field) do
  #   quote do
  #   end
  # end

  # ## Negation
  # def fexpr({:!, _, [expr]}, sources) do
  #   quote do
  #     not expr(unquote(expr), sources)
  #   end
  # end  
  
  # def fexpr({:&, _, [_]}=var, sources) do    
  # end

  # def fexpr({:==, _, [nil, right]}, sources) do
  #   fn()-> right == nil end
  # end

  # def fexpr({:==, _, [left, nil]}, sources) do
  #   quote do
  #     (unquote(left) == nil)
  #   end
  # end

  # def fexpr({:!=, _, [nil, right]}, sources) do
  #   quote do
  #     (unquote(right) == nil)
  #   end
  # end

  # def fexpr({:!=, _, [left, nil]}, sources) do
  #   quote do
  #     (unquote(left) == nil)
  #   end
  # end

  # def fexpr(list, sources) when is_list(list) do
  # end

  # def fexpr(literal, sources) do
  #   quote do
  #     literal(unquote(literal))
  #   end
  # end

  ## ----------------------------------------------------------------------
  ## Binary Expressions
  ## ---------------------------------------------------------------------- 

  defp expr({:., _, [{:&, _, [_]}=var, field]}, sources) when is_atom(field) do
    source = Util.find_source(sources, var)
    entity = Util.entity(source)
    type = entity.__entity__(:field_type, field)
    yz_key(field, type)
  end

  defp expr({:!, _, [expr]}, sources) do
    "-" <> expr(expr, sources)
  end

  defp expr({:==, _, [nil, right]}, sources) do
    "-" <> expr(right, sources) <> ":*"
  end

  defp expr({:==, _, [left, nil]}, sources) do
    "-" <> expr(left, sources) <> ":*"
  end

  defp expr({:!=, _, [nil, right]}, sources) do
    expr(right, sources) <> ":*"
  end

  defp expr({:!=, _, [left, nil]}, sources) do 
    expr(left, sources) <> ":*"
  end

  ## element in range
  defp expr({:in, _, [left, Range[first: first, last: last]]}, sources) do
    field = expr(left, sources)
    range_start = expr(first, sources)
    range_end = expr(last, sources)
    "#{field}:[#{range_start} TO #{range_end}]"
  end

  ## element in collection
  defp expr({:in, _, [left, right]}, sources) do
    expr(left, sources) <> ":(" <> expr(right, sources) <> ")"
  end

  ## range handling
  defp expr(Range[] = range, sources) do
    expr(Enum.to_list(range), sources)
  end

  ## range handling
  defp expr({:.., _, [first, last]}, sources) do
    expr(Enum.to_list(first..last), sources)
  end 

  defp expr({:/, _, _}, _) do
    raise Ecto.QueryError, reason: "where queries to Riak do not permit the `/` operator"
  end

  defp expr({:pow, _, _}, _) do
    raise Ecto.QueryError, reason: "where queries to Riak do not permit the `pow` operator"
  end

  defp expr({:rem, _, _}, _) do
    raise Ecto.QueryError, reason: "where queries to Riak do not permit the `rem` operator"
  end

  defp expr({arg, _, []}, sources) when is_tuple(arg) do
    expr(arg, sources)
  end

  defp expr({fun, _, args}, sources) 
  when is_atom(fun) and is_list(args) and fun in @where_unary_ops do
    arg = expr(Enum.first(args), sources)
    case fun do
      :now ->
        "NOW"
      _ ->
        "#{fun}#{arg}"
    end
  end    

  defp expr({fun, _, [left, right]}, sources)
  when is_atom(fun) and fun in @where_binary_ops do
    cond do
      ## Datetime operations
      not Datetime.ecto_timestamp?(left) and Datetime.ecto_timestamp?(right) ->
        left = op_to_binary(left, sources)
        right = op_to_binary(right, sources)
        case fun do
          :== ->
            left <> ":" <> right
          :!= ->
            "-" <> left <> ":" <> right
          _ ->
            raise Ecto.QueryError, reason: "where query invalid function #{fun} for right-side datetime"
        end
      Datetime.ecto_timestamp?(left) and Datetime.ecto_timestamp?(right) ->
        case fun do
          :date_add ->
            Datetime.solr_datetime(left) <> Datetime.solr_datetime_add(right)
          :date_sub ->
            Datetime.solr_datetime(left) <> Datetime.solr_datetime_subtract(right)
          _ ->
            raise Ecto.QueryError, reason: "where query invalid function #{fun} for datetime args"
        end
      
      ## Rest
      true ->
        left = op_to_binary(left, sources)
        right = op_to_binary(right, sources)
        case fun do
          :== ->
            left <> ":" <> right
          :!= ->
            left <> ":" <> right
          :> ->
            right = try do
                      binary_to_integer(right) + 1 |> to_string
                    catch
                      _,_ -> right
                    end
            left <> ":[" <> right <> " TO *]"
          :>= ->
            left <> ":[" <> right <> " TO *]"
          :< ->
            right = try do
                      binary_to_integer(right) - 1 |> to_string
                    catch
                      _,_ -> right
                    end
            left <> ":[* TO " <> right <> "]"
          :<= ->
            left <> ":[* TO " <> right <> "]"
          :and ->
            left <> " AND " <> right
          :or ->
            left <> " OR " <> right
          :like ->
            left <> ":*" <> right <> "*"
          _ ->
            raise Ecto.QueryError, reason: "where query unknown function #{fun}"
        end
    end
  end

  defp expr(list, sources) when is_list(list) do
    Enum.map_join(list, " ", &expr(&1, sources))
  end

  defp expr(literal, _) do
    literal(literal)
  end 
  
  defp op_to_binary({op, _, [x, y]}=expr, sources) when op in @where_binary_ops do
    case op do
      :== when x == nil or y == nil ->
        expr(expr, sources)
      :== ->
        "(" <> expr(expr, sources) <> ")"
      :!= when x == nil or y == nil ->
        "(" <> expr(expr, sources) <> ")"
      :!= ->
        "-(" <> expr(expr, sources) <> ")"
      :date_add -> ## Dates cannot be enclosed in brackets
        expr(expr, sources)
      :date_sub ->
        expr(expr, sources)
      _ ->
        "(" <> expr(expr, sources) <> ")"        
    end
  end

  defp op_to_binary(expr, sources) do
    expr(expr, sources)
  end

  ## --------------------
  ## Handling of literals
  @spec literal(term) :: binary
  
  defp literal(nil), do: "*"

  defp literal(true), do: "true"
  
  defp literal(false), do: "false"

  defp literal(Ecto.DateTime[] = dt) do
    Datetime.solr_datetime(dt)
  end

  defp literal(Ecto.Interval[] = i) do
    Datetime.solr_datetime(i)
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
  ## Entity Helpers
  ## ----------------------------------------------------------------------

  defp entity_keyword(entity) do
    elem(entity, 0).__entity__(:entity_kw, entity, primary_key: true)
  end

  defp entity_field_type(entity, field) do
    elem(entity, 0).__entity__(:field_type, field)
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