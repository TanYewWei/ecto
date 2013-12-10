defmodule Ecto.Adapters.Riak.Search do
  @moduledoc """

  Yokozuna shares the same query syntax as Apache Solr.
  For Solr query options
  see -- http://wiki.apache.org/solr/CommonQueryParameters
  """

  require Ecto.Adapters.Riak.Datetime
  alias Ecto.Adapters.Riak.Migration
  alias Ecto.Adapters.Riak.Object
  alias Ecto.Adapters.Riak.SearchHaving
  alias Ecto.Adapters.Riak.SearchSelect
  alias Ecto.Adapters.Riak.SearchWhere
  alias Ecto.Adapters.Riak.SearchUtil
  alias Ecto.Query.Query
  alias Ecto.Query.Util

  ## ----------------------------------------------------------------------
  ## Types
  ## ----------------------------------------------------------------------
  
  @type bucket        :: binary
  @type entity        :: Ecto.Entity.t
  @type expr_field    :: atom  ## entity field reference
  @type expr_var      :: {atom, list, list}
  @type expr_order_by :: {:asc | :desc, expr_var, expr_field}
  @type query         :: Ecto.Query.t
  @type querystring   :: binary
  @type search_index  :: binary
  @type query_tuple   :: {search_index, querystring, [search_option]}
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

  ## ----------------------------------------------------------------------
  ## Constants
  ## ----------------------------------------------------------------------

  @yz_bucket_key "_yz_rb"
  @yz_riak_key   "_yz_rk"
  @yz_id_key     "_yz_id"
  @yz_meta_keys  [@yz_bucket_key, @yz_riak_key, @yz_id_key]  

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

    where    = SearchWhere.query(query.wheres, sources)    
    order_by = order_by(query.order_bys)
    limit    = limit(query.limit)
    offset   = offset(query.offset)

    group_by_post_proc = group_by(query.group_bys)
    having_post_proc   = SearchHaving.post_proc(query.havings)
    select_post_proc   = SearchSelect.post_proc(query.select)
    post_proc = fn(entities)->                    
                    group_by_post_proc.(entities)
                    ##|> having_post_proc.()
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
    IO.puts(to_string from_model)
    source = tuple_to_list(sources)
             |> Enum.find(&(from_model == Util.model(&1)))
    {bucket, name} = Util.source(source)
    {"_model_s:#{from_model}", [name]}
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
  ## GROUP BY
  ## ----------------------------------------------------------------------

  @spec group_by(term) :: (([entity] | [[entity]]) -> [entity])
  
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
                  entity_kw = SearchUtil.entity_keyword(entity)
                  values = Enum.map(fields, &(entity_kw[&1]))
                  key = list_to_tuple(values)
                  HashDict.update(dict, key, [entity], fn(e)-> [entity | e] end)
              end
        Enum.reduce(entities, HashDict.new, fun)
        |> HashDict.values
    end
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
  
end