defmodule Ecto.Adapters.Riak.Search do
  @moduledoc """

  Yokozuna shares the same query syntax as Apache Solr.
  For Solr query options
  see -- http://wiki.apache.org/solr/CommonQueryParameters
  """

  alias Ecto.Adapters.Riak.Migration
  alias Ecto.Adapters.Riak.Object
  alias Ecto.Adapters.Riak.SearchHaving
  alias Ecto.Adapters.Riak.SearchOrderBy
  alias Ecto.Adapters.Riak.SearchSelect
  alias Ecto.Adapters.Riak.SearchWhere
  alias Ecto.Adapters.Riak.Util, as: RiakUtil
  alias Ecto.Query.Query
  alias Ecto.Query.Util
  alias :riakc_pb_socket, as: RiakSocket

  ## ----------------------------------------------------------------------
  ## Types
  ## ----------------------------------------------------------------------
  
  @type bucket        :: binary
  @type entity        :: Ecto.Entity.t
  @type expr_field    :: atom  ## entity field reference
  @type expr_var      :: { atom, list, list }
  @type expr_order_by :: { :asc | :desc, expr_var, expr_field }
  @type query         :: Ecto.Query.t
  @type querystring   :: binary
  @type search_index  :: binary
  @type query_tuple   :: { search_index, bucket, querystring, [search_option] }
  @type name          :: { bucket        :: binary,
                           entity_module :: atom,
                           model_module  :: atom}

  ## riak search result
  ## see -- https://github.com/basho/riak-erlang-client/blob/master/include/riakc.hrl
  @type search_doc    :: { index :: binary,
                           attr  :: [{ binary, binary }] }
  @type search_result :: { :search_result,
                           docs :: [search_doc],
                           max_score :: integer,
                           num_found :: integer }

  @type source        :: { { bucket :: binary, unique_name :: integer },
                           entity_module :: atom,
                           model_module  :: atom}
  
  @type source_tuple  :: { { queryable :: binary,   ## the `x` in queryable x do ... end
                             varname   :: binary }, ## unique query variable
                           entity     :: atom,      ## entity module
                           model      :: atom}      ## module module
  @type search_option :: { :index, binary }
                       | { :q, query :: binary }
                       | { :df, default_field :: binary }
                       | { :op, :and | :or }
                       | { :start, integer } ## used for paging
                       | { :rows, integer }  ## max number of results
                       | { :sort, fieldname :: binary }
                       | { :filter, filterquery :: binary }
                       | { :presort, :key | :score }

  ## post processing
  @type post_proc_fun :: (([entity]) -> term)

  ## ----------------------------------------------------------------------
  ## Constants
  ## ----------------------------------------------------------------------

  @timeout        5000
  @yz_bucket_key  "_yz_rb"
  @yz_riak_key    "_yz_rk"
  @yz_id_key      "_yz_id"
  @yz_meta_keys   [ @yz_bucket_key, @yz_riak_key, @yz_id_key ]

  ## ----------------------------------------------------------------------
  ## API
  ## ----------------------------------------------------------------------
  
  """
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
  NOTES
  ----------------------------------------------------------------------
  """

  @doc """
  Constructs a tuple { { querystring, [search_option] }, post_proc_fun }
  The querystring and search_option are sent to YZ as a search request,
  where post_proc_fun/1 should  be called on all results of
  the search query in the execute/3 function below.
  """        
  @spec query(query, Keyword.t) :: { query_tuple, post_proc_fun }
  def query(Query[] = query, opts \\ []) do
    sources = create_names(query)  # :: [source]
    model = Util.model(query.from)
    search_index = RiakUtil.search_index(model)
    bucket = RiakUtil.bucket(model)
    
    ## Check to see if unsupported queries are specified
    ## and raise error if any are present
    joins(query)
    lock(query)
    distincts(query)

    ## Build Query Part
    where  = SearchWhere.query(query.wheres, sources)
    limit  = limit(query.limit)
    offset = offset(query.offset)
    
    ## querystring defaults to all results
    querystring = Enum.join([ where ])
    querystring = if querystring == "", do: "*:*", else: querystring 
    
    options = List.flatten([ limit, offset ] ++ opts)
      |> Enum.filter(&(nil != &1))
    query_part = { search_index, bucket, querystring, options }

    ## Build Post-processing function
    group_by = group_by(query.group_bys)
    having   = SearchHaving.post_proc(query.havings)
    order_by = SearchOrderBy.post_proc(query.order_bys, sources)
    select   = SearchSelect.post_proc(query.select)
    post_proc = fn entities ->
      group_by.(entities)
      |> having.()
      |> order_by.()
      |> select.()
    end
    
    ## DONE
    { query_part, post_proc }
  end

  @doc """
  Executes a search query using a provided worker pid,
  returning a list of valid entities (or an empty list)
  """
  @spec execute(pid, query_tuple, post_proc_fun) :: [entity]

  def execute(worker, query_tuple, post_proc_fun) do
    if is_get?(query_tuple) do
      execute_get(worker, query_tuple)
    else
      execute_search(worker, query_tuple, post_proc_fun)
    end
  end

  defp is_get?(query_tuple) do
    ## returns true if the query is intended to only
    ## get a single riak object
    { _, _, querystring, opts } = query_tuple
    case Regex.split(~r":", querystring) do
      [k, v] when is_binary(k) and is_binary(v) and k != "*" and v != "*" ->
        String.ends_with?(k, "_s") &&
          Keyword.get(opts, :rows) == 1
      _ ->
        false
    end
  end

  defp execute_get(worker, query_tuple) do
    { _, bucket, querystring, opts } = query_tuple
    
    [_, key] = Regex.split(~r":", querystring)
    key = String.strip(key, hd ')')
    
    timeout = opts[:timeout] || @timeout
    case RiakSocket.get(worker, bucket, key, timeout) do
      { :ok, object } ->        
        entity = Object.object_to_entity(object)
          |> Migration.migrate
        [entity]
      _ ->
        []
    end
  end

  defp execute_search(worker, query_tuple, post_proc_fun) do
    { search_index, _, querystring, opts } = query_tuple
    timeout = opts[:timeout] || @timeout
    case RiakSocket.search(worker, search_index, querystring, opts, timeout) do
      ## -----------------
      ## Got Search Result
      { :ok, search_result } ->
        ## get search docs from erlang record representation
        search_docs = elem(search_result, 1)      
        
        ## Reduce search_docs into a HashDict mapping
        ## the riak object key to a list of stateboxes which
        ## can be used for sibling resolution
        doc_dict = Enum.reduce(search_docs, HashDict.new, fn x, acc ->
          case parse_search_result(x) do
            { key, json } ->
              box = Object.resolve_to_statebox(json)
              fun = &([box | &1])
              HashDict.update(acc, key, [box], fun)
            _ ->
              acc
          end
        end)

        ## Use doc_dict to resolve any duplicates (riak siblings).
        ## The resulting list is a list of entity objects
        resolved = Enum.map(HashDict.to_list(doc_dict), fn { _, box_list } ->
          :statebox_orddict.from_values(box_list)
          |> Object.statebox_to_entity
        end)

        ## Perform any migrations if needed
        migrated = Enum.map(resolved, &Migration.migrate/1)
          
        ## Apply post_proc_fun and we're done
        post_proc_fun.(migrated)
      
      ## -------------
      ## unknown error
      _ ->
        []
    end
  end

  @spec parse_search_result(search_doc) :: { riak_key :: binary, 
                                             json     :: tuple}
  defp parse_search_result({ _, doc }) do
    riak_key = Dict.get(doc, @yz_riak_key)
    proplist =
      ## Filter out YZ keys
      Enum.filter(doc, fn x -> ! (x in @yz_meta_keys) end)
    
      ## Riak Search returns list values as multiple key-value tuples.
      ## Any duplicate keys should have their values put into a list
      ## ie: { "a":[1,2] } gets returned as: [{"a",1}, {"a",2}]
      |> Enum.reduce(HashDict.new(), fn { k, v }, acc ->
           HashDict.update(acc, k, v, fn existing ->
             if HashDict.has_key?(acc, k) do
               if is_list(existing) do
                 [v | existing]
               else
                 [v, existing]
               end
             else
               v
             end
           end)
         end)
      
      ## Transform to list and filter out nil values
      |> HashDict.to_list
      |> Enum.filter(fn { _, v } -> v != nil end)

    ## Return
    case Dict.get(proplist, "ectomodel_s") do
      nil ->
        nil
      _ ->
        json = { proplist }
        { riak_key, json }
    end
  end

  ## ----------------------------------------------------------------------
  ## Unsupported Queries
  ## ----------------------------------------------------------------------
  
  defp joins(query) do
    case query.joins do
      [] ->
        nil
      _ ->
        raise Ecto.QueryError, reason: "Riak adapter does not support joins"
    end
  end

  defp lock(query) do
    if query.lock do
      raise Ecto.QueryError, reason: "Riak adapter does not support locks"
    end
  end

  defp distincts(query) do
    case query.distincts do
      [] ->
        nil
      _ ->
        raise Ecto.QueryError, reason: "Riak adapter does not support distinct queries"
    end
  end

  ## ----------------------------------------------------------------------
  ## GROUP BY
  ## ----------------------------------------------------------------------

  @spec group_by(term) :: (([entity]) -> [[entity]])
  
  defp group_by([]) do
    ## Empty group_by query should simply return 
    ## the entire list of entities
    fn entities -> entities end
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
    
    fields = Enum.map(group_bys, fn expr ->
      Enum.map(expr.expr, fn { _, field } -> field end)
    end)
      |> List.flatten

    fn entities ->
        fun = fn entity, dict ->
          ## Get values using fields to create key tuple
          entity_kw = RiakUtil.entity_keyword(entity)
          values = Enum.map(fields, &entity_kw[&1])
          key = list_to_tuple(values)
          HashDict.update(dict, key, [entity], fn e -> [entity | e] end)
        end
        
        Enum.reduce(entities, HashDict.new, fun) |> HashDict.values |> Enum.sort
    end
  end

  ## ----------------------------------------------------------------------
  ## LIMIT, and OFFSET
  ## ---------------------------------------------------------------------- 

  @spec limit(integer) :: search_option

  defp limit(nil), do: nil

  defp limit(num) when is_integer(num) do
    { :rows, num }
  end 

  @spec offset(integer) :: search_option

  defp offset(nil), do: nil

  defp offset(num) when is_integer(num) do
    { :start, num }
  end

  ## ----------------------------------------------------------------------
  ## Variable Handling
  ## ----------------------------------------------------------------------

  @spec create_names(query) :: source_tuple
  def create_names(query) do
    ## Creates unique variable names for a query
    sources = query.sources |> tuple_to_list
    Enum.reduce(sources, [], fn { queryable, entity, model }, acc ->
      name = unique_name(acc, String.first(queryable), 0)
      [{ { queryable, name }, entity, model } | acc]
    end)
      |> Enum.reverse
      |> list_to_tuple
  end

  @spec unique_name(list, binary, integer) :: binary
  defp unique_name(names, name, counter) do
    counted_name = name <> integer_to_binary(counter)
    if Enum.any?(names, fn { { _, n }, _, _ } -> n == counted_name end) do
      unique_name(names, name, counter+1)
    else
      counted_name
    end
  end

  ## ----------------------------------------------------------------------
  ## Search Schema and Index
  ## ----------------------------------------------------------------------

  @doc """
  Sets search indexes for all detected `Ecto.RiakModel` modules.
  Called during Ecto.Adapters.Riak.start_link/2 or during the mix
  task `mix ecto.create Your.Riak.Repo`

  Note: this DOES NOT create and activate the Bucket Type needed for
        operation of Yokozuna. Bucket Type creation and activation is
        done manually via the mix task `mix ecto.create Your.Riak.Repo`
        with functionality implemented in the
        `Ecto.Adapters.Riak.storage_up/1` function
  """
  @spec search_index_reload_all(pid) :: [{ atom, :ok | { :error, term } }]
  def search_index_reload_all(socket) do
    models = riak_models()
    Enum.map(models, fn model ->
      { model, search_index_reload(socket, model) }
    end)
  end

  def search_index_reload(socket, model) do
    schema = RiakUtil.default_search_schema()
    search_index = RiakUtil.search_index(model)
    case RiakSocket.create_search_index(socket, search_index, schema, []) do
      :ok ->
        bucket = RiakUtil.bucket(model)
        case RiakSocket.set_search_index(socket, bucket, search_index) do
          :ok ->
            :ok
          _ ->
            { :error, "failed to set search index '#{search_index}' on bucket '#{bucket}'" }
        end
      _ ->
        { :error, "search index '#{search_index}' failed to be created" }
    end
  end

  defp riak_models() do
    ## returns all models 
    :code.all_loaded
      |> Enum.filter(fn { mod, _ } ->
           function_exported?(mod, :__model__, 1)
         end)
      |> Enum.map(fn { mod, _ } -> mod end)
  end
  
end