defmodule Ecto.Adapters.Riak.Search do
  @moduledoc """

  Yokozuna shares the same query syntax as Apache Solr.
  For Solr query options
  see -- http://wiki.apache.org/solr/CommonQueryParameters
  """

  alias Ecto.Adapters.Riak.Object
  alias Ecto.Adapters.Riak.Migration
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
  @type post_proc_fun :: ((entity) -> term)
  @type query         :: Ecto.Query.t
  @type querystring   :: binary
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

  @type source        :: {{bucket :: string, unique_name :: integer},
                          entity_module :: atom,
                          model_module  :: atom}
  
  @type source_tuple  :: tuple  ## tuple of many source types
  @type search_option :: {:index, binary}
                       | {:q, query :: binary}
                       | {:df, default_field :: binary}
                       | {:op, :and | :or}
                       | {:start, integer} ## used for paging
                       | {:rows, integer}  ## max number of results
                       | {:sort, fieldname :: binary}
                       | {:filter, filterquery :: binary}
                       | {:presort, :key | :score}

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

  @doc """
  Constructs a {querystring, [search_option]} tuple which can be
  sent to YZ as a search request, and then provides a function 
  post_proc_fun/1 which should then be called on all results of
  the search query in the execute/3 function below.
  """        
  @spec query(query) :: {{querystring, [search_option]}, post_proc_fun}
  def query(Query[] = query) do
    sources = create_names(query)  # :: [source]

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
          
        ## DONE
        migrated
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
  
  @spec from(name, source_tuple) :: {bucket, [ binary ]}
  defp from(from, sources) do
    from_model = Util.model(from)
    source = tuple_to_list(sources)
             |> Enum.find(&(from_model == Util.model(&1)))
    {bucket, name} = Util.source(source)
    {"", [name]}
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

  defp join(query, sources, used_names) do
  end

  defp where(wheres, sources) do
  end

  defp group_by() do
  end

  defp having(havings, sources) do
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
    {_, name} = Util.find_source(sources, expr_var) |> Util.source
    str = "#{name}.#{field}"
    str <> case direction do
             :asc  -> " asc"
             :desc -> " desc"
           end
  end

  @doc """
  Adds a 
  """
  @spec limit(integer) :: search_option
  defp limit(num) when is_integer(num) do
    {:rows, num}
  end 

  defp offset(num) when is_integer(num) do
    {:start, num}
  end

  @spec create_names(query) :: source_tuple
  defp create_names(query) do
    sources = query.sources |> tuple_to_list
    Enum.reduce(sources,
                [],
                fn({ bucket, entity, model }, acc)->
                    name = unique_name(acc, String.first(bucket), 0)
                    [{{bucket,name}, entity, model} | acc]
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
  @spec yz_key(binary, atom) :: binary
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
  
end


defrecord Ecto.Adapters.Riak.SearchResult,
                             [keys: nil,      ## keys of all found documents
                              max_score: nil, ## best search score
                              count: nil,     ## number of results found
                             ] do
  @moduledoc """
  Parses a riak search 
  """
  
  @type t :: Record.t
  @type riak_search_result :: tuple
  
  @spec parse_search_result(riak_search_result) :: t
  def parse_search_result(result) do
    {:search_result, docs, max_score, num_found} = result

    ## Get keys.
    ## Each returned search_doc has format
    ## {search_index :: binary, properties :: ListDict}
    ## The key of the document is stored under the "_yz_rk" key
    ##
    ## see -- https://github.com/basho/riak-erlang-client/blob/master/include/riakc.hrl
    keys = Enum.map(docs, fn(doc)->
                              {_, prop} = doc
                              Dict.get(prop, "_yz_rk")
                          end)

    ## Return SearchResult
    __MODULE__[keys: keys, max_score: max_score, count: num_found]
  end

end