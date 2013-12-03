defmodule Ecto.Adapters.Riak.Search do
  @moduledoc """

  Yokozuna shares the same query syntax as Apache Solr.
  For Solr query options
  see -- http://wiki.apache.org/solr/CommonQueryParameters
  """

  alias Ecto.Adapters.Riak.SearchResult
  alias Ecto.Query.Normalizer
  alias Ecto.Query.Query
  alias Ecto.Query.QueryExpr
  alias Ecto.Query.Util  
  
  @type bucket        :: binary
  @type expr_field    :: atom  ## entity field reference
  @type expr_var      :: {atom, list, list}
  @type expr_select   :: {}
  @type expr_order_by :: {:asc | :desc, expr_var, expr_field}
  @type query         :: Ecto.Query.t
  @type querystring   :: binary
  @type name          :: {bucket        :: string, 
                          entity_module :: atom,
                          model_module  :: atom}
  @type search_result :: SearchResult.t
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

  @spec query(query) :: {querystring, [search_option]}
  def query(Query[] = query) do
    sources = create_names(query)  # :: [source]

    select   = select_query(query.select, sources)
    order_by = order_by(query.order_bys, sources)
    limit    = limit(query.limit)
    offset   = limit(query.offset)

    ##querystring = URI.encode_query([q: 1])
    ## querystring is just the "q" part 
    ## of the arguments to Yokozuna
    querystring = ""
    options = List.flatten([order_by, limit, offset])
            |> Enum.filter(&(nil != &1))
    {querystring, options}
  end

  defp search_result() do
  end
  
  @spec from(name, source_tuple) :: {bucket, [ binary ]}
  defp from(from, sources) do
    from_model = Util.model(from)
    source = tuple_to_list(sources)
             |> Enum.find(&(from_model == Util.model(&1)))
    {bucket, name} = Util.source(source)
    {"", [name]}
  end

  @spec select(expr_select, [source]) :: {:fl, binary}
  defp select(expr, sources) do
    ## Selects various fields from the entity object by using
    ## the "fl" option to YZ
    ## doc -- http://wiki.apache.org/solr/CommonQueryParameters#fl
    QueryExpr[expr: expr] = Normalizer.normalize_select(expr)
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
  ## Key Serialization
  ## ----------------------------------------------------------------------

  @search_key_suffix  %r"_ss$"

  @doc """
  Appends the '_ss' suffix to the end of a key.
  This is to ensure that our model serialization 
  works with the default yokozuna schema
  """
  def search_key(x) do
    String.split(x, @search_key_suffix, trim: true) |> hd
  end

  @doc "Returns a key without the '_ss' suffix"
  def search_key_parse(x) do
    if Regex.match?(x, @search_key_suffix) do
      x
    else
      x <> "_ss"
    end
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