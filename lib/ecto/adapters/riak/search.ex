defmodule Ecto.Adapters.Riak.Search do
  @moduledoc """

  Yokozuna shares the same query syntax as Apache Solr.
  For Solr query options
  see -- http://wiki.apache.org/solr/CommonQueryParameters
  """

  alias Ecto.Query.Query
  alias Ecto.Query.Util
  alias Ecto.Adapters.Riak.SearchResult
  
  @type bucket        :: binary
  @type query         :: Ecto.Query.t
  @type name          :: {bucket        :: string, 
                          entity_module :: atom,
                          model_module  :: atom}
  @type querystring   :: binary
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

  @spec select(query) :: {querystring, [search_option]}
  def select(Query[] = query) do
    sources = create_names(query)  # :: [source]

    querystring = URI.encode_query([q: 1])
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

  defp join(query, sources, used_names) do
  end

  defp where(wheres, sources) do
  end

  defp group_by() do
  end

  defp having(havings, sources) do
  end

  defp order_by() do
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