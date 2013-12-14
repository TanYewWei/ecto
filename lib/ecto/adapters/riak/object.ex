defmodule Ecto.Adapters.Riak.Object do
  alias :riakc_obj, as: RiakObject
  alias Ecto.Adapter.Riak.JSON
  alias Ecto.Adapter.Riak.Search
  alias Ecto.Adapter.Riak.Util, as: RiakUtil

  @type statebox    :: :statebox.statebox
  @type entity_type :: Ecto.Entity
  @type entity      :: Ecto.Entity.t
  @type json        :: JSON.json
  @type object      :: :riakc_obj.object

  @content_type         "application/json"
  @sb_key_model_name    "_model"
  @json_key_model_name  "_model_s"
  @json_key_statebox_ts "_ts_i"

  @spec entity_to_object(entity) :: object
  def entity_to_object(entity) do
    fields = RiakUtil.entity_keyword(entity)
    fields = Enum.map(fields, fn({ k, v })->
                                  { to_string(k) |> Search.yz_key, v }
                              end)
    fields = [ { @key_model_name, to_string(entity.model) },
               { @key_statebox_ts, timestamp() }
               | fields ]

    ## Form riak object
    bucket = RiakUtil.model_bucket(entity.model)
    key = entity.primary_key
    value = JSON.encode({ fields })
    object = RiakObject.new(bucket, key, value, @content_type)
  end

  @spec object_to_entity(object) :: entity
  def object_to_entity(object) do
    case RiakObject.get_values(object) do
      [ value ] ->
        resolve_json(value)
      values ->
        resolve_siblings(values)
    end
  end

  @doc """
  Creates a globally unique primary key for an entity
  if it didn't already exist. The format of the key will be

    ${last_segment_of_model_name}_${random_bytes}

  For example: the entity with model My.Great.ModelPost
  may get the primary key: modelpost_AguvEw6232KACP7Mt/bKxbCa
  """
  @spec create_primary_key(entity) :: entity
  def create_primary_key(entity) do
    if is_binary(entity.primary_key) do
      entity
    else
      rand_bytes = :crypto.rand_bytes(18) |> :base64.encode
      last_segment = to_string(entity.model)
        |> String.split(".")
        |> List.last
        |> String.downcase
      entity.primary_key("#{last_segment}_#{rand_bytes}")
    end
  end
  
  @spec resolve_value(binary) :: entity
  defp resolve_value(value) do
    json = JSON.decode(value)
    statebox = resolve_json(json)    
    :statebox_orddict.from_values()
  end  

  @spec resolve_siblings(binary) :: entity
  defp resolve_siblings(values) do
    stateboxes = Enum.map(values, &(JSON.decode(&1) |> resolve_json))
    statebox = :statebox_orddict.from_values(stateboxes)
    statebox_to_entity(statebox)
  end

  @spec resolve_json(json) :: statebox
  def resolve_json(json) do
    { inner } = json
    resolve_listdict(inner)
  end

  @spec resolve_listdict(ListDict) :: statebox
  def resolve_listdict(dict) do
    meta_keys = [ @json_key_model_name, @json_key_statebox_ts ]
    { meta, attr } = Dict.split(dict, meta_keys)

    ## Get entity info.
    ## This is needed to remove YZ suffixes used for search indexing
    module = Keyword.get(meta, @json_key_model_name)
      |> entity_name_from_model

    ## Create new statebox and set Timestamp
    timestamp = Keyword.get(meta, @json_key_statebox_ts, timestamp())
    statebox = :statebox_orddict.from_values([])
    statebox = set_elem(statebox, 3, timestamp)  ## 3rd element is timestamp

    ## Map over json values and create statebox update operations.
    ## Note that we never create nested JSON objects from Ecto entities.
    ops = Enum.reduce(dict,
                      [],
                      fn({ k, v }, acc)->
                          k = Search.key_from_yz(k) |> RiakUtil.to_atom
                          if is_list(v) do
                            ## add-wins behaviour
                            [:statebox_orddict.f_union(k,v) | acc]
                          else
                            [:statebox_orddict.f_store(k,v) | acc]
                          end
                      end)
    statebox.update(ops)
  end

  defp statebox_to_entity(statebox) do
    values = statebox.value
    model = :orddict.fetch(@sb_key_model_name, values)
    module = RiakUtil.to_atom("#{model}.Entity")
    
    ## Use module to get available fields 
    ## and create new entity
    entity_fields = module.__entity__(:field_names)
    Enum.map(entity_fields, fn(x)->
                                case :orddict.find(x, values) do
                                  { :ok, value } ->
                                    { x, value } ## {atom, term}
                                  _ ->
                                    nil
                                end
                            end)
    |> Enum.filter(&(nil != &1))
    |> model.new
  end

  defp timestamp() do
    :statebox_clock.timestamp
  end  

  @spec entity_name_from_model(binary | atom) :: atom
  defp entity_name_from_model(name) when is_binary(name) or is_atom(name) do
    name_str = to_string(name)
    suffix = ".Entity"
    if String.ends_with?(name_str, suffix) do
      name |> RiakUtil.to_atom
    else
      (name_str <> suffix) |> RiakUtil.to_atom
    end
  end

end