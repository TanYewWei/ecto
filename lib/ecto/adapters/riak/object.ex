defmodule Ecto.Adapters.Riak.Object do
  alias :riakc_obj, as: RiakObject
  alias Ecto.Adapters.Riak.Datetime
  alias Ecto.Adapters.Riak.JSON
  alias Ecto.Adapters.Riak.Util, as: RiakUtil

  @type statebox    :: :statebox.statebox
  @type ecto_type   :: Ecto.DateTime | Ecto.Interval | Ecto.Binary
  @type entity_type :: Ecto.Entity
  @type entity      :: Ecto.Entity.t
  @type json        :: JSON.json
  @type object      :: :riakc_obj.object

  @content_type         'application/json'
  @sb_key_model_name    :ectomodel
  @json_key_model_name  "ectomodel_s"
  @json_key_statebox_ts "ectots_l"

  @spec entity_to_object(entity) :: object
  def entity_to_object(entity) do
    fields = RiakUtil.entity_keyword(entity)
    fields = Enum.map(fields, fn({ k, v })->
      type = RiakUtil.entity_field_type(entity, k)
      key = RiakUtil.yz_key(to_string(k), type)
      val = cond do
        is_record(v, Ecto.DateTime) ->
          Datetime.datetime_to_string(v)
        is_record(v, Ecto.Interval) ->
          Datetime.interval_to_string(v)
        is_record(v, Ecto.Binary) ->
          :base64.encode(v.value)
        true ->
          JSON.maybe_null(v)
          #v
      end
      { key, val }
    end)
    fields = Enum.filter(fields, fn({ _, v })-> v != nil end)
    fields = [ { @json_key_model_name, to_string(entity.model) },
               { @json_key_statebox_ts, timestamp() }
               | fields ]

    ## Form riak object
    bucket = RiakUtil.model_bucket(entity.model)
    key = entity.primary_key
    value = JSON.encode({ fields })
    RiakObject.new(bucket, key, value, @content_type)
  end

  @spec object_to_entity(object) :: entity
  def object_to_entity(object) do
    case RiakObject.get_values(object) do
      [] ->
        ## attempt lookup of updatedvalue field
        elem(object, tuple_size(object)-1)
        |> resolve_value
      [ value ] ->
        resolve_value(value)
      values ->
        resolve_siblings(values)
    end
  end

  @doc """
  Creates a globally unique primary key for an entity
  if it didn't already exist.
  """
  @spec create_primary_key(entity) :: entity
  def create_primary_key(entity) do
    if is_binary(entity.primary_key) do
      entity
    else
      :crypto.rand_bytes(18)
        |> :base64.encode
        |> String.replace("/", "_")  ## '/', '+', and '-' are disallowed in Solr
        |> String.replace("+", ".")
        |> entity.primary_key
    end
  end 

  @spec resolve_value(binary) :: entity
  defp resolve_value(value) do
    resolve_json(value) |> statebox_to_entity
  end

  @spec resolve_siblings([binary | json]) :: entity
  def resolve_siblings(values) do
    stateboxes = Enum.map(values, &resolve_json/1)
    statebox = :statebox_orddict.from_values(stateboxes)
    statebox = :statebox.truncate(0, statebox)
    statebox_to_entity(statebox)
  end

  @spec resolve_json(json) :: statebox
  def resolve_json(nil), do: nil

  def resolve_json(bin) when is_binary(bin) do
    JSON.decode(bin) |> resolve_json
  end

  def resolve_json(json) do
    { inner } = json
    resolve_listdict(inner)
  end

  @spec resolve_listdict(ListDict) :: statebox
  defp resolve_listdict(dict) do
    meta_keys = [ @json_key_model_name, @json_key_statebox_ts ]
    { meta, attr } = Dict.split(dict, meta_keys)

    ## Get entity info.
    ## This is needed to remove YZ suffixes used for search indexing
    module = Dict.get(meta, @json_key_model_name)
      |> entity_name_from_model            

    ## Map over json values and create statebox update operations.
    ## Note that we never create nested JSON objects from Ecto entities.
    ops = Enum.reduce(dict,
                      [],
                      fn({ k, v }, acc)->
                          k = RiakUtil.key_from_yz(k) |> RiakUtil.to_atom
                          if is_list(v) do
                            ## add-wins behaviour
                            [:statebox_orddict.f_union(k,v) | acc]
                          else
                            v = JSON.maybe_nil(v)
                            [:statebox_orddict.f_store(k,v) | acc]
                          end
                      end)
    statebox = :statebox.modify(ops, :statebox_orddict.from_values([]))
    
    ## Create new statebox and set Timestamp
    timestamp = Dict.get(meta, @json_key_statebox_ts, timestamp())
    set_elem(statebox, 3, timestamp)  ## 3rd element is timestamp    
  end

  def statebox_to_entity(statebox) do
    values = statebox.value
    model = Dict.get(values, @sb_key_model_name) |> RiakUtil.to_atom
    entity_module = entity_name_from_model(model)
    
    ## Use module to get available fields 
    ## and create new entity
    entity_fields = entity_module.__entity__(:field_names)
    Enum.map(entity_fields, fn(x)->
      case :orddict.find(x, values) do
        { :ok, value } ->
          type = entity_module.__entity__(:field_type, x)
          { x, ecto_value(value, type) }
        _ ->
          nil
      end
    end)
      |> Enum.filter(&(nil != &1))
      |> model.new
  end

  defp ecto_value(nil, _), do: nil
  
  defp ecto_value(val, type) do
    case type do
      :binary ->
        if is_binary(val) do
          Ecto.Binary[value: :base64.decode(val)]
        else
          nil
        end
      :datetime ->
        Datetime.parse_to_ecto_datetime(val)
      :interval ->
        Datetime.parse_to_ecto_interval(val)
      :integer when is_binary(val) ->
        case Integer.parse(val) do
          {i, _} -> i
          _      -> nil
        end
      :float when is_binary(val) ->
        case Float.parse(val) do
          {f, _} -> f
          _      -> nil
        end
      _ ->
        val
    end
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