defmodule Ecto.Adapters.Riak.Object do
  alias :riakc_obj, as: RiakObject
  alias Ecto.Adapters.Riak.Datetime
  alias Ecto.Adapters.Riak.JSON
  alias Ecto.Adapters.Riak.RequiredFieldUndefinedError
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
    validate_entity(entity) ## raise error if validation fails

    context = entity.riak_context
    ts = timestamp

    kws = RiakUtil.entity_keyword(entity)
    kws = Enum.reduce(kws, [], fn { key, val }, acc ->
      hash = context[key] 
      val_hash = value_hash(val)

      type = RiakUtil.entity_field_type(entity, key)
      json_key = yz_key(to_string(key), type)
      json_val = cond do
        is_record(val, Ecto.DateTime) ->
          Datetime.datetime_to_string(val)
        is_record(val, Ecto.Interval) ->
          Datetime.interval_to_string(val)
        is_record(val, Ecto.Binary) ->
          :base64.encode(val.value)
        is_record(val, Ecto.Array) ->
          val.value
        true ->
          JSON.maybe_null(val)
      end

      if nil?(hash) || val_hash == hash do
        ## value did not change since last update
        [{ json_key, json_val } | acc]
      else
        ## append statebox timestamp
        ts_key = statebox_timestamp_key(key)
        [{ ts_key, ts }, { json_key, json_val } | acc]
      end
    end)

    kws = [{ @json_key_model_name, to_string(entity.model) } | kws]
    
    ## Form riak object
    bucket = RiakUtil.bucket(entity)
    key = entity.primary_key
    value = JSON.encode({ kws })
    new_object = RiakObject.new(bucket, key, value, @content_type)
    cond do
      is_binary(entity.riak_vclock) ->
        RiakObject.set_vclock(new_object, entity.riak_vclock)
      true ->
        new_object
    end
  end

  @spec object_to_entity(object) :: entity
  
  def object_to_entity(object) do
    case RiakObject.get_values(object) do
      [] ->
        ## attempt lookup of updatedvalue field
        value = elem(object, tuple_size(object)-1)
        resolve_value(value, object)
      [ value ] ->
        resolve_value(value, object)
      values ->
        resolve_siblings(values, object)
    end
  end

  defp statebox_timestamp_key(key) do
    "_sb_ts_#{key}_l"
  end

  def build_riak_context(entity) do
    kws = RiakUtil.entity_keyword(entity)
    context = Enum.map(kws, fn { key, val } ->
      { key, if(nil?(val), do: nil, else: value_hash(val)) }
    end)
    entity.riak_context(context)
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

  @spec resolve_value(binary, object) :: entity
  defp resolve_value(value, object) do
    entity = resolve_to_statebox(value) |> statebox_to_entity
    if is_binary(object.vclock) do
      entity.riak_vclock(object.vclock)
    else
      entity
    end
  end

  @spec resolve_siblings([binary | json], object) :: entity
  def resolve_siblings(values, object) do
    stateboxes = Enum.map(values, &resolve_to_statebox/1)
    statebox = :statebox_orddict.from_values(stateboxes)
    statebox = :statebox.truncate(0, statebox)
    entity = statebox_to_entity(statebox)
    if is_binary(object.vclock) do
      entity.riak_vclock(object.vclock)
    else
      entity
    end
  end

  @spec resolve_to_statebox(binary | json) :: statebox

  def resolve_to_statebox(nil), do: nil

  def resolve_to_statebox(bin) when is_binary(bin) do
    JSON.decode(bin) |> resolve_to_statebox
  end

  def resolve_to_statebox({ inner } = json) do
    { ops,        ## timestamp-independent ops
      timestamp,  ## timestamp to do :statebox.modify/3 with
      ts_ops      ## timestamp-dependent ops
    } =
      Enum.reduce(inner, { [], 2, [] }, fn kv, acc ->
        { json_key, json_val } = kv
        { _ops, _timestamp, _ts_ops } = acc
        key_str = key_from_yz(json_key)
        key = RiakUtil.to_atom(key_str)

        if is_list(json_val) do
          ## always use add-wins behaviour with lists
          { [:statebox_orddict.f_union(key, json_val) | _ops], _timestamp, _ts_ops }
        else
          ## resolve last write with all other values
          ts_key = statebox_timestamp_key(key_str)   
          ts = JSON.get(json, ts_key)
          val = JSON.maybe_nil(json_val)
          if ts do
            ts = if ts > _timestamp, do: ts, else: _timestamp
            { _ops, ts, [:statebox_orddict.f_store(key, val) | _ts_ops] }
          else
            { [:statebox_orddict.f_store(key, val) | _ops], _timestamp, _ts_ops }
          end
        end
      end)

    ## Construct statebox with straightforward ops,
    ## then modify with timestamped ops
    box = :statebox.modify(1, ops, :statebox.new(0, fn -> [] end))
    :statebox.modify(timestamp, ts_ops, box)
  end

  def statebox_to_entity(statebox) do
    values = statebox.value
    model = Dict.get(values, @sb_key_model_name) |> RiakUtil.to_atom
    entity_module = entity_name_from_model(model)
    
    ## Use module to get available fields 
    ## and create new entity    
    entity_fields = entity_module.__entity__(:field_names)
    Enum.map(entity_fields, fn field ->
      case :orddict.find(field, values) do
        { :ok, value } ->
          type = entity_module.__entity__(:field_type, field)
          { field, ecto_value(value, type) }
        _ ->
          nil
      end
    end)
      |> Enum.filter(&(nil != &1))
      |> model.new
      |> build_riak_context
      |> validate_entity
  end

  defp ecto_value(nil, _), do: nil

  defp ecto_value(Ecto.Array[value: value], _), do: value
  
  defp ecto_value(val, type) do
    case type do
      :binary ->        
        if is_binary(val) do
          :base64.decode(val)
        else
          nil
        end
      :datetime ->
        Datetime.parse_to_ecto_datetime(val)
      :interval ->
        Datetime.parse_to_ecto_interval(val)
      :integer when is_binary(val) ->
        case Integer.parse(val) do
          { i, _ } -> i
          _        -> nil
        end
      :float when is_binary(val) ->
        case Float.parse(val) do
          { f, _ } -> f
          _        -> nil
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

  ## ----------------------------------------------------------------------
  ## Validation
  ## ----------------------------------------------------------------------
  
  defmacrop raise_required_field_error(field, entity) do
    quote do
      raise RequiredFieldUndefinedError,
        field: unquote(field),
        entity: unquote(entity)
    end
  end

  defp validate_entity(entity) do
    ## checks that all fields needed for Riak 
    ## migrations are defined
    cond do
      entity.primary_key == nil || size(entity.primary_key) == 0 ->
        raise_required_field_error(:primary_key, entity)
      ! function_exported?(entity, :riak_version, 0) ->
        raise_required_field_error(:riak_version, entity)
      ! is_integer(entity.riak_version) ->
        raise_required_field_error(:riak_version, entity)
      ! function_exported?(entity, :riak_context, 0) ->
        raise_required_field_error(:riak_context, entity)
      true ->
        entity
    end
  end  

  ## ----------------------------------------------------------------------
  ## Key and Value De/Serialization
  ## ----------------------------------------------------------------------

  @yz_key_regex  ~r"_(i|is|f|fs|b|bs|b64_s|b64_ss|s|ss|i_dt|i_dts|dt|dts)$"

  @spec key_from_yz(binary) :: binary
  def key_from_yz(key) do
    ## Removes the default YZ schema suffix from a key.
    ## schema ref: https://github.com/basho/yokozuna/blob/develop/priv/default_schema.xml
    Regex.replace(@yz_key_regex, to_string(key), "")
  end

  @spec yz_key(binary, atom | { :array, atom }) :: binary
  def yz_key(key, type) do
    ## Adds a YZ schema suffix to a key depending on its type.
    to_string(key) <> "_" <>
      case type do
        :integer  -> "i"
        :float    -> "f"
        :binary   -> "b64_s"
        :string   -> "s"
        :boolean  -> "b"
        :datetime -> "dt"
        :interval -> "i_dt"
        { :array, list_type } ->
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

  defp value_hash(term) do
    bin = :erlang.term_to_binary(term, minor_version: 1)
    :crypto.hash(:sha256, bin)
  end

end