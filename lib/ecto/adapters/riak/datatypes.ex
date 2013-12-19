defmodule Ecto.Adapters.Riak.Datatypes do
  alias Ecto.Adapters.Riak.Datetime, as: DateUtil
  alias Ecto.Adapetrs.Riak.JSON
  alias :riakc_map, as: RiakMap
  alias :riakc_register, as: RiakRegister
  alias :riakc_set, as: RiakSet
  
  @type datatype    :: :map | :set | :register | :counter
  @type datetime    :: Ecto.Datetime
  @type ecto_type   :: atom
  @type entity_type :: Ecto.Entity
  @type entity      :: Ecto.Entity.t
  @type json        :: JSON.json
  @type key         :: {binary, datatype}
  @type register    :: :riakc_register.register
  @type reg_value   :: integer | float | binary
  @type storemap    :: :riakc_map.map
  @type storeset    :: :riakc_set.riakc_set
  @type update_fun  :: ((storemap) -> storemap)
  @type value       :: reg_value | json

  @riak_type_map       "map"
  @riak_type_register  "register"
  @riak_type_set       "set"

  @register_type_integer   1
  @register_type_float     2
  @register_type_boolean   3
  @register_type_binary    4
  @register_type_string    5
  @register_type_datetime  6
  @register_type_interval  7

  @ecto_type_integer   :integer
  @ecto_type_float     :float
  @ecto_type_boolean   :boolean
  @ecto_type_binary    :string
  @ecto_type_string    :string
  @ecto_type_datetime  :datetime
  @ecto_type_interval  :interval

  ## ----------------------------------------------------------------------
  ## Riak Maps
  ## ----------------------------------------------------------------------

  @doc """
  Transform an entity into its riak CRDT Map representation.
  Note that this DOES NOT take into account any existing context.

  Instead, passing back of fetched context
  is handled by the Ecto.Adapter.Riak module.
  """
  @spec entity_to_map(entity, storemap) :: storemap

  def entity_to_map(entity) do
    entity_to_map(entity, map_new())
  end

  def entity_to_map(entity, map) do
    module = elem(entity, 0)
    fields = module.__entity__(:entity_kw, entity, primary_key: true)
    fun = fn(field, acc) ->
              field_type = module.__entity__(:field_type, field)
              value = fn()-> apply(entity, field_type, []) end  ## lazy eval
              add = case field_type do
                      x when is_atom(x) ->
                        { to_register(value, field_type), @riak_type_register }
                      { :list, list_field_type } ->
                        { to_set(value, list_field_type), @riak_type_set }
                    end
              ## DONE
              if nil?(add), do: acc, else: [ add | acc ]
          end
    
    Enum.reduce(fields, map, fun)
  end

  @spec map_to_entity(storemap, entity_type) :: entity
  def map_to_entity(map, type) do
  end

  def map_new() do
    RiakMap.new()
  end

  @spec map_get(storemap, key, term) :: value
  def map_get(map, {_,type}=key, default // nil) do
    case RiakMap.find(key, map) do
      {:ok, value} ->
        case type do
          :register ->
            Datatypes.from_register(value)
          :set ->
            Datatypes.from_set(value)
          :map ->
            map_to_json(value)
          _ ->
            default
        end
      _ ->
        default
    end
  end

  @spec map_to_json(storemap) :: json
  def map_to_json(map) do
    RiakMap.fold(&map_to_json_worker/3, {[]}, map)
  end

  @spec map_to_json_worker(key, term, json) :: json
  defp map_to_json_worker({key,type}, val, acc) do
    case type do
      :register ->
        JSON.put(acc, key, from_register(val))
      :set ->
        JSON.put(acc, key, from_set(val))
      :map ->
        JSON.put(acc, key, map_to_json(val))
      _ ->
        acc
    end
  end

  @spec json_to_map(json, storemap) :: storemap
  def json_to_map(json), do: json_to_map(json, :undefined)

  def json_to_map(json, ctx) do
    { inner } = json
    values = Enum.map(inner, &json_to_map_worker/1)
    RiakMap.new(values, ctx)
  end

  @spec json_to_map_worker({binary, value}) :: {key, datatype}
  defp json_to_map_worker({k,v}) do
    cond do
      JSON.object?(v) ->
        { { k, :map }, json_to_map(v) }
      is_list(v) ->
        { { k, :set }, to_set(v) }
      true ->
        { { k, :register }, to_register(v, ecto_type(v)) }
    end
  end

  @spec map_put(storemap, key, value) :: storemap
  def map_put(map, key, val) do
    update = put_update(val)
    RiakMap.update(key, update, map)
  end

  @spec put_update(value) :: update_fun
  defp put_update(x) when is_number(x) or is_binary(x) do
    fn(curr)-> RiakRegister.set(x, curr) end
  end

  defp put_update(x) when is_tuple(x) do
    fn(ctx)-> json_to_map(x, ctx) end
  end

  ## ----------------------------------------------------------------------
  ## Riak Registers
  ## ----------------------------------------------------------------------

  @doc """
  Generic register parsing.
  """
  @spec from_register(register | binary) :: term
  
  def from_register({ :riakc_register, _, _ } = reg) do
    register_value(reg) |> from_register
  end

  def from_register(x) do
    <<flag :: integer, _ :: binary>> = x
    case flag do
      @register_type_integer ->
        store_to_integer(x)
      @register_type_float ->
        store_to_float(x)
      @register_type_boolean ->
        store_to_boolean(x)
      @register_type_binary ->
        store_to_binary(x)
      @register_type_string ->
        store_to_string(x)
      @register_type_datetime ->
        store_to_datetime(x)
      @register_type_interval ->
        store_to_interval(x)
      _ ->
        nil
    end
  end

  def to_register(x, type) do
    case type do
      @ecto_type_integer ->
        integer_to_register(x)
      @ecto_type_float ->
        float_to_register(x)
      @ecto_type_boolean ->
        boolean_to_register(x)
      @ecto_type_binary ->
        binary_to_register(x)
      @ecto_type_string ->
        string_to_register(x)
      @ecto_type_datetime ->
        datetime_to_register(x)
      @ecto_type_datetime ->
        interval_to_register(x)
      _ ->
        nil
    end
  end

  defp register_new(val) when is_binary(val) do
    RiakRegister.new(val, :undefined)
  end

  defp register_value(bin) when is_binary(bin) do
    bin
  end
  
  defp register_value(reg) do
    RiakRegister.value(reg)
  end 

  ## ----------------------------------------------------------------------
  ## Riak Set
  ## ----------------------------------------------------------------------

  @doc """
  A riak set will be a list of encoded binaries
  which have the same format as our register storage
  """
  @spec from_set(storeset) :: [ term ]
  def from_set(x) do
    fun = fn(reg, acc)->
              [ from_register(reg) | acc ]
          end
    
    RiakSet.fold(fun, [], x)
    |> Enum.filter(&(&1 != nil))
    |> Enum.reverse
  end

  @spec to_set([term], ecto_type) :: storeset

  def to_set([]) do
    RiakSet.new()
  end

  def to_set(x) when is_list(x) do
    to_set(x, ecto_type(hd x), nil)
  end

  def to_set(x, type) when is_list(x) and is_atom(type) do
    to_set(x, type, nil)
  end

  def to_set(x, type, existing)
  when is_list(x) and is_atom(type) do
    values = Enum.map(x, &(to_register(&1, type) |> register_value))
      |> Enum.filter(&(nil != &1))
    RiakSet.new(values, existing)
  end

  @spec set_fold(storeset, [term], ((term, [term])-> [term])) :: [term]
  def set_fold(set, acc, fun) do
    RiakSet.fold(fun, acc, set)
  end

  @spec set_add(storeset, binary) :: storeset
  def set_add(set, ele) do
    RiakSet.add_element(ele, set)
  end

  @spec set_delete(storeset, binary) :: storeset
  def set_delete(set, ele) do
    RiakSet.del_element(ele, set)
  end
  
  ## ----------------------------------------------------------------------
  ## Internal Functions
  ## ----------------------------------------------------------------------

  defp ecto_type(x) do
    cond do
      is_integer(x) ->
        :integer
      is_float(x) ->
        :float
      is_boolean(x) ->
        :boolean
      is_list(x) ->
        { :list, hd(x) |> ecto_type }
      is_binary(x) ->
        cond do          
          String.valid?(x) -> :string
          true             -> :binary
        end
      Datetime.datetime?(x) ->
        :datetime
      true ->
        nil
    end
  end

  defp integer_to_register(x) do
    <<@register_type_integer, integer_to_binary(x) :: binary>>
    |> register_new
  end
  
  defp store_to_integer(x) do
    <<@register_type_integer, bin :: binary>> = register_value(x)
    case Integer.parse(bin) do
      { int, _ } -> int
      _ -> nil
    end
  end

  defp float_to_register(x) do
    <<@register_type_float, float_to_binary(x) :: binary>>
    |> register_new
  end

  defp store_to_float(x) do
    <<@register_type_float, bin :: binary>> = register_value(x)
    binary_to_float(x)
  end

  defp boolean_to_register(x) do
    flag = if x, do: 1, else: 0
    <<@register_type_boolean, flag :: integer>>
    |> register_new
  end

  defp store_to_boolean(x) do
    <<@register_type_boolean, flag :: integer>> = register_value(x)
    flag == 1
  end

  defp binary_to_register(x) do
    <<@register_type_binary, x :: binary>>
    |> register_new
  end

  defp store_to_binary(x) do
    <<@register_type_binary, bin :: binary>> = register_value(x)
    bin
  end

  defp string_to_register(x) do
    case String.valid?(x) do
      true ->
        <<@register_type_string, x :: binary>>
          |> register_new
      _ ->
        raise DatatypeError,
          message: "string_to_register/1 invalid input #{inspect x}"
    end
  end

  defp store_to_string(x) do
    <<@register_type_string, str :: binary>> = register_value(x)
    str
  end

  defp datetime_to_register(x) do
    bin = DateUtil.datetime_to_string(x)
    <<@register_type_datetime, bin :: binary>>
    |> register_new
  end

  defp store_to_datetime(x) do
    <<@register_type_datetime, bin :: binary>> = register_value(x)
    DateUtil.parse_to_ecto_datetime(bin)
  end

  defp interval_to_register(x) do
    bin = DateUtil.interval_to_string(x)
    <<@register_type_interval, bin :: binary>>
    |> register_new
  end

  defp store_to_interval(x) do
    <<@register_type_interval, bin :: binary>> = register_value(x)
    DateUtil.parse_to_ecto_interval(bin)
  end

  ## ----------------------------------------------------------------------
  ## Key and Value De/Serialization
  ## ----------------------------------------------------------------------

  ## default schema --
  ## https://github.com/basho/yokozuna/blob/master/priv/default_schema.xml

  @yz_key_regex  %r"_(counter|flag|register|set)$"
  
  defp key_from_yz(key) do
    Regex.replace(@yz_key_regex, to_string(key), "")
  end

  defp yz_key(key, type) do
    to_string(key) <> "_" <>
      case type do
        { :list, list_type } ->
          "set"
        _ ->
          "register"
      end
  end

end