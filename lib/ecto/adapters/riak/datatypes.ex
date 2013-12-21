defmodule Ecto.Adapters.Riak.Datatypes do
  alias Ecto.Adapters.Riak.Datetime, as: DateUtil
  alias Ecto.Adapters.Riak.JSON
  alias Ecto.Adapters.Riak.Util, as: RiakUtil
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

  @riak_type_map       :map
  @riak_type_register  :register
  @riak_type_set       :set

  @register_type_integer       1
  @register_type_float         2
  @register_type_boolean       3
  @register_type_binary        4
  @register_type_string        5
  @register_type_datetime      6
  @register_type_interval      7
  @register_type_boolean_list  8

  @ecto_type_integer   :integer
  @ecto_type_float     :float
  @ecto_type_boolean   :boolean
  @ecto_type_binary    :binary
  @ecto_type_string    :string
  @ecto_type_datetime  :datetime
  @ecto_type_interval  :interval

  @meta_key_model_name "model_register"

  ## ----------------------------------------------------------------------
  ## Riak Maps
  ## ----------------------------------------------------------------------

  @doc """
  Transform an entity into its riak CRDT Map representation.
  Note that this DOES NOT take into account any existing context.

  Instead, passing back of fetched context
  is handled by the Ecto.Adapter.Riak module.
  """
  @spec entity_to_map(entity, storemap, storemap) :: storemap

  def entity_to_map(entity) do
    entity_to_map(entity, map_new(), nil)
  end

  def entity_to_map(entity, map) do
    entity_to_map(entity, map, nil)
  end

  def entity_to_map(entity, map, context) do
    module = elem(entity, 0)
    fields = module.__entity__(:entity_kw, entity, primary_key: true)

    keyword = Enum.map(fields, fn({ key, val })->
      field_type = module.__entity__(:field_type, key)
      key = yz_key(key, field_type)
      case field_type do
        type when is_atom(type) ->
          { { key, @riak_type_register }, to_store(val, field_type) }
        { :list, :boolean  } ->
          ## boolean lists are stored as registers
          { { key, @riak_type_register }, boolean_list_to_store(val) }
        { :list, list_field_type } ->
          { { key, @riak_type_set }, to_set(val, list_field_type) }        
      end
    end)

    model_key = { @meta_key_model_name, @riak_type_register }
    model_val = to_string(entity.model) |> to_store(:binary)
    keyword = [{ model_key, model_val } | keyword]
    RiakMap.new(keyword, context)
  end

  @spec map_to_entity(storemap) :: entity
  def map_to_entity(map) do
    model = map_get(map, { @meta_key_model_name, @riak_type_register })
      |> RiakUtil.entity_name_from_model

    fun = fn({ key, type }, val, acc)->
      key = key_from_yz(key)
      val = case type do        
              @riak_type_register ->
                from_store(val)
              @riak_type_set ->
                from_set(val)
            end
      [{ key, val } | acc]
    end
    keyword = RiakMap.fold(fun, [], map)
    
    model.new(keyword)
  end

  def map_new() do
    RiakMap.new()
  end

  @spec map_get(storemap, key, term) :: value
  def map_get(map, { _, type } = key, default // nil) do
    case RiakMap.find(key, map) do
      { :ok, value } when value != nil ->
        case type do
          @riak_type_register ->
            from_store(value)
          @riak_type_set ->
            from_set(value)
          _ ->
            default
        end
      _ ->
        default
    end
  end  

  @spec map_put(storemap, key, value) :: storemap
  def map_put(map, key, val) do
    update = put_update(val)
    RiakMap.update(key, update, map)
  end

  @spec put_update(value) :: update_fun

  defp put_update(x) when is_list(x) do
    fn(curr)-> RiakSet.new(x, curr) end
  end

  defp put_update(x) do
    fn(_)-> to_store(x, ecto_type(x)) end
  end

  ## ----------------------------------------------------------------------
  ## Riak Registers
  ## ----------------------------------------------------------------------

  @doc """
  Generic register parsing.
  """
  @spec from_store(register | binary) :: term
  
  def from_store(nil), do: nil

  def from_store({ :register, _, _ } = reg) do
    register_value(reg) |> from_store
  end

  def from_store(x) do
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
      @register_type_boolean_list ->
        store_to_boolean_list(x)
      _ ->
        nil
    end
  end

  @spec to_store(term, atom) :: binary

  def to_store(nil, _), do: nil

  def to_store(x, type) do
    case type do
      @ecto_type_integer ->
        integer_to_store(x)
      @ecto_type_float ->
        float_to_store(x)
      @ecto_type_boolean ->
        boolean_to_store(x)
      @ecto_type_binary ->
        binary_to_store(x)
      @ecto_type_string ->
        string_to_store(x)
      @ecto_type_datetime ->
        datetime_to_store(x)
      @ecto_type_interval ->
        interval_to_store(x)
      _ ->
        nil
    end
  end

  defp register_new(val) when is_binary(val) do
    RiakRegister.new(val, :undefined)
  end

  defp register_value(nil), do: nil
  
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
  
  def from_set({ :register, _, _ } = x) do
    store_to_boolean_list(x)
  end

  def from_set(x) do
    fun = &[ from_store(&1) | &2 ]    
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
  when is_list(x) and type == :boolean do
    boolean_list_to_store(x) |> register_new
  end

  def to_set(x, type, existing)
  when is_list(x) and is_atom(type) do
    values = Enum.map(x, &to_store(&1, type))
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

  defp integer_to_store(x) do
    <<@register_type_integer, integer_to_binary(x) :: binary>>
  end
  
  defp store_to_integer(x) do
    <<@register_type_integer, bin :: binary>> = register_value(x)
    case Integer.parse(bin) do
      { int, _ } -> int
      _ -> nil
    end
  end

  defp float_to_store(x) do
    <<@register_type_float, to_string(x) :: binary>>
  end

  defp store_to_float(x) do
    <<@register_type_float, bin :: binary>> = register_value(x)
    case Float.parse(bin) do
      { float, _ } -> float
      _ -> nil
    end
  end

  defp boolean_to_store(x) do
    flag = if x, do: 1, else: 0
    <<@register_type_boolean, flag :: integer>>
  end  

  defp store_to_boolean(x) do
    <<@register_type_boolean, flag :: integer>> = register_value(x)
    flag == 1
  end
  
  defp boolean_list_to_store(x) when is_list(x) do
    bin = Enum.map(x, &(if &1 == true, do: 1, else: 0)) |> to_string
    <<@register_type_boolean_list, bin :: binary>>
  end

  defp store_to_boolean_list(x) do
    <<@register_type_boolean_list, bin :: binary>> = register_value(x)
    boolean_list_parse(bin, [])
  end

  defp boolean_list_parse(<<>>, acc), do: Enum.reverse(acc)
  defp boolean_list_parse(<<flag :: integer, rest :: binary>>, acc) do
    boolean_list_parse(rest, [flag == 1 | acc])
  end

  defp binary_to_store(x) do
    <<@register_type_binary, x :: binary>>
  end

  defp store_to_binary(x) do
    <<@register_type_binary, bin :: binary>> = register_value(x)
    bin
  end

  defp string_to_store(x) do
    case String.valid?(x) do
      true ->
        <<@register_type_string, x :: binary>>
      _ ->
        raise DatatypeError,
          message: "string_to_store/1 invalid input #{inspect x}"
    end
  end

  defp store_to_string(x) do
    <<@register_type_string, str :: binary>> = register_value(x)
    str
  end

  defp datetime_to_store(x) do
    bin = DateUtil.datetime_to_string(x)
    <<@register_type_datetime, bin :: binary>>
  end

  defp store_to_datetime(x) do
    <<@register_type_datetime, bin :: binary>> = register_value(x)
    DateUtil.parse_to_ecto_datetime(bin)
  end

  defp interval_to_store(x) do
    bin = DateUtil.interval_to_string(x)
    <<@register_type_interval, bin :: binary>>
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