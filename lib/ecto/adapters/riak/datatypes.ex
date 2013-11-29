defmodule Ecto.Adapter.Riak.Datatypes do
  alias Ecto.Adapter.Riak.Datetime, as: DateUtil
  alias Ecto.Adapetr.Riak.JSON
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

  @register_type_int       1
  @register_type_float     2
  @register_type_boolean   3
  @register_type_binary    4
  @register_type_string    5
  @register_type_datetime  6

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
                        {to_register(value, field_type), @riak_type_register}
                      {:list, list_field_type} ->
                        {to_set(value, list_field_type), @riak_type_set}
                      _ ->
                        ## Try fetch association
                        assoc = module.__entity__(:association, field)
                        map = assoc_to_map(assoc)
                        if nil?(map) do
                          nil
                        else
                          {map, @riak_type_map}
                        end
                    end
              ## DONE
              if nil?(add), do: acc, else: [ add | acc ]
          end
    
    List.foldl(fields, map, fun)
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
    {inner} = json
    values = Enum.map(inner, &json_to_map_worker/1)
    RiakMap.new(values, ctx)
  end

  @spec json_to_map_worker({binary, value}) :: {key, datatype}
  defp json_to_map_worker({k,v}) do
    cond do
      JSON.object?(v) ->
        {{k,:map}, json_to_map(v)}
      is_list(v) ->
        {{k,:set}, to_set(v)}
      true ->
        {{k,:register}, to_register(v, ecto_type(v))}
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
  @spec from_register(register) :: term
  def from_register(x) do
    <<flag :: integer, _ :: binary>> = RiakRegister.value(x)
    case flag do
      @register_type_integer ->
        register_to_integer(x)
      @register_type_float ->
        register_to_float(x)
      @register_type_boolean ->
        register_to_boolean(x)
      @register_type_binary ->
        register_to_binary(x)
      @register_type_string ->
        register_to_string(x)
      @register_type_datetime ->
        register_to_datetime(x)
      _ ->
        nil
    end
  end

  def to_register(x, type) do
    case type do
      @register_type_integer ->
        integer_to_register(x)
      @register_type_float ->
        float_to_register(x)
      @register_type_boolean ->
        boolean_to_register(x)
      @register_type_binary ->
        binary_to_register(x)
      @register_type_string ->
        string_to_register(x)
      @register_type_datetime ->
        datetime_to_register(x)
      _ ->
        nil
    end
  end

  def register_new(val) when is_binary(val) do
    Register.new(val, :undefined)
  end

  def register_value(reg) do
    Register.value(reg)
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
              [from_register(reg) | acc]
          end
    
    RiakSet.fold(x, [], fun)
    |> Enum.filter(&(&1 != nil))
  end

  @spec to_set([term], ecto_type) :: storeset
  def to_set(x) do
    to_set(x, ecto_type(x))
  end

  def to_set(x, type) do
    List.foldl(x, RiakSet.new(), fn(ele, acc)->
                                     bin = to_register(ele, type)
                                     RiakSet.add(acc, bin)
                                 end)
    |> Enum.filter(&(&1 != nil))
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
        {:list, hd(x) |> ecto_type}
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
    |> RiakRegister.new
  end
  
  defp register_to_integer(x) do
    <<@register_type_integer, bin :: binary>> = RiakRegister.value(x)
    binary_to_integer(bin)
  end

  defp float_to_register(x) do
    <<@register_type_float, float_to_binary(x) :: binary>>
    |> RiakRegister.new
  end

  defp register_to_float(x) do
    <<@register_type_float, bin :: binary>> = RiakRegister.value(x)
    binary_to_float(x)
  end

  defp boolean_to_register(x) do
    flag = if x, do: 1, else: 0
    <<@register_type_boolean, flag :: integer>>
    |> RiakRegister.new
  end

  defp register_to_boolean(x) do
    <<@register_type_boolean, flag :: integer>> = RiakRegister.value(x)
    flag == 1
  end

  defp binary_to_register(x) do
    <<@register_type_binary, x :: binary>>
    |> RiakRegister.new
  end

  defp register_to_binary(x) do
    <<@register_type_binary, bin :: binary>> = RiakRegister.value(x)
    bin
  end

  defp string_to_register(x) do
    case String.valid?(x) do
      true -> <<@register_type_string, x :: binary>>
              |> RiakRegister.new
      _    -> raise "invalid string"
    end
  end

  defp register_to_string(x) do
    <<@register_type_string, str :: binary>> = RiakRegister.value(x)
    str
  end

  defp datetime_to_register(x) do
    bin = DateUtil.to_string(x)
    <<@register_type_datetime, bin :: binary>>
    |> RiakRegister.new
  end

  defp register_to_datetime(x) do
    <<@register_type_datetime, bin :: binary>> = RiakRegister.value(x)
    DateUtil.parse(bin)
  end

  @assoc_key_type          "t"
  @assoc_key_primary_key   "pk"
  @assoc_key_foreign_key   "fk"
  @assoc_key_owner_id      "owner"  ## only for belongs_to
  @assoc_key_member_list   "many"   ## only for has_many
  @assoc_key_other_id      "one"    ## only for has_one

  @assoc_type_has_many    1
  @assoc_type_has_one     2
  @assoc_type_belongs_to  10

  defp assoc_to_map(x) when is_record(x, Ecto.Reflections.BelongsTo) do
    owner_id = nil
    map_new()
    |> map_put({@assoc_key_type, :integer}, @assoc_type_belongs_to)
    |> map_put({@assoc_key_primary_key, :string},
               x.primary_key |> atom_to_binary)
    |> map_put({@assoc_key_foreign_key, :string},
               x.foreign_key |> atom_to_binary)
    |> map_put(@assoc_key_owner_id, owner_id)
  end

  defp assoc_to_map(x) when is_record(x, Ecto.Reflections.HasMany) do
    member_ids = []
    map_new()
    |> map_put({@assoc_key_type, :integer}, @assoc_type_has_many)
    |> map_put({@assoc_key_primary_key, :string},
               x.primary_key |> atom_to_binary)
    |> map_put({@assoc_key_foreign_key, :string},
               x.foreign_key |> atom_to_binary)
    |> map_put(@assoc_key_member_list, to_set(member_ids, :binary))
  end

  defp assoc_to_map(x) when is_record(x, Ecto.Reflections.HasOne) do
    other_id = nil
    map_new()
    |> map_put({@assoc_key_type, :integer}, @assoc_type_has_one)
    |> map_put({@assoc_key_primary_key, :string},
               x.primary_key |> atom_to_binary)
    |> map_put({@assoc_key_foreign_key, :string}, 
               x.foreign_key |> atom_to_binary)
    |> map_put({@assoc_key_other_id, :binary}, other_id)
  end

  defp assoc_to_map(_), do: nil

  defp map_to_assoc(x) do
    type = map_get(x, {@assoc_key_type, :integer})
    case type do
      @assoc_type_has_many ->
        :ok
      @assoc_type_has_one ->
        :ok
      @assoc_type_belongs_to ->
        :ok
      _ ->
        nil
    end
  end

  defp map_to_assoc_has_many(x) do
    :ok
  end

end