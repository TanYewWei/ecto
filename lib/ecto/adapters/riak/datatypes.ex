defmodule Ecto.Adapter.Riak.Datatypes do
  alias :riakc_map, as: RiakMap
  
  @ytep entity_type :: Ecto.Entity
  @type entity      :: Ecto.Entity.t
  @type storemap    :: :riakc_map.map
  @type storeset    :: :riakc_set.riakc_set
  @type register    :: :riakc_register.register

  @riak_type_map       "map"
  @riak_type_register  "register"
  @riak_type_set       "set"

  @register_type_int       1
  @register_type_float     2
  @register_type_boolean   3
  @register_type_binary    4
  @register_type_string    5
  @register_type_datetime  6

  @doc """
  Transform an entity into its riak CRDT Map representation
  """
  @spec entity_to_map(entity, entity_type, storemap) :: storemap

  def entity_to_map(entity, type) do
    entity_to_map(entity, type, RiakMap.new())
  end

  def entity_to_map(entity, type, map) do
    fields = Enum.map(entity.__record__(:fields), 
                      fn({k,_})-> String.strip(k, ?_) end)
    fun = fn(field, acc) ->
              field_type = type.__entity__(:field_type, field)
              value = fn()-> apply(entity, field_type, []) end  ## lazy eval
              add = case field_type do
                      :integer ->
                        {integer_to_register(value), @riak_type_register}
                      :float ->
                        {float_to_register(value), @riak_type_register}
                      :boolean ->
                        {float_to_register(value), @riak_type_register}
                      :datetime ->
                        {datetime_to_register(value), @riak_type_register}
                      :string ->
                        {string_to_register(value), @riak_type_register}
                      :binary ->
                        {string_to_register(value), @riak_type_register}
                      {:list, list_field_type} ->
                        {list_to_set(value, list_field_type), @riak_type_set}
                      _ ->
                        ## Try fetch association
                        assoc = type.__entity__(:association, field)
                        map = assoc_to_map(assoc)
                        if nil?(map) do
                          nil
                        else
                          {assoc_to_map(assoc), @riak_type_map}
                        end
                    end
              ## DONE
              if nil?(add), do: acc else: [ add | acc ]
          end
    
    List.foldl(fields, map, fun)
  end

  @spec map_to_entity(storemap, entity_type) :: entity
  def map_to_entity(map, type) do
  end
  
  ## ----------------------------------------------------------------------
  ## Internal Functions
  ## ----------------------------------------------------------------------

  defp integer_to_register(x) do
    <<@register_type_integer, integer_to_binary(x) :: binary>>
  end
  
  defp register_to_integer(x) do
    <<@register_type_integer, bin :: binary>> = x
    binary_to_integer(bin)
  end

  defp float_to_register(x) do
    <<@register_type_float, float_to_binary(x) :: binary>>
  end

  defp register_to_float(x) do
    <<@register_type_float, bin :: binary>> = x
    binary_to_float(x)
  end

  defp boolean_to_register(x) do
    flag = if x, do: 1, else: 0
    <<@register_type_boolean, flag :: integer>>
  end

  defp register_to_boolean(x) do
    <<@register_type_boolean, flag :: integer>> = x
    flag == 1
  end

  defp binary_to_register(x) do
    <<@register_type_binary, x :: binary>>
  end

  defp register_to_binary(x) do
    <<@register_type_binary, bin :: binary>> = x
    bin
  end

  defp string_to_register(x) do
    case String.valid?(x) do
      true -> <<@register_type_string, x :: binary>>
      _    -> raise "invalid string"
    end
  end

  defp register_to_string(x) do
    <<@register_type_string, str :: binary>> = x
    str
  end

  defp datetime_to_register(x) do
    <<@register_type_datetime, :iso8601.format(x) :: binary>>
  end

  defp register_to_datetime(x) do
    <<@register_type_datetime, bin :: binary>> = x
  end

  defp list_to_set(x, type) do
  end

  defp set_to_list(x, type) do
  end

  @assoc_key_type          "t"
  @assoc_key_primary_key   "pk"
  @assoc_key_foreign_key   "fk"
  @assoc_key_owner_id      "owner"  ## only for belongs_to assoc
  @assoc_key_member_list   "many"  ## only for has_many
  @assoc_key_other_id      "one"  ## only for has_many

  @assoc_type_has_many    1
  @assoc_type_has_one     2
  @assoc_type_belongs_to  10

  defp assoc_to_map(x) when is_record(x, Ecto.Reflections.BelongsTo) do
    owner_id = nil
    RiakMap.new()
    |> RiakMap.put({@assoc_key_type, :integer}, @assoc_type_belongs_to)
    |> RiakMap.put({@assoc_key_primary_key, :string},
                   x.primary_key |> atom_to_binary)
    |> RiakMap.put({@assoc_key_foreign_key, :string},
                   x.foreign_key |> atom_to_binary)
    |> RiakMap.gut(@assoc_key_owner_id, owner_id)
  end

  defp assoc_to_map(x) when is_record(x, Ecto.Reflections.HasMany) do
    member_ids = []
    RiakMap.new()
    |> RiakMap.put({@assoc_key_type, :integer}, @assoc_type_has_many)
    |> RiakMap.put({@assoc_key_primary_key, :string},
                   x.primary_key |> atom_to_binary)
    |> RiakMap.put({@assoc_key_foreign_key, :string},
                   x.foreign_key |> atom_to_binary)
    |> RiakMap.put(@assoc_key_member_list, list_to_set(member_ids))
  end

  defp assoc_to_map(x) when is_record(x, Ecto.Reflections.HasOne) do
    other_id = nil
    RiakMap.new()
    |> RiakMap.put({@assoc_key_type, :integer}, @assoc_type_has_one)
    |> RiakMap.put({@assoc_key_primary_key, :string},
                   x.primary_key |> atom_to_binary)
    |> RiakMap.put({@assoc_key_foreign_key, :string}, 
                   x.foreign_key |> atom_to_binary)
    |> RiakMap.put({@assoc_key_other_id, :binary}, other_id)
  end

  defp assoc_to_map(_), do: nil

  defp map_to_assoc(x) do
    
  end

end