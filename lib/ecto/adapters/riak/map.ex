defmodule Ecto.Adapters.Riak.Map do
  alias :riakc_map, as: RiakMap
  alias :riakc_set, as: RiakSet
  alias :riakc_register, as: RiakRegister

  @type storemap   :: :riakc_map.map
  @type storeset   :: :riakc_set.riakc_set
  @type register   :: :riakc_register.register
  @type store      :: storemap | storeset | register
  @type key        :: {binary, datatype}
  @type reg_value  :: integer | float | binary
  @type value      :: reg_value | Dict

  def new() do
    RiakMap.new()
  end

  @spec get(storemap, key, term) :: value
  def get(map, {_,type}=key, default // nil) do
    case RiakMap.find(key, map) do
      {:ok, value} ->
        case type do
          :register ->
            from_register(value)
          :set ->
            from_set(value)
          :map ->
            map_to_json(value)
          _ ->
            default
        end
      _ ->
        default
    end
  end

  @spec put(storemap, key, value) :: storemap
  def put(map, key, val) do
    update = put_update(val)
    RiakMap.update(key, update, map)
  end

end