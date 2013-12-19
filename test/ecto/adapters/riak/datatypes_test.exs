defmodule Ecto.Adapters.Riak.DatatypesTest do
  use ExUnit.Case, async: true

  alias Ecto.Adapters.Riak.Datatypes, as: DT
  alias :riakc_map, as: RiakMap
  alias :riakc_set, as: RiakSet

  test "entity_to_map and map_to_entity" do
  end
  
  test "map_get and map_put" do
  end

  test "from_register and to_register" do
  end  

  test "from_set and to_set" do
    int_list = [1, 2, 3]
    assert int_list == (DT.to_set(int_list, :integer) |> DT.from_set)
  end

  test "set_add and set_delete" do
  end

end