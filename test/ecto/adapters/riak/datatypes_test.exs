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
    integer_list = [1, 2, 3]
    float_list = [1.0, 2.0, 3.0]
    bin_list = [<<"look">>, <<1,0,3>>, <<"hello", 1, 3, 123>>]
    str_list = ["hello", "there", "everyone", "is", "happy"]
    dt_list = [ Ecto.DateTime[year: 2000, month: 10, day: 10, hour: 12, min: 12, sec: 12],
                Ecto.DateTime[year: 2001, month: 11, day: 11, hour: 13, min: 13, sec: 13],
                Ecto.DateTime[year: 2002, month: 12, day: 12, hour: 14, min: 14, sec: 14] ]
    interval_list = [ Ecto.Interval[year: 2, month: 3],
                      Ecto.Interval[day: 3, min: 4],
                      Ecto.Interval[year: 1] ]
    
    assert integer_list == (DT.to_set(integer_list, :integer) |> DT.from_set)
    assert float_list == (DT.to_set(float_list, :float) |> DT.from_set)
    assert bin_list == (DT.to_set(bin_list, :binary) |> DT.from_set)
    assert str_list == (DT.to_set(str_list, :string) |> DT.from_set)
    assert dt_list == (DT.to_set(dt_list, :datetime) |> DT.from_set)
    assert interval_list == (DT.to_set(interval_list, :interval) |> DT.from_set)
  end

  test "set_add and set_delete" do
  end

end