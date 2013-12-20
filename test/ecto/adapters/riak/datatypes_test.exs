defmodule Ecto.Adapters.Riak.DatatypesTest do
  use ExUnit.Case, async: true

  alias Ecto.UnitTest.Post
  alias Ecto.Adapters.Riak.Datatypes, as: DT
  alias :riakc_map, as: RiakMap
  ##alias :riakc_set, as: RiakSet

  defmodule AllFields do
    use Ecto.Model

    queryable "all_fields" do
      field :integer, :integer
      field :float, :float
      field :boolean, :boolean
      field :string, :string
      field :binary, :binary
      field :datetime, :datetime
      field :interval, :interval
      field :list_integer, { :list, :integer }
      field :list_float, { :list, :float }
      field :list_boolean, { :list, :boolean }
      field :list_string, { :list, :string }
      field :list_binary, { :list, :binary }
      field :list_datetime, { :list, :datetime }
      field :list_interval, { :list, :interval }

      has_one :permalink, Permalink
      has_many :comments, Comment
      belongs_to :post, Post
    end
  end

  defp mock_entity do
    AllFields.Entity[
      id: 1,
      integer: 1,
      float: 3.0234,
      boolean: true,
      string: "some string",
      binary: <<1, 2, 3>>,
      datetime: Ecto.DateTime[year: 2000, month: 12, day: 3, hour: 3, min: 3, sec: 3],
      interval: Ecto.Interval[year: 1, month: 3, day: 0, hour: 0, min: 0, sec: 0],
      list_integer: [ 1, 2, 3 ],
      list_float: [ 1.0, 3.0, 234.321 ],
      list_boolean: [ true, true, false ],
      list_string: [ "some", "list", "of", "strings" ],
      list_binary: [ "some", "list", "of", <<1,2,3>>, <<0,3>> ],
      list_datetime: [ Ecto.DateTime[year: 2000, month: 8, day: 8, hour: 8, min: 8, sec: 8], 
                       Ecto.DateTime[year: 2001, month: 8, day: 8, hour: 8, min: 8, sec: 8],
                       Ecto.DateTime[year: 2002, month: 8, day: 8, hour: 8, min: 8, sec: 8] ],
      list_interval: [ Ecto.Interval[year: 0, month: 1, day: 0, hour: 0, min: 0, sec: 0],
                       Ecto.Interval[year: 0, month: 0, day: 0, hour: 0, min: 0, sec: 0],
                       Ecto.Interval[year: 0, month: 0, day: 0, hour: 1, min: 0, sec: 0]] ]
  end

  test "entity_to_map and map_to_entity" do
    entity = mock_entity()
    map = DT.entity_to_map(entity)    
    e0 = DT.map_to_entity(map)
    
    assert entity.id == e0.id
    assert entity.integer == e0.integer
    assert entity.float == e0.float
    assert entity.boolean == e0.boolean
    assert entity.string == e0.string
    assert entity.binary == e0.binary
    assert entity.datetime == e0.datetime
    assert entity.interval == e0.interval
    assert Enum.sort(entity.list_integer) == Enum.sort(e0.list_integer)
    assert Enum.sort(entity.list_float) == Enum.sort(e0.list_float)
    assert Enum.sort(entity.list_boolean) == Enum.sort(e0.list_boolean)
    assert Enum.sort(entity.list_string) == Enum.sort(e0.list_string)
    assert Enum.sort(entity.list_binary) == Enum.sort(e0.list_binary)
    assert Enum.sort(entity.list_string) == Enum.sort(e0.list_string)
    assert Enum.sort(entity.list_datetime) == Enum.sort(e0.list_datetime)
    assert Enum.sort(entity.list_interval) == Enum.sort(e0.list_interval)
  end
  
  test "map_get" do
    entity = mock_entity()
    map = DT.entity_to_map(entity)
    
    assert 1 == DT.map_get(map, { "id_register", :register })
    assert [1,2,3] == Enum.sort(DT.map_get(map, { "list_integer_set", :set }))    
  end
  
  # test "map_put" do
  #   map = DT.map_new()
  #   assert [] == RiakMap.value(map)

  #   reg = <<1,2,"34">>
  #   set = [1,2,3]
  #   map = DT.map_put(map, { "some_bin", :register }, reg)
  #   map = DT.map_put(map, { "some_set", :set }, set)
  #   assert reg == DT.map_get(map, { "some_bin", :register })
  #   assert set == DT.map_get(map, { "some_set", :set })

  #   map = DT.map_put(map, { "some_bin", :register }, nil)
  #   assert nil == DT.map_get(map, { "some_bin", :register })
  # end

  # test "from_register and to_register" do
  # end  

  test "from_set and to_set" do
    integer_list = [1, 2, 3]
    float_list = [1.0, 2.0, 3.0]
    bool_list = [true, false, true]
    bin_list = [<<"look">>, <<1,0,3>>, <<"hello", 1, 3, 123>>]
    str_list = ["hello", "there", "everyone", "is", "happy"]
    dt_list = [ Ecto.DateTime[year: 2000, month: 10, day: 10, hour: 12, min: 12, sec: 12],
                Ecto.DateTime[year: 2001, month: 11, day: 11, hour: 13, min: 13, sec: 13],
                Ecto.DateTime[year: 2002, month: 12, day: 12, hour: 14, min: 14, sec: 14] ]
    interval_list = [ Ecto.Interval[year: 2, month: 3, day: 0, hour: 0, min: 0, sec: 0],
                      Ecto.Interval[year: 0, month: 0, day: 3, hour: 0, min: 4, sec: 0],
                      Ecto.Interval[year: 1, month: 0, day: 0, hour: 0, min: 0, sec: 0] ]

    ## Special Case: boolean lists are stored as registers
    assert Enum.sort(bool_list) == Enum.sort(DT.to_set(bool_list, :boolean) |> DT.from_set)

    assert Enum.sort(integer_list) == Enum.sort(DT.to_set(integer_list, :integer) |> DT.from_set)
    assert Enum.sort(float_list) == Enum.sort(DT.to_set(float_list, :float) |> DT.from_set)
    assert Enum.sort(bin_list) == Enum.sort(DT.to_set(bin_list, :binary) |> DT.from_set)
    assert Enum.sort(str_list) == Enum.sort(DT.to_set(str_list, :string) |> DT.from_set)

    dt_set = HashSet.new(dt_list)
    dt_set_res = HashSet.new(DT.to_set(dt_list, :datetime) |> DT.from_set)
    assert HashSet.difference(dt_set, dt_set_res).to_list == []

    interval_set = HashSet.new(interval_list)
    interval_set_res = HashSet.new(DT.to_set(interval_list, :interval) |> DT.from_set)
    assert HashSet.difference(interval_set, interval_set_res).to_list  == []
  end

end