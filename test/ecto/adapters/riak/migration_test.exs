alias Ecto.Adapters.Riak.Datetime
require Ecto.Adapters.Riak.Util, as: RiakUtil

## ----------------------------------------------------------------------
## Model Definitions
## ----------------------------------------------------------------------

defmodule Ecto.Adapters.Riak.MigrationTest.Model.V1 do
  use Ecto.RiakModel  

  queryable "model" do
    field :integer,  :integer
    field :float,    :float
    field :string,   :string
    field :binary,   :binary
    field :datetime, :datetime
    field :interval, :interval
    field :virtual,  :virtual
    field :version,  :integer, default: 1
  end

  def version(), do: 1
  def migrate_from_previous(entity) do
    RiakUtil.entity_keyword(entity) |> __MODULE__.new
  end
  
  def migrate_from_newer(entity) do
    attr = RiakUtil.entity_keyword(entity)
    attr = Keyword.put(attr, :version, version())
    
    ## Restore :integer and :float fields
    attr = Keyword.put(attr, :integer, entity.number)
    attr = Keyword.put(attr, :float,
      if is_integer(entity.int) do
        entity.int * 1.0
      else
        nil
      end)

    ## DONE
    __MODULE__.new(attr)
  end
end

defmodule Ecto.Adapters.Riak.MigrationTest.Model.Ver2 do
  use Ecto.RiakModel

  queryable "model" do
    field :number,   :integer  ## field renaming
    field :int,      :integer  ## type change
    field :string,   :string
    field :binary,   :binary
    field :datetime, :datetime
    field :interval, :interval
    field :virtual,  :virtual
    field :version,  :integer, default: 2
  end

  def version(), do: 2
    
  def migrate_from_previous(entity) do
    attr = RiakUtil.entity_keyword(entity)
    attr = Keyword.put(attr, :version, version())

    ## Rename :integer field
    attr = Keyword.put(attr, :number, entity.integer)
    
    ## change :float field to integer type
    attr = Keyword.put(attr, :int, if is_number(entity.float) do
                                     round(entity.float)
                                   else
                                     nil
                                   end)
  
    ## Add new :virtual field
    attr = Keyword.put(attr, :virtual, if nil?(entity.virtual) do
                                         { :some, "random", 'tuple' }
                                       else
                                         entity.virtual
                                       end)

    ## DONE
    __MODULE__.new(attr)
  end
  
  def migrate_from_newer(entity) do ## version 3 to 2
    attr = RiakUtil.entity_keyword(entity)
    attr = Keyword.put(attr, :version, version())
    
    ## Reverse :string list
    attr = Keyword.put(attr,
                       :string,
                       if is_list(entity.string) and length(entity.string) > 0 do
                         hd(entity.string)
                       else
                         nil
                       end)
    
    ## Decode :binary field
    attr = Keyword.put(attr, :binary, try do entity.binary |> :base64.decode
                                      catch _,_ -> nil end)

    ## Create default value for :datetime, 
    ## but leave other atttributes untouched
    attr = Keyword.put(attr, :datetime, Datetime.now_local_ecto_datetime)

    ## Add new :virtual field
    attr = Keyword.put(attr, :virtual, { :some, "random", 'tuple' })

    ## DONE
    __MODULE__.new(attr)
  end
end

defmodule Ecto.Adapters.Riak.MigrationTest.Model.Version3 do
  use Ecto.RiakModel

  queryable "model" do
    field :number,   :integer
    field :int,      :integer
    field :string,   { :list, :string } ## type change
    field :binary,   :string          ## type change
    ## dropped :datetime, :interval, and :virtual
    field :version,  :integer, default: 3   
  end

  def version(), do: 3
    
  def migrate_from_previous(entity) do
    attr = RiakUtil.entity_keyword(entity)
    attr = Keyword.put(attr, :version, version())

    ## change :string to be a list of strings
    attr = Keyword.put(attr, :string, [entity.string])

    ## change :binary field to be of :string type
    attr = Keyword.put(attr, :binary, entity.binary |> :base64.encode)

    ## ignore :datetime, :interval, and :virtual fields
    attr = Keyword.delete(attr, :datetime)
    attr = Keyword.delete(attr, :interval)
    attr = Keyword.delete(attr, :virtual)
    
    ## DONE
    __MODULE__.new(attr)
  end

  def migrate_from_newer(entity) do
    RiakUtil.entity_keyword(entity)
    |> __MODULE__.new
  end
end

## ----------------------------------------------------------------------
## TESTS
## ----------------------------------------------------------------------

defmodule Ecto.Adapters.Riak.MigrationTest do
  use ExUnit.Case, async: false  ## dependent on ETS table state

  alias Ecto.Adapters.Riak.ETS
  alias Ecto.Adapters.Riak.Migration
  alias Ecto.Adapters.Riak.Search
  alias Ecto.Adapters.Riak.MigrationTest.Model.V1, as: Model1
  alias Ecto.Adapters.Riak.MigrationTest.Model.Ver2, as: Model2
  alias Ecto.Adapters.Riak.MigrationTest.Model.Version3, as: Model3

  ## ------------------------------------------------------------
  ## Callbacks
  ## ------------------------------------------------------------

  setup do
    ETS.delete_table()
  end

  ## ------------------------------------------------------------
  ## Migration Up
  ## ------------------------------------------------------------    

  test "migrate up flags" do
    assert true == Migration.migration_up_allowed?()
    
    Migration.migration_up_disable()
    assert false == Migration.migration_up_allowed?()

    Migration.migration_up_enable()
    assert true == Migration.migration_up_allowed?()
  end

  test "migrate up" do
    Migration.migration_up_enable()
    e0 = mock_entity_ver1()
    Migration.set_current_version(e0, 1)

    ## migration to same version returns same entity
    e1 = Migration.migrate(e0)
    assert e0 == e1

    ## migration to later version returns new entity
    e0 = mock_entity_ver1()
    Migration.set_current_version(e0, 2)
    e2 = Migration.migrate(e0)
    assert e2.version == 2
    assert is_integer(e2.number)
    assert is_integer(e2.int)
    assert is_binary(e2.string)
    assert e2.binary == e0.binary
    assert is_record(e2.datetime, Ecto.DateTime)
    assert is_record(e2.interval, Ecto.Interval)
    assert e0.virtual == e2.virtual
    
    ## migration across multiple versions works
    e0 = mock_entity_ver1()
    Migration.set_current_version(e0, 3)
    e3 = Migration.migrate(e0)
    assert e3.version == 3
    assert is_integer(e3.number)
    assert is_integer(e3.int)
    assert e3.binary == :base64.encode(e0.binary)
    assert e3.string == [e0.string]
    assert_raise UndefinedFunctionError, fn()-> e3.datetime end
    assert_raise UndefinedFunctionError, fn()-> e3.interval end
    assert_raise UndefinedFunctionError, fn()-> e3.virtual end
  end

  ## ------------------------------------------------------------
  ## Migration Down
  ## ------------------------------------------------------------

  test "migrate down flags" do
    assert false == Migration.migration_down_allowed?()
    
    Migration.migration_down_enable()
    assert true == Migration.migration_down_allowed?()

    Migration.migration_down_disable()
    assert false == Migration.migration_down_allowed?()
  end

  test "migrate down" do
    ## migration of base version returns same entity
    e0 = mock_entity_ver1()
    Migration.set_current_version(e0, 1)
    e1 = Migration.migrate(e0)
    assert e1 == e0

    ## Downward Migrations disabled by default
    e3 = mock_entity_ver3()
    Migration.set_current_version(e3, 2)
    e2 = Migration.migrate(e3)
    assert e2 == e3

    ## Migration to same version
    Migration.migration_down_enable()
    e3 = mock_entity_ver3()
    Migration.set_current_version(e3, 3)
    assert e3 == Migration.migrate(e3)

    ## Single Version Downgrade
    e3 = mock_entity_ver3()
    Migration.set_current_version(e3, 2)
    e2 = Migration.migrate(e3)
    assert e2.version == 2
    assert is_integer(e2.number)
    assert is_integer(e2.int)
    assert is_binary(e2.string)
    assert e2.binary == e0.binary
    assert is_record(e2.datetime, Ecto.DateTime)
    assert nil == e2.interval
    assert { :some, "random", 'tuple' } == e2.virtual
    
    ## Multiple Version Downgrade
    e3 = mock_entity_ver3()
    Migration.set_current_version(e3, 1)
    e1 = Migration.migrate(e3)
    assert e1.version == 1
    assert is_integer(e1.integer)
    assert is_float(e1.float)
    assert is_binary(e1.string)
    assert e3.binary |> :base64.decode == e1.binary
    assert is_record(e1.datetime, Ecto.DateTime)
    assert nil == e1.interval
    assert nil == e1.virtual  ## virtual values not persisted over migration
  end

  ## ------------------------------------------------------------
  ## Mock Data
  ## ------------------------------------------------------------

  defp mock_entity_ver1() do
    Model1.new(integer: 1,
               float: 2.0,               
               string: "a string",
               binary: <<0, 1, 2>>,
               datetime: Ecto.DateTime[year: 2000],
               interval: Ecto.Interval[day: 1],
               virtual: "hello")
  end

  defp mock_entity_ver3() do
    Model3.new(number: 2,
               int: 3,
               string: ["hello"],
               binary: "AAEC",
               datetime: Ecto.DateTime[year: 2000],
               interval: Ecto.Interval[day: 1],               
               version: 3)
  end

end