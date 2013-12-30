alias Ecto.Adapters.Riak.Datetime
alias Ecto.Adapters.Riak.Util, as: RiakUtil
require Ecto.Adapters.Riak.Validators, as: RiakValidate

## ----------------------------------------------------------------------
## Model Definitions
## ----------------------------------------------------------------------

defmodule Ecto.Adapters.Riak.MigrationTest.Model.V0 do
  use Ecto.RiakModel  

  queryable "model" do
    field :integer,  :integer
    field :float,    :float
    field :string,   :string
    field :binary,   :binary
    field :datetime, :datetime
    field :interval, :interval
    field :virtual,  :virtual
    field :hello,    :virtual
    field :riak_version, :integer, default: 0

    RiakValidate.validate(float: present,
                          also: validate_some_thing)

    validatep validate_some_thing(x),
      datetime: present(message: "failed validate!")
  end

  def version(), do: 0
  
  def migrate_from_previous(entity) do
    RiakUtil.entity_keyword(entity) |> __MODULE__.new
  end
  
  def migrate_from_newer(entity) do
    attr = RiakUtil.entity_keyword(entity)
    ##attr = Keyword.put(attr, :version, version())
    
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

defmodule Ecto.Adapters.Riak.MigrationTest.Model.Ver1 do
  use Ecto.RiakModel

  queryable "model" do
    field :number,   :integer  ## field renaming
    field :int,      :integer  ## type change
    field :string,   :string
    field :binary,   :binary
    field :datetime, :datetime
    field :interval, :interval
    field :virtual,  :virtual
    field :riak_version, :integer, default: 1
  end

  def version(), do: 1
    
  def migrate_from_previous(entity) do
    attr = RiakUtil.entity_keyword(entity)

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
  
  def migrate_from_newer(entity) do ## version 2 to 1
    attr = RiakUtil.entity_keyword(entity)
    
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

defmodule Ecto.Adapters.Riak.MigrationTest.Model.Version2 do
  use Ecto.RiakModel

  queryable "model" do
    field :number,   :integer
    field :int,      :integer
    field :string,   { :list, :string } ## type change
    field :binary,   :string           ## type change
    field :riak_version, :integer, default: 2
    ## dropped :datetime, :interval, and :virtual
  end

  def version(), do: 2
    
  def migrate_from_previous(entity) do
    attr = RiakUtil.entity_keyword(entity)

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
  alias Ecto.Adapters.Riak.MigrationTest.Model.V0, as: Model0
  alias Ecto.Adapters.Riak.MigrationTest.Model.Version2, as: Model2

  ## ------------------------------------------------------------
  ## Callbacks
  ## ------------------------------------------------------------

  setup do
    ETS.delete_table()
    ETS.init()
    :ok
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
    e0 = mock_entity_ver0()
    Migration.set_current_version(e0, 0)    

    ## migration to same version returns same entity
    e1 = Migration.migrate(e0)
    assert e0 == e1

    ## migration to later version returns new entity
    e0 = mock_entity_ver0()
    Migration.set_current_version(e0, 1)
    e2 = Migration.migrate(e0)
    assert e2.riak_version == 1
    assert is_integer(e2.number)
    assert is_integer(e2.int)
    assert is_binary(e2.string)
    assert e2.binary == e0.binary
    assert is_record(e2.datetime, Ecto.DateTime)
    assert is_record(e2.interval, Ecto.Interval)
    assert e0.virtual == e2.virtual
    
    ## migration across multiple versions works
    e0 = mock_entity_ver0()
    Migration.set_current_version(e0, 2)
    e3 = Migration.migrate(e0)
    assert e3.riak_version == 2
    assert is_integer(e3.number)
    assert is_integer(e3.int)
    assert e3.binary == :base64.encode(e0.binary)
    assert e3.string == [e0.string]
    assert_raise UndefinedFunctionError, fn -> e3.datetime end
    assert_raise UndefinedFunctionError, fn -> e3.interval end
    assert_raise UndefinedFunctionError, fn -> e3.virtual end
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
    e0 = mock_entity_ver0()
    Migration.set_current_version(e0, 0)
    e1 = Migration.migrate(e0)
    assert e1 == e0

    ## Downward Migrations disabled by default
    e2 = mock_entity_ver2()
    Migration.set_current_version(e2, 2)
    e1 = Migration.migrate(e2)
    assert e1 == e2

    ## Migration to same version
    Migration.migration_down_enable()
    Migration.set_current_version(e2, 2)
    assert e2 == Migration.migrate(e2)

    ## Single Version Downgrade
    e2 = mock_entity_ver2()
    Migration.set_current_version(e2, 1)
    e1 = Migration.migrate(e2)
    assert e1.riak_version == 1
    assert is_integer(e1.number)
    assert is_integer(e1.int)
    assert is_binary(e1.string)
    assert e1.binary == e0.binary
    assert is_record(e1.datetime, Ecto.DateTime)
    assert nil == e1.interval
    assert { :some, "random", 'tuple' } == e1.virtual
    
    ## Multiple Version Downgrade
    e2 = mock_entity_ver2()
    Migration.set_current_version(e2, 0)
    e0 = Migration.migrate(e2)
    assert e0.riak_version == 0
    assert is_integer(e0.integer)
    assert is_float(e0.float)
    assert is_binary(e0.string)
    assert e2.binary |> :base64.decode == e0.binary
    assert is_record(e0.datetime, Ecto.DateTime)
    assert nil == e0.interval
    assert nil == e0.virtual  ## virtual values not persisted over migration
  end

  ## ------------------------------------------------------------
  ## Mock Data
  ## ------------------------------------------------------------

  defp mock_entity_ver0() do
    Model0.new(id: "some_unique_id",
               integer: 1,
               float: 2.0,               
               string: "a string",
               binary: <<0, 1, 2>>,
               datetime: Ecto.DateTime[year: 2000],
               interval: Ecto.Interval[day: 1],
               virtual: "hello")
  end

  defp mock_entity_ver2() do
    Model2.new(number: 2,
               int: 3,
               string: ["hello"],
               binary: "AAEC",
               datetime: Ecto.DateTime[year: 2000],
               interval: Ecto.Interval[day: 1],
               riak_version: 2)
  end

end