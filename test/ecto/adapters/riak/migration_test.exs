alias Ecto.Adapters.Riak.Util, as: RiakUtil

defmodule Ecto.Adapters.Riak.MigrationTest.Model.Version1 do
  use Ecto.RiakModel  

  queryable "model" do
    field :integer,  :integer
    field :float,    :float
    field :string,   :string
    field :binary,   :binary
    field :datetime, :datetime
    field :interval, :interval
    field :version,  :integer, default: 1
  end

  def version(), do: 1
  def migrate_down(entity), do: entity
  
  def migrate_up(entity, new_model) do ## version 1 to 2
    attr = RiakUtil.entity_keyword(entity)
    attr = Keyword.put(attr, :version, version()+1)

    ## Rename :integer field
    attr = Keyword.put(attr, :number, entity.integer)
    
    ## change :float field to integer type
    attr = Keyword.put(attr, :float, if is_number(entity.float) do
                                       round(entity.float)
                                     else
                                       nil
                                     end)

    ## Add new :virtual field
    attr = Keyword.put(attr, :virtual, {:some, "random", 'tuple'})

    ## DONE
    new_model.new(attr)
  end
end

defmodule Ecto.Adapters.Riak.MigrationTest.Model.Version2 do
  use Ecto.RiakModel

  queryable "model" do
    field :number,   :integer  ## field renaming
    field :int,      :integer  ## type change
    field :string,   :string
    field :binary,   :binary
    field :datetime, :datetime
    field :interval, :interval
    field :virtual,  :virtual  ## new field
    field :version,  :integer, default: 2
  end

  def version(), do: 2
    
  def migrate_up(entity, new_model) do ## version 2 to 3
    attr = RiakUtil.entity_keyword(entity)
    attr = Keyword.put(attr, :version, version()+1)

    ## change :string to be a list of strings
    attr = Keyword.put(attr, :string, [entity.string])

    ## change :binary field to be of :string type
    attr = Keyword.put(attr, :binary, entity.binary |> :base64.encode)

    ## ignore :datetime, :interval, and :virtual fields
    attr = Keyword.delete(attr, :datetime)
    attr = Keyword.delete(attr, :interval)
    attr = Keyword.delete(attr, :virtual)
    
    ## DONE
    new_model.new(attr)
  end
  
  def migrate_down(entity, new_model) do ## version 2 to 1
  end
end

defmodule Ecto.Adapters.Riak.MigrationTest.Model.Version3 do
  use Ecto.RiakModel

  queryable "model" do
    field :number,   :integer
    field :int,      :integer
    field :string,   {:list, :string} ## type change
    field :binary,   :string          ## type change
    ## dropped :datetime, :interval, and :virtual
    field :version,  :integer, default: 3
  end

  def version(), do: 3
    
  def migrate_up(entity, _), do: entity
  
  def migrate_down(entity, new_model) do ## version 3 to 2
    attr = RiakUtil.entity_keyword(entity)
    attr = Keyword.put(attr, :version, version()-1)
    
    ## Reverse :string list
    attr = Keyword.put(attr,
                       :string,
                       if is_list(entity.string) and length(entity.string) > 1 do
                         hd(entity.string)
                       else
                         nil
                       end)
    
    ## Decode :binary field
    attr = Keyword.put(attr, :binary, try do entity.string |> :base64.decode
                                      catch _,_ -> nil end)

    ## Create default value for :datetime, 
    ## but leave other atttributes untouched
    attr = Keyword.put(attr, :datetime, Datetime.now_local_ecto_datetime)

    ## DONE
    new_model.new(attr)
  end
end

defmodule Ecto.Adapters.Riak.MigrationTest do
  use ExUnit.Case, async: false  ## dependent on ETS table state

  alias Ecto.Adapters.Riak.ETS
  alias Ecto.Adapters.Riak.Migration
  alias Ecto.Adapters.Riak.Search
  alias Ecto.Adapters.Riak.MigrationTest.Model.Version1, as: Model1
  alias Ecto.Adapters.Riak.MigrationTest.Model.Version2, as: Model2
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
    e0 = mock_entity_ver1()
    Migration.set_current_version(e0, 1)

    ## migration to same version returns same entity
    e1 = Migration.migrate(e0)
    assert e0 == e1

    ## migration to later version returns new entity
    e0 = mock_entity_ver1()
    Migration.set_current_version(e0, 2)    
    
    ## migration across multiple versions works
    e0 = mock_entity_ver1()
    Migration.set_current_version(e0, 3)
    e3 = Migration.migrate(e0)
    assert e3.binary == :base64.encode(e0.binary)
    ##assert_raise UndefinedFunctionError, fn()-> e3.datetime end
    ##assert_raise UndefinedFunctionError, fn()-> e3.interval end
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
               interval: Ecto.Interval[day: 1])
  end

end