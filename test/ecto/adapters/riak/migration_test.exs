defmodule Ecto.Adapters.Riak.Migration.Model do
  use Ecto.Model

  queryable "model" do
    field :integer,  :integer
    field :float,    :float
    field :string,   :string
    field :binary,   :binary
    field :datetime, :datetime
    field :interval, :interval
  end
end

defmodule Ecto.Adapters.Riak.Migration.Ver1 do
  def version(), do: 1
  def migrate_down(attr), do: attr
  
  def migrate_up(attr) do
  end
end

defmodule Ecto.Adapters.Riak.Migration.Ver2 do
  def version(), do: 2
    
  def migrate_up(attr) do
  end
  
  def migrate_down(attr) do
  end
end

defmodule Ecto.Adapters.Riak.Migration.Ver3 do
  def version(), do: 3
    
  def migrate_up(attr) do
  end
  
  def migrate_down(attr) do
  end
end

defmodule Ecto.Adapters.Riak.MigrationTest do
  use ExUnit.Case, async: false  ## dependent on ETS table state

  alias Ecto.Adapters.Riak.ETS
  alias Ecto.Adapters.Riak.Migration
  alias Ecto.Adapters.Riak.Search

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

end