defmodule Ecto.Integration.Riak.MigrationTest do
  use Ecto.Integration.Riak.Case
  import Ecto.Integration.Riak.Util
  alias Ecto.Integration.Riak.Object

  alias Model, as: Model0
  alias Model.Ver1, as: Model1
  alias Model.Vers2, as: Model2

  ## -----------------------------------------------------------------
  ## Tests
  ## -----------------------------------------------------------------

  setup do
    :ok
  end

  test "migrate to same version" do
  end

  test "migrate up" do    
  end

  test "migrate up disable" do
    Migration.migration_up_disable()
  end

  test "migrate down disabled by default" do
    
  end

  test "migrate down enabled" do
    Migration.migration_down_enable()
    
    e2 = mock_entity_ver2()
    Migration.set_current_version(e2, 1)
    e1 = Migration.migrate(e2)
    
    wait_assert e1 == e2
  end

  ## -----------------------------------------------------------------
  ## Test Modules
  ## -----------------------------------------------------------------

  defp mock_entity_ver0() do
  end

  defp mock_entity_ver2() do
  end  

  defmodule Model do
    use Ecto.RiakModel

    queryable "models" do
      field :riak_version, :integer, default: 0
    end

    def version(), do: 0

    def migrate_from_previous(entity) do
    end

    def migrate_from_newer(entity) do
    end
  end

  defmodule Model.Ver1 do
    use Ecto.RiakModel
    
    queryable "models" do
      field :riak_version, :integer, default: 1
    end

    def version(), do: 1

    def migrate_from_previous(entity) do
    end

    def migrate_from_newer(entity) do
    end
  end

  defmodule Model.Vers2 do
    use Ecto.RiakModel

    queryable "models" do
      field :riak_version, :integer, default: 2
    end

    def version(), do: 2

    def migrate_from_previous(entity) do
    end

    def migrate_from_newer(entity) do
    end
  end

end