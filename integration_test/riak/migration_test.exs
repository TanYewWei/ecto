defmodule Ecto.Integration.Riak.MigrationTest do
  use Ecto.Integration.Riak.Case, async: false

  import Ecto.Integration.Riak.Util
  alias Ecto.Adapters.Riak.Object
  alias Ecto.Adapters.Riak.Migration
  alias Ecto.Adapters.Riak.ETS
  alias Ecto.Adapters.Riak.Search
  alias :riakc_pb_socket, as: RiakSocket

  ## -----------------------------------------------------------------
  ## Test Modules
  ## -----------------------------------------------------------------

  defmodule Model do
    use Ecto.RiakModel

    queryable "integration.riak.migration.models" do
      field :integer, :integer
      field :riak_version, :integer, default: 0
    end

    def version(), do: 0

    def migrate_from_previous(entity), do: entity

    def migrate_from_newer(entity) do
      attr = RiakUtil.entity_keyword(entity)
      attr = Keyword.put(attr, :integer, Float.round(entity.int))
      __MODULE__.new(attr)
    end
  end

  defmodule Model.Ver1 do
    use Ecto.RiakModel
    
    queryable "integration.riak.migration.models" do
      field :int, :integer
      field :float, :float, default: 0.1
      field :riak_version, :integer, default: 1
    end

    def version(), do: 1

    def migrate_from_previous(entity) do
      attr = RiakUtil.entity_keyword(entity)
      attr = Keyword.put(attr, :int, entity.integer)
      __MODULE__.new(attr)
    end

    def migrate_from_newer(entity) do
      attr = [int: if(nil?(entity.int), do: Float.floor(entity.int), else: nil)]
      __MODULE__.new(attr)
    end
  end

  defmodule Model.Vers2 do
    use Ecto.RiakModel

    queryable "integration.riak.migration.models" do
      field :int, :float
      field :riak_version, :integer, default: 2
    end

    def version(), do: 2

    def migrate_from_previous(entity) do
      attr = RiakUtil.entity_keyword(entity)
      attr = if nil?(entity.int) do
               attr
             else
               Keyword.put(attr, :int, entity.int * 1.0 )
             end
      __MODULE__.new(attr)
    end

    def migrate_from_newer(entity), do: entity
  end

  alias Model, as: Model0
  alias Model.Ver1, as: Model1
  alias Model.Vers2, as: Model2

  ## -----------------------------------------------------------------
  ## Tests
  ## -----------------------------------------------------------------

  setup_all do
    { :ok, socket } = RiakSocket.start_link('127.0.0.1', 8000)
    bucket = RiakUtil.bucket(Model0)
    
    { :ok, keys } = RiakSocket.list_keys(socket, bucket)
    Enum.map(keys, fn key ->
      :ok == RiakSocket.delete(socket, bucket, key)
    end)
    
    :ok = RiakSocket.reset_bucket(socket, bucket)
    Search.search_index_reload(socket, Model0)
  end

  setup do
    ETS.delete_table()
    ETS.init()
    :ok
  end

  test "migrate to same version" do
    e0 = TestRepo.create(Model0.Entity[integer: 9])
    Migration.set_current_version(e0, 0)
    wait_assert e0 == TestRepo.get(Model0, e0.id)
  end

  test "migrate up" do
    ## Single version migrate
    e0 = TestRepo.create(Model0.Entity[integer: 9])
    #IO.puts("e0: #{inspect e0}")
    Migration.set_current_version(e0, 1)
    e1 = wait_assert Model1.Entity[] = TestRepo.get(Model0, e0.id)
    assert e1.riak_version == 1
    assert e1.id == e0.id
    assert is_integer(e1.int)
    assert e1.int == e0.integer
    assert e1.float == 0.1

    ## Multiple version migrate
    e0 = TestRepo.create(Model0.Entity[integer: 10])
    Migration.set_current_version(e0, 2)
    e2 = wait_assert Model2.Entity[] = TestRepo.get(Model0, e0.id)
    IO.puts("e2: #{inspect e2}")
    assert e2.riak_version == 2
    assert e2.id == e0.id
    assert is_float(e2.int)
    assert e2.int == e0.integer
  end

  test "migrate up disable" do
    Migration.migration_up_disable()
    
    e0 = TestRepo.create(Model0.Entity[integer: 9])
    Migration.set_current_version(e0, 1)
    wait_assert e0 == TestRepo.get(Model0, e0.id)
  end

  test "migrate down disabled by default" do
    e1 = TestRepo.create(Model1.Entity[int: 9, float: 1.0])
    Migration.set_current_version(e1, 0)
    wait_assert e1 == TestRepo.get(Model1, e1.id)
  end

  test "migrate down enabled" do
    Migration.migration_down_enable()

    ## Single version
    e1 = TestRepo.create(Model1.Entity[int: 9, float: 1.0])
    Migration.set_current_version(e1, 0)
    e0 = wait_assert Model0.Entity[] = TestRepo.get(Model1, e1.id)
    IO.puts("e0: #{inspect e0}")
    assert e0.riak_version == 0
    assert is_integer(e0.integer)
    assert e0.integer == e1.int
    
    ## Multiple versions
    e2 = TestRepo.create(Model2.Entity[int: 8.0])
    Migration.set_current_version(e2, 0)
    e0 = wait_assert Model0.Entity[] = TestRepo.get(Model2, e2.id)
    assert e0.riak_version == 0
    assert is_integer(e0.integer)
    assert e0.integer == Float.round(e2.int)
  end  

end