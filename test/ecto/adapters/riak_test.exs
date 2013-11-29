defmodule Ecto.Adapters.PostgresTest do

  defmodule Repo do
    use Ecto.Repo, adapter: Ecto.Adapters.Riak

    def url do
      [ "ecto://localhost:8100",
        "ecto://localhost:8101" ]
    end

    test "stores pool_group metadata" do
      assert Repo.__riak__(:pool_group) == __MODULE__.Repo.PoolGroup
    end
  end

end