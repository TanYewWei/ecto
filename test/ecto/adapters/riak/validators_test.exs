defmodule Ecto.Adapters.Riak.ValidatorsTest do
  use ExUnit.Case, async: true

  defmodule BadModel do
    use Ecto.RiakModel
    
    queryable "models" do
      field :integer, :integer
    end

    riak_validate model,
      integer: present(message: "give me an integer!")

    def version(), do: 0
    def migrate_from_previous(x), do: x
    def migrate_from_newer(x), do: x
  end

  defmodule Model do
    use Ecto.RiakModel

    queryable "models" do
      field :integer, :integer
      field :list, { :list, :float }
      field :riak_version, :integer, default: 0
    end
    
    riak_validate model,
      integer: present(),
      also: validate_some_list
    
    validatep validate_some_list(x),
      list: validate_is_list(message: "should be a list")

    def version(), do: 0
    def migrate_from_previous(x), do: x
    def migrate_from_newer(x), do: x
  end

  test "bad models should not validate" do
    m1 = Model.Entity[id: 1, integer: 1, list: [1.0]]
    assert [primary_key: "is not a string"] == Model.validate(m1)
  end

  test "good models should validate" do
    m0 = Model.Entity[id: "some_id", integer: 1]
    assert [list: "should be a list"] == Model.validate(m0)

    m0 = Model.Entity[id: "some_id", integer: 1, list: [1.0]]
    assert [] == Model.validate(m0)
  end
  
end