Code.require_file "test_helper.exs", __DIR__

defmodule Ecto.Adapters.Riak.ObjectTest do
  use ExUnit.Case, async: true

  alias Ecto.Test.Riak.Post
  alias Ecto.Adapters.Riak.Datetime
  alias Ecto.Adapters.Riak.JSON
  alias Ecto.Adapters.Riak.Object
  alias Ecto.Adapters.Riak.RequiredFieldUndefinedError
  
  test "model without riak_version field should raise error" do
    defmodule BadModel do
      use Ecto.RiakModel
      
      queryable "models" do
        field :int, :integer
      end

      def version(), do: 0
      def migrate_from_previous(x), do: x
      def migrate_from_newer(x), do: x
    end

    entity = BadModel.new()
    assert_raise RequiredFieldUndefinedError,
      fn -> Object.entity_to_object(entity) end
  end

  test "create_primary_key" do
    p0 = mock_post()
    p0 = p0.primary_key(nil)
    assert p0.primary_key == nil

    p1 = Object.create_primary_key(p0)
    assert size(p1.primary_key) == 24

    p0 = mock_post()
    p1 = Object.create_primary_key(p0)
    assert p0.primary_key == p1.primary_key
  end

  test "(entity_to_object <-> object_to_entity) basic" do
    entity = mock_post()
    object = Object.entity_to_object(entity)
    new_entity = Object.object_to_entity(object) ##.temp(entity.temp)
    new_entity = new_entity.temp("test temp")
    new_entity = new_entity.riak_context([])  ## nullify context for comparison
    assert new_entity == entity.id(new_entity.id)
  end

  test "(entity_to_object <-> object_to_entity) with siblings" do    
    e0 = mock_post() |> Object.build_riak_context
    e1 = e0.title("test title 1").count(7)
    e2 = e0.rating(6)
    
    j0 = Object.entity_to_object(e0) |> object_updatedvalue |> JSON.decode
    j1 = Object.entity_to_object(e1) |> object_updatedvalue |> JSON.decode
    j2 = Object.entity_to_object(e2) |> object_updatedvalue |> JSON.decode
    
    entity = Object.resolve_siblings([ j0, j1, j2 ])
    assert entity.id == e0.id
    assert entity.title == e1.title
    assert entity.text == e0.text
    assert entity.count == e1.count
    assert entity.rating == e2.rating
    assert entity.posted == e0.posted
    assert entity.temp == "temp"  ## not carried over
  end

  defp object_updatedvalue(riak_obj) do
    elem(riak_obj, tuple_size(riak_obj)-1)
  end

  defp mock_post() do
    Post.new(id: "some_id",
             title: "test title",
             text: "test text",
             count: 4,
             rating: 5,
             posted: Datetime.now_ecto_datetime,
             temp: "test temp")
  end  
  
end
