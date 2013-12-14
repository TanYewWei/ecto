defmodule Ecto.Adapters.Riak.ObjectTest do
  use ExUnit.Case, async: true

  alias Ecto.UnitTest.Post
  alias Ecto.Adapters.Riak.Datetime
  alias Ecto.Adapters.Riak.Object
  
  test "create_primary_key" do
    p0 = mock_post()
    p0 = p0.primary_key(nil)
    assert p0.primary_key == nil

    p1 = Object.create_primary_key(p0)
    <<"post_", rand_bytes :: binary>> = p1.primary_key
    assert size(:base64.decode(rand_bytes)) == 18

    p0 = mock_post()
    p1 = Object.create_primary_key(p0)
    assert p0.primary_key == p1.primary_key
  end

  test "(entity_to_object <-> object_to_entity) basic" do
    entity = mock_post()
    object = Object.entity_to_object(entity)
    new_entity = Object.object_to_entity(object) ##.temp(entity.temp)
    new_entity = new_entity.temp("test temp")
    assert new_entity == entity
  end

  test "(entity_to_object <-> object_to_entity) with siblings" do
    ## We can't test this directly using
    ## the object_to_entity/1 function because a :riakc_obj
    ## comes packaged in a unique format
    ## 
    ## Instead, we'll call the resolve_siblings/1 function directly
    
    e0 = mock_post()
    e1 = e0.title("test title 1")
    e1 = e1.count(7)
    
    j0 = Object.entity_to_object(e0) |> object_updatedvalue
    j1 = Object.entity_to_object(e1) |> object_updatedvalue

    ## e1 attributes should take precedence
    entity = Object.resolve_siblings([ j0, j1 ]).temp(e1.temp)
    assert entity == e1
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
