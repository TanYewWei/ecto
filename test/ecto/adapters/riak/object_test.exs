defmodule Ecto.Adapters.Riak.ObjectTest do
  use ExUnit.Case, async: true

  alias Ecto.UnitTest.Post
  alias Ecto.Adapters.Riak.Datetime
  alias Ecto.Adapters.Riak.Object
  
  test "create_primary_key" do
    p0 = mock_post()
    assert p0.primary_key == nil

    p1 = Object.create_primary_key(p0)
    <<"post_", rand_bytes :: binary>> = p1.primary_key
    assert size(:base64.decode(rand_bytes)) == 18
  end

  defp mock_post() do
    Post.new(title: "test title",
             text: "test text",
             count: 4,
             rating: 5,
             posted: Datetime.now_ecto_datetime,
             temp: "test temp")
  end
  
end
