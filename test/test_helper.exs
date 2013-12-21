# For tasks/generators testing
Mix.start()
Mix.shell(Mix.Shell.Process)
System.put_env("ECTO_EDITOR", "")

# Commonly used support feature
Code.require_file "support/file_helpers.exs", __DIR__
Code.require_file "support/compile_helpers.exs", __DIR__

defmodule Ecto.Test.Riak.Post do
  use Ecto.RiakModel

  queryable "posts" do
    field :title, :string
    field :text, :string
    field :posted, :datetime
    field :temp, :virtual, default: "temp"
    field :count, :integer
    field :rating, :float
    has_many :comments, Ecto.UnitTest.Comment
    has_one :permalink, Ecto.UnitTest.Permalink
  end
end

defmodule Ecto.Test.Riak.Comment do
  use Ecto.RiakModel

  queryable "comments" do
    field :text, :string
    field :posted, :datetime
    field :interval, :interval
    field :bytes, :binary
    belongs_to :post, Ecto.UnitTest.Post
    belongs_to :author, Ecto.UnitTest.User
  end
end

defmodule Ecto.Test.Riak.Permalink do
  use Ecto.RiakModel

  queryable "permalinks" do
    field :url, :string
    belongs_to :post, Ecto.UnitTest.Post
  end
end

defmodule Ecto.Test.Riak.User do
  use Ecto.RiakModel

  queryable "users" do
    field :name, :string
    has_many :comments, Ecto.UnitTest.Comment
  end
end

defmodule Ecto.Test.Riak.Custom do
  use Ecto.RiakModel

  queryable "customs", primary_key: false do
    field :foo, :string, primary_key: true
  end
end

ExUnit.start()
