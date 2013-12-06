# For tasks/generators testing
Mix.start()
Mix.shell(Mix.Shell.Process)
System.put_env("ECTO_EDITOR", "")

# Commonly used support feature
Code.require_file "support/file_helpers.exs", __DIR__
Code.require_file "support/compile_helpers.exs", __DIR__

defmodule Ecto.UnitTest.Post do
  use Ecto.Model

  queryable "posts" do
    field :title, :string
    field :text, :string
    field :temp, :virtual, default: "temp"
    field :count, :integer
    has_many :comments, Ecto.UnitTest.Comment
    has_one :permalink, Ecto.UnitTest.Permalink
  end
end

defmodule Ecto.UnitTest.Comment do
  use Ecto.Model

  queryable "comments" do
    field :text, :string
    field :posted, :datetime
    field :interval, :interval
    field :bytes, :binary
    belongs_to :post, Ecto.UnitTest.Post
    belongs_to :author, Ecto.UnitTest.User
  end
end

defmodule Ecto.UnitTest.Permalink do
  use Ecto.Model

  queryable "permalinks" do
    field :url, :string
    belongs_to :post, Ecto.UnitTest.Post
  end
end

defmodule Ecto.UnitTest.User do
  use Ecto.Model

  queryable "users" do
    field :name, :string
    has_many :comments, Ecto.UnitTest.Comment
  end
end

defmodule Ecto.UnitTest.Custom do
  use Ecto.Model

  queryable "customs", primary_key: false do
    field :foo, :string, primary_key: true
  end
end

ExUnit.start()
