defmodule Ecto.Test.Riak.Post do
  use Ecto.RiakModel

  queryable "ecto.test.riak.posts" do
    field :title, :string
    field :text, :string
    field :posted, :datetime
    field :temp, :virtual, default: "temp"
    field :count, :integer
    field :rating, :float
    field :riak_version, :integer, default: 0
    has_many :comments, Ecto.Test.Riak.Comment
    has_one :permalink, Ecto.Test.Riak.Permalink
  end

  def version(), do: 0
  def migrate_from_previous(x), do: x
  def migrate_from_newer(x), do: x
end

defmodule Ecto.Test.Riak.Comment do
  use Ecto.RiakModel

  queryable "ecto.test.riak.comments" do
    field :text, :string
    field :posted, :datetime
    field :interval, :interval
    field :bytes, :binary
    field :riak_version, :integer, default: 0
    belongs_to :post, Ecto.Test.Riak.Post
    belongs_to :author, Ecto.Test.Riak.User
  end

  def version(), do: 0
  def migrate_from_previous(x), do: x
  def migrate_from_newer(x), do: x
end

defmodule Ecto.Test.Riak.Permalink do
  use Ecto.RiakModel

  queryable "ecto.test.riak.permalinks" do
    field :url, :string
    field :riak_version, :integer, default: 0
    belongs_to :post, Ecto.Test.Riak.Post
  end

  def version(), do: 0
  def migrate_from_previous(x), do: x
  def migrate_from_newer(x), do: x
end

defmodule Ecto.Test.Riak.User do
  use Ecto.RiakModel

  queryable "ecto.test.riak.users" do
    field :name, :string
    field :riak_version, :integer, default: 0
    has_many :comments, Ecto.Test.Riak.Comment
  end

  def version(), do: 0
  def migrate_from_previous(x), do: x
  def migrate_from_newer(x), do: x
end

defmodule Ecto.Test.Riak.Custom do
  use Ecto.RiakModel

  queryable "ecto.test.riak.customs", primary_key: false do
    field :foo, :string, primary_key: true
    field :riak_version, :integer, default: 0
  end

  def version(), do: 0
  def migrate_from_previous(x), do: x
  def migrate_from_newer(x), do: x
end
