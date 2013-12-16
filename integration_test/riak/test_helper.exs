ExUnit.start

Code.require_file "../../test/support/file_helpers.exs", __DIR__

alias Ecto.Adapters.Riak.Util, as: RiakUtil
alias Ecto.Integration.Riak.TestRepo
alias :riakc_pb_socket, as: RiakSocket

defmodule Ecto.Integration.Riak.CustomAPI do
  use Ecto.Query.Typespec

  deft integer
  defs custom(integer) :: integer
end

defmodule Ecto.Integration.Riak.TestRepo do
  use Ecto.Repo, adapter: Ecto.Adapters.Riak

  def priv do
    "integration_test/riak/ecto/priv"
  end

  def url do
    [ "ecto://test@127.0.0.1:8001/test?max_count=10&init_count=3",
      "ecto://test@127.0.0.1:8001/test?max_count=10&init_count=2",
      "ecto://test@127.0.0.1:8002/test?max_count=10&init_count=1" ]
  end

  def query_apis do
    [ Ecto.Integration.Riak.CustomAPI, Ecto.Query.API ]
  end
end

defmodule Ecto.Integration.Riak.Post do
  use Ecto.RiakModel

  queryable "posts" do
    field :title, :string
    field :text, :string
    field :temp, :virtual, default: "temp"
    field :count, :integer
    field :version,  :integer, default: 0
    has_many :comments, Ecto.Integration.Riak.Comment
    has_one :permalink, Ecto.Integration.Riak.Permalink
  end

  def version(), do: 0
end

defmodule Ecto.Integration.Riak.Comment do
  use Ecto.RiakModel

  queryable "comments" do
    field :text,     :string
    field :posted,   :datetime
    field :interval, :interval
    field :bytes,    :binary
    field :version,  :integer, default: 0
    belongs_to :post, Ecto.Integration.Riak.Post
  end

  def version(), do: 0
end

defmodule Ecto.Integration.Riak.Permalink do
  use Ecto.RiakModel

  queryable "permalinks" do
    field :url, :string
    field :version,  :integer, default: 0
    belongs_to :post, Ecto.Integration.Riak.Post
  end

  def version(), do: 0
end

defmodule Ecto.Integration.Riak.Custom do
  use Ecto.RiakModel

  queryable "customs", primary_key: false do
    field :version,  :integer, default: 0
    field :foo, :string, primary_key: true
  end

  def version(), do: 0
end

defmodule Ecto.Integration.Riak.Case do
  use ExUnit.CaseTemplate

  using do
    quote do
      import unquote(__MODULE__)
      require TestRepo

      import Ecto.Query
      alias Ecto.Integration.Riak.TestRepo
      alias Ecto.Integration.Riak.Post
      alias Ecto.Integration.Riak.Comment
      alias Ecto.Integration.Riak.Permalink
      alias Ecto.Integration.Riak.Custom
    end
  end
end

## ----------------------------------------------------------------------
## Setup
## ----------------------------------------------------------------------

{ :ok, _ } = TestRepo.start_link

## Delete all test data
{ :ok, socket } = RiakSocket.start_link('127.0.0.1', 8000)

posts_bucket = RiakUtil.model_bucket(Ecto.Integration.Riak.Post)
comments_bucket = RiakUtil.model_bucket(Ecto.Integration.Riak.Comment)
permalinks_bucket = RiakUtil.model_bucket(Ecto.Integration.Riak.Permalink)
custom_bucket = RiakUtil.model_bucket(Ecto.Integration.Riak.Custom)
buckets = [ posts_bucket, comments_bucket, permalinks_bucket, custom_bucket ]

IO.puts """
----------------------------------------------------------------------
Deleting test buckets and search indexes

This will take 2-3 seconds
----------------------------------------------------------------------
"""
Enum.map(buckets, fn(bucket)->
  :ok == RiakSocket.reset_bucket(socket, bucket)
  ##:ok == RiakSocket.delete_search_index(socket, bucket)
  { :ok, keys } = RiakSocket.list_keys(socket, bucket)
  res = Enum.map(keys, fn(key)->
    :ok == RiakSocket.delete(socket, bucket, key)
  end)
  ##IO.puts("setup deleted test bucket #{bucket} => #{Enum.all?(res, &(true == &1))}")
end)
