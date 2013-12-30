ExUnit.start

Code.require_file "../../test/support/file_helpers.exs", __DIR__

alias Ecto.Adapters.Riak.Util, as: RiakUtil
alias Ecto.Integration.Riak.TestRepo
alias :riakc_pb_socket, as: RiakSocket

defmodule Ecto.Integration.Riak.TestRepo do
  use Ecto.Repo, adapter: Ecto.Adapters.Riak

  def priv do
    "integration_test/riak/ecto/priv"
  end

  def url do
    [ "ecto://test@127.0.0.1:8000/test?max_count=10&init_count=1",
      "ecto://test@127.0.0.1:8001/test?max_count=10&init_count=1",
      "ecto://test@127.0.0.1:8002/test?max_count=10&init_count=1" ]
  end

  def query_apis do
    [ Ecto.Query.API ]
  end
end

defmodule Ecto.Integration.Riak.Post do
  use Ecto.RiakModel

  queryable "posts" do
    field :title, :string
    field :text, :string
    field :temp, :virtual, default: "temp"
    field :count, :integer
    has_many :comments, Ecto.Integration.Riak.Comment
    has_one :permalink, Ecto.Integration.Riak.Permalink
  end

  def version(), do: 0
  def migrate_from_previous(x), do: x
  def migrate_from_newer(x), do: x
end

defmodule Ecto.Integration.Riak.Comment do
  use Ecto.RiakModel

  queryable "comments" do
    field :text,     :string
    field :posted,   :datetime
    field :interval, :interval
    field :bytes,    :binary
    belongs_to :post, Ecto.Integration.Riak.Post ##, type: :string
  end

  def version(), do: 0
  def migrate_from_previous(x), do: x
  def migrate_from_newer(x), do: x
end

defmodule Ecto.Integration.Riak.Permalink do
  use Ecto.RiakModel

  queryable "permalinks" do
    field :url, :string
    belongs_to :post, Ecto.Integration.Riak.Post ##, type: :string
  end

  def version(), do: 0
  def migrate_from_previous(x), do: x
  def migrate_from_newer(x), do: x
end

defmodule Ecto.Integration.Riak.Custom do
  use Ecto.RiakModel

  queryable "customs", primary_key: false do
    field :foo,     :string, primary_key: true
  end

  def version(), do: 0
  def migrate_from_previous(x), do: x
  def migrate_from_newer(x), do: x
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
      alias Ecto.Adapters.Riak.Util, as: RiakUtil
      alias :riakc_pb_socket, as: RiakSocket
    end
  end
end

defmodule Ecto.Integration.Riak.Util do  
  defmacro wait_assert(clauses) do
    ## Yokozuna can take some time to index data (1-2 seconds)
    ## The wait_until function retries an operation multiple 
    ## times to take into account this delay
    quote do
      unquote(__MODULE__).wait_until(fn()-> assert unquote(clauses) end)
    end    
  end
  
  @type wait_until_fun :: (() -> ExUnit.ExpectationError.t | any)
  @type msec :: non_neg_integer
  @spec wait_until(wait_until_fun, msec, msec) :: any

  def wait_until(fun) do
    wait_until(fun, 10, 800)
  end

  def wait_until(fun, retry, delay) when retry > 0 do
    res = try do
            fun.()
          catch
            _,rsn -> rsn
          end
    if is_exception(res) do
      if retry == 1 do
        raise res
      else
        :timer.sleep(delay)
        wait_until(fun, retry-1, delay)
      end
    else
      res
    end
  end
end

## ----------------------------------------------------------------------
## Setup
## ----------------------------------------------------------------------

posts_bucket = RiakUtil.model_bucket(Ecto.Integration.Riak.Post)
comments_bucket = RiakUtil.model_bucket(Ecto.Integration.Riak.Comment)
permalinks_bucket = RiakUtil.model_bucket(Ecto.Integration.Riak.Permalink)
custom_bucket = RiakUtil.model_bucket(Ecto.Integration.Riak.Custom)
buckets = [ posts_bucket, comments_bucket, permalinks_bucket, custom_bucket ]

{ :ok, socket } = RiakSocket.start_link('127.0.0.1', 8000)
Enum.map(buckets, fn bucket ->
  :ok = RiakSocket.reset_bucket(socket, bucket)
  { :ok, keys } = RiakSocket.list_keys(socket, bucket)
  Enum.map(keys, fn key ->
    :ok == RiakSocket.delete(socket, bucket, key)
  end)
end)

{ :ok, _ } = TestRepo.start_link