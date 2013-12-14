ExUnit.start

Code.require_file "../../test/support/file_helpers.exs", __DIR__

alias Ecto.Adapters.Riak
alias Ecto.Integration.Riak.TestRepo

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
    [ "ecto://localhost:8100?max_count=10&init_count=3",
      "ecto://localhost:8101?max_count=10&init_count=2",
      "ecto://localhost:8102?max_count=10&init_count=1" ]
  end

  def query_apis do
    [Ecto.Integration.Postgres.CustomAPI, Ecto.Query.API]
  end
end

defmodule Ecto.Integration.Riak.Post do
  use Ecto.Model

  queryable "posts" do
    field :title, :string
    field :text, :string
    field :temp, :virtual, default: "temp"
    field :count, :integer
    has_many :comments, Ecto.Integration.Riak.Comment
    has_one :permalink, Ecto.Integration.Riak.Permalink
  end
end

defmodule Ecto.Integration.Riak.Comment do
  use Ecto.Model

  queryable "comments" do
    field :text,     :string
    field :posted,   :datetime
    field :interval, :interval
    field :bytes,    :binary
    belongs_to :post, Ecto.Integration.Riak.Post
  end
end

defmodule Ecto.Integration.Riak.Permalink do
  use Ecto.Model

  queryable "permalinks" do
    field :url, :string
    belongs_to :post, Ecto.Integration.Riak.Post
  end
end

defmodule Ecto.Integration.Riak.Custom do
  use Ecto.Model

  queryable "customs", primary_key: false do
    field :foo, :string, primary_key: true
  end
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

  setup do
  end

  teardown do
  end
end

## ----------------------------------------------------------------------
## Database Setup
## ----------------------------------------------------------------------

{ :ok, _ } = TestRepo.start_link

setup_cmds = [
  %s(psql -U postgres -c "DROP DATABASE IF EXISTS ecto_test;"),
  %s(psql -U postgres -c "CREATE DATABASE ecto_test ENCODING='UTF8' LC_COLLATE='en_US.UTF-8' LC_CTYPE='en_US.UTF-8';")
]

Enum.each(setup_cmds, fn(cmd) ->
  key = :ecto_setup_cmd_output
  Process.put(key, "")
  status = Mix.Shell.cmd(cmd, fn(data) ->
    current = Process.get(key)
    Process.put(key, current <> data)
  end)

  if status != 0 do
    IO.puts """
    Test setup command error'd:

        #{cmd}

    With:

        #{Process.get(key)}
    Please verify the user "postgres" exists and it has permissions
    to create databases. If not, you can create a new user with:

        createuser postgres --no-password -d
    """
    System.halt(1)
  end
end)

setup_database = [
  "CREATE TABLE posts (id serial PRIMARY KEY, title varchar(100), text varchar(100), count integer)",
  "CREATE TABLE comments (id serial PRIMARY KEY, text varchar(100), posted timestamp, interval interval, bytes bytea, post_id integer)",
  "CREATE TABLE permalinks (id serial PRIMARY KEY, url varchar(100), post_id integer)",
  "CREATE TABLE customs (foo text PRIMARY KEY)",
  "CREATE TABLE transaction (id serial, text text)",
  "CREATE FUNCTION custom(integer) RETURNS integer AS 'SELECT $1 * 10;' LANGUAGE SQL"
]

{ :ok, _pid } = TestRepo.start_link

Enum.each(setup_database, fn(sql) ->
  result = Postgres.query(TestRepo, sql)
  if match?({ :error, _ }, result) do
    IO.puts("Test database setup SQL error'd: `#{sql}`")
    IO.inspect(result)
    System.halt(1)
  end
end)
