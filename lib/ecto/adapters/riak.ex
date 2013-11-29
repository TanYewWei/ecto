defmodule Ecto.Adapters.Riak do
  @moduledoc """
  """

  @behaviour Ecto.Adapter
  @behaviour Ecto.Adapter.Migrations

  @bucket_name  "50"
  @bucket_type  "map"
  @datatype_update_options [:create, :return_body]

  alias Ecto.Query.Query
  alias Ecto.Query.QueryExpr
  alias Ecto.Query.Util
  alias Ecto.Query.Normalizer
  
  alias Ecto.Adapters.Riak.Connection
  alias Ecto.Adapters.Riak.Supervisor

  require :pooler, as: Pool
  require :riakc_pb_socket, as: Riak

  @type repo :: Ecto.Repo.t

  ## Adapter API

  defmacro __using__(_opts) do
    quote do
      def __riak__(:pool_group) do
        __MODULE__.PoolGroup
      end

      def __riak__(:schema) do
        __MODULE__.Schema
      end
    end
  end

  def start_link(repo, url_opts) do
    pool_opts = pool_opts(repo, url_opts)
    worker_opts = []
    Supervisor.start_link(pool_opts, worker_opts)
  end

  @spec pool_opts(repo, ListDict | [ListDict]) :: [ ListDict ]
  defp pool_opts(repo, opts) when length(opts) > 0 do
    case hd(opts) |> is_list do
      true ->
        ## opts is a list of ListDicts
        Enum.map(opts, &(pool_opt(repo, &1)))
      _ ->
        [ pool_opt(repo, opts) ]
    end
  end

  @spec pool_opt(repo, ListDict) :: ListDict
  defp pool_opt(repo, opts) do
    pool_group = repo.__riak__(:pool_group)
    pool_name = "riak_#{:crypto.rand_bytes(12) |> :base64.encode}"
    max_count = Keyword.get(opts, :max_count, 10)
    init_count = Keyword.get(opts, :init_count, 2)
    host = Keyword.get!(opts, :host)
    port = Keyword.get!(opts, :port)
    [name: pool_name,
     group: pool_group,
     max_count: max_count,
     init_count: init_count,
     start_mfa: {:riakc_pb_socket, :start_link, [host,port]} ]
  end

  @doc """
  Stops any connection pooling or supervision started with `start_link/1`.
  """
  def stop(repo) do
    Supervisor.disconnect(repo.__riak__(:pool_group))
  end
  
  @doc """
  Fetchs all results from the data store based on the given query.
  """
  def all(repo, query) do
  end

  @doc """
  Stores a single new entity in the data store. And return a primary key
  if one was created for the entity.
  """
  def create(repo, entity) do
    key = entity.primary_key
    update = &RiakDatatypes.entity_to_map(entity, &1)
    fun = &Riak.modify_type(&1, update, @bucket_name, key, @datatype_modify_options)
    case use_worker(repo, fun) do
      _ -> :ok
    end
  end

  @doc """
  Updates an entity using the primary key as key.
  """
  def update(repo, entity) do
  end

  @doc """
  Updates all entities matching the given query with the values given. The
  query will only have where expressions and a single from expression. Returns
  the number of affected entities.
  """
  def update_all(repo, query, values) do
  end

  @doc """
  Deletes an entity using the primary key as key.
  """
  @spec delete(Ecto.Repo.t, Ecto.Entity.t) :: :ok
  def delete(repo, entity) do
    key = entity.primary_key
    fun = fn worker -> Riak.delete(worker, @bucket_name, key) end
    use_worker(repo, fun)
  end

  @doc """
  Deletes all entities matching the given query. The query will only have
  where expressions and a single from expression. Returns the number of affected
  entities.
  """
  def delete_all(repo, query) do
  end  

  ## ----------------------------------------------------------------------
  ## Worker Pools
  ## ----------------------------------------------------------------------

  defp use_worker(repo, fun) do
    ## Check if we're currently in a transaction,
    ## and make use of existing worker if it exists       
    pool_group = repo.__riak__(:pool_group)
    case Pool.take_group_member(pool_group) do
      {name, worker} when is_atom(name) and is_pid(worker) ->
        try do
          fun.(worker)
        after
          Pool.return_member(name, worker, :ok)
        end
      rsn ->
        { :error, rsn }
    end
  end

  ## ----------------------------------------------------------------------
  ## Migration API
  ## ----------------------------------------------------------------------

  @doc """
  Runs an up migration on the given repo, the migration is identified by the
  supplied version.

  ## Examples

    MyRepo.migrate_up(Repo, 20080906120000, "CREATE TABLE users(id serial, name text)")

  """
  def migrate_up(repo, version, commands) do
  end

  @doc """
  Runs a down migration on the given repo, the migration is identified by the
  supplied version.

  ## Examples

    MyRepo.migrate_down(Repo, 20080906120000, "DROP TABLE users")

  """
  def migrate_down(repo, version, commands) do
  end

  @doc """
  Returns the versions of all migrations that have been run on the given repo.
  """
  def migrated_versions(repo) do
  end
  
  
end