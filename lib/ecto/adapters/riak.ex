defmodule Ecto.Adapters.Riak do
  @moduledoc """
  """

  @behaviour Ecto.Adapter
  @behaviour Ecto.Adapter.Migrations

  @bucket_name  "50"
  @bucket_type  "map"
  @datatype_update_options [:create, :return_body]

  alias Ecto.Associations.Assoc
  alias Ecto.Query.Query
  alias Ecto.Query.QueryExpr
  alias Ecto.Query.Util
  alias Ecto.Query.Normalizer
  
  alias Ecto.Adapters.Riak.Connection
  alias Ecto.Adapters.Riak.Search
  alias Ecto.Adapters.Riak.Supervisor

  require :pooler, as: Pool
  require :riakc_pb_socket, as: Riak

  @type entity      :: Ecto.Entity.t
  @type primary_key :: binary
  @type repo        :: Ecto.Repo.t

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
    host = Keyword.get!(opts, :hostname)
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
  @spec all(Ecto.Repo.t, Ecto.Query.t) :: [term] | no_return
  def all(repo, query) do
    query = query.select |> normalize_select |> query.select
    {query_tuple, post_proc_fun} = Search.query(query)
    entities = use_worker(repo, &Search.execute(&1, query_tuple, post_proc_fun))
    transformed = Enum.map(entities,
                           fn(entity)->
                               values = tuple_to_list(entity)
                               QueryExpr[expr: expr] = normalize_select(query.select)
                               transform_row(expr, values, query.sources) |> elem(0)
                           end)
    transformed
      |> Ecto.Associations.Assoc.run(query)
      |> preload(repo, query)
  end

  def normalize_select(QueryExpr[expr: { :assoc, _, [_, _] } = assoc] = expr) do
    normalize_assoc(assoc) |> expr.expr
  end

  def normalize_select(QueryExpr[expr: _] = expr), do: expr

  defp normalize_assoc({ :assoc, _, [_, _] } = assoc) do
    { var, fields } = Assoc.decompose_assoc(assoc)
    normalize_assoc(var, fields)
  end

  defp normalize_assoc(var, fields) do
    nested = Enum.map(fields, fn { _field, nested } ->
      { var, fields } = Assoc.decompose_assoc(nested)
      normalize_assoc(var, fields)
    end)
    { var, nested }
  end

  defp transform_row({ :{}, _, list }, values, sources) do
    { result, values } = transform_row(list, values, sources)
    { list_to_tuple(result), values }
  end

  defp transform_row({ _, _ } = tuple, values, sources) do
    { result, values } = transform_row(tuple_to_list(tuple), values, sources)
    { list_to_tuple(result), values }
  end

  defp transform_row(list, values, sources) when is_list(list) do
    { result, values } = Enum.reduce(list, { [], values }, fn elem, { res, values } ->
      { result, values } = transform_row(elem, values, sources)
      { [result|res], values }
    end)

    { Enum.reverse(result), values }
  end

  defp transform_row({ :&, _, [_] } = var, values, sources) do
    entity = Util.find_source(sources, var) |> Util.entity
    entity_size = length(entity.__entity__(:field_names))
    { entity_values, values } = Enum.split(values, entity_size)

    if Enum.all?(entity_values, &(nil?(&1))) do
      { nil, values }
    else
      { entity.__entity__(:allocate, entity_values), values }
    end
  end

  defp transform_row(_, values, _entities) do
    [value|values] = values
    { value, values }
  end

  defp preload(results, repo, Query[] = query) do
    pos = Util.locate_var(query.select.expr, { :&, [], [0] })
    fields = Enum.map(query.preloads, &(&1.expr)) |> Enum.concat
    Ecto.Associations.Preloader.run(results, repo, fields, pos)
  end
  
  @doc """
  Stores a single new entity in the data store. And return a primary key
  if one was created for the entity.
  """
  def create(repo, entity) :: primary_key
  def create(repo, entity) do
    key = entity.primary_key
    update = &RiakDatatypes.entity_to_map(entity, &1)
    fun = &Riak.modify_type(&1, update, @bucket_name, key, @datatype_modify_options)
    case use_worker(repo, fun) do
      {:ok, new_datatype} ->
        :ok
      _ ->
        nil
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
        {:error, rsn}
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