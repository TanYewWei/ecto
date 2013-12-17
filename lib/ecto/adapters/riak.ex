defmodule Ecto.Adapters.Riak do
  @moduledoc """
  """

  @behaviour Ecto.Adapter
  @behaviour Ecto.Adapter.Migrations

  @bucket_name  "50"
  @bucket_type  "map"
  @datatype_update_options [:create, :return_body]
  @put_options [:return_body]

  alias Ecto.Query.Query
  alias Ecto.Query.QueryExpr
  alias Ecto.Query.Util
  ##alias Ecto.Query.Normalizer
  
  alias Ecto.Adapters.Riak.AdapterStartError
  alias Ecto.Adapters.Riak.Connection
  alias Ecto.Adapters.Riak.Object, as: RiakObj
  alias Ecto.Adapters.Riak.Search
  alias Ecto.Adapters.Riak.Supervisor  
  alias Ecto.Adapters.Riak.Util, as: RiakUtil

  alias :pooler, as: Pool
  alias :pooler_sup, as: PoolSup
  alias :riakc_pb_socket, as: Riak

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
    case PoolSup.start_link do
      { :error, { :already_started, pid } } ->
        { :error, { :already_started, pid } }
      { :ok, supervisor } ->
        ## Ensure that all pools are setup
        pool_opts = pool_opts(repo, url_opts)
        if length(pool_opts) == 0 do
          raise AdapterStartError,
            message: "Must have at least one valid pooler config to start Riak Adapter"
        end

        Enum.map(pool_opts, fn(config)->
          case PoolSup.new_pool(config) do
            { :ok, pid } when is_pid(pid) ->
              pid
            rsn ->
              raise AdapterStartError,
                message: "pooler failed to start pool: #{inspect config}"
          end
        end)

        ## Setup search indexes for all models
        :timer.sleep(5)
        failed_index_res = use_worker(repo, &Search.search_index_reload_all(&1))
          |> Enum.filter(fn({ _, res })-> res != :ok end)
        if length(failed_index_res) > 0 do
          raise AdapterStartError,
            message: "Failed to create search indexs for required models: #{inspect failed_index_res}"
        end
        
        ## return supervisor pid
        { :ok, supervisor }
      _ ->
        raise AdapterStartError,
        message: "pooler supervisor failed to start"
    end
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
      |> RiakUtil.to_atom      
    max_count = Keyword.get(opts, :max_count, "10") |> binary_to_integer
    init_count = Keyword.get(opts, :init_count, "2") |> binary_to_integer
    host = Keyword.fetch!(opts, :hostname) |> to_char_list
    port = Keyword.fetch!(opts, :port)
    [ name: pool_name,
      group: pool_group,
      max_count: max_count,
      init_count: init_count,
      start_mfa: { :riakc_pb_socket, :start_link, [host, port] } ]
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
    query = Util.normalize(query)
    { query_tuple, post_proc_fun } = Search.query(query)
    { _, _, querystring, _ } = query_tuple
    ##IO.puts("riak all: #{inspect query_tuple}")
    
    if String.strip(querystring) == "" do
      []
    else
      case use_worker(repo, &Search.execute(&1, query_tuple, post_proc_fun)) do
        entities when is_list(entities) ->
          entities
          |> Ecto.Associations.Assoc.run(query)
          |> preload(repo, query)
        _ ->
          []
      end
    end
  end  

  defp preload(results, repo, Query[] = query) do
    pos = Util.locate_var(query.select.expr, { :&, [], [0] })
    fields = Enum.map(query.preloads, &(&1.expr)) |> Enum.concat
    Ecto.Associations.Preloader.run(results, repo, fields, pos)
  end
  
  @doc """
  Stores a single new entity in the data store,
  and returns the newly stored value
  """
  @spec create(repo, entity) :: entity
  def create(repo, entity) do
    entity = RiakObj.create_primary_key(entity)
    object = RiakObj.entity_to_object(entity)
    fun = fn(socket)-> Riak.put(socket, object, @put_options) end
    
    case use_worker(repo, fun) do
      { :ok, new_object } ->
        ##IO.puts("successfully created entity: #{new_object.get_value}, with content_type: #{new_object.get_content_type}")
        RiakObj.object_to_entity(new_object)
      rsn ->
        nil
    end
  end  

  @doc """
  Updates an entity using the primary key as key,
  returning a new entity.
  """
  @spec update(repo, entity) :: integer
  def update(repo, entity) do
    object = RiakObj.entity_to_object(entity)
    fun = &Riak.put(&1, object)

    case use_worker(repo, fun) do
      :ok -> 1
      _   -> 0
    end
  end

  @doc """
  Updates all entities matching the given query with the values given. The
  query will only have where expressions and a single from expression. Returns
  the number of affected entities.
  """
  def update_all(repo, query, values) do
    entities = all(repo, query)
    objects = Enum.map(entities, &RiakObj.entity_to_object(&1))
    fun = fn(socket)->
              Enum.map(objects, &Riak.put(socket, &1))
          end

    case use_worker(repo, fun) do
      res when is_list(res) ->
        Enum.reduce(res, 0, fn(x, acc)->
          if(x == :ok, do: acc+1, else: acc)
        end)
      _ ->
        0
    end
  end

  @doc """
  Deletes an entity using the primary key as key.
  """
  @spec delete(Ecto.Repo.t, Ecto.Entity.t) :: :ok
  def delete(repo, entity) do
    bucket = RiakUtil.model_bucket(entity.model)
    key = entity.primary_key
    fun = fn(socket)-> Riak.delete(socket, bucket, key) end
    case use_worker(repo, fun) do
      :ok -> 1
      rsn -> rsn
    end
  end

  @doc """
  Deletes all entities matching the given query. The query will only have
  where expressions and a single from expression. Returns the number of affected
  entities.
  """
  def delete_all(repo, query) do
    query = Util.normalize(query)
    { query_tuple, post_proc_fun } = Search.query(query)
    { _, _, querystring, _ } = query_tuple
    
    fun = fn(socket)->
      case Search.execute(socket, query_tuple, post_proc_fun) do
        entities when is_list(entities) ->
          Enum.reduce(entities, 0, fn(entity, acc)->
            bucket = RiakUtil.model_bucket(entity.model)
            key = entity.primary_key
            case Riak.delete(socket, bucket, key) do
              :ok -> acc + 1
              _   -> acc
            end
          end)
        rsn ->
          rsn
      end
    end
    
    use_worker(repo, fun)
  end  

  ## ----------------------------------------------------------------------
  ## Worker Pools
  ## ----------------------------------------------------------------------

  defp use_worker(repo, fun) do
    ## Check if we're currently in a transaction,
    ## and make use of existing worker if it exists       
    pool_group = repo.__riak__(:pool_group)
    case Pool.take_group_member(pool_group) do
      worker when is_pid(worker) ->
        try do
          fun.(worker)
        after
          Pool.return_group_member(pool_group, worker, :ok)
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