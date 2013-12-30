defmodule Ecto.Adapters.Riak do
  @behaviour Ecto.Adapter

  @bucket_name  "50"
  @bucket_type  "map"
  @put_options  [:return_body]
    
  alias Ecto.Adapters.Riak.AdapterStartError
  alias Ecto.Adapters.Riak.ETS
  alias Ecto.Adapters.Riak.Object, as: RiakObj
  alias Ecto.Adapters.Riak.Search
  alias Ecto.Adapters.Riak.Supervisor
  alias Ecto.Adapters.Riak.Util, as: RiakUtil
  alias Ecto.Query.Query
  alias Ecto.Query.Util

  alias :pooler, as: Pool
  alias :pooler_sup, as: PoolSup
  alias :riakc_pb_socket, as: Riak

  @type entity      :: Ecto.Entity.t
  @type primary_key :: binary
  @type repo        :: Ecto.Repo.t

  ## ----------------------------------------------------------------------
  ## Adapter API
  ## ----------------------------------------------------------------------

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

        Enum.map(pool_opts, fn config ->
          case PoolSup.new_pool(config) do
            { :ok, pid } when is_pid(pid) ->
              pid
            _ ->
              raise AdapterStartError,
                message: "pooler failed to start pool: #{inspect config}"
          end
        end)

        ## Setup search indexes for all models.
        ## This will require us to wait until pooler
        ## is ready to accept connections.

        setup_search_indexes = fn ->
          failures = use_worker(repo, &Search.search_index_reload_all(&1))
            |> Enum.filter(fn { _, res } -> res != :ok end)
          if length(failures) > 0 do
            raise AdapterStartError,
              message: "Failed to create search indexes for required models: #{inspect failures}"
          else
            :ok
          end
        end

        wait_until(setup_search_indexes)

        ## setup ETS table for migration state management
        ETS.init()
        
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
    fun = fn socket -> Riak.put(socket, object, @put_options) end
    
    case use_worker(repo, fun) do
      { :ok, new_object } ->
        RiakObj.object_to_entity(new_object)
      _ ->
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
  def update_all(repo, Query[] = query, values) do
    query = Util.normalize(query)
    { query_tuple, post_proc_fun } = Search.query(query)

    fun = fn socket ->
      case Search.execute(socket, query_tuple, post_proc_fun) do
        entities when is_list(entities) ->
          objects = Enum.map(entities, fn entity ->
            entity.update(values) |> RiakObj.entity_to_object
          end)
          
          Enum.reduce(objects, 0, fn object, acc ->
            case Riak.put(socket, object) do
              :ok -> acc + 1
              _   -> acc
            end
          end)
        _ ->
          0
      end
    end

    use_worker(repo, fun)
  end

  @doc """
  Deletes an entity using the primary key as key.
  """
  @spec delete(Ecto.Repo.t, Ecto.Entity.t) :: :ok
  def delete(repo, entity) do
    bucket = RiakUtil.model_bucket(entity.model)
    key = entity.primary_key
    fun = fn socket -> Riak.delete(socket, bucket, key) end
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
  def delete_all(repo, Query[] = query) do
    query = Util.normalize(query)
    { query_tuple, post_proc_fun } = Search.query(query)

    fun = fn socket ->
      case Search.execute(socket, query_tuple, post_proc_fun) do
        entities when is_list(entities) ->
          Enum.reduce(entities, 0, fn entity, acc ->
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
  ## Util
  ## ----------------------------------------------------------------------

  defp wait_until(fun), do: wait_until(fun, 5, 500)

  defp wait_until(fun, retry, delay) when retry > 0 do
    res = try do
            fun.()
          rescue
            x in [AdapterStartError] -> x
          catch
            _,rsn -> rsn
          end
    if res != :ok do
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

  defp wait_until(_, _, _) do
    { :error, :timed_out }
  end
  
end