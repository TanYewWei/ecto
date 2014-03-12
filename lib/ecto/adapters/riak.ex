defmodule Ecto.Adapters.Riak do
  @behaviour Ecto.Adapter
  @behaviour Ecto.Adapter.Storage

  @bucket_name  "50"
  @bucket_type  "map"
  @put_options  [:return_body]

  @timeout      5000
    
  alias Ecto.Adapters.Riak.AdapterStartError
  alias Ecto.Adapters.Riak.ETS
  alias Ecto.Adapters.Riak.Object, as: RiakObj
  alias Ecto.Adapters.Riak.Search
  alias Ecto.Adapters.Riak.Util, as: RiakUtil
  alias Ecto.Query.Normalizer
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

  defmacro __using__(_opts) do
    quote do
      def __riak__(:pool_group) do
        __MODULE__.PoolGroup
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
        ## is ready to accept connections
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
    Pool.rm_group(repo.__riak__(:pool_group))
  end
  
  @doc """
  Fetchs all results from the data store based on the given query.
  """
  @spec all(Ecto.Repo.t, Ecto.Query.t, Keyword.t) :: [term] | no_return
  def all(repo, query, opts) do
    query = Normalizer.normalize(query)
    { query_tuple, post_proc_fun } = Search.query(query, opts)
    { _, _, querystring, _ } = query_tuple
    
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
  @spec create(repo, entity, Keyword.t) :: entity
  def create(repo, entity, opts) do
    entity = RiakObj.create_primary_key(entity)
    object = RiakObj.entity_to_object(entity)
    timeout = opts[:timeout] || @timeout
    fun = fn socket -> Riak.put(socket, object, @put_options, timeout) end
    
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
  @spec update(repo, entity, Keyword.t) :: integer
  def update(repo, entity, opts) do
    object = RiakObj.entity_to_object(entity)
    timeout = opts[:timeout] || @timeout
    fun = &Riak.put(&1, object, timeout)

    case use_worker(repo, fun) do
      :ok -> 1
      _   -> 0
    end
  end

  @doc """
  Updates all entities matching the given query with the values given.
  The query will only have where expressions and a single from expression. 
  Returns the number of affected entities.
  """
  @spec update_all(Ecto.Repo.t, Ecto.Query.t, Keyword.t, Keyword.t) :: integer
  def update_all(repo, Query[] = query, values, opts) do
    query = Normalizer.normalize(query)
    { query_tuple, post_proc_fun } = Search.query(query, opts)

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
  @spec delete(Ecto.Repo.t, Ecto.Entity.t, Keyword.t) :: :ok
  def delete(repo, entity, opts) do
    bucket = RiakUtil.bucket(entity.model)
    key = entity.primary_key
    timeout = opts[:timeout] || @timeout
    fun = fn socket -> Riak.delete(socket, bucket, key, timeout) end
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
  @spec delete_all(Ecto.Repo.t, Ecto.Query.t, Keyword.t) :: integer
  def delete_all(repo, Query[] = query, opts) do
    query = Normalizer.normalize(query)
    { query_tuple, post_proc_fun } = Search.query(query, opts)

    fun = fn socket ->
      case Search.execute(socket, query_tuple, post_proc_fun) do
        entities when is_list(entities) ->
          Enum.reduce(entities, 0, fn entity, acc ->
            bucket = RiakUtil.bucket(entity.model)
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

  defp use_worker(repo, fun) do
    pool_group = repo.__riak__(:pool_group)
    case Pool.take_group_member(pool_group) do
      worker when is_pid(worker) ->
        try do
          fun.(worker)
        after
          Pool.return_group_member(pool_group, worker)
        end
      rsn ->
        { :error, rsn }
    end
  end

  ## ----------------------------------------------------------------------
  ## Storage API

  @doc """
  Creates the appropriate 
  """
  def storage_up(opts) do
    riak_admin_script = opts[:riak_admin]
    ensure_riak_admin_script!(riak_admin_script)

    case create_bucket_type(riak_admin_script) do
      { :error, rsn } ->
        { :error, rsn }
      :ok ->
        fun = fn socket ->
          case Search.search_index_reload_all(socket) do
            result when is_list(result) ->
              if Enum.all?(result, fn { _, res } -> res == :ok end) do
                :ok
              else
                { :error, Enum.filter(result, fn { _, res } -> res != :ok end) }
              end
            _ ->
              { :error, :reload_search_indexes }
          end
        end

        ## TODO: figure out a way to get repo
        use_worker(nil, fun)
    end
  end

  def storage_down(_opts) do
    { :error, "Riak creates a bucket per Model, and cannot support dropping of keys without Model information" }
  end

  defp ensure_riak_admin_script!(path) do
    unless File.exists?(path) do
      raise Mix.Error, "#{path} is not a valid `riak-admin` executable. (Path must be absolute)"
    end
  end

  defp create_bucket_type(script) do
    ## Make commands
    init_props = "{\"props\": {}}"
    bucket_type = "ecto_search"
    bucket_create = "#{script} bucket-type create #{bucket_type} #{init_props}"
    bucket_activate = "#{script} bucket-type activate #{bucket_type}"

    ## Attempt to create bucket
    already_active = ~r" already_active\n$"i
    create_success = ~r" created\n$"i
    success_regex = [ create_success, already_active ]
    
    create_output = System.cmd(bucket_create)
    if Enum.all?(success_regex, &Regex.match?(&1, create_output)) do

      ## Attempt to activate bucket
      activate_success = ~r" has been activated\n$"i
      activate_output = System.cmd(bucket_activate)      
      if Regex.match?(activate_success, activate_output) do
        :ok
      else
        { :error, activate_output }
      end
      
    else
      { :error, create_output }
    end
  end
  
  ## ----------------------------------------------------------------------
  ## Util

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