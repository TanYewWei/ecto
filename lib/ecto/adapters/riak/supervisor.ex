defmodule Ecto.Adapter.Riak.Supervisor do
  use Supervisor.Behaviour

  alias Ecto.Adapter.Riak.SchemaCheck
  alias :pooler, as: Pool

  @default_check_interval_ms  60000  ## every minute

  @type model      :: Ecto.Model.t
  @type repo       :: Ecto.Repo.t
  @type field_spec :: {field_name :: atom, field_type :: atom}
  @type version    :: integer
  @type schema     :: {version, [ field_spec ]}

  def start_link(pool_opts, worker_opts) do
    :gen_server.start_link(__MODULE__, {pool_opts, worker_opts}, [])
  end

  @doc """
  Starts up all the worker pools and starts the SchemaCheck operation.
  
  pool_opts can be one of:

  * `name` - the name of the pool
  * `group` - the pool group.
      This should be a fixed string for each Repo
  * `max_count` - maximum number of pool workers for the pool
  * `init_count` - baseline level of pool workers  
  * `start_mfa` - {mod, fun, args} tuple to start a pooler worker
      The module part is always :riakc_pb_socket
      The fun part is always be :start_link
      The args is in format [host :: string, port :: integer],
      and is based of the ecto URLs supplied in the repo

  worker_opts can be one of:
  * `interval` - the time (in milliseconds) between each SchemaCheck
  """
  def init({pool_opts, worker_opts}) do
    case connect(pool_opts) do
      :ok ->
        interval = Key.get(worker_opts, :interval, @default_check_interval_ms)
        state = State.new(interval: interval)
        {:ok, state}
      rsn ->
        rsn
    end
  end

  def handle_call({:check_interval, interval}, _, state) do
    {:reply, :ok, state.interval(interval)}
  end

  def handle_info({:check_schema}, _, state) do
    {:noreply, check_schema(state)}
  end
  
  @spec current_schema(repo) :: {:ok, schema}
  def current_schema(repo) do
    ets_get(repo.__riak__(:schema))
  end

  @spec set_poll_interval(integer) :: [ :ok | {:error, pid} ]
  def set_poll_interval(interval) do
    :supervisor.which_children()
    |> Enum.map(fn({_,pid,_,_})->
                    case is_pid(pid) do
                      true -> :gen_server.call(pid, {:check_interval, interval})
                      _    -> {:error, pid}
                    end
                end)
  end

  defp connect(pool_opts) do
    res = Enum.map(pool_opts, fn(x)->
                                  case Pool.new_pool(x) do
                                    {:ok, pid} -> pid
                                    _          -> :error
                                  end
                              end)
    case Enum.all?(res, &is_pid/1) do
      true -> :ok
      _    -> {:error, :failed_start_pools}
    end
  end

  @doc """
  Disconnects all workers in the pool group
  """
  @spec disconnect(atom) :: :ok
  def disconnect(pool_group) do
    case Pool.take_group_member(pool_group) do
      {pool_name, _} ->
        Pool.rm_pool(pool_name)
        disconnect_worker(pool_group)
      _ ->
        :ok
    end
  end      
  
  defp check_schema(state) do
    state
  end

  ## ----------------------------------------------------------------------
  ## ETS
  ## ----------------------------------------------------------------------

  defp ets_init() do    
  end

  defp ets_get(x) do
  end

  defp ets_set(x) do
  end

end