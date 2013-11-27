defmodule Ecto.Adapter.Riak.Connection do

  @doc "Reloads "
  def reload_config(opts) do
  end

  @doc "Reloads riak an erlang configuration file "
  def reload_config_file(path) do
  end
  
  def start_link() do
  end

  @spec connected?() :: boolean
  def connected?() do
    @socket.ping(pool_pid()) == :pong
  end

  @spec disconnect() :: :ok
  def disconnect() do
    disconnect_worker(pool_name())
  end

  defp disconnect_worker(name) when is_atom(name) do
    Pool.rm_pool(name)
    disconnect_worker(pool_name())
  end
  defp disconnect_worker(_), do: :ok

  ## ----------------------------------------------------------------------
  ## Pool Management
  ## ----------------------------------------------------------------------

  @spec pool_pid() :: pid
  def pool_pid() do
    case pool() do
      {:ok, {_,pid}} ->
        pid
      rsn ->
        {:error, rsn}
    end
  end

  @spec pool_name() :: atom
  def pool_name() do
    case pool() do
      {:ok, {name,_}} ->
        name
      rsn ->
        {:error, rsn}
    end
  end

  @spec pool() :: {name, pid}
  def pool() do
    case Pool.take_group_member(@pool_group) do
      {name, pid} when is_atom(name) and is_pid(pid) ->
        {:ok, {name, pid}}
      rsn ->
        {:error, rsn}
    end
  end

  def pool_done(name, pid) do
    Pool.return_member(name, pid, :ok)
  end
  
end