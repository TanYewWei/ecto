
defmodule Ecto.Integration.Riak.RepoTest.OperationQueue do
  use GenServer.Behaviour

  ## State record to hold enqueued operations
  defrecord State, queue: []

  defp call(msg), do: :gen_server.call(__MODULE__, msg)

  @doc "Queues up an operation which be"
  @spec queue((() -> boolean)) :: :ok
  def queue(fun), do: call({ :queue, fun })
  
  @doc """
  Executes queued up operations in order,
  and returns a list of results for
  """
  @spec execute() :: [any]
  def execute(), do: call({ :execute })

  def stop(), do: call({ :stop })

  def start_link() do
    :gen_server.start_link({ :local, __MODULE__ }, __MODULE__, [], [])
  end

  def init(_) do
    {:ok, State.new}
  end

  def handle_call({ :queue, fun }, _, state) do
    new_state = state.queue([fun | state.queue])
    { :reply, :ok, new_state }
  end

  def handle_call({ :execute }, _, state) do
    res = List.foldl(state.queue, [], &([&1.() | &2]))
      |> Enum.reverse
    
    { :reply, res, state.queue([]) }
  end

  def handle_call({ :stop }, _, state) do
    { :stop, :normal, state }
  end
end