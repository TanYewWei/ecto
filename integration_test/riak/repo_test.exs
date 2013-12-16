defmodule Ecto.Integration.Riak.RepoTest do
  use Ecto.Integration.Riak.Case
  alias Ecto.Associations.Preloader
  alias Ecto.Integration.Riak.RepoTest.OperationQueue, as: OpQueue

  setup_all do
    case OpQueue.start_link do
      { :ok, pid } when is_pid(pid) ->
        { :ok, [op_queue: pid] }
      rsn ->
        rsn
    end
  end

  test "run" do
    test_already_started
    fetch_empty
    basic
  end

  defp test_already_started do
    assert { :error, { :already_started, _ } } = TestRepo.start_link
  end

  defp fetch_empty do
    assert [] == TestRepo.all(from p in Post)
  end

  defp basic do
    ## ---------------------------------------------------------------------
    ## Create
    ## ---------------------------------------------------------------------
    p0 = TestRepo.create(Post.Entity[title: "The shiny new Ecto", text: "coming soon..."])
    assert size(p0.id) == 24

    c0 = TestRepo.create(Comment.new(text: "test text 0", post_id: p0.id))
    assert size(c0.id) == 24

    c1 = TestRepo.create(Comment.new(text: "test text 1", post_id: p0.id))
    assert size(c1.id) == 24

    l0 = TestRepo.create(Permalink.new(url: "http://test.com", post_id: p0.id))
    assert size(l0.id) == 24
    
    OpQueue.queue(fn()-> 
       [post] = TestRepo.all(from(p in Post) |> preload(:comments))
       comments = post.comments.to_list
       IO.puts "comments: #{length comments}"

       [ if(post.id == p0.id, do: nil, else: "nonmatch post id"),
         if(post.title == p0.title, do: nil, else: "nonmatch post title"),
         if(post.text == p0.text, do: nil, else: "nonmatch post text"),
         if(c0 in comments, do: nil, else: "nonmatch post comment 0"),
         if(c1 in comments, do: nil, else: "nonmatch post comment 1") ]
    end)
    
    IO.puts """
----------------------------------------------------------------------
Waiting for Yokozuna to index created values ...
----------------------------------------------------------------------
    """
    :timer.sleep(2000)

    ## ---------------------------------------------------------------------
    ## Fetch
    ## ---------------------------------------------------------------------
    
    Enum.each(OpQueue.execute(), fn(res)->
                                     if res == [] do
                                       :ok
                                     else
                                       raise "error: #{inspect res}"
                                     end
                                 end)

    ## ---------------------------------------------------------------------
    ## Update
    ## ---------------------------------------------------------------------

    ## ---------------------------------------------------------------------
    ## Verify Update
    ## ---------------------------------------------------------------------

    ## ---------------------------------------------------------------------
    ## Delete
    ## ---------------------------------------------------------------------

    ## single delete
    assert :ok == TestRepo.delete(p0)

    IO.puts """
----------------------------------------------------------------------
Waiting for Deletes to Propagate ...
----------------------------------------------------------------------
    """
    :timer.sleep(3000)

    ## ---------------------------------------------------------------------
    ## Verify Delete
    ## ---------------------------------------------------------------------

    assert [] == TestRepo.all(from p in Post)
  end

end

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