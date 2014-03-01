defmodule Ecto.Integration.WorkerTest do
  use ExUnit.Case, async: true

  alias Ecto.Adapters.Postgres.Worker

  test "worker starts without an active connection" do
    { :ok, worker } = Worker.start_link(worker_opts(database: "gary"))

    assert Process.alive?(worker)
    refute :sys.get_state(worker) |> elem(1)
  end

  test "worker starts with an active connection" do
    { :ok, worker } = Worker.start_link(worker_opts(lazy: "false"))

    assert Process.alive?(worker)
    assert :sys.get_state(worker) |> elem(1)
  end

  test "worker reconnects to database when connecton exits" do
    { :ok, worker } = Worker.start_link(worker_opts)

    assert Postgrex.Result[] = Worker.query!(worker, "SELECT TRUE")
    conn = :sys.get_state(worker) |> elem(1)

    Process.exit(conn, :normal)
    assert Postgrex.Result[] = Worker.query!(worker, "SELECT TRUE")
  end

  test "worker stops if caller dies" do
    { :ok, worker } = Worker.start(worker_opts(lazy: "false"))
    conn = :sys.get_state(worker) |> elem(1)
    worker_mon = Process.monitor(worker)
    conn_mon = Process.monitor(conn)

    spawn(fn ->
      Worker.monitor_me(worker)
      Worker.query!(worker, "SELECT TRUE")
    end)

    assert_receive({ :DOWN, ^worker_mon, :process, ^worker, _ }, 1000)
    assert_receive({ :DOWN, ^conn_mon, :process, ^conn, _ }, 1000)
  end

  defp worker_opts(opts \\ []) do
    opts
    |> Keyword.put_new(:hostname, "localhost")
    |> Keyword.put_new(:database, "ecto_test")
    |> Keyword.put_new(:username, "postgres")
    |> Keyword.put_new(:password, "postgres")
  end
end
