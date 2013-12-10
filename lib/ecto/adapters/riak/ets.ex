defmodule Ecto.Adapters.Riak.ETS do
  @table_name  :ecto_riak_adapter

  @spec get(term, term) :: tuple
  def get(key, default // nil) do
    case :ets.lookup(table_name, key) do
      [{_,x}] -> x
      _       -> default
    end
  end

  @spec put(term, term) :: :ok | {:error, term}
  def put(key, val) do
    case :ets.insert(table_name, {key, val}) do
      true -> :ok
      _    -> {:error, :failed_insert}
    end
  end

  @spec delete(term) :: :ok
  def delete(key) do
    case :ets.delete(table_name, key) do
      true -> :ok
      _    -> {:error, :failed_delete}
    end
  end

  @spec delete_table() :: :ok
  def delete_table() do
    case :ets.delete(table_name) do
      true -> :ok
      rsn  -> {:error, rsn}
    end
  end

  @spec table_name() :: atom
  defp table_name() do
    case :ets.info(@table_name) do
      :undefined ->
        opts = [ :set,
                 :named_table,
                 {:read_concurrency, true} ]
        :ets.new(@table_name, opts)
      _ ->
        @table_name
    end
  end

end