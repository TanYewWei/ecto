defmodule Ecto.Adapters.Riak.Queryable do

  @doc false
  defmacro __using__(_) do
    quote do
    end
  end

  defmacro queryable(source, [], entity) do
    nil
  end

end