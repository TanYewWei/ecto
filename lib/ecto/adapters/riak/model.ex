defmodule Ecto.RiakModel do
  @moduledoc """
  When using the Riak Adapter, we require random string ids.
  Using Ecto.RiakModel instead of Ecto.Model simply ensures
  that this constraint is enforced.
  """

  defmacro __using__(_) do
    quote do
      @queryable_defaults primary_key: {:id, :string, []}
      @behaviour Ecto.Adapters.Riak.Migration
      use Ecto.Model
    end
  end

end