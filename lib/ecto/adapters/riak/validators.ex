defmodule Ecto.Adapters.Riak.Validators do

  @doc """
  Used to validate that a model conforms to the riak 
  """
  defmacro validate() do
    quote do
      validate x,
        id: unquote(__MODULE__).is_binary,
        version: unquote(__MODULE__).is_integer
    end
  end

  defmacro validate(fields) do
    quote do
      validate x,
        [ { :id, unquote(__MODULE__).is_binary },
          { :version, unquote(__MODULE__).is_integer },
          unquote_splicing(fields) ]
    end
  end

  def is_binary(attr, value, opts // []) do
    if is_binary(value) do
      []
    else
      [{ attr, opts[:message] || "is not a string" }]
    end
  end

  def is_integer(attr, value, opts // []) do
    if is_integer(value) do
      []
    else
      [{ attr, opts[:message] || "is not an integer" }]
    end
  end

end