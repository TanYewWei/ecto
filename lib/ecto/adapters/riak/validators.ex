defmodule Ecto.Adapters.Riak.Validators do

  @doc """
  Used to validate that a model conforms to the riak 
  """
  defmacro validate() do
    quote do
      validate x,
        primary_key: unquote(__MODULE__).is_binary,
        riak_version: unquote(__MODULE__).is_integer,
        riak_context: unquote(__MODULE__).is_list,
    end
  end

  defmacro validate(fields) do
    quote do
      validate x,
        [ { :primary_key, unquote(__MODULE__).is_binary },
          { :riak_version, unquote(__MODULE__).is_integer },
          { :riak_context, unquote(__MODULE__).is_list },
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

  def is_list(attr, value, opts // []) do
    if is_list(value) do
      []
    else
      [{ attr, opts[:message] || "is not a list" }]
    end
  end

end