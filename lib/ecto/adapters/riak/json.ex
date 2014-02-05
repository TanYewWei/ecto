defmodule Ecto.Adapters.Riak.JSON do  
  
  def encode(x) do
    :jiffy.encode(x)
  end

  def decode(x) do
    :jiffy.decode(x)
  end

  def get({ inner }, key, default \\ nil) when is_binary(key) do
    Dict.get(inner, key, default)
  end

  @doc "Substitutes elixir nil for JSON atom :null"
  def maybe_null(x) do
    if(nil?(x), do: :null, else: x)
  end

  @doc "Substitutes JSON :null for elixir nil"
  def maybe_nil(x) do
    if(x == :null || x == "null", do: nil, else: x)
  end

end