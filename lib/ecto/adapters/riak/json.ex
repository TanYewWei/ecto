defmodule Riak.Adapter.Riak.JSON do

  @type json :: {[ any ]}
  
  def encode(x) do
    :jiffy.encode(x)
  end

  def decode(x) do
    :jiffy.decode(x)
  end

  def get(json, key, default // nil) when is_binary(key) do
    get(json, {key}, default)
  end
  
  def get(json, keys, default) when is_tuple(keys) do
    case nil?(json) do
      true -> default
      _    -> :ej.get(keys, json, default) |> maybe_nil
    end
  end

  def put(json, key, val) when is_binary(key) do
    put(json, {key}, val)
  end
  
  def put(json, keys, val) when is_tuple(keys) do
    case nil?(json) do
      true -> nil
      _    -> :ej.set(keys, json, maybe_null(val))
    end
  end

  def delete(json, key) when is_binary(key) do
    delete(json, {key})
  end
  
  def delete(json, keys) when is_tuple(keys) do
    :ej.delete(keys, json)
  end

  def valid?(x) do
    case x do
      { inner } ->
        Enum.all?(inner, 
                  fn(p)->
                      case p do
                        {_,_} -> true
                        _     -> false
                      end
                  end)
      _ ->
        false
    end
  end

  @doc "returns all keys on the current level of JSON object"
  
  def keys(x) do
    {inner} = x
    Enum.map(inner, fn({k,_})-> k end)
  end

  @spec values(json) :: [term]
  def values(x) do
    {inner} = x
    Enum.map(inner, fn({_,v})-> v end)
  end

  @doc "Substitutes elixir nil for JSON atom :null"
  def maybe_null(x) do
    if(nil?(x), do: :null, else: x)
  end

  @doc "Substitutes JSON :null for elixir nil"
  def maybe_nil(x) do
    if(x == :null, do: nil, else: x)
  end

end