defmodule Ecto.Adapters.Riak.Validators do

  defmacro riak_validate() do
    quote do
      riak_validate(x)
    end
  end

  defmacro riak_validate(x) do
    quote do
      riak_validate(unquote(x), [])
    end
  end

  defmacro riak_validate(x, fields) do
    quote do
      validate unquote(x),
        [ { :primary_key, unquote(__MODULE__).validate_is_binary },
          { :riak_version, unquote(__MODULE__).validate_is_integer },
          { :riak_context, unquote(__MODULE__).validate_is_list },
          unquote_splicing(fields) ]
    end
  end

  def validate_is_binary(attr, value, opts // []) do
    if is_binary(value) do
      []
    else
      [{ attr, opts[:message] || "is not a string" }]
    end
  end

  def validate_is_integer(attr, value, opts // []) do
    if is_integer(value) do
      []
    else
      [{ attr, opts[:message] || "is not an integer" }]
    end
  end

  def validate_is_list(attr, value, opts // []) do
    if is_list(value) do
      []
    else
      [{ attr, opts[:message] || "is not a list" }]
    end
  end

end