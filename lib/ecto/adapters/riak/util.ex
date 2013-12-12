defmodule Ecto.Adapters.Riak.Util do

  ## ----------------------------------------------------------------------
  ## Search Schema
  ## ----------------------------------------------------------------------e

  def search_index(entity_module) do
    Regex.replace(%r"^Elixir.|.Entity$", to_string(entity_module), "")
  end

  ## ----------------------------------------------------------------------
  ## Key and Value De/Serialization
  ## ----------------------------------------------------------------------

  @yz_key_regex  %r"_(i|is|f|fs|b|bs|b64_s|b64_ss|s|ss|i_dt|i_dts|dt|dts)$"

  @doc """
  Removes the default YZ schema suffix from a key
  schema: https://github.com/basho/yokozuna/blob/develop/priv/default_schema.xml
  """
  @spec key_from_yz(binary) :: binary
  def key_from_yz(key) do
    Regex.replace(@yz_key_regex, to_string(key), "")
  end

  @doc """
  adds a YZ schema suffix to a key depending on its type
  """
  @spec yz_key(binary, atom | {:list, atom}) :: binary
  def yz_key(key, type) do
    to_string(key) <> "_" <>
      case type do
        :integer  -> "i"
        :float    -> "f"
        :binary   -> "b64_s"
        :string   -> "s"
        :boolean  -> "b"
        :datetime -> "dt"
        :interval -> "i_dt"
        {:list, list_type} ->
          case list_type do
            :integer  -> "is"
            :float    -> "fs"
            :binary   -> "b64_ss"
            :string   -> "ss"
            :boolean  -> "bs"
            :datetime -> "dts"
            :interval -> "i_dts"
          end
      end
  end

  def yz_key_atom(key, type) do
    yz_key(key, type) |> to_atom
  end

  @spec yz_key_type(binary) :: atom | {:list, atom}
  def yz_key_type(key) do
    [suffix] = Regex.run(@yz_key_regex, key)
      |> Enum.filter(&String.starts_with?(&1, "_"))
    case suffix do
      "i"      -> :integer
      "f"      -> :float
      "b64_s"  -> :binary
      "s"      -> :string
      "b"      -> :boolean
      "dt"     -> :datetime
      "i_dt"   -> :interval
      "is"     -> {:list, :integer}
      "fs"     -> {:list, :float}
      "b64_ss" -> {:list, :binary}
      "ss"     -> {:list, :string}
      "bs"     -> {:list, :boolean}
      "dts"    -> {:list, :datetime}
      "i_dts"  -> {:list, :interval}
    end
  end

  @doc """
  Returns true if the key has a YZ suffix that indicates
  a multi-value (list) type
  """
  def is_list_key?(key) when is_binary(key) do
    regex = %r"_[is|fs|bs|ss|b64_ss|dts]$"
    Regex.match?(regex, key)
  end

  def to_atom(x) when is_atom(x), do: x

  def to_atom(x) when is_binary(x) do
    try do
      binary_to_existing_atom(x)
    catch
      _,_ -> binary_to_atom(x)
    end
  end

  ## ----------------------------------------------------------------------
  ## Entity Helpers
  ## ----------------------------------------------------------------------

  def entity_keyword(entity) do
    elem(entity, 0).__entity__(:entity_kw, entity, primary_key: true)
  end

  def entity_field_type(entity, field) do
    elem(entity, 0).__entity__(:field_type, field)
  end

end