defmodule Ecto.Adapters.Riak.Util do
  @type entity :: Ecto.Entity.t

  ## ----------------------------------------------------------------------
  ## Search Schema and Buckets
  ## ----------------------------------------------------------------------e

  @doc """
  Returns the bucket name for an Ecto Model.
  Each bucket should only store one type of Ecto Model.
  """
  @spec bucket(atom | entity) :: binary
  
  def bucket(x) when is_atom(x) do
    if function_exported?(x, :__model__, 1) do
      x.__model__(:source)
    else
      nil
    end
  end

  def bucket(entity) do
    bucket(entity.model)
  end

  @doc """
  Returns the search index for an Ecto Model.
  Each Model should have it's own search index
  """
  @spec search_index(atom) :: binary
  def search_index(model) do
    bucket(model)
    #Regex.replace(%r"^Elixir.|.Entity$", to_string(model), "")
  end

  @doc """
  Returns the name of the default Yokozuna search index
  which comes pre-built in Riak 2.0pre5 and later
  """
  def default_search_schema(), do: "_yz_default"

  def default_bucket_type(), do: "ecto_riak"  

  ## ----------------------------------------------------------------------
  ## Misc Helpers
  ## ----------------------------------------------------------------------

  ## Turns anything that implements 
  ## the String.Chars protocol into an atom
  @spec to_atom(term) :: atom
  
  def to_atom(x) when is_atom(x), do: x

  def to_atom(x) do
    try do
      to_string(x) |> binary_to_existing_atom
    catch
      ## case where there isn't an existing atom
      _,_ -> to_string(x) |> binary_to_atom
    end
  end

  ## ----------------------------------------------------------------------
  ## Entity Helpers
  ## ----------------------------------------------------------------------

  @doc """
  Returns a keyword list of all fields of an entity.
  """
  @spec entity_keyword(entity) :: Keyword.t
  def entity_keyword(entity) do
    elem(entity, 0).__entity__(:keywords, entity, primary_key: true)
  end
  
  @doc """
  Returns the type of a specified entity field.  
  """
  @spec entity_field_type(entity, atom) :: atom
  def entity_field_type(entity, field) do
    elem(entity, 0).__entity__(:field_type, field)
  end

  @spec entity_name_from_model(binary | atom) :: atom
  def entity_name_from_model(name) when is_binary(name) or is_atom(name) do
    name_str = to_string(name)
    suffix = ".Entity"
    if String.ends_with?(name_str, suffix) do
      name |> to_atom
    else
      (name_str <> suffix) |> to_atom
    end
  end
  
  @doc """
  Checks if an entity passes a basic Riak Entity validation.
  (has the minimum of information to be persisted/migrated on Riak)
  Returns an empty list if there are no validation errors.
  """
  @spec entity_validate(entity) :: [] | [term]
  def entity_validate(entity) do
    version = 
      try do
        if is_integer(entity.version) && entity.version >= 0 do
          []
        else
          [version: "version must be a non-negative integer"]
        end
      rescue x -> [version: x.message] end
    
    id =
      try do
        if is_binary(entity.id) do
          []
        else
          [id: "ID must be a globally unique string"]
        end
      rescue x -> [id: x.message] end
      
    version ++ id
  end

end