defmodule Ecto.Adapters.Riak.Migration do
  use Behaviour  

  @type entity :: Ecto.Entity.t
  @type module :: atom

  @default_migrations_dir  "priv/repo/migrations/riak"

  ## ----------------------------------------------------------------------
  ## Callbacks
  ## ----------------------------------------------------------------------
  
  defcallback migrate_up()

  defcallback migrate_down()

  @doc "Returns the version number for the model"
  defcallback version() :: binary

  ## ----------------------------------------------------------------------
  ## API
  ## ----------------------------------------------------------------------
  
  def resolve(entity, schema) do
  end

  def migration_required?(entity) do
  end

  @doc """
  Returns a list of all migration classes
  in order of model version
  for an entity.

  We expect the migration classes to be sorted in 
  lexicographic order.

  example reply: [ Post.Version1, Post.Version2, Post.Version3 ]
  """
  @spec migration_modules(entity) :: [module]
  def migration_modules(entity) do
    model = entity.model
    regex = %r(to_string(entity) <> "_\d+\.ex$")

    ## grab all loaded modules,
    ## and then filter them out to contain
    ## only those that have some other name other than
    ## "#{entity}" or "#{entity}.Entity"
    case File.ls(migration_dir()) do
      {:ok, files} ->
        Enum.filter(files, &Regex.match?(regex, &1))
      _ ->
        []
    end
  end

  @doc """
  returns a string path to the directory 
  where migration modules should be found.

  This can be configured via the :riak_migrations key in your mix.exs
  and should be a path relative to the root directory of the project
  """
  def migration_dir() do
    root = File.cwd!
    dir = Mix.project[:riak_migrations]
    dir = if nil?(dir), do: @default_migrations_dir, else: dir
    root <> "/" <> dir
  end

end