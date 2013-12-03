defmodule Ecto.Adapters.Riak.Migration do
  @moduledoc %S"""
  Riak 1.4 has no notion of stop-the-world strong consistency.
  Riak 2.0 will ship with strong consistency, but we will not rely
  on that because it requires bucket_type configuration that will
  not be compatible with CRDT support
  (we want to use CRDTs in the future)

  ## Migration Process

  Migrations are dynamic and require 3 steps:
  
  1. On each read of an entity, we check that:
 
     - the entity is NOT up to date with the latest version
       that the server knows how to handle
  
     - migrations are enabled

     If both those are true, then migration continues.       
  
     If the version is higher than anything that the server
     knows how to handle, then the read should fail with
     {:error, :version_too_high}  
  
     In other words, the appropriate migration modules MUST
     be present on the server instance for it to be able to 
     serve queries as needed. For this reason, you SHOULD update
     all your servers with the appropriate migration modules
     as close to the same time as possible.

     The model should be updated before any processing should
     take place, and the next put to Riak should write the entity
     in this new format.
  
  2. 
  
  3. The next update to Riak will commit the new object to disk.

     This means that just reading an entity from a different version
     DOES NOT persist the new version of the entity to Riak.

  ## Intended Migration Procedure

  1. Disable migrations on all servers
  2. Copy migration specs to all servers
  3. Enable migrations across all servers

  ## Migration Specs
  
  Each migration version spec is specified in an elixir file.
  The filename should follow the format:
  
    `#{module_with_underscores}_#{version_number}.ex`
  
  eg: the first version of `Some.Funny.ModuleToTest`
      becomes `some.funny.module_to_test_1.ex`
  
  Each migration version spec has to implement the callbacks
  specified by this module, and have to be stored in a directory
  as specified by the :riak_migrations key in the mix.exs project dict
  or by dynamic reconfiguration via a call to the
  set_migrations_root_dir/1 function below
  """

  use Behaviour

  alias Ecto.Adapters.Riak.ETS
  
  ## ----------------------------------------------------------------------
  ## Types
  ## ----------------------------------------------------------------------

  @type attributes :: Dict
  @type entity     :: Ecto.Entity.t
  @type module     :: atom
  @type store      :: term  ## riak storage format

  ## ----------------------------------------------------------------------
  ## Types
  ## ----------------------------------------------------------------------

  @default_migrations_dir "priv/repo/migrations/riak"

  @key_migrations_root    "migrations_root"
  @key_migrations_dir     "default_migrations_dir"

  @after_compile __MODULE__

  def __after_compile__(_, _) do
    ETS.put(@key_migrations_root, File.cwd!)
    ETS.put(@key_migrations_dir,
            Keyword.get(Mix.project, :riak_migrations, @default_migrations_dir))
  end

  ## ----------------------------------------------------------------------
  ## Callbacks
  ## ----------------------------------------------------------------------
  
  @doc """
  Perform a dynamic migration to an entity
  from current entity version N to N+1
  based on `attributes` given from the entity

  The input will be a ListDist of key-value pairs,
  representing the attributes parsed from the storage object
  (with keys in accordance to the version of the stored entity)

  The return value should then be a similar list of attributes,
  now of the new version, which will then be used to construct
  a new entity
  """
  defcallback migrate_up(attributes) :: attributes
  
  @doc """
  Same as the above, but from a version from N to N-1
  Immediately returns `attributes` if N=0
  """
  defcallback migrate_down(attributes) :: attributes

  @doc "Returns the version number for the model"
  defcallback version() :: binary

  ## ----------------------------------------------------------------------
  ## Admin Switches
  ## ----------------------------------------------------------------------
  
  @migration_up_enabled_key   "migration_up_is_enabled"
  @migration_down_enabled_key "migration_down_is_enabled"

  @doc """
  Enables migrations to newer entity versions on the current server.
  """
  def migration_up_enable() do
    ETS.put(@migration_up_enabled_key, true)
  end

  @doc """
  Disables migrations to newer entity versions
  from happening on the current server.
  """
  def migration_up_disable() do
    ETS.put(@migration_up_enabled_key, false)
  end

  @doc """
  Same as migration_up_enable/0 but for downgrades to older versions.
  This is set to false by default, thereby avoiding spontaneous
  downgrading of just-upgraded entities in the face of 
  concurrent server upgrades.

  If you want to perform downgrades, it is recommended that you first
  call the migration_up_disable/0 function on all servers, and then 
  call the migration_down_enable/0 function to initiate downgrade
  """
  def migration_down_enable() do
    ETS.put(@migration_down_enabled_key, true)
  end
  
  @doc """
  Disables migrations to older entity versions
  from happening on the current server.
  """
  def migration_down_disable() do
    ETS.put(@migration_down_enabled_key, false)
  end

  @doc """
  Predicate telling us whether migrations to newer versions are allowed.
  This is set to false by default.
  """
  @spec migration_up_allowed?() :: boolean
  def migration_up_allowed?() do
    ETS.get(@migration_up_enabled_key, false) == true
  end

  @doc """
  Predicate telling us whether migrations to newer versions are allowed.
  This is set to false by default.
  """
  @spec migration_down_allowed?() :: boolean
  def migration_down_allowed?() do
    ETS.get(@migration_down_enabled_key, false) == true
  end

  ## ----------------------------------------------------------------------
  ## API
  ## ----------------------------------------------------------------------

  @doc """
  Compares an entity's version to the latest version
  which the server knows of.
  """
  def migration_required?(entity) do
    entity.version != current_version(entity)
  end

  @spec migrate(entity) :: entity
  def migrate(entity) do
    version = entity.version
    current = current_version(entity)
    
    ## Make upgrades implicit 
    ## and set current version accordingly
    if version > current do
      set_current_version(entity, version)
    end
    
    cond do
      version == current ->
        entity
      version < current ->
        ## migrate up
        if migration_up_allowed? do
          migrate_up(entity, version)
        end
      version > current ->
        ## migrate down
        if migration_down_allowed? do
          migrate_down(entity, version)
        end
      true ->
        entity
    end
  end

  @spec migrate_up(entity, integer) :: entity
  def migrate_up(entity, version) do
    ## Get relevant migration modules in ascending order
    modules = migration_modules(entity)
            |> Enum.filter(&(&1 <= version && &1 >= entity.version))
            |> Enum.sort(fn({_,v1}, {_,v2})-> v1 < v2 end)
            |> Enum.map(fn({mod,_})-> mod end)
    
    case length(modules) == (version - entity.version + 1) do
      true ->
        List.foldl(modules, entity, fn(module, acc)->
                                        module.migrate_up(acc)
                                    end)
      _ ->
        raise "missing migration files"
    end
  end

  @spec migrate_down(entity, integer) :: entity
  def migrate_down(entity, version) do
    ## Get relevant migration modules in descrending order
    modules = migration_modules(entity)
            |> Enum.filter(&(&1 >= version && &1 <= entity.version))
            |> Enum.sort(fn({_,v1}, {_,v2})-> v1 > v2 end)
            |> Enum.map(fn({mod,_})-> mod end)
    
    case length(modules) == (entity.version - version + 1) do
      true ->
        List.foldl(modules, entity, fn(module, acc)->
                                        module.migrate_down(acc)
                                    end)
      _ ->
        raise "missing migration files"
    end
  end

  @doc """
  Sets the latest version of an entity to server state.
  This gets called 
  """
  def set_current_version(entity, version) do
    key = "#{entity.model}_ver"
    ETS.put(key, version)
  end
  
  @doc """
  Returns the latest version for the entity that the 
  server knows about. This must have been set by set_latest_version/2
  and does not reflect the global state of the backend Riak cluster
  """
  @spec current_version(entity) :: integer
  def current_version(entity) do
    key = "#{entity.model}_ver"
    ETS.get(key, 0)
  end

  @doc """
  Returns a list of all migration classes
  in order of model version
  for an entity.

  We expect the migration classes to be sorted in 
  lexicographic order.

  example reply: [ {Post.Version1, 1}, 
                   {Post.Version2, 2},
                   {Post.Version3, 3} ]
  """
  @spec migration_modules(entity) :: [{module, version :: integer}]
  def migration_modules(entity) do
    model = entity.model
    filename = module_to_filename(model)
    regex = %r"#{filename}\.version\d+\.ex$"
    
    case File.ls(migration_dir()) do
      {:ok, files} ->
        Enum.filter(files, &Regex.match?(regex, &1))
        |> Enum.sort
        |> Enum.map(fn(filename)->
                        [ver_str] = Regex.run(%r"\d+.ex$", filename)
                        version = binary_to_integer(ver_str)
                        module = filename_to_module(filename)
                        {module, version}
                    end)
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
    root = ETS.get(@key_migrations_root)
    dir = ETS.get(@key_migrations_dir)
    root <> "/" <> dir
  end

  @doc """
  Sets the path relative to migration_root where migration modules
  are located. 
 
  It is recommended that you have migrations (up and down) turned 
  off before calling this function with a different directory
  """
  def set_migration_dir(dir) when is_binary(dir) do
    ETS.put(@key_migrations_dir, dir)
  end

  @doc """
  ## Examples

  module_to_filename(Some.Funny.ModuleToTest)
  #=> "some.funny.module_to_test"
  """
  defp module_to_filename(mod) do
    ## Remove elixir prefix
    [mod_without_elixir_prefix] =
      to_string(mod) |> String.split("Elixir.", [trim: true])

    ## separate along uppercase characters
    Regex.split(mod_without_elixir_prefix, ".")
    |> Enum.map(fn(x)->
                    Regex.replace(%r"\p{Lu}", x, "_&")
                    |> String.downcase
                    |> String.lstrip(hd('_'))
                end)
    |> Enum.join(".")
  end

  @doc """
  ## Examples

  filename_to_module("some.funny_module.version_1")
  #=> Some.FunnyModule.Version1
  """
  @spec filename_to_module(binary) :: module
  defp filename_to_module(filename) do
    components = String.split(filename, ".", [trim: true])
    string = Enum.map(components, 
                      fn(x)->
                          String.split(x, "_", [trim: true])
                          |> Enum.map(&String.capitalize/1)
                          |> to_string
                      end)
      
    try do
      binary_to_existing_atom(string)
    catch
      _,_ -> binary_to_atom(string)
    end
  end
                                       
end