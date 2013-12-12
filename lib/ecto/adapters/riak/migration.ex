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
  alias Ecto.Adapters.Riak.Util, as: RiakUtil
  alias Ecto.Adapters.Riak.MigrationModulesException
  
  ## ----------------------------------------------------------------------
  ## Types
  ## ----------------------------------------------------------------------

  @type attributes :: Dict
  @type entity     :: Ecto.Entity.t
  @type module     :: atom
  @type store      :: term  ## riak storage format
  @type new_model  :: module
  @type old_model  :: module

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
  defcallback migrate_from_previous(entity) :: entity
  
  @doc """
  Same as the above, but from a version from N to N-1
  Immediately returns `attributes` if N=0
  """
  defcallback migrate_from_newer(entity) :: entity

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
  This is set to true by default.
  """
  @spec migration_up_allowed?() :: boolean
  def migration_up_allowed?() do
    ETS.get(@migration_up_enabled_key, true) == true
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
    entity_version = entity.version
    target_version = current_version(entity)
    IO.puts("attempt migration of #{entity_model(entity)} from version #{entity_version} to #{target_version}")
    
    ## Make upgrades implicit 
    ## and set current version accordingly
    if entity_version > target_version do
      set_current_version(entity, entity_version)
    end
    
    cond do
      entity_version == target_version ->
        entity
      target_version > entity_version ->
        ## migrate up
        if migration_up_allowed? do
          migrate_up(entity, target_version)
        else
          entity
        end
      target_version < entity_version ->
        ## migrate down
        if migration_down_allowed? do
          migrate_down(entity, target_version)
        else
          entity
        end
      true ->
        entity
    end
  end

  @spec migrate_up(entity, integer) :: entity
  def migrate_up(entity, version) do
    ## Get relevant migration modules in ascending order
    modules = migration_modules(entity, version)
    List.foldl(modules,
               entity,
               fn(module, ent)-> module.migrate_from_previous(ent) end)
  end

  @spec migrate_down(entity, integer) :: entity
  def migrate_down(entity, version) do
    ## Get relevant migration modules in descending order
    modules = migration_modules(entity, version)
    List.foldl(modules,
               entity,
               fn(module, ent)-> module.migrate_from_newer(ent) end)
  end

  @spec migration_modules(entity, integer) :: [module] | no_return
  defp migration_modules(entity, version) do
    ## checks that all modules from the current entity version
    ## to the required new version have been loaded,
    ## raising an error if any modules are missing
    range = cond do
      version > entity.version -> entity.version+1..version
      version < entity.version -> version..entity.version
                                  |> Enum.to_list
                                  |> Enum.reverse
      true                     -> version..version
    end
    
    modules = Enum.map(range, &entity_module(entity, &1))
    Enum.map(modules, fn(module)->
                          if Code.ensure_loaded?(module) do
                            module
                          else
                            raise MigrationModulesException, 
                              model: entity.model,
                              entity_version: entity.version,
                              target_version: version,
                              failed_module: module,
                              expected_modules: modules
                          end
                      end)
  end

  defp entity_module(entity, version) do
    version_suffix = ".Version#{version}"
    version_suffix_regex = %r"\.Version\d+$"
    str = (to_string(entity.model)
           |> String.replace(version_suffix_regex, "")) <> version_suffix
    RiakUtil.to_atom(str)
  end

  @doc """
  Sets the latest version of an entity to server state.
  This gets called 
  """
  def set_current_version(entity, version) do
    key = "#{entity_model(entity)}_ver"
    ETS.put(key, version)
  end
  
  @doc """
  Returns the latest version for the entity that the 
  server knows about. This must have been set by set_latest_version/2
  and does not reflect the global state of the backend Riak cluster
  """
  @spec current_version(entity) :: integer
  def current_version(entity) do
    key = "#{entity_model(entity)}_ver"
    ETS.get(key, 0)
  end

  defp entity_model(entity) do
    ## returns the entity model with any version suffix removed
    to_string(entity.model)
    |> String.replace(%r"\.Version\d+$", "")
    |> RiakUtil.to_atom
  end 
                                       
end