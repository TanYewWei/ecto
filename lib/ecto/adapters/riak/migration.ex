defmodule Ecto.Adapters.Riak.Migration do
  ## Documentation available in the "migrations.md" file
  ## located in the same directory as this module
  
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

  @doc """
  Checks if a module implements the Ecto.Adapters.Riak.Migration behaviour
  """
  def is_migration_module?(module) do
    function_exported?(module, :version, 1)
    function_exported?(module, :migrate_from_previous, 1)
    function_exported?(module, :migrate_from_newer, 1)
  end

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
  defp migrate_up(entity, version) do
    ## Get relevant migration modules in ascending order
    modules = migration_modules(entity, version)
    List.foldl(modules,
               entity,
               fn(module, ent)-> module.migrate_from_previous(ent) end)
  end

  @spec migrate_down(entity, integer) :: entity
  defp migrate_down(entity, version) do
    ## Get relevant migration modules in descending order
    modules = migration_modules(entity, version)
    List.foldl(modules,
               entity,
               fn(module, ent)-> module.migrate_from_newer(ent) end)
  end

  @spec migration_modules(entity, integer) :: [module] | no_return

  defp migration_modules(entity, version) do
    migration_modules_worker(entity, entity.version, version)
  end

  @spec migration_modules_worker(entity, integer, integer) :: [module]

  defp migration_modules_worker(entity, current, target) when current == target do
    [entity.model]
  end

  defp migration_modules_worker(entity, current, target) when current < target do
    ## Upgrade
    prefix = entity_prefix(entity.model)
    modules = :code.all_loaded
      |> Enum.filter(fn({mod, _})->
                         is_migration_module?(mod)
                         && prefix == entity_prefix(mod)
                         && mod.version <= target
                         && mod.version > current
                     end)
      |> Enum.map(fn({mod, _})-> mod end)

    ## Check for duplicates, raising error if any exist
    migration_modules_deduplicate!(modules, entity, target)
    
    ## Sort in ascending order
    Enum.sort(modules, fn(m0, m1)-> m0.version < m1.version end)
  end

  defp migration_modules_worker(entity, current, target) when current > target do
    ## Downgrade
    prefix = entity_prefix(entity.model)
    modules = :code.all_loaded
      |> Enum.filter(fn({mod, _})->
                         is_migration_module?(mod)
                         && prefix == entity_prefix(mod)
                         && mod.version >= target
                         && mod.version < current
                     end)
      |> Enum.map(fn({mod, _})-> mod end)

    ## Check for duplicates, raising error if any exist
    migration_modules_deduplicate!(modules, entity, target)
    
    ## Sort in descending order
    Enum.sort(modules, fn(m0, m1)-> m0.version > m1.version end)
  end

  defp migration_modules_deduplicate!(modules, entity, target_version) do
    ## checks for any duplicates in the modules list
    ## and raises an error if so
    version_set = Enum.reduce(modules, HashSet.new, fn(mod, acc)->
      try do
        HashSet.put(acc, mod.version)
      rescue
        UndefinedFunctionError ->
          raise MigrationModulesException,
            model: entity.model,
            entity_version: entity.version,
            target_version: target_version,
            failed_module: mod,
            modules: modules
        end
      end)   
    
    if HashSet.size(version_set) != length(modules) do
      raise MigrationModulesException,
        model: entity.model,
        entity_version: entity.version,
        target_version: target_version,
        modules: modules
    else
      false
    end
  end

  defp entity_prefix(module) do
    components = module |> to_string |> String.split(".")
    case components do
      [first | rest] when first == "Elixir" ->
        List.delete_at(rest, -1)
      _ ->
        nil
    end
  end

  # defp migration_modules(entity, version) do
  #   ## checks that all modules from the current entity version
  #   ## to the required new version have been loaded,
  #   ## raising an error if any modules are missing,
  #   ## or if any modules have duplicate definitions
    
  #   range = cond do
  #     version > entity.version -> entity.version+1..version
  #     version < entity.version -> version..entity.version
  #                                 |> Enum.to_list
  #                                 |> Enum.reverse
  #     true                     -> version..version
  #   end
    
  #   modules = Enum.map(range, &entity_module(entity, &1))
  #   Enum.map(modules, fn(module)->
  #                         if Code.ensure_loaded?(module) do
  #                           module
  #                         else
  #                           raise MigrationModulesException, 
  #                             model: entity.model,
  #                             entity_version: entity.version,
  #                             target_version: version,
  #                             failed_module: module,
  #                             expected_modules: modules
  #                         end
  #                     end)
  # end

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
  Returns the current traget version for the entity to which migration
  should be performed. This must have been set by set_current_version/2.
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