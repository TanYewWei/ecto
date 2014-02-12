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
  defcallback version() :: integer

  @doc """
  Checks if a module can be used in migration.
  """
  def is_migration_module?(module) do
    (function_exported?(module, :version, 0) ||
     function_exported?(module, :version, 1)) &&
    (function_exported?(module, :migrate_from_previous, 1) ||
     function_exported?(module, :migrate_from_previous, 2)) &&
    (function_exported?(module, :migrate_from_newer, 1) ||
     function_exported?(module, :migrate_from_newer, 2))
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
    entity.riak_version != current_version(entity)
  end

  @spec migrate(entity) :: entity
  def migrate(entity) do
    entity_version = entity.riak_version
    target_version = current_version(entity)
    
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
    List.foldl(modules, entity, fn module, ent ->
      ent.riak_version(model_version(module))
      |> module.migrate_from_previous
    end)
  end

  @spec migrate_down(entity, integer) :: entity
  defp migrate_down(entity, version) do
    ## Get relevant migration modules in descending order
    modules = migration_modules(entity, version)
    List.foldl(modules, entity, fn module, ent ->
       ent.riak_version(model_version(module))
       |> module.migrate_from_newer
    end)
  end

  @spec migration_modules(entity, integer) :: [module] | no_return

  defp migration_modules(entity, version) do
    migration_modules_worker(entity, entity.riak_version, version)
  end

  @spec migration_modules_worker(entity, integer, integer) :: [module]

  defp migration_modules_worker(entity, current, target) when current == target do
    [entity.model]
  end

  defp migration_modules_worker(entity, current, target) when current < target do
    ## Upgrade
    modules = :code.all_loaded
      |> Enum.filter(fn { mod, _ } ->
           if is_migration_module?(mod) do
             mod_ver = model_version(mod)
             sibling_modules?(entity.model, mod)
             && mod_ver <= target
             && mod_ver > current
           else
             false
           end
         end)
      |> Enum.map(fn { mod, _ } -> mod end)

    ## Check for duplicates, raising error if any exist
    migration_modules_deduplicate!(modules, entity, target)
    
    ## Sort in ascending order
    Enum.sort(modules, fn m0, m1 -> model_version(m0) < model_version(m1) end)
  end

  defp migration_modules_worker(entity, current, target) when current > target do
    ## Downgrade
    #mod_str = to_string(entity.model)
    #prefix = entity_prefix(entity.model)
    modules = :code.all_loaded
      |> Enum.filter(fn { mod, _ } ->
           if is_migration_module?(mod) do
             mod_ver = model_version(mod)
             sibling_modules?(entity.model, mod)
             && mod_ver >= target
             && mod_ver < current
           else
             false
           end
         end)
      |> Enum.map(fn { mod, _ } -> mod end)

    ## Check for duplicates, raising error if any exist
    migration_modules_deduplicate!(modules, entity, target)
    
    ## Sort in descending order
    Enum.sort(modules, fn m0, m1 -> model_version(m0) > model_version(m1) end)
  end

  defp sibling_modules?(m0, m1) when m0 == m1 do
    ## should not have duplicates
    false
  end

  defp sibling_modules?(m0, m1) do
    p0 = entity_prefix(m0)
    p1 = entity_prefix(m1)
    
    ## case where prefix is the name of another model
    ## Example: `My.Models.Post` vs `My.Models.Post.Ver1`
    r0 = m0 == p1 || m1 == p0

    ## common shared prefix
    ## Example: `My.Models.Post.Ver0` vs `My.Models.Post.V2`
    ## Note that this shouldn't succeed with an example like
    ## `My.Models.Post` vs `My.Models.Comment`, so we must check
    ## that the previous condition returned false
    r1 = !r0 && p1 == p0

    ## Check that the target bucket is the same for models
    b0 = RiakUtil.bucket(m0)
    b1 = RiakUtil.bucket(m1)
    s0 = b0 != nil && b1 != nil && b0 == b1

    ## DONE
    (r0 || r1) && s0
  end

  defp migration_modules_deduplicate!(modules, entity, target_version) do
    ## checks for any duplicates in the modules list
    ## and raises an error if so
    version_set = Enum.reduce(modules, HashSet.new, fn mod, acc ->
      try do
        HashSet.put(acc, model_version(mod))
      rescue
        UndefinedFunctionError ->
          raise MigrationModulesException,
            model: entity.model,
            entity_version: entity.riak_version,
            target_version: target_version,
            failed_module: mod,
            modules: modules
        end
      end)   
    
    if HashSet.size(version_set) != length(modules) do
      raise MigrationModulesException,
        model: entity.model,
        entity_version: entity.riak_version,
        target_version: target_version,
        modules: modules
    else
      false
    end
  end

  defp entity_prefix(mod) when is_atom(mod) do
    regex = ~r"^(.*)(\..*)$"
    res = Regex.run(regex, to_string(mod))
    if is_list(res) do
      Enum.slice(res, -2..-2)  ## get second last capture group
      |> hd
      |> RiakUtil.to_atom
    else
      nil
    end
  end

  defp entity_prefix(module) do
    ## Used to check for common module prefixes,
    ## which indicate different versions of the same model
    ##
    ## eg: Given module My.Great.Model.Ver
    ##     this function returns the list ["My", "Great", "Model"]
    components = module |> to_string |> String.split(".")
    case components do
      [first | rest] when first == "Elixir" ->
        List.delete_at(rest, -1)
      _ ->
        nil
    end
  end

  defp model_version(module) do
    if function_exported?(module, :version, 0) do
      module.version
    else
      module.version(:default)
    end
  end

  @doc """
  Sets the version of an entity. Any entities with a non-corresponding
  version will be migrated (if migration in direction is enabled)
  """
  @spec set_current_version(entity, integer) :: :ok | { :error, term }
  def set_current_version(entity, version) do
    key = RiakUtil.bucket(entity) <> "_ver"
    ETS.put(key, version)
  end
  
  @doc """
  Returns the current traget version for the entity to which migration
  should be performed. This must have been set by set_current_version/2.
  """
  @spec current_version(entity) :: integer
  def current_version(entity) do
    key = RiakUtil.bucket(entity) <> "_ver"
    ETS.get(key, 0)
  end
                                       
end