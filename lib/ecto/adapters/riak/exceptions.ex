defexception Ecto.Adapters.Riak.MigrationModulesException,
  [:modules, :failed_module, :model, :entity_version, :target_version] do
  @moduledoc """
  Exception raised when there was an error in loading
  all required modules to migrate an entity to a specified version
  """
  def message(e) do
    modules = Enum.map(e.modules, &("* #{&1}\n"))

    """
    Failed attempt to migrate entity #{e.model} from version #{e.entity_version} to version #{e.target_version}. One of the following modules is missing or has errors:

    #{modules}

    Failed module (if applicable): #{e.failed_module}
    """  
  end
end

defexception Ecto.Adapters.Riak.AdapterStartError, [:message]

defexception Ecto.Adapters.Riak.DatatypeError, [:message]

defexception Ecto.Adapters.Riak.RequiredFieldUndefinedError,
  [:field, :entity] do
  @moduledoc """
  Using `Ecto.RiakModel`s requires that certain fields be
  defined for use with Migrations and Conflict resolution.
  This exception is raised if any of those fields are not set.
  """
  def message(e) do
  end
end