defexception Ecto.Adapters.Riak.MigrationModulesException,
  [:expected_modules, :failed_module, :model, :entity_version, :target_version] do
  @moduledoc """
  Exception raised when there was an error in loading
  all required modules to migrate an entity to a specified version
  """
  def message(e) do
    modules = Enum.map(e.expected_modules, &("* #{&1}\n"))

    """
    Failed attempt to migrate entity #{e.model} from version {e.entity_version} to version #{e.target_version}. One of the following modules is missing or has errors:

    #{modules}
    """
  end
end