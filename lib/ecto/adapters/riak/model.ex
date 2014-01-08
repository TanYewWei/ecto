defmodule Ecto.RiakModel do
  @moduledoc """
  When using the Riak Adapter, we require:

  * globally unique ids - these will be unique strings
  * A `__riak_version__` field - an integer version used for dynamic migrations
  * A `__riak_context__` field - a Keyword list of field to a hash of its values.
    used in conflict resolution to determine if an entity has been updated since
    the last get, and to attach the appropriate information for future conflict
    resolution work (done on read of entity)
  * default migration functions

  Using Ecto.RiakModel instead of Ecto.Model ensures
  that these constraints is enforced.
  """

  defmacro __using__(_) do
    quote do
      @queryable_defaults [
        primary_key: { :id, :string, [] },
        foreign_key_type: :string,
        default_fields: [ { :riak_version, :integer, default: 0, overridable?: true },
                          { :riak_vclock, :virtual, [] },
                          { :riak_context, :virtual, default: [] } ] ]
      
      use Ecto.Model
      import Ecto.Adapters.Riak.Validators
      
      def version(:default), do: 0
      def migrate_from_previous(x, :default), do: x
      def migrate_from_newer(x, :default), do: x
    end
  end
  
end