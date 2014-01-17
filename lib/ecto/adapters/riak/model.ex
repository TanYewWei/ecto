defmodule Ecto.RiakModel do
  @moduledoc """
  When using the Riak Adapter, we require:

  * globally unique ids - these will be unique strings
  * A `riak_version` field - an integer version used for dynamic migrations
  * A `riak_vclock` field - a binary used for conflict resolution
    of riak siblings
  * A `riak_context` field - to 
  * migration callbacks defined by the `Ecto.Adapters.Riak.Migration` module

  Using `Ecto.RiakModel` instead of `Ecto.Model` ensures
  that these constraints is enforced.
  """

  defmacro __using__(_) do
    quote do
      @queryable_defaults [
        primary_key: { :id, :string, [] },
        foreign_key_type: :string,
        default_fields: [ { :riak_version, :integer, [default: 0] },
                          { :riak_vclock, :virtual, [] },
                          { :riak_context, :virtual, [default: []] } ] ]
      
      use Ecto.RiakModel.Queryable
      use Ecto.Model.Validations
      import Ecto.Adapters.Riak.Validators
      
      def version(:default), do: 0
      def migrate_from_previous(x, :default), do: x
      def migrate_from_newer(x, :default), do: x
    end
  end
  
end