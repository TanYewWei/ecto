# Migration Handling in Riak

Riak is a distributed database which does not provide strong consistency.

Hence, migrations have to be dynamic, and performed as entites are read from the database.

## Implementation

1. Each model name **MUST** end with a suffix `Version#{N}`. So `My.Great.Model.Ver3` is NOT a valid module name, while `My.Great.Model.Version3` is a valid module name.

    For example, the following set of modules constitutes a valid set of Riak modules.
    
    ```
    My.Great.Model.V0
    My.Great.Model.Ver1
    My.Great.Model.Version2
    My.Great.Model.Zap
    ```

    But the following is not a valid set of Riak Modules (because `My.Great.Model` does not give us indication of a shared module prefix)

    ```
    My.Great.Model
    My.Great.Model.Ver1
    ```

    The `Ecto.Adapters.Riak.Migration` will look for a module, and any other module that shares a common prefix ("My.Great.Model" in the example above), and use the return value of the `version/0` function (see step 4) to order the modules.

    If any required module is not available during runtime, a `Ecto.Adapters.Riak.MigrationModulesException` is raised. For example, a migration from entity version 1 to version 4 of a module must have modules declared for versions 2, 3, and 4.

    Likewise, if any modules share the same version number (and is thus a duplicate), the same `Ecto.Adapters.Riak.MigrationModulesException` exception is raised.

    Dynamic reconfiguration of modules can be done using [the `Code.load_file/2` function](http://elixir-lang.org/docs/master/Code.html#load_file/2)

1. Each entity that is to be persisted with Riak should `use Ecto.RiakModel` instead of `use Ecto.Model`. This simply makes a change to the `@queryable_defaults` attribute to allow for primary keys to be a random string instead of being an integer.

1. Each entity must also implement the `Ecto.Adapters.Riak.Migration` behaviour, which has 3 required callbacks:

    ```
## returns the appropriate version number of the entity
version() :: integer

## Takes an entity with version number N-1 and migrates
## it to the version N returned by the version/0 function above
migrate_from_previous(entity) :: entity

## Takes an entity with version number N+1 and migrates
## it to the version N returned by the version/0 function above
migrate_from_newer(entity) :: entity
    ```

    It will then be the developer's responsibility to implement these functions accordingly

1. It is recommended that the developer validates with `RiakUtil.entity_validate/1` when working with Riak Entities. This is distinct from the `validate/1` macro (see the [Validations documentation](http://elixir-lang.org/docs/ecto/Ecto.Model.Validations.html) for details).

---

Here is an example of two versions of the same model

```elixir
defmodule My.Great.Model.Version1 do
  @behaviour Ecto.Adapters.Riak.Migration

  use Ecto.RiakModel
  alias Ecto.Adapters.Riak.Util, as: RiakUtil

  queryable "models" do
    field :version, :integer, default: 2
    field :hello,   :string

    ## Default validation
    validate model,
      version: present(message: "must have version") and greater_than(0),
      id: present(message: "ID should be a unique string")
  end

  def version(), do: 1

  def migrate_from_previous(_) do
    ## There should never be a previous version
    raise "What?!"
  end

  def migrate_from_newer(entity) do
    ## Called during migration from version 2 to 1

    ## Call a Riak Adapter utility function to extract entity fields
    ## as a keyword: [ id: "hello", version: 1, ... ]
    attr = RiakUtil.entity_keyword(entity)
      |> Keyword.put(:version, version)

    ## Map back the :hello field
    attr = Keyword.put(attr, :hello, entity.world)

    ## Generate new entity
    __MODULE__.new(attr)
  end
end
```

```elixir
defmodule My.Great.Model.Version2 do
  @behaviour Ecto.Adapters.Riak.Migration

  use Ecto.RiakModel
  alias Ecto.Adapters.Riak.Util, as: RiakUtil

  queryable "models" do
    field :version,   :integer, default: 2
    field :world,     :string
    field :some_list, { :list, :string }

    ## Validations
    validate model,
      version: present(message: "must have version") and greater_than(1),
      id: present(message: "must have globally unique string as ID"),
      world: has_format(%r/Happy/, message: "why aren't you happy!!"),
      also:  validate_some_list

    validatep validate_some_list(x),
      some_list: present(message: "give me a list!")
  end

  def version(), do: 2

  def migrate_from_previous(entity) do
    ## Called during migration upgrade from version 1 to 2
    attr = RiakUtil.entity_keyword(entity)
      |> Keyword.put(:version, version)

    ## Perform arbitrary transformation on the original :hello field
    attr = Keyword.put(attr, :world, "#{entity.hello}, Weeee!")
    __MODULE__.new(attr)
  end

  def migrate_from_newer(entity) do
    ## Called during migration downgrade from version 2 to 1
    attr = RiakUtil.entity_keyword(entity)
    
    ## Make your needed changes ...
    
    __MODULE__.new(attr)
  end

end
```

## Migration Process

A migration is attempted every time an entity is read from the database.
