# Migration Handling in Riak

Riak is a distributed database which does not provide strong consistency.

Hence, migrations have to be dynamic, and performed as entites are read from the database.

## Implementation

1. Each new successive version of a model must share the same module prefix as previous versions.

    For example, the following set of modules constitutes a valid set of Ecto.RiakModel modules.
    
    ```elixir
    My.Great.Model
    My.Great.Model.Ver1
    My.Great.Model.Version2
    My.Great.Model.Zap
    ```

    In this case, the `My.Great.Model` prefix is common to all modules.

    The `Ecto.Adapters.Riak.Migration` will look for a module, and any other module that shares a common prefix, and use the return value of the `version/0` function (see step 3) to order the modules.

    If any required module is not available during runtime, a `Ecto.Adapters.Riak.MigrationModulesException` is raised. For example, a migration from entity version 1 to version 4 of a module must have modules declared for versions 2, 3, and 4.

    Likewise, if any modules share the same version number (and is thus a duplicate), the same `Ecto.Adapters.Riak.MigrationModulesException` exception is raised.

    Dynamic reconfiguration of modules can be done using [the `Code.load_file/2` function](http://elixir-lang.org/docs/master/Code.html#load_file/2)

2. Each entity that is to be persisted with Riak should `use Ecto.RiakModel` instead of `use Ecto.Model`. This simply makes a change to the `@queryable_defaults` attribute to allow for primary keys to be a random string instead of being an integer.

3. Each entity must also implement the `Ecto.Adapters.Riak.Migration` behaviour, which has 3 required callbacks:

    ```elixir
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

---

## Example

Here is an example of two versions of the same model

```elixir
defmodule Model.Post do
  @behaviour Ecto.Adapters.Riak.Migration

  use Ecto.RiakModel
  alias Ecto.Adapters.Riak.Util, as: RiakUtil

  ## queryable source determines the bucket to put to in riak.
  ## this should be a unique namespace for each model.
  queryable "model.posts" do
    field :title, :string
    field :hello, :string
    has_many :comments, Model.Comment
    has_one :permalink, Model.Permalink
    field :riak_version, :integer, default: 1

    ## Default validation
    ## checks for :id and :riak_version fields
    riak_validate model
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

    ## Map back the :hello field
    attr = Keyword.put(attr, :hello, entity.world)

    ## Generate new entity
    __MODULE__.new(attr)
  end
end
```

```elixir
defmodule Model.Post.Version2 do
  @behaviour Ecto.Adapters.Riak.Migration

  use Ecto.RiakModel
  alias Ecto.Adapters.Riak.Util, as: RiakUtil

  queryable "model.posts" do
    field :title, :string
    filed :world, :string
    field :some_list, { :list, :string }
    has_many :comments, Model.Comment
    has_one :permalink, Model.Permalink
    field :riak_version, :integer, default: 2

    ## Checks for :version and :id fields
    ## along with whatever other fields are provided through
    ## a keyword list. Note that 
    riak_validate model,
      world: has_format(%r/Happy/, message: "why aren't you happy!!"),
      also:  validate_some_list

    validatep validate_some_list(x),
      some_list: present(message: "give me a list!")
  end

  def version(), do: 2

  def migrate_from_previous(entity) do
    ## Called during migration upgrade from version 1 to 2
    attr = RiakUtil.entity_keyword(entity)

    ## Perform arbitrary transformation on previous entity version fields
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

```elixir
defmodule Model.Comment do
  use Ecto.RiakModel

  queryable "model.comments" do
    field :text, :string
    field :posted, :datetime
    field :interval, :interval
    field :bytes, :binary
    field :riak_version, :integer, default: 0
    belongs_to :post, Model.Post
  end

  def version(), do: 0
  def migrate_from_previous(x), do: x
  def migrate_from_newer(x), do: x
end
```

```elixir
defmodule Model.Permalink do
  use Ecto.RiakModel

  queryable "model.permalinks" do
    field :url, :string
    field :riak_version, :integer, default: 0
    belongs_to :post, Ecto.Test.Riak.Post
  end

  def version(), do: 0
  def migrate_from_previous(x), do: x
  def migrate_from_newer(x), do: x
end
```

```elixir
defmodule TestRepo do
  use Ecto.Repo, adapter: Ecto.Adapters.Riak

  def priv do
    "integration_test/riak/ecto/priv"
  end

  def url do
    [ "ecto://test@127.0.0.1:8000/test?max_count=10&init_count=1",
      "ecto://test@127.0.0.1:8001/test?max_count=10&init_count=1",
      "ecto://test@127.0.0.1:8002/test?max_count=10&init_count=1" ]
  end

  def query_apis do
    ## only default is allowed
    [ Ecto.Query.API ]
  end
end
```

## Migration Process

A migration is attempted every time an entity is read from the database.

```elixir
{ :ok, _ } = TestRepo.start_link

## Create a new entity and save it to Riak
m0 = TestRepo.create(Model.Post.Entity[title: "hello", hello: "world"])

## Set new version for model
:ok = Ecto.Adapters.Riak.Migration.set_current_version(m0, 2)

## Read model, which should have been migrated over
m1 = TestRepo.get(Model.Post, m0.primary_key)
2 = m1.riak_version
m0.hello = m1.world

```
