# Ecto with Riak Support

The `riak` branch of the ecto repo implements support for the Riak Adapter.

## Requirements

You will need to be on Riak 2.0, which supports CRDTs and the newly revamped Yokozuna search implementation.

## Architecture

Say we have the following models:

```elixir
defmodule Post do
  use Ecto.Model

  queryable "posts" do
    field :title, :string
    field :text, :string
    field :temp, :virtual, default: "temp"
    field :count, :integer
    has_many :comments, Comment
    has_one :permalink, Permalink
  end
end

defmodule Comment do
  use Ecto.Model

  queryable "comments" do
    field :text, :string
    field :posted, :datetime
    field :interval, :interval
    field :bytes, :binary
    belongs_to :post, Post
  end
end

defmodule Permalink do
  use Ecto.Model

  queryable "permalinks" do
    field :url, :string
    belongs_to :post, Post
  end
end
```

A post object would be transformed to a riak map in the straightforward fashion pseudo-structure.

```
{
    "id":  1,
    "title": "some title",
    "text": "some source text",
    "count": 1
}
```

A comment object would be transformed to a riak map in the pseudo-structure:

```
{
    "id": 1,
    "text": "some text",
    "posted": "2014-01-01T01:01:01Z",
    "interval":  "2014-01-01T01:01:01Z",
    "bytes": "some binary",
    "post_id": some_post_pk
}
```

Essentially, any association will require that we store the owner id on any `belongs_to` attribute

### Query

Querying will be achieved via standard gets when possible and delegate the heavy lifting to liberal use of Riak Search. This will require the version of search which comes in Riak 2.0

## Major Changes from Ecto master

1. **Added pooler dependency**

    We require a worker pool solution which manages multiple connections directly to multiple nodes in a riak cluster. (ideally all clients are connected to all nodes for maximum availability)

    Unfortunately, poolboy does not support the notion of a "pool group". [pooler](https://github.com/seth/pooler) has been introduced as an additional dependency to manage riak clusters.

2. **Repo url() callback now can return either a single string, or a list of strings**

    if the riak adapter is being used, and if a list of ecto URLs are supplied, the client should then attempt to connect to all of them. The username and password part of the URL will be ignored for now, until Riak introduces some notion of [ACLs (which are in the works)](https://github.com/basho/riak/issues/355). 

    The postgres adapter **must** only be supplied a single URL.

3. **There is no implementation for Transactions**

    Riak has no native notion of a transaction. The only way to to implement some notion of transactions would be to map over each operation, read the existing value (if any), update the value, and manually revert the value (with proper error handling).

    This is a fragile operation which has no means of generic handling semantics, and it will be left up to the developer to initiate individual operations for now.

4. **Migrations are Lazy**

    Riak has no native notion of schema migrations. Instead, migrations are run lazily as data is read and updated (this emulates the [curator](https://github.com/braintree/curator) model framework).

    Schema migrations for each model are stored within riak. A supervisor that we introduce for the Riak Adapter will poll riak to check for new versions (this operation can be explicitly executed).

    The `migrate_up/3` and `migrate_down/3` callbacks for the `Ecto.Adapter.Migrations` API simply serve to insert this schema migration information into Riak.

    Each entity must then store it's current version number, which will be used for runtime migrations.

    * Everytime an Entity is read, we check to ensure that it is of the latest version, and if not, intiate the migration process. The update WILL NOT be sent back to riak until the entity is explictly written back to Riak.

    * Everytime we attempt an Entity update, we check to see if the version is the latest, and perform migration if needed.


5. **Modification of primary_key is (currently) not allowed**

    It is recommended that you override the primary key to be a random `:string` type.

    After being assigned, the field name of the primary key cannot be changed.

## Things that would be nice

* Cache support and possibly `cacheble` annotations

    For example, we should have adapters to a datastore like redis or memcached.

    After which, it could be possible for developers to specify certain `cacheble` attributes on an Ecto Model, which will cause those attributes of derived entities to be cached when possible, and retrieved from the cache when possible.
