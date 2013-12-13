# Ecto with Riak Support

The `riak` branch of the ecto repo implements support for the Riak Adapter.

## Requirements

At present, you will need either:

* Riak 1.4 with Yokozuna installed
* Riak 2.0pre5

The current implementation uses regular Riak objects with statebox. Eventually, we will look to use CRDTs, which are only going to be available with Riak 2.0 and higher.

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

## Major Differences from Ecto master

1. **Added pooler dependency**

    We require a worker pool solution which manages multiple connections directly to multiple nodes in a riak cluster. (ideally all clients are connected to all nodes for maximum availability)

    Unfortunately, poolboy does not support the notion of a "pool group". [pooler](https://github.com/seth/pooler) has been introduced as an additional dependency to manage riak clusters.

2. **Repo url() callback now can return either a single string, or a list of strings**

    if the riak adapter is being used, and if a list of ecto URLs are supplied, the client should then attempt to connect to all of them. The username and password part of the URL will be ignored for now, until Riak introduces some notion of [ACLs (which are in the works)](https://github.com/basho/riak/issues/355). 

    The postgres adapter **must** only be supplied a single URL.

3. **JOINs are not allowed in queries**

    This runs extremely counter to Riak's semantics, and has not been implemented at present. Even if an implementation is possible, it may not be very efficient.

4. **There is no implementation for Transactions**

    Riak has no native notion of a transaction. The only way to to implement some notion of transactions would be to map over each operation, read the existing value (if any), update the value, and manually revert the value (with proper error handling).

    This is a fragile operation which has no means of generic handling semantics, and it will be left up to the developer to initiate individual operations for now.

5. **Migrations are Lazy**

    Riak has no native notion of schema migrations. Instead, migrations are run lazily as data is read and updated.
    See the (Migrations Documentation)["/lib/ecto/adapters/riak/migrations.md"] for more details

## TODO

* We depend on a single ETS table to track migration information. Ideally, this should have one ETS table per repo
