# Ecto with Riak Support

The Riak Adapter implements a subset of the Ecto API, and allows interfacing with a riak cluster.

You should consider this **ALPHA** software until Riak 2.0 is officially released, where we can build the adapter with stable assumptions on how Riak handles search indexes and bucket types.

## Requirements

At present, you will need either:

* Riak 1.4 with Yokozuna 0.11 installed
* Riak 2.0pre5

Note that this WILL NOT work with Yokozuna 0.12 and above. (Riak 2.0pre5 comes with Yokozuna 0.11)

## Usage

See the examples in the [Migration Documentation][migration] for full usage examples.

## Query

Query follows the same semantics as the Postgres adapter. With the follow exceptions:

* **JOINs are not supported**

    This runs extremely counter to Riak's semantics, and has not been implemented at present. Even if an implementation is possible, it may not be very efficient.

* **Transactions are not supported**

     Riak has no native notion of a transaction. The only way to to implement some notion of transactions would be to map over each operation, read the existing value (if any), update the value, and manually revert the value (with proper error handling).

    This is a fragile operation which has no means of generic handling semantics, and it will be left up to the developer to initiate individual operations for now.

* **Only uses Ecto.Query.API**

    The Riak Adapter will ignore additional `query_api`s other than Ecto.Query.API

* **Limited Query API support**

    The list of UNSUPPORTED functions are:
    
    * ilike

## Migrations

Riak has no native notion of schema migrations. Instead, migrations are run lazily as data is read and updated.

See the [Migrations Documentation for details](/lib/ecto/adapters/riak/migrations.md).

## Other Major Differences from Ecto master

1. **Added pooler dependency**

    We require a worker pool solution which manages multiple connections directly to multiple nodes in a riak cluster. (ideally all clients are connected to all nodes for maximum availability)

    Unfortunately, poolboy does not support the notion of a "pool group". [pooler](https://github.com/seth/pooler) has been introduced as an additional dependency to manage riak clusters.

2. **Repo url() callback now can return either a single string, or a list of strings**

    if the riak adapter is being used, and if a list of ecto URLs are supplied, the client should then attempt to connect to all of them. The username and password part of the URL will be ignored for now. (Potentially until Riak introduces some notion of [ACLs](https://github.com/basho/riak/issues/355)).

8. **repo update_all/3 callback does not allow update expression**

    ```elixir
    ## Not permitted
    SomeRepo.update_all(p in Post, count: p.count + 41)

    ## Permitted
    SomeRepo.update_all(from(p in Post), count: 3)
    
    ```

## TODO

* Allow for migration config file specifying versions of various models


[migration]: /lib/ecto/adapters/migrations.md
