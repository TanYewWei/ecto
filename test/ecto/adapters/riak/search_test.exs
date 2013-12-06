defmodule Ecto.Adapters.Riak.Searchtest do
  use ExUnit.Case, async: true
  
  ##require Ecto.Adapters.Riak.Search, as: Search
  alias Ecto.Adapters.Riak.Search

  test "function expr" do
    input = {:==, [], [nil, 1]}
    fun = Search.fexpr(input, [])
    assert !fun.()

    ##input = {:&, [], [0]}
    ##assert :ok == Search.fexpr(input, [])
  end

  test "expr - x in y" do
    ## x.state in 1..5
    input = {:in, [], [{{:., [], [{:&, [], [0]}, :count]}, [], []}, {:.., [], [1, 5]}]}
    sources = {{{"posts", "p0"}, Ecto.UnitTest.Post.Entity, Ecto.UnitTest.Post.Model}}
    assert "count_i:(1 2 3 4 5)" == Search.expr(input, sources)

    input = {:in, [], [1, [1,2,3]]}
    ##sources = {{{"model", "m0"}, Ecto.Adapters.Postgres.SQLTest.Model.Entity, Ecto.Adapters.Postgres.SQLTest.Model}}
    assert "1:(1 2 3)" == Search.expr(input, [])
  end

  test "expr - ranges" do
  end

end