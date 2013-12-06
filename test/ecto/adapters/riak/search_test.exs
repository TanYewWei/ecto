defmodule Ecto.Adapters.Riak.Searchtest do
  use ExUnit.Case, async: true
  
  ##require Ecto.Adapters.Riak.Search, as: Search
  import Ecto.Query
  import Ecto.Query.Util, only: [normalize: 1]
  alias Ecto.Adapters.Riak.Search
  alias Ecto.UnitTest.Post

  test "function expr" do
    input = {:==, [], [nil, 1]}
    fun = Search.fexpr(input, [])
    assert !fun.()

    ##input = {:&, [], [0]}
    ##assert :ok == Search.fexpr(input, [])
  end

  test "where expr" do
    ## == and !=
    query = (from(p in Post)
             |> where([c], c.title == "Sweden" and c.text != nil and c.count == nil)
             |> normalize)
    {{querystring, options}, post_proc} = Search.query(query)
    assert "((title_s:Sweden) AND (text_s:*)) AND -count_i:*" == querystring

    ## <=, >=, <, and >
    query = (from(p in Post)
             |> where([p], (p.count > 8 and p.count > 1 and p.count <= 7) or p.count >= 2)
             |> normalize)
    {{querystring, options}, post_proc} = Search.query(query)
    assert "(((count_i:[9 TO *]) AND (count_i:[2 TO *])) AND (count_i:[* TO 7])) OR (count_i:[2 TO *])" == querystring

    ## x in Range
    query = (from(p in Post)
             |> where([p], p.count in 1..5 and nil != p.title)
             |> normalize)
    {{querystring, options}, post_proc} = Search.query(query)
    assert "count_i:(1 2 3 4 5) AND (title_s:*)" == querystring
  end

end