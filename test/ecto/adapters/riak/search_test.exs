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

  test "where clause" do
    base = from(p in Post)

    ## == and !=
    query = where(base, [c], c.title == "Sweden" and c.text != nil and c.count == nil) |> normalize
    {{querystring, _}, _} = Search.query(query)
    assert "((title_s:Sweden) AND (text_s:*)) AND -count_i:*" == querystring

    ## <=, >=, <, and >
    query = where(base, [p], (p.count > 8 and p.count > 1 and p.count <= 7) or p.count >= -2) |> normalize
    {{querystring, _}, _} = Search.query(query)
    assert "(((count_i:[9 TO *]) AND (count_i:[2 TO *])) AND (count_i:[* TO 7])) OR (count_i:[-2 TO *])" == querystring

    insert_var = 10
    query = where(base, [p], p.count == ^insert_var) |> normalize
    {{querystring, _}, _} = Search.query(query)
    assert "count_i:10" == querystring
    
    ## x in Range
    query = where(base, [p], p.count in 1..5 and nil != p.title) |> normalize
    {{querystring, _}, _} = Search.query(query)
    assert "count_i:(1 2 3 4 5) AND (title_s:*)" == querystring

    ## like
    query = where(base, [p], like(p.text, "hello")) |> normalize
    {{querystring, _}, _} = Search.query(query)
    assert "text_s:*hello*" == querystring

    ## disallowed math operators
    query = where(base, [p], pow(p.count, 2) == rem(p.count, 3) == p.count / 4) |> normalize
    assert_raise Ecto.QueryError, fn()-> Search.query(query) end
  end

  test "select clause" do
    base = from(p in Post)

    ## tuples
    query = select(base, [c], {c.title, c.text}) |> normalize
    {_, post_proc} = Search.query(query)
    assert [{"test title", "test text"}] == post_proc.([mock_post])
    
    ## operators
    query = select(base, [c], pow(c.count, 2)) |> normalize
    ##IO.puts(query.select.expr)
    {_, post_proc} = Search.query(query)
    assert [16.0] == post_proc.([mock_post])
  end

  defp mock_post() do
    Post.new(id: "post_id_0",
             title: "test title",
             text: "test text",
             count: 4,
             temp: "test temp")
  end

  defp mock_comment() do
    Comment.new(id: "comment_id_0",
                bytes: <<1,2,3>>,
                post_id: "post_id_0")
  end

  defp current_datetime() do    
    Ecto.DateTime.new(year: 2013)
  end

end