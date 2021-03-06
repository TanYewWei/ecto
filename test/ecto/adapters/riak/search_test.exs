Code.require_file "test_helper.exs", __DIR__

defmodule Ecto.Adapters.Riak.SearchTest do
  use ExUnit.Case, async: true
  
  ##require Ecto.Adapters.Riak.Search, as: Search
  import Ecto.Query
  import Ecto.Query.Normalizer, only: [normalize: 1]
  alias Ecto.Adapters.Riak.Search
  alias Ecto.Adapters.Riak.DateTime
  alias Ecto.Test.Riak.Post
  alias Ecto.Test.Riak.Comment
  
  defp test_query(query, expected, opts \\ []) do
    { { _, _, querystring, _ }, _ } = Search.query(query |> normalize, opts)
    assert expected == querystring
  end

  test "unsupported operations" do
    query = Post |> join(:inner, [p], c in Comment, c.post_id == p.id) |> normalize
    assert_raise Ecto.QueryError, fn -> Search.query(query, []) end

    query = Post |> distinct([c], c.post_id) |> normalize
    assert_raise Ecto.QueryError, fn -> Search.query(query, []) end

    query = Post |> lock(true) |> normalize
    assert_raise Ecto.QueryError, fn -> Search.query(query, []) end
  end

  test "from" do
    query = from(p in Post, []) |> normalize
    { { _ , _, querystring, _ }, _ } = Search.query(query, [])
    assert "*:*" == querystring
  end

  test "where clause" do
    base = Post

    ## basic
    where(base, [p], nil)
    |> test_query "*"

    ## == and !=
    where(base, [c], c.title == "Sweden" and c.text != nil and c.count == nil)
    |> test_query "((title_s:Sweden) AND (text_s:*)) AND -count_i:*"

    ## <=, >=, <, and >
    where(base, [p], (p.count > 8 and p.count > 1 and p.count <= 7) or p.count >= -2)
    |> test_query "(((count_i:[9 TO *]) AND (count_i:[2 TO *])) AND (count_i:[* TO 7])) OR (count_i:[-2 TO *])"

    insert_var = 10
    where(base, [p], p.count == ^insert_var)
    |> test_query "count_i:10"
    
    ## x in Range
    where(base, [p], p.count in 1..5 and nil != p.title)
    |> test_query "count_i:(1 2 3 4 5) AND (title_s:*)"

    ## arrays
    insert_var = 5
    where(base, [p], p.count in array([1, ^insert_var, 3], ^:integer))
    |> test_query "count_i:(1 #{insert_var} 3)"

    where(base, [p], p.title in array(~w(abc def), ^:string))
    |> test_query "title_s:(abc def)"

    ## like
    where(base, [p], like(p.text, "hello"))
    |> test_query "text_s:*hello*"
    
    ## DateTime and Interval
    datetime = DateTime.now_ecto()
    datetime_str = DateTime.solr_datetime(datetime)
    interval = Ecto.Interval.new(day: 1)
    ##query = where(base, [p], p.posted >= date_add(now(), ^interval))

    interval_str = DateTime.solr_datetime_add(interval)
    where(base, [p], p.posted > date_add(^datetime, ^interval))
    |> test_query "posted_dt:[#{datetime_str}#{interval_str} TO *]"

    interval_str = DateTime.solr_datetime_subtract(interval)
    where(base, [p], p.posted < date_sub(^datetime, ^interval))
    |> test_query "posted_dt:[* TO #{datetime_str}#{interval_str}]"

    where(base, [p], p.posted == ^datetime)
    |> test_query "posted_dt:#{datetime_str}"

    where(base, [p], p.posted != ^datetime)
    |> test_query "-posted_dt:#{datetime_str}"

    ## Self-referencing datetime where queries not allowed
    query = where(base, [p], p.posted != date_add(p.posted, ^interval)) |> normalize
    assert_raise Ecto.QueryError, fn()-> Search.query(query) end
  end

  test "select clause" do
    base = Post
    post = mock_post

    query = select(base, [p], p) |> normalize
    { _, post_proc } = Search.query(query)
    assert [mock_post] == post_proc.([post])

    ## tuples
    query = select(base, [p], {p.title, p.text}) |> normalize
    { _, post_proc } = Search.query(query)
    assert [{ "test title", "test text" }] == post_proc.([post])

    ## unary operators
    query = select(base, [p], round(-4.4)) |> normalize
    { _, post_proc } = Search.query(query)
    assert [-4] == post_proc.([post])

    query = select(base, [p], upcase("hello ") <> downcase("PEOPLE")) |> normalize
    { _, post_proc } = Search.query(query)
    assert ["HELLO people"] == post_proc.([post])
    
    ## binary operators
    query = select(base, [p], [pow(p.count, 2), rem(p.count, 5)]) |> normalize
    { _, post_proc } = Search.query(query)
    assert [ [16.0, 4] ] == post_proc.([post])
    
    query = select(base, [p], p.count*4 + p.count/2 + 2) |> normalize
    { _, post_proc } = Search.query(query)
    assert [20] == post_proc.([post])

    query = select(base, [p], [p.count*4] ++ [p.rating]) |> normalize
    { _, post_proc } = Search.query(query)
    assert [[post.count*4, post.rating]] == post_proc.([post])

    query = select(base, [p], p.title <> p.text) |> normalize
    { _, post_proc } = Search.query(query)
    assert [post.title <> post.text] == post_proc.([post])

    query = select(base, [p], round(p.rating, 1)) |> normalize
    { _, post_proc } = Search.query(query)
    assert [Float.round(post.rating, 1)] == post_proc.([post])
    
  end

  test "custom functions in select are ignored" do
    base = Post
    post = mock_post

    query = base |> select([p], custom(p.id)) |> normalize
    { _, post_proc } = Search.query(query)
    assert_raise Ecto.QueryError, fn()-> post_proc.([post]) end
  end

  test "group_by with select" do
    base = Post
    [p0, p1, p2, p3, p4] = posts = mock_posts()
    
    ## avg
    { _, post_proc } = group_by(base, [p], p.title)
      |> select([p], avg(p.count)) |> normalize |> Search.query
    expected = [ (p0.count),
                 (p1.count + p2.count) / 2,
                 (p3.count + p4.count) / 2 ]
    assert expected == post_proc.(posts)

    ## count
    { _, post_proc } = group_by(base, [p], p.id)
      |> select([p], count(p.id)) |> normalize |> Search.query
    assert [1,1,1,1,1] === post_proc.(posts)

    ## max
    { _, post_proc } = group_by(base, [p], p.title)
      |> select([p], max(p.count)) |> normalize |> Search.query
    expected = [ p0.count,
                 Enum.max([p1.count, p2.count]),
                 Enum.max([p3.count, p4.count]) ]
    assert expected === post_proc.(posts)

    ## min
    { _, post_proc } = group_by(base, [p], p.title)
      |> select([p], min(p.count)) |> normalize |> Search.query
    expected = [ p0.count,
                 Enum.min([p1.count, p2.count]),
                 Enum.min([p3.count, p4.count]) ]
    assert expected === post_proc.(posts)

    ## sum
    { _, post_proc } = group_by(base, [p], p.text)
      |> select([p], sum(p.count)) |> normalize |> Search.query
    expected = [ p0.count, p1.count+p2.count, p3.count, p4.count ]
    assert expected === post_proc.(posts)

    { _, post_proc } = group_by(base, [p], p.text)
      |> select([p], sum(p.rating)) |> normalize |> Search.query
    expected = [ p0.rating, p1.rating+p2.rating, p3.rating, p4.rating ]
    assert expected === post_proc.(posts)

  end

  test "group_by with having with select" do
    base = Post
    [_, p1, p2, p3, p4] = posts = mock_posts()
    
    ## - avg aggregate function
    ## - having binary operators:
    ##   [ pow, rem, +, -, /, *, <>, ++, date_add, date_sub ]

    { _, post_proc } = group_by(base, [p], p.title) ## groups will be [ [p0], [p1,p2], [p3,p4] ]
      |> having([p], avg(p.count) - 1 >= 3)
      |> select([p], count(p.id)) |> normalize |> Search.query
    expected = [1, 2]
    assert expected == post_proc.(posts)

    { _, post_proc } = group_by(base, [p], p.title)
      |> having([p], avg(p.count) + 1 <= 5 * 2)
      |> select([p], count(p.id)) |> normalize |> Search.query
    expected = [1, 2, 2]
    assert expected == post_proc.(posts)

    { _, post_proc } = group_by(base, [p], p.title)
      |> having([p], avg(p.count) == 8/2)
      |> select([p], count(p.id)) |> normalize |> Search.query
    expected = [1, 2]
    assert expected == post_proc.(posts)

    { _, post_proc } = group_by(base, [p], p.title)
      |> having([p], avg(p.count) != 4)
      |> select([p], count(p.id)) |> normalize |> Search.query
    expected = [2]
    assert expected == post_proc.(posts)    

    ## count aggregate function
    { _, post_proc } = group_by(base, [p], p.title)
      |> having([p], count(p.id) > 1)
      |> select([p], p.id) |> normalize |> Search.query
    [r0, r1] = post_proc.(posts)
    assert p1.id in r0 && p2.id in r0
    assert p3.id in r1 && p4.id in r1

    ## max, min, and sum
    { _, post_proc } = group_by(base, [p], p.title)
      |> having([p], max(p.count) > 3)
      |> select([p], count(p.id)) |> normalize |> Search.query
    expected = [1, 2]
    assert expected == post_proc.(posts)

    { _, post_proc } = group_by(base, [p], p.title)
      |> having([p], min(p.count) < 3)
      |> select([p], count(p.id)) |> normalize |> Search.query
    expected = [2, 2]
    assert expected == post_proc.(posts)

    { _, post_proc } = group_by(base, [p], p.title)
      |> having([p], sum(p.count) > avg(p.count))
      |> select([p], count(p.id)) |> normalize |> Search.query
    expected = [2, 2]
    assert expected == post_proc.(posts)
    
    ## multiple clauses
    { _, post_proc } = group_by(base, [p], p.title)
      |> having([p], sum(p.count) > avg(p.count))
      |> having([p], min(p.count) < 3)
      |> select([p], count(p.id)) |> normalize |> Search.query
    expected = [2, 2]
    assert expected == post_proc.(posts)

    ## having without group_by
    { _, post_proc } = having(base, [p], count(p.id) > 1)
      |> select([p], p.id) |> normalize |> Search.query
    expected = Enum.map(posts, &(&1.id)) |> Enum.sort
    assert expected == post_proc.(posts) |> Enum.sort
  end

  test "order_by" do
    base = Post
    [p0, p1, p2, p3, p4] = posts = mock_posts()

    { _, post_proc } = order_by(base, [p], p.title)
      |> select([p], p.id) |> normalize |> Search.query
    expected = Enum.map(posts, &(&1.id))
    assert expected == post_proc.(posts)

    { _, post_proc } = order_by(base, [p], [desc: p.title])
      |> select([p], p.id) |> normalize |> Search.query
    expected = [p3.id, p4.id, p1.id, p2.id, p0.id]
    assert expected == post_proc.(posts)

    ## Multiple order_bys
    ## order should be:
    ## - p3 => count = 2, title = "test title 2"
    ## - p2 => count = 2, title = "test title 1"
    ## - p4 => count = 3, title = "test title 2"
    ## - p0 => count = 4, title = "test title"
    ## - p1 => count = 6, title = "test title 1"
    { _, post_proc } = order_by(base, [p], [asc: p.count, desc: p.title])
      |> select([p], p.id) |> normalize |> Search.query
    expected = [p3.id, p2.id, p4.id, p0.id, p1.id]
    assert expected == post_proc.(posts)
  end
  
  defp mock_posts() do
    [ mock_post,
      mock_post.update(id: "post_id_1", 
                       title: "test title 1",
                       text: "test text 1",
                       rating: 4.88,
                       count: 6),
      mock_post.update(id: "post_id_2",
                       title: "test title 1",
                       text: "test text 1",
                       rating: 3.5,
                       count: 2),
      mock_post.update(id: "post_id_3",
                       title: "test title 2",
                       text: "test text 2",
                       rating: 4.5,
                       count: 2),
      mock_post.update(id: "post_id_4",
                       title: "test title 2",
                       text: "test text 3",
                       rating: 3.9234,
                       count: 3) ]
  end

  defp mock_post() do
    Post.new(id: "post_id_0",
             title: "test title",
             text: "test text",
             count: 4,
             rating: 4.9999,
             posted: DateTime.now_ecto,
             temp: "test temp")
  end

end