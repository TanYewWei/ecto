defmodule Ecto.Adapters.Riak.Searchtest do
  use ExUnit.Case, async: true
  
  ##require Ecto.Adapters.Riak.Search, as: Search
  import Ecto.Query
  import Ecto.Query.Util, only: [normalize: 1]
  alias Ecto.Adapters.Riak.Search
  alias Ecto.Adapters.Riak.Datetime
  alias Ecto.UnitTest.Post

  defp test_query(query, expected) do
    {{_, querystring, _}, _} = Search.query(query |> normalize)
    assert expected == querystring
  end

  test "where clause" do
    base = from(p in Post)

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

    ## like
    where(base, [p], like(p.text, "hello"))
    |> test_query "text_s:*hello*"
    
    ## DateTime and Interval
    datetime = Datetime.now_ecto_datetime()
    datetime_str = Datetime.solr_datetime(datetime)
    interval = Ecto.Interval.new(day: 1)
    query = where(base, [p], p.posted >= date_add(now(), ^interval))

    interval_str = Datetime.solr_datetime_add(interval)
    where(base, [p], p.posted > date_add(^datetime, ^interval))
    |> test_query "posted_dt:[#{datetime_str}#{interval_str} TO *]"

    interval_str = Datetime.solr_datetime_subtract(interval)
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
    base = from(p in Post)

    query = select(base, [p], p) |> normalize
    {_, post_proc} = Search.query(query)
    assert [mock_post] == post_proc.([mock_post])

    ## tuples
    query = select(base, [p], {p.title, p.text}) |> normalize
    {_, post_proc} = Search.query(query)
    assert [{"test title", "test text"}] == post_proc.([mock_post])

    ## unary operators
    query = select(base, [p], round(-4.4)) |> normalize
    {_, post_proc} = Search.query(query)
    assert [-4] == post_proc.([mock_post])

    query = select(base, [p], upcase("hello ") <> downcase("PEOPLE")) |> normalize
    {_, post_proc} = Search.query(query)
    assert ["HELLO people"] == post_proc.([mock_post])
    
    ## binary operators
    query = select(base, [p], [pow(p.count, 2), rem(p.count, 5)]) |> normalize
    {_, post_proc} = Search.query(query)
    assert [[16.0, 4]] == post_proc.([mock_post])
    
    query = select(base, [p], p.count*4 + p.count/2 + 2) |> normalize
    {_, post_proc} = Search.query(query)
    assert [20] == post_proc.([mock_post])
    
  end

  test "group_by with select" do
    base = from(p in Post)
    [p0, p1, p2, p3, p4] = posts = mock_posts()
    
    ## avg
    {_, post_proc} = group_by(base, [p], p.title)
    |> select([p], avg(p.count)) |> normalize |> Search.query
    expected = [ (p0.count),
                 (p1.count + p2.count) / 2,
                 (p3.count + p4.count) / 2 ]
    assert expected == post_proc.(posts)

    ## count
    {_, post_proc} = group_by(base, [p], p.id)
    |> select([p], count(p.id)) |> normalize |> Search.query
    assert [1,1,1,1,1] === post_proc.(posts)

    ## max
    {_, post_proc} = group_by(base, [p], p.title)
    |> select([p], max(p.count)) |> normalize |> Search.query
    expected = [ p0.count,
                 Enum.max([p1.count, p2.count]),
                 Enum.max([p3.count, p4.count]) ]
    assert expected === post_proc.(posts)

    ## min
    {_, post_proc} = group_by(base, [p], p.title)
    |> select([p], min(p.count)) |> normalize |> Search.query
    expected = [ p0.count,
                 Enum.min([p1.count, p2.count]),
                 Enum.min([p3.count, p4.count]) ]
    assert expected === post_proc.(posts)

    ## sum
    {_, post_proc} = group_by(base, [p], p.text)
    |> select([p], sum(p.count)) |> normalize |> Search.query
    expected = [ p0.count, p1.count+p2.count, p3.count, p4.count ]
    assert expected === post_proc.(posts)

    {_, post_proc} = group_by(base, [p], p.text)
    |> select([p], sum(p.rating)) |> normalize |> Search.query
    expected = [ p0.rating, p1.rating+p2.rating, p3.rating, p4.rating ]
    assert expected === post_proc.(posts)

  end

  test "group_by with having with select" do
    base = from(p in Post)
    [p0, p1, p2, p3, p4] = posts = mock_posts()
    
    ## avg
    {_, post_proc} = group_by(base, [p], p.title)
    |> having([p], avg(p.count) >= 4)
    |> select([p], count(p.id)) |> normalize |> Search.query
    expected = [ 2, 2 ]  ## groups will be [ [p0], [p1,p2], [p3,p4] ]
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
             rating: 5,
             posted: Datetime.now_ecto_datetime,
             temp: "test temp")
  end

  defp mock_comment() do
    Comment.new(id: "comment_id_0",
                bytes: <<1,2,3>>,
                posted: Datetime.now_ecto_datetime,
                post_id: "post_id_0")
  end

end