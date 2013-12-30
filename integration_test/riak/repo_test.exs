defmodule Ecto.Integration.Riak.RepoTest do
  use Ecto.Integration.Riak.Case, async: true
  import Ecto.Integration.Riak.Util
  alias Ecto.Associations.Preloader

  teardown_all do
    delete_all_test_data
    { :ok, [] }
  end

  test "custom functions are ignored" do
    assert_raise Ecto.QueryError, fn()-> TestRepo.all(from p in Post, select: custom(p.id)) end
  end

  test "already started" do
    assert { :error, { :already_started, _ } } = TestRepo.start_link
  end

  test "create and fetch single" do
    assert Post.Entity[id: id] = TestRepo.create(Post.Entity[title: "The shiny new Ecto", text: "coming soon..."])
    assert valid_id?(id)
        
    wait_assert [Post.Entity[id: ^id, title: "The shiny new Ecto", text: "coming soon..."]] =
      TestRepo.all(from(p in Post, where: p.id == ^id))
  end

  test "fetch without entity" do
    Post.Entity[id: id] = TestRepo.create(Post.Entity[title: "title1"])
    Post.Entity[] = TestRepo.create(Post.Entity[title: "title2"])

    wait_assert ["title1", "title2"] =
      TestRepo.all(from(p in "posts", where: like(p.title, "title"), order_by: p.title, select: p.title))
    wait_assert [^id] =
      TestRepo.all(from(p in "posts", where: p.title == "title1", select: p.id))
  end

  test "create and delete single, fetch nothing" do
    post = Post.Entity[title: "The shiny new Ecto", text: "coming soon..."]

    assert Post.Entity[] = created = TestRepo.create(post)
    assert :ok == TestRepo.delete(created)

    wait_assert [] = TestRepo.all(from(p in Post, where: p.id == ^post.id))
  end

  test "create and delete single, fetch empty" do
    post = Post.Entity[title: "The shiny new Ecto", text: "coming soon..."]

    assert Post.Entity[] = c0 = TestRepo.create(post)
    assert Post.Entity[] = c1 = TestRepo.create(post)
    assert :ok == TestRepo.delete(c1)

    wait_assert [Post.Entity[]] = TestRepo.all(from p in Post, where: p.id == ^c0.id or p.id == ^c1.id)
  end

  test "create and update single, fetch updated" do
    post = Post.Entity[title: "The shiny new Ecto", text: "coming soon..."]

    post = TestRepo.create(post)
    post = post.text("coming very soon...")
    assert :ok == TestRepo.update(post)

    wait_assert [Post.Entity[text: "coming very soon..."]] = TestRepo.all(from p in Post, where: p.id == ^post.id)
  end

  test "create and fetch multiple" do
    assert Post.Entity[] = p0 = TestRepo.create(Post.Entity[title: "1", text: "hai"])
    assert Post.Entity[] = p1 = TestRepo.create(Post.Entity[title: "2", text: "hai"])
    assert Post.Entity[] = p2 = TestRepo.create(Post.Entity[title: "3", text: "hai"])

    post_ids = Enum.map([p0, p1, p2], &(&1.id))

    wait_assert [Post.Entity[title: "1"], Post.Entity[title: "2"], Post.Entity[title: "3"]] =
      TestRepo.all(from p in Post, where: p.id in ^post_ids, order_by: p.title)

    wait_assert [Post.Entity[title: "2"]] =
      TestRepo.all(from p in Post, where: p.id == ^p1.id and p.title == "2")
  end

  test "get entity" do
    post1 = TestRepo.create(Post.Entity[title: "1", text: "hai"])
    post2 = TestRepo.create(Post.Entity[title: "2", text: "hai"])

    wait_assert post1 == TestRepo.get(Post, post1.id)
    wait_assert post2 == TestRepo.get(Post, post2.id)
    wait_assert nil == TestRepo.get(Post, "bleh")
  end

  test "get entity with custom primary key" do
    c0 = TestRepo.create(Custom.Entity[foo: "1"])
    c1 = TestRepo.create(Custom.Entity[foo: "2"])

    wait_assert c0 == TestRepo.get(Custom, "1")
    wait_assert c1 == TestRepo.get(Custom, "2")
    wait_assert nil == TestRepo.get(Custom, "3")
  end

  test "transform row" do
    assert Post.Entity[] = post = TestRepo.create(Post.Entity[title: "1", text: "hai"])

    wait_assert ["1"] == TestRepo.all(from p in Post, where: p.id == ^post.id, select: p.title)

    wait_assert [{ "1", "hai" }] ==
      TestRepo.all(from p in Post, where: p.id == ^post.id, select: { p.title, p.text })

    wait_assert [["1", "hai"]] ==
      TestRepo.all(from p in Post, where: p.id == ^post.id, select: [p.title, p.text])
  end

  test "update some entites" do
    assert Post.Entity[id: id1] = TestRepo.create(Post.Entity[title: "1", text: "hai"])
    assert Post.Entity[id: id2] = TestRepo.create(Post.Entity[title: "2", text: "hai"])
    assert Post.Entity[id: id3] = TestRepo.create(Post.Entity[title: "3", text: "hai"])

    query = from(p in Post, where: p.id == ^id1 or p.id == ^id2)
    wait_assert 2 = TestRepo.update_all(query, title: "x")
    wait_assert Post.Entity[title: "x"] = TestRepo.get(Post, id1)
    wait_assert Post.Entity[title: "x"] = TestRepo.get(Post, id2)
    wait_assert Post.Entity[title: "3"] = TestRepo.get(Post, id3)
  end

  test "update all entites" do
    assert Post.Entity[id: id1] = TestRepo.create(Post.Entity[title: "1", text: "hai"])
    assert Post.Entity[id: id2] = TestRepo.create(Post.Entity[title: "2", text: "hai"])
    assert Post.Entity[id: id3] = TestRepo.create(Post.Entity[title: "3", text: "hai"])

    query = from(p in Post, where: p.id == ^id1 or p.id == ^id2 or p.id == ^id3)
    wait_assert 3 = TestRepo.update_all(query, title: "x")
    wait_assert Post.Entity[title: "x"] = TestRepo.get(Post, id1)
    wait_assert Post.Entity[title: "x"] = TestRepo.get(Post, id2)
    wait_assert Post.Entity[title: "x"] = TestRepo.get(Post, id3)
  end

  test "update no entites" do
    assert Post.Entity[id: id1] = TestRepo.create(Post.Entity[title: "1", text: "hai"])
    assert Post.Entity[id: id2] = TestRepo.create(Post.Entity[title: "2", text: "hai"])
    assert Post.Entity[id: id3] = TestRepo.create(Post.Entity[title: "3", text: "hai"])

    query = from(p in Post, where: p.title == "4" and (p.id != ^id1 or p.id != ^id2 or p.id != ^id3))
    wait_assert 0 = TestRepo.update_all(query, title: "x")
    wait_assert Post.Entity[title: "1"] = TestRepo.get(Post, id1)
    wait_assert Post.Entity[title: "2"] = TestRepo.get(Post, id2)
    wait_assert Post.Entity[title: "3"] = TestRepo.get(Post, id3)
  end

  test "delete some entites" do
    assert Post.Entity[id: id1] = TestRepo.create(Post.Entity[title: "1", text: "hai"])
    assert Post.Entity[id: id2] = TestRepo.create(Post.Entity[title: "2", text: "hai"])
    assert Post.Entity[id: id3] = p3 = TestRepo.create(Post.Entity[title: "3", text: "hai"])

    query = from(p in Post, where: p.id == ^id1 or p.id == ^id2)
    wait_assert 2 = TestRepo.delete_all(query)
    query = query |> where([p], p.id == ^id3)
    wait_assert [p3] == TestRepo.all(query)
  end

  test "delete all entites" do
    assert Post.Entity[id: id1] = TestRepo.create(Post.Entity[title: "1", text: "hai"])
    assert Post.Entity[id: id2] = TestRepo.create(Post.Entity[title: "2", text: "hai"])
    assert Post.Entity[id: id3] = TestRepo.create(Post.Entity[title: "3", text: "hai"])
    
    query = from(p in Post, where: p.id == ^id1 or p.id == ^id2 or p.id == ^id3)
    wait_assert 3 = TestRepo.delete_all(query)
    wait_assert [] = TestRepo.all(query)
  end

  test "delete no entites" do
    assert Post.Entity[id: id1] = TestRepo.create(Post.Entity[title: "1", text: "hai"])
    assert Post.Entity[id: id2] = TestRepo.create(Post.Entity[title: "2", text: "hai"])
    assert Post.Entity[id: id3] = TestRepo.create(Post.Entity[title: "3", text: "hai"])

    query = from(p in Post, where: p.title == "4")
    wait_assert 0 = TestRepo.delete_all(query)
    wait_assert Post.Entity[title: "1"] = TestRepo.get(Post, id1)
    wait_assert Post.Entity[title: "2"] = TestRepo.get(Post, id2)
    wait_assert Post.Entity[title: "3"] = TestRepo.get(Post, id3)
  end

  test "virtual field" do
     assert Post.Entity[id: id] = TestRepo.create(Post.Entity[title: "1", text: "hai"])
     wait_assert TestRepo.get(Post, id).temp == "temp"
  end

  test "preload empty" do
    assert [] == Preloader.run([], TestRepo, :anything_goes)
  end

  test "preload has_many" do
    p1 = TestRepo.create(Post.Entity[title: "1"])
    p2 = TestRepo.create(Post.Entity[title: "2"])
    p3 = TestRepo.create(Post.Entity[title: "3"])

    c1 = Comment.Entity[] = TestRepo.create(Comment.Entity[text: "1", post_id: p1.id])
    c2 = Comment.Entity[] = TestRepo.create(Comment.Entity[text: "2", post_id: p1.id])
    c3 = Comment.Entity[] = TestRepo.create(Comment.Entity[text: "3", post_id: p2.id])
    c4 = Comment.Entity[] = TestRepo.create(Comment.Entity[text: "4", post_id: p2.id])

    assert_raise Ecto.AssociationNotLoadedError, fn ->
      p1.comments.to_list
    end
    assert p1.comments.loaded? == false

    ##wait_for_index
    query = from(p in Post,
                 where: p.id in [^p1.id, ^p2.id, ^p3.id],
                 order_by: p.title,
                 preload: :comments)
    [p1, p2, p3] =
      wait_assert [Post.Entity[], Post.Entity[], Post.Entity[]] =
      TestRepo.all(query)
    p1_comments = p1.comments.to_list
    p2_comments = p2.comments.to_list
    assert c1 in p1_comments
    assert c2 in p1_comments
    assert c3 in p2_comments
    assert c4 in p2_comments
    assert [] = p3.comments.to_list
    assert p1.comments.loaded? == true
  end

  test "preload has_one" do
    p1 = TestRepo.create(Post.Entity[title: "1"])
    p2 = TestRepo.create(Post.Entity[title: "2"])
    p3 = TestRepo.create(Post.Entity[title: "3"])

    Permalink.Entity[id: pid1] = TestRepo.create(Permalink.Entity[url: "1", post_id: p1.id])
    Permalink.Entity[]         = TestRepo.create(Permalink.Entity[url: "2", post_id: nil])
    Permalink.Entity[id: pid3] = TestRepo.create(Permalink.Entity[url: "3", post_id: p3.id])

    assert_raise Ecto.AssociationNotLoadedError, fn ->
      p1.permalink.get
    end
    assert_raise Ecto.AssociationNotLoadedError, fn ->
      p2.permalink.get
    end
    assert p1.permalink.loaded? == false

    query = from(p in Post,
                 where: p.id in [^p1.id, ^p2.id, ^p3.id],
                 order_by: p.title,
                 preload: :permalink)
    [p1, p2, p3] =
      wait_assert [Post.Entity[], Post.Entity[], Post.Entity[]] =
      TestRepo.all(query)
    wait_assert Permalink.Entity[id: ^pid1] = p1.permalink.get
    wait_assert nil = p2.permalink.get
    wait_assert Permalink.Entity[id: ^pid3] = p3.permalink.get
    assert p1.permalink.loaded? == true
  end

  test "preload belongs_to" do
    Post.Entity[id: pid1] = TestRepo.create(Post.Entity[title: "1"])
    TestRepo.create(Post.Entity[title: "2"])
    Post.Entity[id: pid3] = TestRepo.create(Post.Entity[title: "3"])

    pl1 = TestRepo.create(Permalink.Entity[url: "3", post_id: pid1])
    pl2 = TestRepo.create(Permalink.Entity[url: "1", post_id: nil])
    pl3 = TestRepo.create(Permalink.Entity[url: "2", post_id: pid3])

    assert_raise Ecto.AssociationNotLoadedError, fn ->
      pl1.post.get
    end
    assert pl1.post.loaded? == false
    
    query = from(l in Permalink,
                 where: l.id in [^pl1.id, ^pl2.id, ^pl3.id],
                 order_by: l.url,
                 preload: :post)
    [pl2, pl3, pl1] = 
      wait_assert [Permalink.Entity[], Permalink.Entity[], Permalink.Entity[]] = 
      TestRepo.all(query)
    wait_assert Post.Entity[id: ^pid1] = pl1.post.get
    wait_assert nil = pl2.post.get
    wait_assert Post.Entity[id: ^pid3] = pl3.post.get
    assert pl1.post.loaded? == true
  end

  test "preload belongs_to with shared assocs 1" do
    Post.Entity[id: pid1] = TestRepo.create(Post.Entity[title: "1"])
    Post.Entity[id: pid2] = TestRepo.create(Post.Entity[title: "2"])

    c1 = TestRepo.create(Comment.Entity[text: "1", post_id: pid1])
    c2 = TestRepo.create(Comment.Entity[text: "2", post_id: pid1])
    c3 = TestRepo.create(Comment.Entity[text: "3", post_id: pid2])

    query = from(c in Comment,
                 where: c.id in [^c1.id, ^c2.id, ^c3.id],
                 order_by: [desc: c.text],
                 preload: [:post])
    [c3, c2, c1] =
      wait_assert [Comment.Entity[], Comment.Entity[], Comment.Entity[]] = 
      TestRepo.all(query)
    assert Post.Entity[id: ^pid1] = c1.post.get
    assert Post.Entity[id: ^pid1] = c2.post.get
    assert Post.Entity[id: ^pid2] = c3.post.get
  end

  test "preload belongs_to with shared assocs 2" do
    Post.Entity[id: pid1] = TestRepo.create(Post.Entity[title: "1"])
    Post.Entity[id: pid2] = TestRepo.create(Post.Entity[title: "2"])

    c1 = TestRepo.create(Comment.Entity[text: "1", post_id: pid1])
    c2 = TestRepo.create(Comment.Entity[text: "3", post_id: pid2])
    c3 = TestRepo.create(Comment.Entity[text: "2", post_id: nil])

    ##wait_for_index
    query = from(c in Comment,
                 where: c.id in [^c1.id, ^c2.id, ^c3.id],
                 preload: [:post],
                 order_by: c.text)
    [c1, c3, c2] = wait_assert [Comment.Entity[], Comment.Entity[], Comment.Entity[]] = TestRepo.all(query)
    assert Post.Entity[id: ^pid1] = c1.post.get
    assert Post.Entity[id: ^pid2] = c2.post.get
    assert nil = c3.post.get
  end

  test "preload nested" do
    p1 = TestRepo.create(Post.Entity[title: "1"])
    p2 = TestRepo.create(Post.Entity[title: "2"])

    c1 = TestRepo.create(Comment.Entity[text: "1", post_id: p1.id])
    c2 = TestRepo.create(Comment.Entity[text: "2", post_id: p1.id])
    c3 = TestRepo.create(Comment.Entity[text: "3", post_id: p2.id])
    c4 = TestRepo.create(Comment.Entity[text: "4", post_id: p2.id])
    
    query = from(p in Post,
                 where: p.id in [^p1.id, ^p2.id],
                 order_by: [asc: p.title],
                 preload: [comments: :post])
    [p1, p2] =
      wait_assert [Post.Entity[], Post.Entity[]] =
      TestRepo.all(query)
    assert [c1, c2] = p1.comments.to_list
    assert [c3, c4] = p2.comments.to_list
    assert p1.id == c1.post.get.id
    assert p1.id == c2.post.get.id
    assert p2.id == c3.post.get.id
    assert p2.id == c4.post.get.id
  end

  test "preload keyword query" do
    p1 = TestRepo.create(Post.Entity[title: "1"])
    p2 = TestRepo.create(Post.Entity[title: "2"])
    p3 = TestRepo.create(Post.Entity[title: "3"])

    c1 = Comment.Entity[] = TestRepo.create(Comment.Entity[text: "1", post_id: p1.id])
    c2 = Comment.Entity[] = TestRepo.create(Comment.Entity[text: "2", post_id: p1.id])
    c3 = Comment.Entity[] = TestRepo.create(Comment.Entity[text: "3", post_id: p2.id])
    c4 = Comment.Entity[] = TestRepo.create(Comment.Entity[text: "4", post_id: p2.id])

    query = from(p in Post, 
                 where: p.id in [^p1.id, ^p2.id, ^p3.id],
                 preload: [:comments],
                 order_by: p.title,
                 select: p)

    [p1, p2, p3] =
      wait_assert [Post.Entity[], Post.Entity[], Post.Entity[]] =
      TestRepo.all(query)
    p1_comments = p1.comments.to_list
    p2_comments = p2.comments.to_list
    assert c1 in p1_comments
    assert c2 in p1_comments
    assert c3 in p2_comments
    assert c4 in p2_comments
    assert [] = p3.comments.to_list

    query = from(p in Post,
                 where: p.id in [^p1.id, ^p2.id, ^p3.id],
                 preload: [:comments],
                 order_by: p.title,
                 select: { 0, [p] })
    posts = TestRepo.all(query)
    [p1, p2, p3] = Enum.map(posts, fn { 0, [p] } -> p end)

    p1_comments = p1.comments.to_list
    p2_comments = p2.comments.to_list
    assert c1 in p1_comments
    assert c2 in p1_comments
    assert c3 in p2_comments
    assert c4 in p2_comments
    assert [] = p3.comments.to_list
  end

  test "row transform" do
    post = TestRepo.create(Post.Entity[title: "1", text: "hi"])
    query = from(p in Post, select: { p.title, [ p, { p.text } ] })    
    wait_assert [{ "1", [ ^post, { "hi" } ] }] = TestRepo.all(query)
  end   

  defp valid_id?(x) do
    is_binary(x) && size(x) == 24
  end

  defp delete_all_test_data() do
    posts_bucket = RiakUtil.model_bucket(Ecto.Integration.Riak.Post)
    comments_bucket = RiakUtil.model_bucket(Ecto.Integration.Riak.Comment)
    permalinks_bucket = RiakUtil.model_bucket(Ecto.Integration.Riak.Permalink)
    custom_bucket = RiakUtil.model_bucket(Ecto.Integration.Riak.Custom)
    buckets = [ posts_bucket, comments_bucket, permalinks_bucket, custom_bucket ]

    { :ok, socket } = RiakSocket.start_link('127.0.0.1', 8000)
    Enum.map(buckets, fn(bucket)->
      :ok = RiakSocket.reset_bucket(socket, bucket)
      { :ok, keys } = RiakSocket.list_keys(socket, bucket)
      Enum.map(keys, fn(key)->
        :ok == RiakSocket.delete(socket, bucket, key)
      end)
    end)
  end

end