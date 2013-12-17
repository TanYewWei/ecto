defmodule Ecto.Integration.Riak.RepoTest do
  use Ecto.Integration.Riak.Case, async: true
  alias Ecto.Associations.Preloader

  teardown_all do
    ##delete_all_test_data
    { :ok, [] }
  end

  test "already started" do
    assert { :error, { :already_started, _ } } = TestRepo.start_link
  end

  test "create and fetch single" do
    assert Post.Entity[id: id] = TestRepo.create(Post.Entity[title: "The shiny new Ecto", text: "coming soon..."])
    assert valid_id?(id)

    wait_for_index()

    assert [Post.Entity[id: ^id, title: "The shiny new Ecto", text: "coming soon..."]] =
           TestRepo.all(from(p in Post, where: p.id == ^id))
  end

  test "fetch without entity" do
    Post.Entity[id: id] = TestRepo.create(Post.Entity[title: "title1"])
    Post.Entity[] = TestRepo.create(Post.Entity[title: "title2"])

    wait_for_index()
    IO.puts inspect(from(p in "posts", where: like(p.title, "title"), order_by: p.title, select: p.title))

    assert ["title1", "title2"] =
      TestRepo.all(from(p in "posts", where: like(p.title, "title"), order_by: p.title, select: p.title))

    assert [^id] =
      TestRepo.all(from(p in "posts", where: p.title == "title1", select: p.id))
  end


  # test "basic" do85
  #   p0 = TestRepo.create(Post.Entity[title: "The shiny new Ecto", text: "coming soon..."])
  #   assert valid_id?(p0.id)

  #   c0 = TestRepo.create(Comment.new(text: "test text 0", post_id: p0.id))
  #   assert valid_id?(c0.id)

  #   c1 = TestRepo.create(Comment.new(text: "test text 1", post_id: p0.id))
  #   assert valid_id?(c1.id)

  #   l0 = TestRepo.create(Permalink.new(url: "http://test.com", post_id: p0.id))
  #   assert valid_id?(l0.id)
    
  #   wait_for_index()

  #   query = from(p in Post)
  #     |> where([p], p.id == ^p0.id)
  #     |> preload(:comments)
  #   [post] = TestRepo.all(query)
  #   comments = post.comments.to_list

  #   assert post.id == p0.id
  #   assert post.title == p0.title
  #   assert post.text == p0.text
  #   assert c0 in comments
  #   assert c1 in comments
  #   assert_raise Ecto.AssociationNotLoadedError, fn()-> p0.permalink.get end
  # end

  test "create and delete single, fetch nothing" do
    post = Post.Entity[title: "The shiny new Ecto", text: "coming soon..."]

    assert Post.Entity[] = created = TestRepo.create(post)
    assert :ok == TestRepo.delete(created)

    wait_for_index()
    assert [] = TestRepo.all(from(p in Post, where: p.id == ^post.id))
  end

  test "create and delete single, fetch empty" do
    post = Post.Entity[title: "The shiny new Ecto", text: "coming soon..."]

    assert Post.Entity[] = c0 = TestRepo.create(post)
    assert Post.Entity[] = c1 = TestRepo.create(post)
    assert :ok == TestRepo.delete(c1)

    wait_for_index()
    assert [Post.Entity[]] = TestRepo.all(from p in Post, where: p.id == ^c0.id or p.id == ^c1.id)
  end

  test "create and update single, fetch updated" do
    post = Post.Entity[title: "The shiny new Ecto", text: "coming soon..."]

    post = TestRepo.create(post)
    post = post.text("coming very soon...")
    assert :ok == TestRepo.update(post)

    wait_for_index()
    assert [Post.Entity[text: "coming very soon..."]] = TestRepo.all(from p in Post, where: p.id == ^post.id)
  end

  test "create and fetch multiple" do
    assert Post.Entity[] = p0 = TestRepo.create(Post.Entity[title: "1", text: "hai"])
    assert Post.Entity[] = p1 = TestRepo.create(Post.Entity[title: "2", text: "hai"])
    assert Post.Entity[] = p2 = TestRepo.create(Post.Entity[title: "3", text: "hai"])

    post_ids = Enum.map([p0, p1, p2], &(&1.id))
    wait_for_index()

    assert [Post.Entity[title: "1"], Post.Entity[title: "2"], Post.Entity[title: "3"]] =
           TestRepo.all(from p in Post, where: p.id in ^post_ids, order_by: p.title)

    assert [Post.Entity[title: "2"]] =
           TestRepo.all(from p in Post, where: p.id == ^p1.id and p.title == "2")
  end

  test "get entity" do
    post1 = TestRepo.create(Post.Entity[title: "1", text: "hai"])
    post2 = TestRepo.create(Post.Entity[title: "2", text: "hai"])

    wait_for_index()
    assert post1 == TestRepo.get(Post, post1.id)
    assert post2 == TestRepo.get(Post, post2.id)
    assert nil == TestRepo.get(Post, "bleh")
  end

  test "preload belongs_to" do
    Post.Entity[id: pid1] = TestRepo.create(Post.Entity[title: "1"])
    TestRepo.create(Post.Entity[title: "2"])
    Post.Entity[id: pid3] = TestRepo.create(Post.Entity[title: "3"])

    pl1 = TestRepo.create(Permalink.Entity[url: "1", post_id: pid1])
    pl2 = TestRepo.create(Permalink.Entity[url: "2", post_id: nil])
    pl3 = TestRepo.create(Permalink.Entity[url: "3", post_id: pid3])

    assert_raise Ecto.AssociationNotLoadedError, fn ->
      pl1.post.get
    end
    assert pl1.post.loaded? == false

    wait_for_index()
    assert [pl3, pl1, pl2] = Preloader.run([pl3, pl1, pl2], TestRepo, :post)
    assert Post.Entity[id: ^pid1] = pl1.post.get
    assert nil = pl2.post.get
    assert Post.Entity[id: ^pid3] = pl3.post.get
    assert pl1.post.loaded? == true
  end

  defp wait_for_index() do
    :timer.sleep(1100)
  end

  defp wait_until(fun) do
    ## retries fun until it succeeds
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