defmodule Ecto.Adapters.Riak.SearchHaving do
  alias Ecto.Adapters.Riak.SearchUtil

  @type entity :: Ecto.Entity.t

  @doc """
  Takes a list of Ecto.Query.QueryExpr having clauses,
  and returns a function that can be called on a list of entity grous
  to determine if the group satisfies the constratins of the having
  clauses.
  """
  @spec post_proc([term]) :: (([entity] | [[entity]]) -> boolean)

  def post_proc(havings) do
    ## construct predicate function which gets called
    ## with a list of entities, returning true if the group
    ## satisfies the havings conditions
    
    fn(entities) ->
        cond do
          havings == [] ->
            []
          entities == [] ->
            []
          hd(entities) |> is_list ->
            ## entities :: [ [entity] ]
            ## as a result of a group_by post processing function
            Enum.all?(entities, &post_proc_fn(havings, &1))
          true ->
            ## In this case, treat all entities as a single group
            post_proc_fn(havings, entities)
        end
    end
  end

  defp post_proc_fn(op, entities) do
  end

  defp post_proc_fn({{:., _, [{:&, _, [_]}, field]}, _, _}, entities) do
    ## 
    Enum.map(entities, fn(x)-> SearchUtil.entity_keyword(x)[field] end)
  end

end