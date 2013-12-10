defmodule Ecto.Adapters.Riak.SearchHaving do
  alias Ecto.Adapters.Riak.SearchUtil

  @type entity :: Ecto.Entity.t
  
  ## Constants
  
  @funs           [:random, :now, :localtimestamp]
  @unary_ops      [:-, :+, :round,
                   :downcase, :upcase]
  @binary_ops     [:pow, :rem, :/, :*, :+, :-]
  @predicate_ops  [:==, :!=, :<=, :<, :>=, :>, :and, :or]
  @aggregate_ops  [:avg, :count, :max, :min, :sum]

  ## ----------------------------------------------------------------------
  ## API
  ## ----------------------------------------------------------------------

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
            entities
          entities == [] ->
            []
          hd(entities) |> is_list ->
            ## entities :: [ [entity] ]
            ## as a result of a group_by post processing function
            ##Enum.map(entities, fn(x)-> IO.puts length(x) end)
            rs = Enum.filter(entities,
                        fn(entity_list)->
                            res = Enum.map(havings, &having_filter(&1.expr, entity_list))
                            Enum.all?(res, &(true == &1))
                        end)
          true ->
            ## In this case, treat all entities as a single group.
            ## This means that either the entire group evaluates to true
            ## or an empty return is given
            res = Enum.map(havings, &having_filter(&1.expr, entities))
            if Enum.all?(res, &(&1 == true)) do
              entities
            else
              []
            end
        end
    end
  end
  
  @spec having_filter(term, [entity]) :: boolean 
  ## though intermediate results can be any term

  defp having_filter({{:., _, [{:&, _, [_]}, field]}, _, _}, entities) when is_atom(field) do
    ## attribute accessor
    Enum.map(entities, fn(entity)-> SearchUtil.entity_keyword(entity)[field] end)
  end

  defp having_filter({op, _, args}, entities)
  when is_atom(op) and op in @aggregate_ops do
    ## first argument of args must be a field accessor
    {{:., _, [{:&, _, _}, field]}, _, _} = hd(args)
    
    ## Extractor functions
    value_fn = fn(entity)-> SearchUtil.entity_keyword(entity)[field] end
    value_type_fn = fn(entity)-> SearchUtil.entity_field_type(entity, field) end
    
    ## Dispatch
    [case op do
      :avg ->
        length = length(entities)
        sum = Enum.reduce(entities, 0, fn(entity, acc)-> acc + value_fn.(entity) end)
        sum / length
      :count ->
        length(entities)
      :max ->
        Enum.map(entities, value_fn)
        |> Enum.max
      :min ->
        Enum.map(entities, value_fn)
        |> Enum.min
      :sum ->
        Enum.reduce(entities, 0, fn(entity, acc)->
                                     value = value_fn.(entity)
                                     value = case value_type_fn.(entity) do
                                               :integer -> round(value)
                                               :float   -> value
                                             end
                                     acc + value
                                 end)
      _ ->
        raise Ecto.QueryError, reason: "unsupported select aggregate op: #{op}"
    end]
  end

  defp having_filter({op, _, [left, right]}, entities)
  when is_atom(op) and op in @binary_ops do
    ## takes an operator, and recursively operates on 
    ##IO.puts("left: #{left}")
    ##IO.puts("right: #{right}")
    left = having_filter(left, entities)
    right = having_filter(right, entities)
    keyword = List.zip([left, right])
    case op do
      :pow ->
        Enum.map(keyword, fn({x,y})-> :math.pow(x, y) end)
      :rem ->
        Enum.map(keyword, fn({x,y})-> rem(x, y) end)
      :+ ->
        Enum.map(keyword, fn({x,y})-> x + y end)
      :- ->
        Enum.map(keyword, fn({x,y})-> x - y end)
      :/ ->
        Enum.map(keyword, fn({x,y})-> x / y end)
      :* ->
        Enum.map(keyword, fn({x,y})-> x * y end)
      _ ->
        raise Ecto.QueryError, reason: "unsupported having binary op: #{op}"
    end
  end

  defp having_filter({op, _, [left, right]}, entities)
  when is_atom(op) and op in @predicate_ops do
    ## Resolve left and right (they must be equal-length lists)
    ## and then apply op to each argument in the list.
    left = having_filter(left, entities)
    right = having_filter(right, entities)
    keyword = List.zip([left, right])
    case op do           
      :== ->
        Enum.all?(keyword, fn({x,y})-> x == y end)
      :!= ->
        Enum.all?(keyword, fn({x,y})-> x != y end)
      :< ->
        Enum.all?(keyword, fn({x,y})-> x < y end)
      :<= ->
        Enum.all?(keyword, fn({x,y})-> x <= y end)
      :> ->
        Enum.all?(keyword, fn({x,y})-> x > y end)
      :>= ->
        Enum.all?(keyword, fn({x,y})-> x >= y end)
      :- ->
        Enum.all?(keyword, fn({x,y})-> x - y end)
      :and ->
        Enum.all?(keyword, fn({x,y})-> x and y end)
      :or ->
        Enum.all?(keyword, fn({x,y})-> x or y end)
      _ ->
        raise Ecto.QueryError, reason: "unsupported having predicate op: #{op}"
    end
  end

  defp having_filter(x, entities) do
    List.duplicate(x, length(entities))
  end

end