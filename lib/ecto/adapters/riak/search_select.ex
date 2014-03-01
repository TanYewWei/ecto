defmodule Ecto.Adapters.Riak.SearchSelect do
  alias Ecto.Adapters.Riak.Util, as: SearchUtil

  ## Types
  
  @type entity :: Ecto.Entity.t

  ## Constants

  @funs           [:random, :now, :localtimestamp]
  @unary_ops      [:-, :+, :round,
                   :downcase, :upcase]
  @binary_ops     [:pow, :rem, :/, :*, :+, :-,
                   :==, :!=, :<=, :<, :>=, :>,
                   :and, :or,
                   :<>, :++,
                   :date_add, :date_sub,
                   :round]
  @aggregate_ops  [:avg, :count, :max, :min, :sum]

  @doc """
  Takes a Ecto.Query.QueryExpr select clause
  a returns a function that can be called on an entity list 
  or entity-group list to perform the transformers specified 
  in the select clause
  """
  @spec post_proc(term) :: (([entity] | [[entity]]) -> term)

  def post_proc(select) do
    ## Returns a function that takes an entity,
    ## extracts the needed fields for the select expression
    ## and transform it to the appropriate datastructure
    ##
    ## Note that because joins are not suppoted in Riak
    ## you can only perform select transformstions on the 
    ## model referenced in the `from` clause
    fn entities ->
        cond do
          entities == [] ->
            []
          hd(entities) |> is_list ->
            ## case where we have a group_by or having clause.
            ## entities :: [ [entity] ]
            Enum.map(entities, &select_aggregate_transform(select.expr, &1))
          true ->
            ## entities :: [entity]
            Enum.map(entities, &select_transform(select.expr, &1))
        end
    end
  end

  ## select_transform/3 takes an expr 
  @spec select_transform(tuple, entity)
    :: {transformed :: term, expr_acc :: tuple}

  defp select_transform({ :{}, _, list }, entity) do
    select_transform(list, entity)
    |> list_to_tuple
  end

  defp select_transform({ _, _ } = tuple, entity) do
    select_transform(tuple_to_list(tuple), entity)
    |> list_to_tuple
  end

  defp select_transform(list, entity) when is_list(list) do
    Enum.map(list, fn elem -> select_transform(elem, entity) end)
  end

  defp select_transform({ { :., _, [{ :&, _, [_] }, field] }, _, _ }, entity) when is_atom(field) do
    ## attribute accessor
    SearchUtil.entity_keyword(entity)[field]
  end

  defp select_transform({ fun, _, _ }, _)
  when is_atom(fun) and fun in @funs do
    case fun do
      :random ->
        :random.uniform()
      :now ->
        ## GMT timestamp
        DateTime.now_ecto()
      :localtimestamp ->
        ## timestamp with respect to the current timezone
        DateTime.now_local_ecto()
      _ ->
        raise Ecto.QueryError, reason: "unsupported select function: #{fun}"
    end
  end

  defp select_transform({ op, _, args }, entity)
  when is_atom(op) and length(args) == 1 and op in @unary_ops do
    arg = select_transform(List.first(args), entity)
    case op do
      :- ->
        -1 * arg
      :+ ->
        arg
      :round -> ## always returns float
        Kernel.round(arg) * 1.0
      :downcase ->
        String.downcase(arg)
      :upcase ->
        String.upcase(arg)
      _ ->
        raise Ecto.QueryError, reason: "unsupported select unary op: #{op}"
    end
  end

  defp select_transform({ op, _, [left, right] = args }, entity)
  when is_atom(op) and op in @binary_ops do
    left = select_transform(left, entity)
    right = select_transform(right, entity)
    case op do
      :pow ->
        :math.pow(left, right)
      :rem ->
        rem(left, right)
      :+ ->
        left + right
      :- ->
        left - right
      :/ ->
        left / right
      :* ->
        left * right      
      :== ->
        left == right
      :!= ->
        left != right
      :<= ->
        left <= right
      :< ->
        left < right
      :>= ->
        left >= right
      :> ->
        left > right
      :and ->
        left and right
      :or ->
        left or right
      :<> ->
        left <> right
      :++ ->
        left ++ right
      :round when is_number(left) and is_integer(right) ->
        Float.round(left * 1.0, right)
      _ ->
        raise Ecto.QueryError, reason: "unsupported select binary op: #{op} with args #{inspect args}"
    end
  end

  defp select_transform({ :&, _, [_] }, entity) do
    entity
  end

  defp select_transform({ op, _, args }, entity) do
    raise Ecto.QueryError, reason: "Riak select unknown op: #{op}/#{length(args)} for entity: #{inspect entity}"
  end

  defp select_transform(x, _) do
    x
  end

  ## ----------------------------------------------------------------------
  ## Aggregate Transforms
  ## ----------------------------------------------------------------------

  defp select_aggregate_transform({ { :., _, [_, field] }, _, _ }, entities) when is_atom(field) do
    Enum.map(entities, fn x -> SearchUtil.entity_keyword(x)[field] end)
  end

  defp select_aggregate_transform({op, _, args}, entities)
  when is_atom(op) and op in @aggregate_ops do
    ## first argument of args must be a field accessor
    { {:., _, [{ :&, _, _ }, field] }, _, _ } = hd(args)
    
    ## Extractor functions
    value_fn = fn entity -> SearchUtil.entity_keyword(entity)[field] end
    value_type_fn = fn entity -> SearchUtil.entity_field_type(entity, field) end
    
    ## Dispatch
    case op do
      :avg ->
        length = length(entities)
        sum = Enum.reduce(entities, 0, fn entity, acc -> acc + value_fn.(entity) end)
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
        Enum.reduce(entities, 0, fn entity, acc ->
          value = value_fn.(entity)
          value = case value_type_fn.(entity) do
                    :integer -> round(value)
                    :float   -> value
                  end
          acc + value
        end)
      _ ->
        raise Ecto.QueryError, reason: "unsupported select aggregate op: #{op}"
    end
  end

end