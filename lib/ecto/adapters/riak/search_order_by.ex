defmodule Ecto.Adapters.Riak.SearchOrderBy do
  alias Ecto.Query.Util

  @type entity     :: term
  @type entities   :: [entity] | [[entity]]
  @type comparison :: :lt | :gt | :eq
  
  @spec post_proc(term, term) :: (entities -> entities)
  
  def post_proc(order_bys, sources) do
    fn entities ->
       cond do
         entities == [] ->
           []
         hd(entities) |> is_list ->
           Enum.map(entities, &order_by(&1, order_bys, sources))
         true ->
           order_by(entities, order_bys, sources)
       end
    end
  end

  defp order_by(entities, [], _) do
    entities
  end

  defp order_by(entities, order_bys, sources) do
    ## Construct list of functions to apply to entities
    ## in order. Each fn should return one of `:lt`, `:gt`, `:eq`,
    ## and the next function should only be called if `:eq` 
    ## was returned.
    comparisons = Enum.map(order_bys, fn expr ->
      Enum.map(expr.expr, fn { direction, var, field } ->
        source = Util.find_source(sources, var)
        entity = Util.entity(source)
        field_type = entity.__entity__(:field_type, field)

        ## return function
        fn e0, e1 ->
           v0 = apply(e0, field, [])
           v1 = apply(e1, field, [])
           case field_type do
             :datetime ->
               compare_datetime(direction, v0, v1)
             :interval ->
               compare_interval(direction, v0, v1)
             _ ->
               compare_simple(direction, v0, v1)
           end
        end
      end)
    end)
      |> List.flatten

    ## Construct function which performs the operation through funs,
    ## and returns either true or false to give the entities the
    ## appropriate ordering
    pred = fn x, y ->
      { _, res } = Enumerable.reduce(comparisons, { :cont, false }, fn fun, _ ->
        case fun.(x, y) do
          :eq ->            
            { :cont, :eq }
          :lt ->
            { :halt, :lt }
          :gt ->
            { :halt, :gt }
        end
      end)

      case res do
        :lt -> true
        :eq -> true
        _   -> false
      end
    end
    
    Enum.sort(entities, fn x, y -> pred.(x, y) end)
  end

  ## ------------------------------------------------------------
  ## Comparison
  ## ------------------------------------------------------------

  defmacrop compare_simple_clause(dir, x, y) do
    quote do
      cond do
        unquote(x) < unquote(y) ->
          if ascending?(unquote(dir)), do: :lt, else: :gt
        unquote(x) > unquote(y) ->
          if ascending?(unquote(dir)), do: :gt, else: :lt
        true  ->
          :eq
      end
    end
  end

  defp compare_simple(dir, x, y) do
    compare_simple_clause(dir, x, y)
  end

  defp compare_datetime(dir, x, y) do
    cond do
      x.year != y.year ->
        compare_simple_clause(dir, x.year, y.year)
      x.month != y.month ->
        compare_simple_clause(dir, x.month, y.month)
      x.day != y.day ->
        compare_simple_clause(dir, x.day, y.day)
      x.hour != y.hour ->
        compare_simple_clause(dir, x.hour, y.hour)
      x.min != y.min ->
        compare_simple_clause(dir, x.min, y.min)
      x.sec != y.sec ->
        compare_simple_clause(dir, x.sec, y.sec)
      true ->
        :eq
    end
  end

  defp compare_interval(dir, x, y) do
    compare_datetime(dir, x, y)
  end

  defp ascending?(dir) do
    dir == :asc
  end

end