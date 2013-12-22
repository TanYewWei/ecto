defmodule Ecto.Adapters.Riak.SearchOrderBy do
  alias Ecto.Adapters.Riak.Util, as: RiakUtil
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
        fn x, y ->
             case field_type do
               :datetime ->
                 compare_datetime(direction, x, y)
               :interval ->
                 compare_interval(direction, x, y)
               _ ->
                 compare_simple(direction, x, y)
             end
        end
      end)
    end)
      |> List.flatten

    IO.puts("comparisons: #{inspect comparisons}")

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
  end

  defp compare_interval(dir, x, y) do
  end

  defp ascending?(dir) do
    dir == :asc
  end

end