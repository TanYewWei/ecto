defmodule Ecto.Adapters.Riak.SearchWhere  do
  alias Ecto.Query.QueryExpr

  ## Constants

  @unary_ops   [:-, :+, :now]
  @binary_ops  [:==, :!=, :<=, :>=, :<, :>, :and, :or, :like, :date_add, :date_sub]

  ## API

  def query(wheres, sources) do
    Enum.map_join(wheres,
                  " ",
                  fn(QueryExpr[expr: expr])->
                      where_expr(expr, sources)
                  end)
  end

  defp where_expr({:., _, [{:&, _, [_]}=var, field]}, sources) when is_atom(field) do
    source = Util.find_source(sources, var)
    entity = Util.entity(source)
    type = entity.__entity__(:field_type, field)
    SearchUtil.yz_key(field, type)
  end

  defp where_expr({:!, _, [expr]}, sources) do
    "-" <> where_expr(expr, sources)
  end

  defp where_expr({:==, _, [nil, right]}, sources) do
    "-" <> where_expr(right, sources) <> ":*"
  end

  defp where_expr({:==, _, [left, nil]}, sources) do
    "-" <> where_expr(left, sources) <> ":*"
  end

  defp where_expr({:!=, _, [nil, right]}, sources) do
    where_expr(right, sources) <> ":*"
  end

  defp where_expr({:!=, _, [left, nil]}, sources) do 
    where_expr(left, sources) <> ":*"
  end

  ## element in range
  defp where_expr({:in, _, [left, Range[first: first, last: last]]}, sources) do
    field = where_expr(left, sources)
    range_start = where_expr(first, sources)
    range_end = where_expr(last, sources)
    "#{field}:[#{range_start} TO #{range_end}]"
  end

  ## element in collection
  defp where_expr({:in, _, [left, right]}, sources) do
    where_expr(left, sources) <> ":(" <> where_expr(right, sources) <> ")"
  end

  ## range handling
  defp where_expr(Range[] = range, sources) do
    where_expr(Enum.to_list(range), sources)
  end

  ## range handling
  defp where_expr({:.., _, [first, last]}, sources) do
    where_expr(Enum.to_list(first..last), sources)
  end 

  defp where_expr({:/, _, _}, _) do
    raise Ecto.QueryError, reason: "where queries to Riak do not permit the `/` operator"
  end

  defp where_expr({:pow, _, _}, _) do
    raise Ecto.QueryError, reason: "where queries to Riak do not permit the `pow` operator"
  end

  defp where_expr({:rem, _, _}, _) do
    raise Ecto.QueryError, reason: "where queries to Riak do not permit the `rem` operator"
  end

  defp where_expr({arg, _, []}, sources) when is_tuple(arg) do
    where_expr(arg, sources)
  end

  defp where_expr({fun, _, args}, sources) 
  when is_atom(fun) and is_list(args) and fun in @unary_ops do
    arg = where_expr(Enum.first(args), sources)
    case fun do
      :now ->
        "NOW"
      _ ->
        "#{fun}#{arg}"
    end
  end    

  defp where_expr({fun, _, [left, right]}, sources)
  when is_atom(fun) and fun in @binary_ops do
    cond do
      ## Datetime operations
      not Datetime.ecto_timestamp?(left) and Datetime.ecto_timestamp?(right) ->
        left = op_to_binary(left, sources)
        right = op_to_binary(right, sources)
        case fun do
          :== ->
            left <> ":" <> right
          :!= ->
            "-" <> left <> ":" <> right
          _ ->
            raise Ecto.QueryError, reason: "where query invalid function #{fun} for right-side datetime"
        end
      Datetime.ecto_timestamp?(left) and Datetime.ecto_timestamp?(right) ->
        case fun do
          :date_add ->
            Datetime.solr_datetime(left) <> Datetime.solr_datetime_add(right)
          :date_sub ->
            Datetime.solr_datetime(left) <> Datetime.solr_datetime_subtract(right)
          _ ->
            raise Ecto.QueryError, reason: "where query invalid function #{fun} for datetime args"
        end
      
      ## Rest
      true ->
        left = op_to_binary(left, sources)
        right = op_to_binary(right, sources)
        case fun do
          :== ->
            left <> ":" <> right
          :!= ->
            left <> ":" <> right
          :> ->
            right = try do
                      binary_to_integer(right) + 1 |> to_string
                    catch
                      _,_ -> right
                    end
            left <> ":[" <> right <> " TO *]"
          :>= ->
            left <> ":[" <> right <> " TO *]"
          :< ->
            right = try do
                      binary_to_integer(right) - 1 |> to_string
                    catch
                      _,_ -> right
                    end
            left <> ":[* TO " <> right <> "]"
          :<= ->
            left <> ":[* TO " <> right <> "]"
          :and ->
            left <> " AND " <> right
          :or ->
            left <> " OR " <> right
          :like ->
            left <> ":*" <> right <> "*"
          _ ->
            raise Ecto.QueryError, reason: "where query unknown function #{fun}"
        end
    end
  end

  defp where_expr(list, sources) when is_list(list) do
    Enum.map_join(list, " ", &where_expr(&1, sources))
  end

  defp where_expr(literal, _) do
    literal(literal)
  end 
  
  defp op_to_binary({op, _, [x, y]}=expr, sources) when op in @binary_ops do
    case op do
      :== when x == nil or y == nil ->
        where_expr(expr, sources)
      :!= when x == nil or y == nil ->
        "(" <> where_expr(expr, sources) <> ")"
      :!= ->
        "-(" <> where_expr(expr, sources) <> ")"
      :date_add -> ## Dates cannot be enclosed in brackets
        where_expr(expr, sources)
      :date_sub ->
        where_expr(expr, sources)
      _ ->
        "(" <> where_expr(expr, sources) <> ")"        
    end
  end

  defp op_to_binary(expr, sources) do
    where_expr(expr, sources)
  end

  ## --------------------
  ## Handling of literals
  @spec literal(term) :: binary
  
  defp literal(nil), do: "*"

  defp literal(true), do: "true"
  
  defp literal(false), do: "false"

  defp literal(Ecto.DateTime[] = dt) do
    Datetime.solr_datetime(dt)
  end

  defp literal(Ecto.Interval[] = i) do
    Datetime.solr_datetime(i)
  end

  defp literal(Ecto.Binary[value: binary]) do
    :base64.encode(binary)
  end

  defp literal(literal) when is_binary(literal) do
    literal  ## TODO: escaping
  end

  defp literal(literal) when is_number(literal) do
    to_string(literal)
  end  
  

end