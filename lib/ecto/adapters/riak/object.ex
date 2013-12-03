defmodule Ecto.Adapter.Riak.Object do
  alias :riakc_obj, as: RiakObject
  alias Ecto.Adapter.Riak.JSON

  @type statebox    :: :statebox.statebox
  @type entity_type :: Ecto.Entity
  @type entity      :: Ecto.Entity.t
  @type json        :: JSON.json
  @type object      :: :riakc_obj.object

  @content_type  "application/json"

  @spec entity_to_object(entity) :: object
  def entity_to_object(entity) do
    module = elem(entity, 0)
    model_name = entity.model |> to_string
    fields = module.__entity__(:entity_kw, entity, primary_key: true)
    fields = Enum.map(fields, fn({k,v})-> {to_string(k), v} end)
    fields = [ {"_t", model_name}, 
               {"_ts", timestamp()} | fields ]

    ## Form riak object
    bucket = model_name
    key = entity.primary_key
    value = JSON.encode({ fields })
    object = RiakObject.new(bucket, key, value, @content_type)
  end

  @spec object_to_entity(object) :: entity
  def object_to_entity(object) do
    case RiakObject.get_values(object) do
      [value] ->
        resolve_json(value)
      values ->
        resolve_siblings(values)
    end
  end
  
  @spec resolve_value(binary) :: entity
  defp resolve_value(value) do
    json = JSON.decode(value)
    statebox = resolve_json(json)    
    :statebox_orddict.from_values()
  end  

  @spec resolve_siblings(binary) :: entity
  defp resolve_siblings(values) do
    stateboxes = Enum.map(values, &(JSON.decode(&1) |> resolve_json))
    statebox = :statebox_orddict.from_values(stateboxes)
    statebox_to_entity({ statebox.value })
  end

  @spec resolve_json(json) :: statebox
  defp resolve_json(json) do
    { inner } = json
    {meta, attr} = Dict.split(inner, ["_ts"])

    ## Create new statebox and set Timestamp
    timestamp = Keyword.get(meta, "_ts", timestamp())
    statebox = :statebox_orddict.from_values([])
    statebox = set_elem(statebox, 3, timestamp)  ## 3rd element is timestamp

    ## Map over json values and create statebox update operations.
    ## Note that we never create nested JSON objects from Ecto entities.
    ops = Enum.reduce(inner,
                      [],
                      fn({k,v}, acc)->
                          case k do
                            "_ts" ->
                              acc
                            _ ->
                              if is_list(v) do
                                ## add-wins behaviour
                                [:statebox_orddict.f_union(k,v) | acc]
                              else
                                [:statebox_orddict.f_store(k,v) | acc]
                              end
                          end
                      end)
    statebox.update(ops)
  end

  defp statebox_to_entity(statebox) do
    values = statebox.value
    type = :orddict.fetch("_t", values)
    module = binary_to_atom("#{type}.Entity")
    
    ## Use module to get available fields 
    ## and create new entity
    entity_fields = module.__entity__(:field_names)
    Enum.map(entity_fields, fn(x)->
                                case :orddict.find(x, values) do
                                  {:ok, value} ->
                                    {x, value} ## {atom, term}
                                  _ ->
                                    nil
                                end
                            end)
    |> Enum.filter(&(nil != &1))
    |> type.new()
  end

  defp timestamp() do
    :statebox_clock.timestamp
  end

end