defmodule Ecto.Adapters.Riak.Datetime do
  @type year      :: non_neg_integer
  @type month     :: non_neg_integer
  @type day       :: non_neg_integer
  @type hour      :: non_neg_integer
  @type min       :: non_neg_integer
  @type sec       :: non_neg_integer
  @type msec      :: non_neg_integer
  @type date      :: {year, month, day}
  @type time      :: {hour, min, sec}
  @type datetime  :: {date, time}
  @type timestamp :: {megasec::integer, sec::integer, microsec::integer}
  @type dt        :: date | time | datetime
  @type ecto_dt   :: Ecto.DateTime
  @type ecto_int  :: Ecto.Interval
  @type ecto_type :: ecto_dt | ecto_int

  @spec parse(binary) :: datetime
  def parse(x) do
    [ year, month, day, hour, min, sec ] =
      (String.split(x, %r"-|:|T|Z", trim: true)
       |> Enum.map(fn(bin)->
                       { int, _ } = Integer.parse(bin)
                       int
                   end))
    case String.last(x) do
      "Z" ->
        { { year, month, day }, { hour, min, sec} }
      _ ->
        raise "invalid argument"
    end
  end

  @spec parse_to_ecto_datetime(binary) :: ecto_type
  def parse_to_ecto_datetime(x) do
    { { year, mon, day }, { hour, min, sec } } = parse(x)
    Ecto.DateTime.new(year: year, month: mon, day: day, 
                      hour: hour, min: min, sec: sec)
  end

  @spec parse_to_ecto_interval(binary) :: ecto_type
  def parse_to_ecto_interval(x) do
    { { year, mon, day }, { hour, min, sec } } = parse(x)
    Ecto.Interval.new(year: year, month: mon, day: day, 
                      hour: hour, min: min, sec: sec)
  end
  
  @spec compare(datetime, datetime) :: boolean
  def compare(date0, date1) do
    {{y0,m0,d0}, {h0,n0,s0}} = date0
    {{y1,m1,d1}, {h1,n1,s1}} = date1
    cond do
      y0 != y1 -> y0 < y1
      m0 != m1 -> m0 < m1
      d0 != d1 -> d0 < d1
      h0 != h1 -> h0 < h1
      n0 != n1 -> n0 < n1
      s0 != s1 -> s0 < s1
      true     -> false
    end
  end

  @spec datetime_to_timestamp(datetime | date) :: timestamp
  
  def datetime_to_timestamp({_,_,_}=x) do 
    datetime_to_timestamp({x, {0,0,0}})
  end
  
  def datetime_to_timestamp(x) do
    sec = :calendar.datetime_to_gregorian_seconds(x) - 62167219200
    {div(sec,1000000), rem(sec,1000000), 0}
  end
  
  @spec now_datetime() :: datetime
  def now_datetime(), do: :calendar.now_to_universal_time(:os.timestamp)

  def now_local_datetime(), do: :calendar.now_to_local_time(:os.timestamp)

  def now_ecto_datetime(), do: now_datetime |> to_ecto_datetime

  def now_local_ecto_datetime(), do: now_local_datetime |> to_ecto_datetime

  @spec now_string() :: binary
  def now_string() do
    datetime_to_string(:os.timestamp)
  end  
  
  @spec datetime_to_string(datetime) :: binary
  def datetime_to_string({_,_,_}=x) do
    datetime_to_string({x,{0,0,0}})
  end
  
  def datetime_to_string(x) when is_record(x, Ecto.DateTime) do
    ecto_type_to_datetime(x) |> datetime_to_string()
  end

  def datetime_to_string({ { year, month, day }, { hour, min, sec } }) do
    "#{year}-#{month}-#{day}T#{hour}:#{min}:#{sec}Z"
  end

  def interval_to_string(x) when is_record(x, Ecto.Interval) do
    { { year, month, day }, { hour, min, sec } } = ecto_type_to_datetime(x)
    "#{year}-#{month}-#{day}T#{hour}:#{min}:#{sec}I"
  end

  @spec to_ecto_datetime(dt) :: ecto_dt
  def to_ecto_datetime(x) do
    case x do
      {{year, mon, day}, {hour, min, sec}} ->
        Ecto.DateTime.new(year: year, month: mon, day: day, 
                          hour: hour, min: min, sec: sec)
      {a, b, c} ->
        cond do
          year?(a) && month?(b) && day?(c) ->
            Ecto.DateTime.new(year: a, month: b, day: c)
          hour?(a) && minute?(b) && second?(c) ->
            Ecto.DateTime.new(hour: a, min: b, sec: c)
          true ->
            nil
        end
      _ ->
        nil
    end
  end

  @spec ecto_datetime_to_datetime(ecto_dt) :: datetime
  def ecto_datetime_to_datetime(x) do
    year  = if x.year  != nil, do: x.year,  else: 0
    month = if x.month != nil, do: x.month, else: 0
    day   = if x.day   != nil, do: x.day,   else: 0
    hour  = if x.hour  != nil, do: x.hour,  else: 0
    min   = if x.min   != nil, do: x.min,   else: 0
    sec   = if x.sec   != nil, do: x.sec,   else: 0
    {{year, month, day}, {hour, min, sec}}
  end  
  
  ## ------------------------------------------------------------
  ## Predicates and Guards
  ## ------------------------------------------------------------

  defmacro ecto_timestamp?(x) do
    quote do
      (is_record(unquote(x), Ecto.DateTime) or is_record(unquote(x), Ecto.Interval))
    end
  end

  def ecto_interval?(x) do
    ## treat nil values as valid
    if is_record(x, Ecto.Interval) do
      year  = if x.year  != nil, do: year?(x.year),   else: true
      month = if x.month != nil, do: month?(x.month), else: true
      day   = if x.day   != nil, do: day?(x.day),     else: true
      hour  = if x.hour  != nil, do: hour?(x.hour),   else: true
      min   = if x.min   != nil, do: minute?(x.min),  else: true
      sec   = if x.sec   != nil, do: second?(x.sec),  else: true
      (year && month && day && hour && min && sec)
    else
      false
    end
  end

  def ecto_datetime?(x) do 
    if is_record(x, Ecto.Datetime) do
      datetime?({{x.year, x.month, x.day}, {x.hour, x.min, x.sec}})
    else
      false
    end
  end

  def datetime?(x) when is_tuple(x) do
    case x do
      {date, time} ->
        date?(date) && time?(time)
      _ ->
        false
    end
  end

  def date?(x) do
    case x do
      {year,month,day} ->
        year?(year) && month?(month) && day?(day)
      _ ->
        false
    end
  end

  def time?(x) do
    case x do
      {hr, min, sec} ->
        hour?(hr) && minute?(min) && second?(sec)
      _ ->
        false
    end
  end

  def year?(x), do: x > 0

  def month?(x), do: x >= 1 && x <= 12

  def day?(x), do: x >= 1 && x <= 31

  def hour?(x), do: x >= 0 && x <= 23

  def minute?(x), do: x >= 0 && x <= 59

  def second?(x), do: x >= 0 && x <= 59

  ## ------------------------------------------------------------
  ## Ecto Types
  ## ------------------------------------------------------------
  
  def ecto_type_to_datetime(x)
    when is_record(x, Ecto.DateTime) or is_record(x, Ecto.Interval) do
    default = &(if nil?(&1), do: 0, else: &1)
    date = { default.(x.year), default.(x.month), default.(x.day) }
    time = { default.(x.hour), default.(x.min), default.(x.sec) }
    { date, time }
  end

  ## ------------------------------------------------------------
  ## Solr Specific
  ## ------------------------------------------------------------

  def solr_datetime(x) when x == "NOW" do
    solr_datetime(now_datetime)
  end

  def solr_datetime(x) when is_binary(x), do: x

  def solr_datetime(x) when ecto_timestamp?(x) do
    ecto_datetime_to_datetime(x) |> solr_datetime
  end

  def solr_datetime({a, b, c}) do
    cond do
      year?(a) && month?(b) && day?(c) ->
        "#{a}-#{b}-#{c}T00:00:00Z"
      hour?(a) && minute?(b) && second?(c) ->
        {{year, month, day}, _} = now_datetime()
        "#{year}-#{month}-#{day}T#{a}:#{b}:#{c}Z"
      true ->
        raise "bad datetime"
    end
  end

  def solr_datetime({{year, month, day}, {hour, min, sec}}) do
    "#{year}-#{month}-#{day}T#{hour}:#{min}:#{sec}Z"
  end

  @spec solr_datetime_add(dt | ecto_dt) :: binary

  def solr_datetime_add(x) when is_binary(x), do: x

  def solr_datetime_add(x) do
    {{year, month, day}, {hour, min, sec}} = ecto_datetime_to_datetime(x)
    "+#{year}YEARS+#{month}MONTHS+#{day}DAYS+#{hour}HOURS+#{min}MINUTES+#{sec}SECONDS"
  end

  @spec solr_datetime_subtract(dt | ecto_dt) :: binary
  
  def solr_datetime_subtract(x) when is_binary(x), do: x

  def solr_datetime_subtract(x) do
    {{year, month, day}, {hour, min, sec}} = ecto_datetime_to_datetime(x)
    "-#{year}YEARS-#{month}MONTHS-#{day}DAYS-#{hour}HOURS-#{min}MINUTES-#{sec}SECONDS"
  end

end