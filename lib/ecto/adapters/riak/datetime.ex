defmodule Ecto.Adapters.Riak.DateTime do
  @type year      :: non_neg_integer
  @type month     :: non_neg_integer
  @type day       :: non_neg_integer
  @type hour      :: non_neg_integer
  @type min       :: non_neg_integer
  @type sec       :: non_neg_integer
  @type msec      :: non_neg_integer
  @type date      :: { year, month, day }
  @type time      :: { hour, min, sec }
  @type datetime  :: { date, time }
  @type dt        :: date | time | datetime
  @type ecto_dt   :: Ecto.DateTime
  @type ecto_int  :: Ecto.Interval
  @type ecto_type :: ecto_dt | ecto_int
  
  @spec now() :: datetime
  
  def now(), do: :calendar.now_to_universal_time(:os.timestamp)

  def now_local(), do: :calendar.now_to_local_time(:os.timestamp)

  def now_ecto(), do: now |> to_datetime

  def now_local_ecto(), do: now_local |> to_datetime
  
  @spec to_str(datetime | ecto_type) :: binary
  
  def to_str({ _, _, _ } = x) do
    to_str({ x, { 0, 0, 0 } })
  end
  
  def to_str(Ecto.DateTime[] = x) do
    ecto_to_erl(x) |> to_str()
  end

  def to_str(Ecto.Interval[] = x) do
    ecto_to_erl(x) |> to_str()
  end

  def to_str({ { year, month, day }, { hour, min, sec } }) do
    "#{pad(year, 4)}-#{pad(month, 2)}-#{pad(day, 2)}T#{pad(hour, 2)}:#{pad(min, 2)}:#{pad(sec, 2)}Z"
  end

  @spec to_datetime(dt) :: ecto_dt

  def to_datetime(x) when is_binary(x) do
    parse_string(x) |> to_datetime
  end

  def to_datetime(Ecto.DateTime[] = x), do: x
  
  def to_datetime(Ecto.Interval[] = x) do
    Ecto.DateTime[year: x.year, month: x.month, day: x.day,
                  hour: x.hour, min: x.min, sec: x.sec]
  end
  
  def to_datetime({ year, month, day }) do
    Ecto.DateTime.new(year: year, month: month, day: day)
  end

  def to_datetime({ { year, month, day }, { hour, min, sec } }) do
    Ecto.DateTime.new(year: year, month: month, day: day, 
                      hour: hour, min: min, sec: sec)
  end

  @spec to_interval(dt) :: ecto_dt

  def to_interval(x) when is_binary(x) do
    parse_string(x) |> to_datetime
  end

  def to_interval(Ecto.Interval[] = x), do: x
  
  def to_interval(Ecto.DateTime[] = x) do
    Ecto.Interval[year: x.year, month: x.month, day: x.day,
                  hour: x.hour, min: x.min, sec: x.sec]
  end
  
  def to_interval({ year, month, day }) do
    Ecto.Interval.new(year: year, month: month, day: day)
  end

  def to_interval({ { year, month, day }, { hour, min, sec } }) do
    Ecto.Interval.new(year: year, month: month, day: day, 
                      hour: hour, min: min, sec: sec)
  end

  ## ------------------------------------------------------------
  ## Predicates and Guards
  ## ------------------------------------------------------------

  # defmacro ecto_timestamp?(x) do
  #   quote do
  #     (is_record(unquote(x), Ecto.DateTime) or is_record(unquote(x), Ecto.Interval))
  #   end
  # end

  def ecto_timestamp?(x) do
    is_record(x, Ecto.DateTime) || is_record(x, Ecto.Interval)
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
    if is_record(x, Ecto.DateTime) do
      datetime?({ { x.year, x.month, x.day }, { x.hour, x.min, x.sec } })
    else
      false
    end
  end

  def datetime?(x) when is_tuple(x) do
    case x do
      { date, time } ->
        date?(date) && time?(time)
      _ ->
        false
    end
  end

  def date?(x) do
    case x do
      { year,month,day } ->
        year?(year) && month?(month) && day?(day)
      _ ->
        false
    end
  end

  def time?(x) do
    case x do
      { hr, min, sec } ->
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
  ## Solr Specific

  def solr_datetime(x) when x == "NOW" do
    solr_datetime(now)
  end

  def solr_datetime(x) when is_binary(x), do: x

  def solr_datetime(Ecto.DateTime[] = x) do
    ecto_to_erl(x) |> solr_datetime
  end

  def solr_datetime(Ecto.Interval[] = x) do
    ecto_to_erl(x) |> solr_datetime
  end

  def solr_datetime({ year, month, day } = arg) do
    cond do
      year?(year) && month?(month) && day?(day) ->
        "#{pad(year, 4)}-#{pad(month, 2)}-#{pad(day, 2)}T00:00:00Z"
      true ->
        raise Ecto.Adapters.Riak.DateTimeError,
          message: "invalid solr_datetime: #{inspect arg}"
    end
  end

  def solr_datetime({ { _, _, _ }, { _, _, _ } } = dt) do
    to_str(dt)
  end

  @spec solr_datetime_add(binary | ecto_dt) :: binary

  def solr_datetime_add(x) when is_binary(x), do: x

  def solr_datetime_add(x) do
    { { year, month, day }, { hour, min, sec } } = ecto_to_erl(x)
    "+#{year}YEARS+#{month}MONTHS+#{day}DAYS+#{hour}HOURS+#{min}MINUTES+#{sec}SECONDS"
  end

  @spec solr_datetime_subtract(binary | ecto_type) :: binary
  
  def solr_datetime_subtract(x) when is_binary(x), do: x

  def solr_datetime_subtract(x) do
    { { year, month, day }, { hour, min, sec } } = ecto_to_erl(x)
    "-#{year}YEARS-#{month}MONTHS-#{day}DAYS-#{hour}HOURS-#{min}MINUTES-#{sec}SECONDS"
  end

  defp ecto_to_erl(x) do
    year  = if x.year  != nil, do: x.year,  else: 0
    month = if x.month != nil, do: x.month, else: 0
    day   = if x.day   != nil, do: x.day,   else: 0
    hour  = if x.hour  != nil, do: x.hour,  else: 0
    min   = if x.min   != nil, do: x.min,   else: 0
    sec   = if x.sec   != nil, do: x.sec,   else: 0
    { { year, month, day }, { hour, min, sec } }
  end

  ## ----------------------------------------------------------------------
  ## Util

  defp parse_string(nil), do: nil

  defp parse_string(x) do
    [ year, month, day, hour, min, sec ] =
      String.split(x, ~r"-|:|T|Z", trim: true)
      |> Enum.map(fn bin ->
           { int, _ } = Integer.parse(bin)
           int
         end)
    { { year, month, day }, { hour, min, sec} }
  end

  defp pad(int, padding) do
    str = to_string(int)
    padding = max(padding-byte_size(str), 0)
    do_pad(str, padding)
  end

  defp do_pad(str, 0), do: str
  defp do_pad(str, n), do: do_pad("0" <> str, n-1)

end