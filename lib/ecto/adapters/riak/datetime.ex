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
  @type ecto_dt   :: Ecto.Datetime

  @spec parse(binary) :: datetime
  def parse(x) do
    :iso8601.parse(x)
  end

  @spec parse_to_ecto_datetime(binary) :: ecto_dt
  def parse_to_ecto_datetime(x) do
    {{year, mon, day}, {hour, min, sec}} = parse(x)
    Ecto.Datetime.new(year: year, month: mon, day: day, 
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

  @spec now_string() :: binary
  def now_string() do
    datetime_to_string(:os.timestamp)
  end  
  
  @spec datetime_to_string(datetime) :: binary
  def datetime_to_string({_,_,_}=x) do
    datetime_to_string({x,{0,0,0}})
  end
  
  def datetime_to_string(x) when is_record(x, Ecto.Datetime) do
    datetime_to_string({{x.year, x.month, x.day},
                        {x.hour, x.min, x.second}})
  end

  def datetime_to_string(x) do
    :iso8601.format(datetime_to_timestamp(x))
  end
  
  ## ------------------------------------------------------------
  ## Predicates
  ## ------------------------------------------------------------

  def datetime?(x) when is_record(x, Ecto.Datetime) do
    datetime?({{x.year, x.month, x.day}, {x.hour, x.min, x.second}})
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

end