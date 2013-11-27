defmodule TywUtil.Datetime do
  @range_month    1..12
  @range_day      1..31
  @range_hour     0..23
  @range_min      0..59
  @range_sec      0..59
  
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

  def parse(x) when is_binary(x) or is_list(x) do
    :iso8601.parse(x)
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
  
  @spec now() :: datetime
  def now(), do: :calendar.now_to_universal_time(:os.timestamp)

  @spec now_string() :: binary
  def now_string() do
    to_string(:os.timestamp)
  end
  
  @spec now_unix() :: integer
  def now_unix() do
    {mega_sec, sec, _} = :os.timestamp
    mega_sec * 1000000 + sec
  end
  
  @spec to_string(datetime | date) :: binary
  def to_string({_,_,_}=x), do: to_binary({x,{0,0,0}})
  def to_string(x) do
    :iso8601.format(datetime_to_timestamp(x))
  end
  
  ## ------------------------------------------------------------
  ## Predicates
  ## ------------------------------------------------------------
  
  def datetime?(x) do
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

  def month?(x), do: x in @range_month

  def day?(x), do: x in @range_day

  def hour?(x), do: x in @range_hour

  def minute?(x), do: x in @range_min

  def second?(x), do: x in @range_sec

  @type msec_unit :: :year | :month | :week | :day | :hour | :minute | :second
  @spec msec(integer, msec_unit) :: integer
  def msec(x, :year),   do: msec(x*365, :day)
  def msec(x, :month),  do: msec(x*30, :day)
  def msec(x, :week),   do: msec(x*7, :day)
  def msec(x, :day),    do: msec(x*24, :hour)
  def msec(x, :hour),   do: msec(x*60, :minute)
  def msec(x, :minute), do: msec(x*60, :second)
  def msec(x, :second), do: x*1000
  def msec(x, _),       do: x

end