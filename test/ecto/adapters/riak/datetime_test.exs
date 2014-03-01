Code.require_file "test_helper.exs", __DIR__

defmodule Ecto.Adapters.Riak.DateTimeTest do
  use ExUnit.Case, async: true

  alias Ecto.Adapters.Riak.DateTime

  test "now" do
    assert erl_datetime?(DateTime.now)
    assert erl_datetime?(DateTime.now_local)
    assert Ecto.DateTime[] = DateTime.now_ecto
    assert Ecto.DateTime[] = DateTime.now_local_ecto
  end

  test "to_str" do
    dt = Ecto.DateTime[year: 2000, month: 10, day: 10,
                       hour: 10, min: 10, sec: 10]
    assert "2000-10-10T10:10:10Z" == DateTime.to_str(dt)

    int = Ecto.Interval[year: 2001, month: 10, day: 10,
                        hour: 10, min: 10, sec: 10]
    assert interval_string?(DateTime.to_str(int))
    
    assert "2000-10-10T00:00:00Z" == 
           DateTime.to_str({ 2000, 10, 10 })
    assert "2000-10-10T10:10:10Z" == 
           DateTime.to_str({ { 2000, 10, 10 }, { 10, 10, 10 } })
  end

  test "to_datetime" do
    dt = Ecto.DateTime[year: 2000, month: 10, day: 10,
                       hour: 10, min: 10, sec: 10]
    assert dt == DateTime.to_datetime(dt)

    int = Ecto.Interval[year: 2000, month: 10, day: 10,
                        hour: 10, min: 10, sec: 10]
    assert dt == DateTime.to_datetime(int)

    assert Ecto.DateTime[year: 2000, month: 10, day: 10] =
           DateTime.to_datetime({ 2000, 10, 10 })

    assert Ecto.DateTime[year: 2000, month: 10, day: 10,
                         hour: 10, min: 9, sec: 8] =
           DateTime.to_datetime({ { 2000, 10, 10 }, { 10, 9, 8 } })
  end

  test "to_interval" do
    int = Ecto.Interval[year: 2000, month: 10, day: 10,
                        hour: 10, min: 10, sec: 10]
    assert int == DateTime.to_interval(int)
    
    dt = Ecto.DateTime[year: 2000, month: 10, day: 10,
                       hour: 10, min: 10, sec: 10]
    assert int == DateTime.to_interval(dt)

    assert Ecto.Interval[year: 2000, month: 10, day: 10] =
           DateTime.to_interval({ 2000, 10, 10 })

    assert Ecto.Interval[year: 2000, month: 10, day: 10,
                         hour: 10, min: 9, sec: 8] =
           DateTime.to_interval({ { 2000, 10, 10 }, { 10, 9, 8 } })
  end

  ## ----------------------------------------------------------------------
  ## Solr Opts

  test "solr_datetime" do
    dt = Ecto.DateTime[year: 2000, month: 10, day: 10,
                       hour: 10, min: 10, sec: 10]
    int = Ecto.Interval[year: 2001, month: 10, day: 10,
                        hour: 10, min: 10, sec: 10]
    
    assert datetime_string?(DateTime.solr_datetime("NOW"))

    assert "2000-10-10T10:10:10Z"
           == DateTime.solr_datetime(dt)
    assert "2001-10-10T10:10:10Z"
           == DateTime.solr_datetime(int)
    assert "2000-10-10T00:00:00Z" == DateTime.solr_datetime({ 2000, 10, 10 })
  end

  test "solr datetime add" do
    assert "+2YEARS+0MONTHS+1DAYS+0HOURS+0MINUTES+1SECONDS" ==
           DateTime.solr_datetime_add(Ecto.DateTime[year: 2, day: 1, sec: 1])
    
    assert "+0YEARS+1MONTHS+1DAYS+0HOURS+3MINUTES+0SECONDS" ==
           DateTime.solr_datetime_add(Ecto.Interval[month: 1, day: 1, min: 3])
  end

   test "solr datetime subtract" do
    assert "-2YEARS-0MONTHS-1DAYS-0HOURS-0MINUTES-1SECONDS" ==
           DateTime.solr_datetime_subtract(Ecto.DateTime[year: 2, day: 1, sec: 1])
    
    assert "-0YEARS-1MONTHS-1DAYS-0HOURS-3MINUTES-0SECONDS" ==
           DateTime.solr_datetime_subtract(Ecto.Interval[month: 1, day: 1, min: 3])
  end

  ## ----------------------------------------------------------------------
  ## Utils
  
  defp erl_datetime?({ { year, month, day }, { hour, min, sec } }) do
    DateTime.year?(year) &&
    DateTime.month?(month) &&
    DateTime.day?(day) &&
    DateTime.hour?(hour) &&
    DateTime.minute?(min) &&
    DateTime.second?(sec)
  end
  
  defp erl_datetime?(_), do: false

  defp datetime_string?(x) do
    <<year  :: [binary, size(4)], "-",
      month :: [binary, size(2)], "-",
      day   :: [binary, size(2)], "T",
      hour  :: [binary, size(2)], ":",
      min   :: [binary, size(2)], ":",
      sec   :: [binary, size(2)], "Z">> = x

    [year, month, day, hour, min, sec] =
      Enum.map([year, month, day, hour, min, sec], fn x ->
        { int, _ } = Integer.parse(x)
        int
      end)

    erl_datetime?({ { year, month, day }, { hour, min, sec } })
  end

  defp interval_string?(x), do: datetime_string?(x)
  
end