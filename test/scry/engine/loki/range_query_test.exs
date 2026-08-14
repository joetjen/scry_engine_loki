defmodule Scry.Engine.Loki.RangeQueryTest do
  use ExUnit.Case, async: true

  alias Scry.Engine.Loki.RangeQuery

  describe "compile/4 -- timestamp bounds" do
    test "a lone lower bound defaults the upper bound to now" do
      lower = DateTime.new!(~D[2026-01-01], ~T[00:00:00])
      wheres = [{:cmp, :ge, ["timestamp"], lower}]

      assert {:ok, %{start_ns: start_ns, end_ns: end_ns, selector: selector}} =
               RangeQuery.compile(wheres, "job", "myapp", %{})

      assert start_ns == DateTime.to_unix(lower, :nanosecond)
      assert end_ns <= DateTime.to_unix(DateTime.utc_now(), :nanosecond)
      assert selector == ~s({job="myapp"})
    end

    test "an explicit range (AND of ge/le) sets both bounds" do
      from = DateTime.new!(~D[2026-01-01], ~T[00:00:00])
      to = DateTime.new!(~D[2026-01-02], ~T[00:00:00])
      wheres = [{:and, {:cmp, :ge, ["timestamp"], from}, {:cmp, :le, ["timestamp"], to}}]

      assert {:ok, %{start_ns: start_ns, end_ns: end_ns}} =
               RangeQuery.compile(wheres, "job", "myapp", %{})

      assert start_ns == DateTime.to_unix(from, :nanosecond)
      assert end_ns == DateTime.to_unix(to, :nanosecond)
    end

    test "gt/lt adjust by exactly one nanosecond" do
      from = DateTime.new!(~D[2026-01-01], ~T[00:00:00])
      to = DateTime.new!(~D[2026-01-02], ~T[00:00:00])
      wheres = [{:and, {:cmp, :gt, ["timestamp"], from}, {:cmp, :lt, ["timestamp"], to}}]

      assert {:ok, %{start_ns: start_ns, end_ns: end_ns}} =
               RangeQuery.compile(wheres, "job", "myapp", %{})

      assert start_ns == DateTime.to_unix(from, :nanosecond) + 1
      assert end_ns == DateTime.to_unix(to, :nanosecond) - 1
    end

    test "multiple bounds on the same side narrow to the tightest" do
      earlier = DateTime.new!(~D[2026-01-01], ~T[00:00:00])
      later = DateTime.new!(~D[2026-01-02], ~T[00:00:00])
      wheres = [{:cmp, :ge, ["timestamp"], earlier}, {:cmp, :ge, ["timestamp"], later}]

      assert {:ok, %{start_ns: start_ns}} = RangeQuery.compile(wheres, "job", "myapp", %{})
      assert start_ns == DateTime.to_unix(later, :nanosecond)
    end

    test ":eq sets both bounds to the same instant" do
      at = DateTime.new!(~D[2026-01-01], ~T[00:00:00])
      wheres = [{:cmp, :eq, ["timestamp"], at}]

      assert {:ok, %{start_ns: start_ns, end_ns: end_ns}} =
               RangeQuery.compile(wheres, "job", "myapp", %{})

      assert start_ns == end_ns
      assert start_ns == DateTime.to_unix(at, :nanosecond)
    end

    test "no lower bound at all declines outright" do
      assert {:error, {:unsupported, :missing_time_lower_bound}} =
               RangeQuery.compile([], "job", "myapp", %{})
    end

    test "a lower bound hidden inside an OR is not extracted, and still declines" do
      lower = DateTime.new!(~D[2026-01-01], ~T[00:00:00])
      wheres = [{:or, {:cmp, :ge, ["timestamp"], lower}, {:cmp, :eq, ["line"], "x"}}]

      assert {:error, {:unsupported, :missing_time_lower_bound}} =
               RangeQuery.compile(wheres, "job", "myapp", %{})
    end

    test "a {:param, name} threshold resolves against params" do
      lower = DateTime.new!(~D[2026-01-01], ~T[00:00:00])
      wheres = [{:cmp, :ge, ["timestamp"], {:param, "since"}}]

      assert {:ok, %{start_ns: start_ns}} =
               RangeQuery.compile(wheres, "job", "myapp", %{"since" => lower})

      assert start_ns == DateTime.to_unix(lower, :nanosecond)
    end

    test "a missing param is a clear query error" do
      wheres = [{:cmp, :ge, ["timestamp"], {:param, "since"}}]

      assert {:error, {:query_error, {:missing_param, "since"}}} =
               RangeQuery.compile(wheres, "job", "myapp", %{})
    end
  end

  describe "compile/4 -- label equality folding" do
    test "an ordinary label equality folds into the selector" do
      lower = DateTime.new!(~D[2026-01-01], ~T[00:00:00])
      wheres = [{:cmp, :ge, ["timestamp"], lower}, {:cmp, :eq, ["level"], "error"}]

      assert {:ok, %{selector: selector}} = RangeQuery.compile(wheres, "job", "myapp", %{})
      assert selector == ~s({job="myapp", level="error"})
    end

    test "a label value containing a quote or backslash is escaped" do
      lower = DateTime.new!(~D[2026-01-01], ~T[00:00:00])
      wheres = [{:cmp, :ge, ["timestamp"], lower}, {:cmp, :eq, ["level"], ~s(a"b\\c)}]

      assert {:ok, %{selector: selector}} = RangeQuery.compile(wheres, "job", "myapp", %{})
      assert selector == ~s({job="myapp", level="a\\"b\\\\c"})
    end

    test "a non-equality comparison on a label is not extracted, but doesn't fail compilation" do
      lower = DateTime.new!(~D[2026-01-01], ~T[00:00:00])
      wheres = [{:cmp, :ge, ["timestamp"], lower}, {:cmp, :not_eq, ["level"], "debug"}]

      assert {:ok, %{selector: selector}} = RangeQuery.compile(wheres, "job", "myapp", %{})
      assert selector == ~s({job="myapp"})
    end
  end
end
