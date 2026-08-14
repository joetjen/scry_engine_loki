defmodule Scry.Engine.LokiTest do
  @moduledoc """
  `Scry.Engine.Loki` -- confirms `execute/3` translates a `LAST`-lowered
  (or hand-written) timestamp-range `WHERE` into real LogQL `query_range`
  bounds and executes it against a real Loki container, that a query
  with no extractable lower bound at all declines outright rather than
  guessing a default, that an ordinary label equality both folds into
  the stream selector *and* still composes correctly with everything
  else (`GROUP BY`/`ORDER BY`/`LIMIT`/`OFFSET`/projection, all applied
  generically via `Scry.Core.QueryOps.run_flat/3`), and that
  `%Scry.Core.CombinedQuery{}`/a `WITH`-bound source both resolve via
  `Scry.Core.QueryOps.run_document/4` -- all against a real
  `grafana/loki:3.0.0` container, not just plausible-looking output.

  **Requires a real, reachable Loki instance** -- run one locally via
  `docker run -d --name scry-loki -p 3100:3100 grafana/loki:3.0.0
  -config.file=/etc/loki/local-config.yaml`. Runs `async: false` --
  every test shares one real server and a small, fixed set of log
  lines pushed once in `setup_all`, all timestamped relative to one
  captured `now` so every test's own `WHERE` bound is exact and
  deterministic rather than racing the real wall clock.

  **The `job` label is unique per test run, not a fixed literal** -- a
  real, confirmed finding, not a style choice: Loki rejects an
  *out-of-order* push to an existing stream (a timestamp older than
  what that exact label set has already ingested), confirmed directly
  by re-running this suite against a long-lived container -- the
  second run's own older-relative-to-the-first-run's-already-ingested-
  data timestamps were rejected with `"entry too far behind"`, even
  though they were only slightly (not implausibly) old in absolute
  terms. `System.unique_integer/1` in the `job` label sidesteps this
  entirely -- every run gets its own, never-before-seen stream.
  """

  use ExUnit.Case, async: false

  alias Scry.Core.{CombinedQuery, Query}
  alias Scry.Engine.Loki, as: Engine
  alias Scry.Engine.Loki.Conn

  setup_all do
    {:ok, conn} = Conn.open()
    now = DateTime.utc_now()
    job = "scry_test_app_#{System.unique_integer([:positive])}"
    seed!(conn, job, now)
    %{conn: conn, now: now, job: job}
  end

  defp seed!(conn, job, now) do
    push_at(conn, job, now, -300, "older line", %{"level" => "info"})
    push_at(conn, job, now, -120, "recent error", %{"level" => "error"})
    push_at(conn, job, now, -30, "recent info", %{"level" => "info"})
  end

  defp push_at(conn, job, now, offset_seconds, line, extra_labels) do
    ts_ns = DateTime.to_unix(now, :nanosecond) + offset_seconds * 1_000_000_000
    labels = Map.merge(%{"job" => job}, extra_labels)
    :ok = Conn.push(conn, labels, [{ts_ns, line}])
  end

  defp materialize({:ok, rows}), do: {:ok, rows |> Enum.to_list()}
  defp materialize(other), do: other

  defp since(now, offset_seconds), do: DateTime.add(now, offset_seconds, :second)

  describe "an explicit lower bound is required" do
    test "no WHERE at all declines outright", %{conn: conn, job: job} do
      query = %Query{source: [job], select: [{:field, ["line"]}]}

      assert {:error, {:unsupported, :missing_time_lower_bound}} =
               materialize(Engine.execute(conn, query, %{}))
    end
  end

  describe "an ordinary timestamp lower bound fetches everything since then" do
    test "reaches every log line since the given instant", %{conn: conn, now: now, job: job} do
      query = %Query{
        source: [job],
        wheres: [{:cmp, :ge, ["timestamp"], since(now, -400)}],
        select: [{:field, ["line"]}]
      }

      assert {:ok, rows} = materialize(Engine.execute(conn, query, %{}))

      assert Enum.map(rows, & &1["line"]) |> Enum.sort() ==
               ["older line", "recent error", "recent info"]
    end

    test "a tighter lower bound excludes the older line", %{conn: conn, now: now, job: job} do
      query = %Query{
        source: [job],
        wheres: [{:cmp, :ge, ["timestamp"], since(now, -200)}],
        select: [{:field, ["line"]}]
      }

      assert {:ok, rows} = materialize(Engine.execute(conn, query, %{}))
      assert Enum.map(rows, & &1["line"]) |> Enum.sort() == ["recent error", "recent info"]
    end

    test "an explicit upper bound narrows further", %{conn: conn, now: now, job: job} do
      query = %Query{
        source: [job],
        wheres: [
          {:cmp, :ge, ["timestamp"], since(now, -200)},
          {:cmp, :le, ["timestamp"], since(now, -60)}
        ],
        select: [{:field, ["line"]}]
      }

      assert {:ok, [row]} = materialize(Engine.execute(conn, query, %{}))
      assert row == %{"line" => "recent error"}
    end
  end

  describe "label equality folds into the LogQL selector and still composes correctly" do
    test "WHERE level = ... narrows to matching entries only", %{conn: conn, now: now, job: job} do
      query = %Query{
        source: [job],
        wheres: [
          {:cmp, :ge, ["timestamp"], since(now, -400)},
          {:cmp, :eq, ["level"], "error"}
        ],
        select: [{:field, ["line"]}]
      }

      assert {:ok, [row]} = materialize(Engine.execute(conn, query, %{}))
      assert row == %{"line" => "recent error"}
    end

    test "an unmatched label value returns an empty result, not an error", %{
      conn: conn,
      now: now,
      job: job
    } do
      query = %Query{
        source: [job],
        wheres: [
          {:cmp, :ge, ["timestamp"], since(now, -400)},
          {:cmp, :eq, ["level"], "critical"}
        ],
        select: [{:field, ["line"]}]
      }

      assert {:ok, []} = materialize(Engine.execute(conn, query, %{}))
    end
  end

  describe "GROUP BY/ORDER BY/LIMIT/OFFSET/projection all apply generically" do
    test "GROUP BY/aggregate works", %{conn: conn, now: now, job: job} do
      query = %Query{
        source: [job],
        wheres: [{:cmp, :ge, ["timestamp"], since(now, -400)}],
        group_bys: [["level"]],
        select: [
          {:field, ["level"]},
          {:computed, "total", {:call, "count", [{:field, ["line"]}]}}
        ]
      }

      assert {:ok, rows} = materialize(Engine.execute(conn, query, %{}))
      by_level = Map.new(rows, &{&1["level"], &1["total"]})
      assert by_level == %{"info" => 2, "error" => 1}
    end

    test "ORDER BY + LIMIT compose", %{conn: conn, now: now, job: job} do
      query = %Query{
        source: [job],
        wheres: [{:cmp, :ge, ["timestamp"], since(now, -400)}],
        order_bys: [{{:field, ["timestamp"]}, :desc}],
        limit: 1,
        select: [{:field, ["line"]}]
      }

      assert {:ok, [row]} = materialize(Engine.execute(conn, query, %{}))
      assert row == %{"line" => "recent info"}
    end
  end

  describe "%Scry.Core.CombinedQuery{} and a WITH-bound source" do
    test "CombinedQuery delegates to Scry.Core.QueryOps.run_document/4", %{
      conn: conn,
      now: now,
      job: job
    } do
      base_wheres = [{:cmp, :ge, ["timestamp"], since(now, -400)}]

      left = %Query{
        source: [job],
        wheres: base_wheres ++ [{:cmp, :eq, ["level"], "error"}],
        select: [{:field, ["line"]}]
      }

      right = %Query{
        source: [job],
        wheres: base_wheres ++ [{:cmp, :eq, ["line"], "older line"}],
        select: [{:field, ["line"]}]
      }

      combined = %CombinedQuery{op: :union, left: left, right: right}

      assert {:ok, rows} = materialize(Engine.execute(conn, combined, %{}))
      assert rows |> Enum.map(& &1["line"]) |> Enum.sort() == ["older line", "recent error"]
    end

    test "a WITH-bound top-level source runs the binding instead of a real stream selector", %{
      conn: conn,
      now: now,
      job: job
    } do
      binding = %Query{
        source: [job],
        wheres: [
          {:cmp, :ge, ["timestamp"], since(now, -400)},
          {:cmp, :eq, ["level"], "error"}
        ],
        select: [{:field, ["line"]}]
      }

      query = %Query{
        source: ["errors_only"],
        with_bindings: %{"errors_only" => binding},
        select: [{:field, ["line"]}]
      }

      assert {:ok, rows} = materialize(Engine.execute(conn, query, %{}))
      assert Enum.map(rows, & &1["line"]) == ["recent error"]
    end
  end

  describe "describe_source/2" do
    test "reports the two fixed fields plus every real label observed", %{conn: conn, job: job} do
      assert {:ok, fields} = Engine.describe_source(conn, job)
      by_name = Map.new(fields, &{&1.name, &1})

      assert by_name["timestamp"].nullable == false
      assert by_name["line"].scalar == :string
      assert by_name["line"].nullable == false
      assert by_name["job"].nullable == true
      assert by_name["level"].nullable == true
    end

    test "a job with no observed streams at all is not found", %{conn: conn} do
      assert {:error, :not_found} = Engine.describe_source(conn, "no_such_job")
    end
  end
end
