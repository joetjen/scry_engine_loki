defmodule Scry.Engine.Loki do
  @moduledoc """
  A real `Scry.Core.EngineBehaviour` implementation over
  [Loki](https://grafana.com/oss/loki/), via
  [`req`](https://hex.pm/packages/req) -- no dedicated Elixir driver
  exists (Loki's own query API is plain HTTP), the identical situation
  `scry_engine_elasticsearch`/`scry_engine_couchdb` already resolved
  the same way. The `time-series` kind's *third* real backend, after
  `scry_engine_ch`/`scry_engine_redistimeseries` -- and the first over
  genuinely non-numeric sample content: a Loki entry's own "value" is a
  log *line* (arbitrary text), not a number, unlike every prior
  time-series adapter in this family. `scry_time_series`'s own `LAST`-
  lowering pass already rewrites `LAST <duration> OF <field>`/`LAST
  <from> TO <to> OF <field>` into an ordinary `WHERE` predicate (or two,
  `AND`-combined) before this module -- or any engine -- ever sees the
  query, the identical "zero time-series-specific code needed" finding
  `scry_engine_ch`/`scry_engine_redistimeseries`/`scry_engine_postgrex`'s
  own TimescaleDB validation already established three times over; this
  package's only real work is translating the resulting timestamp-range
  `WHERE` into LogQL's own `query_range` bounds (`Scry.Engine.Loki.
  RangeQuery`).

  ## `source` maps onto one Loki label value, not a full selector

  `SELECT myapp { ... }` becomes the LogQL stream selector
  `{job="myapp"}` -- `job` (configurable per `Scry.Engine.Loki.Conn.
  open/1`, since real deployments vary) is this package's own chosen
  "primary identifier" label, the same "one name maps onto one primary
  identifier" convention every adapter in this family already has
  (a SQL table, an index, a label, a collection). Any *other* label a
  `WHERE` clause names as an ordinary equality (`WHERE level = "error"`)
  folds into the same selector as an optimization, not a requirement --
  `Scry.Engine.Loki.RangeQuery`'s own moduledoc has the full "why
  extraction failure is never an error here" reasoning, a deliberate,
  stated divergence from `scry_engine_redistimeseries`'s own stricter
  `RangeQuery`.

  ## Two rows fields fixed by this package, plus every real label

  Every row exposes `"timestamp"` (a real `DateTime.t()`, decoded from
  Loki's own Unix-nanosecond string) and `"line"` (the raw log text) --
  deliberately not named `"value"` the way `scry_engine_redistimeseries`
  names its own numeric sample field, since forcing that name onto text
  content would misrepresent what it actually is. Every label Loki
  itself attaches to the matched stream (`job`, plus whatever else a
  real deployment's own pipeline sets) rides along as an ordinary
  string-valued row field too -- an honest, direct representation of
  Loki's own data model, not a lossy projection of it.

  ## An explicit lower time bound is required, not optional

  `Scry.Engine.Loki.RangeQuery`'s own moduledoc has the full reasoning:
  Loki enforces a real maximum query time range and silently applies
  its own default window when none is given at all, so a query with no
  extractable `"timestamp"` lower bound declines outright
  (`{:unsupported, :missing_time_lower_bound}`) rather than guessing.
  An explicit `limit` (currently #{5000}) is always sent to `query_range`
  -- Loki's own default (100 lines) is exactly the silent-truncation
  trap `scry_engine_elasticsearch`'s own default `size: 10` already is
  -- and `execute/3` declines with `{:query_error, {:result_window_exceeded,
  _}}` rather than silently handing back a truncated page whenever the
  real result reaches that limit exactly.

  ## A real, stated precision limit

  Loki's own timestamps are nanosecond-precision; Elixir's native
  `DateTime.t()` is microsecond-precision internally -- confirmed
  directly, round-tripping a real nanosecond value through `DateTime.
  from_unix!/2` truncates its last three digits. Two log lines within
  the same stream differing only in their own sub-microsecond arrival
  time are indistinguishable once decoded into a row -- a real,
  documented limit, not silently relied upon.
  """

  @behaviour Scry.Core.EngineBehaviour

  alias Scry.Core.{CombinedQuery, Query, QueryOps}
  alias Scry.Engine.Loki.{Conn, RangeQuery}

  @limit 5000
  @describe_window_ns 24 * 60 * 60 * 1_000_000_000

  @impl true
  def execute(conn, %CombinedQuery{} = combined, params),
    do: QueryOps.run_document(conn, combined, params, __MODULE__)

  def execute(%Conn{} = conn, %Query{source: source} = query, params) do
    if with_bound_source?(query) do
      QueryOps.run_document(conn, query, params, __MODULE__)
    else
      execute_flat(conn, source, query, params)
    end
  end

  defp with_bound_source?(%Query{source: [name], with_bindings: with_bindings}),
    do: Map.has_key?(with_bindings, name)

  defp with_bound_source?(_query), do: false

  defp execute_flat(conn, source, query, params) do
    with {:ok, source_value} <- source_value(source),
         {:ok, compiled} <- RangeQuery.compile(query.wheres, conn.label_key, source_value, params),
         {:ok, response} <-
           Conn.query_range(conn, compiled.selector, compiled.start_ns, compiled.end_ns, @limit) do
      rows = decode_rows(response)

      if length(rows) >= @limit do
        {:error, {:query_error, {:result_window_exceeded, length(rows)}}}
      else
        QueryOps.run_flat(rows, query, params)
      end
    end
  end

  defp source_value([name]) when is_binary(name) and byte_size(name) > 0, do: {:ok, name}
  defp source_value(source), do: {:error, {:unsupported, {:source, source}}}

  defp decode_rows(%{"data" => %{"result" => streams}}) do
    Enum.flat_map(streams, fn %{"stream" => labels, "values" => values} ->
      Enum.map(values, &decode_entry(labels, &1))
    end)
  end

  defp decode_rows(_other), do: []

  defp decode_entry(labels, [ts_ns_str, line]) do
    labels
    |> Map.put("timestamp", decode_timestamp(ts_ns_str))
    |> Map.put("line", line)
  end

  defp decode_timestamp(ns_str) do
    {ns, ""} = Integer.parse(ns_str)
    DateTime.from_unix!(ns, :nanosecond)
  end

  @doc """
  `Scry.Core.EngineBehaviour`'s optional `describe_source/2` callback --
  reports the two fixed fields (`"timestamp"`, `"line"`) plus every real
  label observed across `source`'s own streams in the last 24 hours
  (`GET /loki/api/v1/series`, the same bounded-recent-window posture
  `Mongo`/`CouchDB`'s own sampling-based introspection already takes for
  a schemaless store). `nullable: true` for every label -- confirmed
  directly, two entries in the same named stream can carry different
  companion labels (a real deployment's own `pod`/`instance` labels
  vary run to run even when `job` doesn't) -- `nullable: false` only for
  `"timestamp"`/`"line"`, always present on every real entry Loki's own
  protocol returns at all.
  """
  @impl true
  @spec describe_source(Conn.t(), String.t()) ::
          {:ok, [Scry.Core.EngineBehaviour.introspected_field()]}
          | {:error, :not_found}
          | {:error, {:introspection_error, term()}}
  def describe_source(%Conn{} = conn, source) do
    case series(conn, source) do
      {:ok, []} -> {:error, :not_found}
      {:ok, series} -> {:ok, fields_from_series(series)}
      {:error, {:query_error, reason}} -> {:error, {:introspection_error, reason}}
    end
  end

  defp series(%Conn{base_url: base_url, label_key: label_key}, source) do
    now_ns = DateTime.to_unix(DateTime.utc_now(), :nanosecond)
    selector = ~s({#{label_key}="#{source}"})

    params = [
      "match[]": selector,
      start: now_ns - @describe_window_ns,
      end: now_ns
    ]

    case Req.get(base_url <> "/loki/api/v1/series", params: params) do
      {:ok, %Req.Response{status: 200, body: %{"data" => data}}} -> {:ok, data}
      {:ok, %Req.Response{body: body}} -> {:error, {:query_error, body}}
      {:error, reason} -> {:error, {:query_error, reason}}
    end
  end

  defp fields_from_series(series) do
    label_fields =
      series
      |> Enum.reduce(MapSet.new(), fn labels, acc -> Enum.into(Map.keys(labels), acc) end)
      |> Enum.map(&%{name: &1, nullable: true, scalar: :string})

    [
      %{name: "timestamp", nullable: false, scalar: :unknown},
      %{name: "line", nullable: false, scalar: :string} | label_fields
    ]
  end
end
