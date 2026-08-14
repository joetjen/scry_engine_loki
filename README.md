# scry_engine_loki

A real [`Scry.Core.EngineBehaviour`](https://github.com/joetjen/scry_core)
implementation over [Loki](https://grafana.com/oss/loki/), via
[`req`](https://hex.pm/packages/req) -- no dedicated Elixir driver
exists (Loki's own query API is plain HTTP). The `time-series` kind's
*third* real backend, after
[`scry_engine_ch`](https://github.com/joetjen/scry_engine_ch)/
[`scry_engine_redistimeseries`](https://github.com/joetjen/scry_engine_redistimeseries)
-- and the first over genuinely non-numeric sample content: a Loki
entry's own "value" is a log *line* (arbitrary text), not a number.

`scry_time_series`'s own `LAST`-lowering pass already rewrites `LAST
<duration> OF <field>`/`LAST <from> TO <to> OF <field>` into an
ordinary `WHERE` predicate (or two, `AND`-combined) before this module
-- or any engine -- ever sees the query, the identical "zero time-
series-specific code needed" finding `scry_engine_ch`/`scry_engine_
redistimeseries`/`scry_engine_postgrex`'s own TimescaleDB validation
already established three times over; this package's only real work is
translating the resulting timestamp-range `WHERE` into LogQL's own
`query_range` bounds.

Source: <https://github.com/joetjen/scry_engine_loki>. Specs live in
the separate [`scry`](https://github.com/joetjen/scry) repository; the
behaviour this implements lives in
[`scry_core`](https://github.com/joetjen/scry_core).

## Usage

```elixir
{:ok, conn} = Scry.Engine.Loki.Conn.open()

{:ok, query} = Scry.Core.parse(~s(SELECT myapp WHERE timestamp >= "2026-01-01T00:00:00Z" { line }))
{:ok, cursor} = Scry.Core.Executor.run(query, Scry.Engine.Loki, conn)
rows = Scry.Core.Cursor.to_list(cursor)
# rows == [%{"line" => "..."}]
```

Pushing log lines is entirely the caller's own job -- this package is
schema-agnostic and issues nothing but `GET /loki/api/v1/query_range`/
`GET /loki/api/v1/series` reads.

### Local development / running the test suite

```sh
docker run -d --name scry-loki -p 3100:3100 grafana/loki:3.0.0 -config.file=/etc/loki/local-config.yaml
```

## `source` maps onto one Loki label value, not a full selector

`SELECT myapp { ... }` becomes the LogQL stream selector
`{job="myapp"}` -- `job` (configurable per `Conn.open/1`, since real
deployments vary) is this package's own chosen "primary identifier"
label, the same "one name maps onto one primary identifier" convention
every adapter in this family already has. Any *other* label a `WHERE`
clause names as an ordinary equality (`WHERE level = "error"`) folds
into the same selector as an optimization, not a requirement -- see
"Extraction, not translation" below.

## Two row fields fixed by this package, plus every real label

Every row exposes `"timestamp"` (a real `DateTime.t()`, decoded from
Loki's own Unix-nanosecond string) and `"line"` (the raw log text) --
deliberately not named `"value"` the way `scry_engine_redistimeseries`
names its own numeric sample field, since forcing that name onto text
content would misrepresent what it actually is. Every label Loki itself
attaches to the matched stream (`job`, plus whatever else a real
deployment's own pipeline sets) rides along as an ordinary string-
valued row field too.

## Extraction, not translation -- a deliberate divergence from `scry_engine_redistimeseries`

That package's own `RangeQuery` declines outright the moment a
predicate doesn't reduce to a single `timestamp`/`value` range, because
a RedisTimeSeries key genuinely has *no other real field* -- anything
else is a query error waiting to happen regardless. A Loki row has
real, legitimate fields beyond `"timestamp"`/`"line"` (every label Loki
itself attaches), so `Scry.Engine.Loki.RangeQuery` only ever *extracts*
what it can safely fold into the LogQL request itself (the required
timestamp lower bound, an optional upper bound, and any `WHERE <label>
= <literal>` equality it finds) and leaves `wheres` completely
untouched for `Scry.Core.QueryOps.run_flat/3` to re-evaluate afterward.
A construct this module doesn't recognize (an `OR`/`NOT` node, an
`:in` leaf, a non-equality comparison on a label) is simply *not
extracted* -- never an error -- since `run_flat/3` still applies it
correctly regardless of what did or didn't make it into the selector.

## An explicit lower time bound is required, not optional

Loki itself enforces a maximum query time range (confirmed directly: a
real server rejects an epoch-to-now request with `"query time range
exceeds the limit"`), and silently applies its *own* default window
when `start`/`end` are omitted entirely (confirmed directly: a bound-
free `query_range` call still returns `200`, just scoped to whatever
Loki's own default is) -- there is no default window this package could
pick that's honest for every deployment. So a query naming no
`"timestamp"` lower bound at all declines outright
(`{:unsupported, :missing_time_lower_bound}`) rather than guessing --
supply one via `LAST` (which always lowers to a `:ge` predicate) or an
ordinary `WHERE timestamp >= ...`. An explicit `limit` (5000) is always
sent to `query_range` -- Loki's own default (100 lines) is exactly the
silent-truncation trap `scry_engine_elasticsearch`'s own default
`size: 10` already is -- and `execute/3` declines with
`{:query_error, {:result_window_exceeded, _}}` rather than silently
handing back a truncated page.

## Two real findings, not assumed

- **Loki rejects out-of-order pushes to an existing stream**, not
  (only) old-in-absolute-terms ones -- confirmed directly, re-running
  this package's own test suite against a long-lived container: the
  second run's own timestamps, only slightly older than the *first*
  run's already-ingested data for the identical label set, were
  rejected with `"entry too far behind"`. The test suite gives every
  run its own unique `job` label to sidestep this entirely.
- **Timestamp precision loss, real and stated**: Loki's own timestamps
  are nanosecond-precision; Elixir's native `DateTime.t()` is
  microsecond-precision internally -- confirmed directly, round-
  tripping a real nanosecond value through `DateTime.from_unix!/2`
  truncates its last three digits.

## Installation

```elixir
def deps do
  [
    {:scry_engine_loki, "~> 0.1.0"}
  ]
end
```

## Documentation

Documentation is generated with [ExDoc](https://github.com/elixir-lang/ex_doc):

- Released versions are published to [HexDocs](https://hexdocs.pm) once the
  package ships, at <https://hexdocs.pm/scry_engine_loki>.
