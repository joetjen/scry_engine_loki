defmodule Scry.Engine.Loki.RangeQuery do
  @moduledoc """
  Translates a `Scry.Core.Query.t()`'s own `wheres` into a LogQL stream
  selector plus `query_range`'s own `start`/`end` Unix-nanosecond
  bounds, for `Scry.Engine.Loki`'s own `execute/3`.

  ## A deliberately different posture from `scry_engine_redistimeseries`'s own `RangeQuery`

  That module declines outright the moment a predicate doesn't reduce
  to a single `timestamp`/`value` range, because a RedisTimeSeries key
  genuinely has *no other real field* -- anything else is a query error
  waiting to happen regardless. A Loki row has real, legitimate fields
  beyond `"timestamp"`/`"line"` (every label Loki itself attaches --
  `job`, `level`, whatever a real deployment's own log pipeline sets),
  so this module only ever *extracts* what it can safely fold into the
  LogQL request itself (the required timestamp lower bound, an optional
  upper bound, and any `WHERE <label> = <literal>` equality it finds)
  and leaves `wheres` completely untouched for `Scry.Core.QueryOps.
  run_flat/3` to re-evaluate afterward, exactly the same "safe even
  when redundant" posture `scry_engine_neo4j`/`scry_engine_mongodb_
  driver`/`scry_engine_couchdb` already established for not clearing
  anything they push down. A construct this module doesn't recognize
  (an `OR`/`NOT` node, an `:in` leaf, `:not_eq` on `"timestamp"`, a
  `WHERE` on a label using anything but `=`) is simply *not extracted*
  -- never an error -- since `run_flat/3` still applies it correctly
  regardless of what did or didn't make it into the selector.

  ## Why an explicit lower bound is required, not optional

  Loki itself enforces a maximum query time range (confirmed directly:
  a real server rejects an epoch-to-now request with `"query time range
  exceeds the limit"`), and silently applies its *own* default window
  when `start`/`end` are omitted entirely (confirmed directly: a
  bound-free `query_range` call still returns `200`, just scoped to
  whatever Loki's own default is) -- there is no default window this
  module could pick that's honest for every deployment (a small default
  risks silently missing older data exactly like Elasticsearch's own
  default `size: 10` already does; a large one risks the same `400`
  confirmed above, since the real server-side maximum isn't discoverable
  generically). So a query naming no `"timestamp"` lower bound at all
  declines outright (`{:unsupported, :missing_time_lower_bound}`) rather
  than guessing -- the caller supplies one, via `LAST` (which always
  lowers to a `:ge` predicate, lang_spec.md §8.2) or an ordinary `WHERE
  timestamp >= ...`. An upper bound is genuinely optional -- absent,
  it defaults to the current instant (`"as of now"` is always a safe,
  honest default the way "since the beginning of time" is not).

  ## Timestamp precision

  Every bound converts to Unix *nanoseconds* -- Loki's own native
  precision. A `DateTime.t()`/`NaiveDateTime.t()` value (what `LAST`
  itself always produces) converts via `DateTime.to_unix(dt,
  :nanosecond)` -- confirmed directly that this round-trips exactly for
  a value that already came from a nanosecond source, though Elixir's
  own `DateTime` struct is natively microsecond-precision internally,
  so a *literal* nanosecond value with genuine sub-microsecond
  significance would already have been truncated by the time it became
  a `DateTime.t()` in the first place -- a real, stated precision
  limit, not something this module could recover.
  """

  alias Scry.Core.Query

  @timestamp_field "timestamp"
  @line_field "line"

  @type compiled :: %{selector: String.t(), start_ns: integer(), end_ns: integer()}

  @spec compile([Query.predicate()], String.t(), String.t(), map()) ::
          {:ok, compiled()} | {:error, {:unsupported, term()}}
  def compile(wheres, label_key, source_value, params) do
    initial = %{start_ns: nil, end_ns: nil, label_eqs: %{}}

    with {:ok, bounds} <- collect(wheres, params, initial),
         {:ok, start_ns} <- require_lower_bound(bounds) do
      end_ns = bounds.end_ns || DateTime.to_unix(DateTime.utc_now(), :nanosecond)

      {:ok,
       %{
         selector: selector(label_key, source_value, bounds.label_eqs),
         start_ns: start_ns,
         end_ns: end_ns
       }}
    end
  end

  defp collect(predicates, params, acc) do
    Enum.reduce_while(predicates, {:ok, acc}, fn predicate, {:ok, acc2} ->
      case walk(predicate, params, acc2) do
        {:ok, _acc3} = ok -> {:cont, ok}
        {:error, _} = err -> {:halt, err}
      end
    end)
  end

  defp walk({:and, l, r}, params, acc) do
    with {:ok, acc2} <- walk(l, params, acc), do: walk(r, params, acc2)
  end

  defp walk({:cmp, op, [@timestamp_field], value}, params, acc)
       when op in [:eq, :ge, :gt, :le, :lt] do
    with {:ok, resolved} <- resolve_value(value, params),
         {:ok, ns} <- timestamp_ns(resolved) do
      {:ok, apply_bound(op, ns, acc)}
    end
  end

  defp walk({:cmp, :eq, [field], value}, params, acc)
       when field not in [@timestamp_field, @line_field] do
    with {:ok, resolved} <- resolve_value(value, params) do
      {:ok, %{acc | label_eqs: Map.put(acc.label_eqs, field, to_string(resolved))}}
    end
  end

  defp walk(_other, _params, acc), do: {:ok, acc}

  defp apply_bound(:eq, ns, acc), do: tighten(acc, start_ns: ns, end_ns: ns)
  defp apply_bound(:ge, ns, acc), do: tighten(acc, start_ns: ns)
  defp apply_bound(:gt, ns, acc), do: tighten(acc, start_ns: ns + 1)
  defp apply_bound(:le, ns, acc), do: tighten(acc, end_ns: ns)
  defp apply_bound(:lt, ns, acc), do: tighten(acc, end_ns: ns - 1)

  defp tighten(acc, start_ns: v), do: %{acc | start_ns: max_or(acc.start_ns, v)}
  defp tighten(acc, end_ns: v), do: %{acc | end_ns: min_or(acc.end_ns, v)}

  defp tighten(acc, start_ns: s, end_ns: e),
    do: %{acc | start_ns: max_or(acc.start_ns, s), end_ns: min_or(acc.end_ns, e)}

  defp max_or(nil, v), do: v
  defp max_or(current, v), do: max(current, v)
  defp min_or(nil, v), do: v
  defp min_or(current, v), do: min(current, v)

  defp require_lower_bound(%{start_ns: nil}),
    do: {:error, {:unsupported, :missing_time_lower_bound}}

  defp require_lower_bound(%{start_ns: start_ns}), do: {:ok, start_ns}

  defp resolve_value({:param, name}, params) do
    case Map.fetch(params, name) do
      {:ok, value} -> {:ok, value}
      :error -> {:error, {:query_error, {:missing_param, name}}}
    end
  end

  defp resolve_value(value, _params), do: {:ok, value}

  defp timestamp_ns(%DateTime{} = dt), do: {:ok, DateTime.to_unix(dt, :nanosecond)}

  defp timestamp_ns(%NaiveDateTime{} = dt),
    do: {:ok, dt |> DateTime.from_naive!("Etc/UTC") |> DateTime.to_unix(:nanosecond)}

  defp timestamp_ns(v) when is_integer(v), do: {:ok, v}
  defp timestamp_ns(other), do: {:error, {:unsupported, {:timestamp_value, other}}}

  # Sorted by key -- deterministic output regardless of map internals,
  # not just cosmetic: this string is asserted on directly in tests.
  defp selector(label_key, source_value, label_eqs) do
    pairs = label_eqs |> Map.put(label_key, source_value) |> Enum.sort_by(fn {k, _v} -> k end)
    inner = Enum.map_join(pairs, ", ", fn {k, v} -> ~s(#{k}="#{escape(v)}") end)
    "{" <> inner <> "}"
  end

  # Backslash first, then quote -- doubling an already-doubled backslash
  # would be wrong the other way around.
  defp escape(value) do
    value
    |> to_string()
    |> String.replace("\\", "\\\\")
    |> String.replace(~s("), ~s(\\"))
  end
end
