defmodule Scry.Engine.Loki.Conn do
  @moduledoc """
  Wraps the base URL of a reachable Loki server and the label key a
  query's own `source` maps onto -- no actual connection exists to open
  at all, unlike most other adapters in this family: Loki's own REST
  API is plain, stateless JSON-over-HTTP, the identical shape `Scry.
  Engine.Elasticsearch.Conn`/`Scry.Engine.CouchDB.Conn` already
  document.

  **`label_key` (default `"job"`)** is the one real, deliberate scope
  choice this package makes about how a Scry `source` (a single name)
  maps onto Loki's own multi-dimensional label model: `SELECT myapp
  {...}` becomes the LogQL stream selector `{job="myapp"}`, exactly the
  same "one primary identifier" convention every other adapter in this
  family already has for its own `source` (a SQL table, an
  Elasticsearch index, a Neo4j label, a MongoDB/CouchDB collection). A
  real Loki deployment's own convention for the "primary" label varies
  (`job`, `app`, `container`, ...) -- configurable per `open/1` call
  rather than hardcoded, since there's no universal default across real
  deployments the way there is for, say, a SQL table name.
  """

  @type t :: %__MODULE__{base_url: String.t(), label_key: String.t()}

  defstruct base_url: "http://localhost:3100", label_key: "job"

  @doc """
  Wraps `base_url` (default `"http://localhost:3100"`, a stock local
  Loki container) and `label_key` (default `"job"`).
  """
  @spec open(keyword()) :: {:ok, t()}
  def open(opts \\ []) do
    base_url =
      opts |> Keyword.get(:base_url, "http://localhost:3100") |> String.trim_trailing("/")

    label_key = Keyword.get(opts, :label_key, "job")
    {:ok, %__MODULE__{base_url: base_url, label_key: label_key}}
  end

  @doc """
  Runs `logql` (a full LogQL stream-selector query) against
  `/loki/api/v1/query_range`, `start_ns`/`end_ns` inclusive Unix-
  nanosecond bounds -- both required, since Loki itself enforces a
  maximum query time range (confirmed directly against a real server:
  a bound-free query silently applies Loki's own default window rather
  than "everything," and an overly wide one -- confirmed directly,
  epoch-to-now -- is a real, clean `400` error, `"query time range
  exceeds the limit"`) -- `Scry.Engine.Loki`'s own moduledoc has the
  full "why this package requires an explicit lower bound rather than
  guessing a default" reasoning. `limit` is always sent explicitly,
  Loki's own default (100 lines) being exactly the kind of silent-
  truncation trap `scry_engine_elasticsearch`'s own default `size: 10`
  already is.
  """
  @spec query_range(t(), String.t(), integer(), integer(), pos_integer()) ::
          {:ok, map()} | {:error, {:query_error, term()}}
  def query_range(%__MODULE__{base_url: base_url}, logql, start_ns, end_ns, limit) do
    params = [query: logql, start: start_ns, end: end_ns, limit: limit, direction: "forward"]

    case Req.get(base_url <> "/loki/api/v1/query_range", params: params) do
      {:ok, %Req.Response{status: 200, body: body}} -> {:ok, body}
      {:ok, %Req.Response{body: body}} -> {:error, {:query_error, body}}
      {:error, reason} -> {:error, {:query_error, reason}}
    end
  end

  @doc "Pushes `lines` (a list of `{timestamp_ns :: integer(), text :: String.t()}` pairs) into a stream carrying `labels` (a plain string-keyed map) -- test/fixture use only, never called from `execute/3` itself."
  @spec push(t(), map(), [{integer(), String.t()}]) :: :ok | {:error, {:query_error, term()}}
  def push(%__MODULE__{base_url: base_url}, labels, lines) do
    body = %{
      streams: [
        %{
          stream: labels,
          values: Enum.map(lines, fn {ts, text} -> [Integer.to_string(ts), text] end)
        }
      ]
    }

    case Req.post(base_url <> "/loki/api/v1/push", json: body) do
      {:ok, %Req.Response{status: 204}} -> :ok
      {:ok, %Req.Response{body: resp_body}} -> {:error, {:query_error, resp_body}}
      {:error, reason} -> {:error, {:query_error, reason}}
    end
  end
end
