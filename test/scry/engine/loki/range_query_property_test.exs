defmodule Scry.Engine.Loki.RangeQueryPropertyTest do
  @moduledoc """
  Property coverage for the label-value escaping `RangeQuery.compile/4`
  applies before interpolating a `WHERE <label> = <literal>` value into
  a generated LogQL selector string -- the invariant a real injection
  risk depends on: *every* backslash and double-quote in an arbitrary
  label value must come back escaped, never passed through raw into a
  string Loki's own LogQL parser will treat as the selector's own
  syntax. AGENTS.md calls for a property test here rather than
  enumerating hand-picked examples, since a label value is arbitrary,
  user/log-pipeline-controlled text once `WHERE <label> = ...` is
  considered.
  """

  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Scry.Engine.Loki.RangeQuery

  defp compile_selector(level_value) do
    lower = DateTime.new!(~D[2026-01-01], ~T[00:00:00])
    wheres = [{:cmp, :ge, ["timestamp"], lower}, {:cmp, :eq, ["level"], level_value}]
    {:ok, %{selector: selector}} = RangeQuery.compile(wheres, "job", "myapp", %{})
    selector
  end

  property "every backslash and double-quote in a label value is escaped" do
    check all(text <- StreamData.string(:printable, max_length: 30)) do
      selector = compile_selector(text)

      # Extract exactly the level="..." value back out of the selector
      # and confirm unescaping it round-trips to the original text --
      # a stronger guarantee than just counting characters, since it
      # also confirms the escaping is unambiguously reversible.
      [_, escaped] = Regex.run(~r/level="((?:[^"\\]|\\.)*)"/, selector)

      unescaped =
        escaped
        |> String.replace(~s(\\"), ~s("))
        |> unescape_backslashes()

      assert unescaped == text
    end
  end

  defp unescape_backslashes(text), do: String.replace(text, "\\\\", "\\")

  property "a label value with neither backslash nor quote round-trips unescaped" do
    check all(text <- StreamData.string(?a..?z, max_length: 20)) do
      selector = compile_selector(text)
      assert selector == ~s({job="myapp", level="#{text}"})
    end
  end
end
