# spill_columns and first_match in add_calculated_column (2026-09-03)

Two additions, one convention, no new processor.

## spill_columns

A calculation may fill more than one column, the way an Excel formula
spills horizontally. `new_column` is the calculated column; `spill_columns`
names the rest, in order. The result is classified by shape and checked
against the declaration:

- Series or scalar -> `new_column`; a declared spill that did not arrive is an error.
- DataFrame, or tuple/list of Series -> columns 2..k to `spill_columns`; an
  undeclared spill or a width mismatch is an error naming both counts.
- Anything not one value per row (a vertical or 2-D spill, a scalar where a
  frame was expected) is refused, not coerced.

Only the evaluated types (`expression`, `first_match`) may spill; the
declarative types (`math`, `text`, `date`, `concat`, `conditional`) reject it.

## first_match

The IFS / CASE of the processor with paired outputs. Ordered rules; the first
true `when` wins the row and supplies EVERY declared column from its own `then`
row, so a value and the label explaining it cannot drift. Each `then` and the
required `pandas_default` carry exactly one slot per declared column: an
expression, a quoted literal or number (broadcast), or `""` for the column's
typed blank. A blank is always visible, never an omitted term.

Validation runs before any evaluation: rule shape, slot counts, unknown keys,
and every `{col:}` reference compiled against the frame, so a mistake is
reported by rule number. The empty for an output is typed from its populated
slots (datetime -> NaT, numeric -> NaN, else missing object). The log reports
per-rule hit counts, with never-matched rules gathered onto one line.

## Key convention

An evaluated string never sits under a bare key. The key names the dialect
(`pandas_formula`, `pandas_rules`, `pandas_default`); plain names (`when`,
`then`) are structure inside a dialect-declared container. Bare `rules` /
`default` are a guided error. This leaves the obvious twin open - `excel_rules`
in a formula-injecting processor rendering the same table live.

Also: the `typing` import left this module; `execute(self, data)` takes the
guard clause instead.

Tests: `tests/test_add_calculated_column_spill_first_match.py`.
