# Two keys for a column the source system added mid-season (2026-10-06)

`dev_notes/optional_columns_and_skip_if_present_2026-10-06.md`

The VMS online database gained `Due Date Indicator` on 2026-10-06 and the
user put it in their view, so every export from that day carries it and
none before it does. `vms_process.yaml` had three ways to be wrong about
that column, and two small keys close them. Defaults are unchanged; no
existing recipe behaves differently.

## verify_columns: `optional_columns`

A list of columns that may be present or absent: neither NEW when
present nor MISSING when absent. Listed in `expected_columns` the column
would halt every older export (`on_missing_expected: error`); left off,
every new export would raise the NEW notice, and a notice that fires
every day is one nobody reads - which is how a real change gets ignored.
A column that is NOT optional still gets its notice.

## add_calculated_column: `skip_if_present`

When `new_column` already exists, the step does nothing and passes the
data on unchanged; the calculation runs only when the column is absent.
So an older export gets a blank column, a newer one keeps what the source
wrote, and every later step can address the column by name. `overwrite`
says the opposite, so the two refuse to combine rather than letting one
silently win.

## Checked

- `tests/test_verify_columns.py::test_optional_columns_are_neither_new_nor_missing`
  - present, absent, a real new column still noticed, a missing
  non-optional column still halts.
- `tests/test_add_calculated_column_processor.py::test_skip_if_present_creates_only_when_absent`
  - absent created blank, present untouched, refused with overwrite.
- The recipe's three new steps (verify with the optional column; ensure
  the column exists; `DDI Check` against the contract-derived relevant
  date) were run through the real CLI on a download WITH the column and
  one WITHOUT it: both exit 0, the check reads match / VMS only / both
  values side by side / blank as designed.
- `--validate-recipe vms_process.yaml` passes; all 147 test modules pass.
- `current_capabilities.json` refreshed.
