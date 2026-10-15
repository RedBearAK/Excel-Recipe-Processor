# Two opt-in policies for "no rows" (2026-09-19)

`dev_notes/empty_stage_and_empty_input_policies_2026-09-19.md`

Both came from one recipe need: `vms_process.yaml` now applies van number
corrections from a shared hand sheet and must (a) report the corrected
rows in a separate file ONLY when some were corrected, and (b) still run
on a machine where the sheet does not exist yet. Defaults are unchanged;
no existing recipe behaves differently.

## export_file: `empty_stage_policy`

`write` (default) | `skip` | `error`. Judged on `source_stage`, in every
mode (single sheet, `sheets_to_create`, `template_file`).

- `skip`: no rows, no file - so the file's PRESENCE is the signal. The
  same idea as deduplicate_data's `conflicts_file`, made general. No
  backup is made and an existing file of that name is not touched.
- `error`: an export that must never be hollow stops the recipe.

Named as a policy (`_policy`) because there are three outcomes and a
boolean could only say two.

## filter_data: `empty_input_policy`

`error` (default) | `pass_through`. An empty MAIN table almost always
means an import failed upstream, so the default still halts.
`pass_through` is for an optional input - `import_file` with
`on_missing_file: create_empty` - and returns the empty frame with its
columns so the joins after it still work. A frame with no COLUMNS is
still an error. This follows select_columns (2026-08-17), clean_data,
deduplicate_data and aggregate_data, which already treat "nothing in,
nothing out" as defined; filter_data keeps its tripwire unless asked.

## Checked

- `tests/test_export_file_processor.py::test_empty_stage_policy` - write
  / skip / error; the near miss (`skip` with rows writes normally); skip
  over an existing file leaves it and makes no backup; unknown value
  refused by the schema.
- `tests/test_filter_data_processor.py::test_empty_input_policy` - default
  still halts; pass_through keeps columns; the near miss (rows present:
  identical result with and without it); no-columns still refused; a
  pandas_expression is not evaluated on nothing.
- The recipe's new steps were run through the real CLI four ways: sheet
  present (3 rows renamed, report written, merge flagged, split / rejected
  / contradicted pairs untouched, an all-digit number matched); no sheet;
  a sheet with no rows; a sheet with nothing to correct - the last three
  exit 0 and write no report.
- `--validate-recipe vms_process.yaml` passes; all 147 test modules pass
  (parallel map, then the three it missed and one load timeout serially).
- `current_capabilities.json` refreshed. It had ALREADY drifted before
  this change (audit_external_ties and sever_external_ties were added
  without a refresh), so the diff carries those too.
