# format_excel `missing_file_policy` (2026-11-02)

dev_notes/format_excel_missing_file_policy_2026-11-02.md

## Why

The VMS recipe writes several side reports beside the processed workbook
under `export_file` `empty_stage_policy: skip` - the file exists only
when there is something to report (future dates, missing carrier
tracking, van sequence corrections). Those files had no formatting at
all: raw column widths, plain header, 100% zoom. Formatting them with a
`format_excel` step after the export failed on a clean run, because the
file is not there and `format_excel` halts with "Target file not found".

The pipeline's per-step `on_error: continue` would have let the step
survive, but it swallows EVERY failure on that step - an unresolved
color variable, a corrupt file, a bad zoom - as a red line that scrolls
past while the run reports success. A missing presence-signal file is
not a failure; it is the expected outcome of a clean run and deserves a
quiet check mark, while a real formatting defect must still halt.

## What

`format_excel` gains `missing_file_policy`:

- `error` (default): halt when `target_file` is absent - unchanged.
- `skip`: log `No file to format; <name> was not written this run` and
  do nothing. Only the missing-file case is tolerated; config validation
  runs first as before and every later failure still halts.

It is the format-side mirror of `export_file` `empty_stage_policy: skip`.
Code: one schema key and one branch at the existence check in
`perform_file_operation`.

## Recipe pattern (vms_process.yaml)

One anchored formatting entry addresses "the only sheet" of any
single-sheet file by position token, so it is reused verbatim:

    - &fmt_side_report
      sheet_names: ["?sheet_001?"]
      apply_templates: ["tpl_header", "tpl_sizing"]
      zoom_percent: 125

and every presence-signal export is followed by:

    - step_description: "Format the <report> report"
      processor_type: "format_excel"
      target_file: "{output_dir}/{output_basename}_<report>.xlsx"
      missing_file_policy: "skip"
      templates: [*tpl_header, *tpl_sizing]
      formatting: [*fmt_side_report]

Convention recorded in the recipe header: a new presence-signal report
is an export + format PAIR.

## Also in this delivery: undeclared template names fail `--validate`

Noticed while drilling: an unknown name in `apply_templates` was a
WARNING in `format_excel` ("Template 'x' not found - will be skipped"),
so a typo'd recipe validated clean (0 warnings, exit 0), ran for
minutes, and produced an unstyled sheet with the only trace in the log.
Kris's ruling: the templates are declared in the same step, so this is
a validation issue, not a style preference.

Two changes:

1. `format_excel` refuses the undeclared name at construction (and again
   at apply time, for callers that bypass validation), naming the sheet,
   the bad name and the declared names.
2. The validation phase gains a per-processor hook. `--validate` was
   schema + stage graph only; processor-specific semantic checks ran at
   step construction, which in a real run is when the step's turn comes.
   `BaseStepProcessor.config_errors(config) -> list` (default `[]`) is
   a sibling of `computed_stage_writes`: defects knowable from the
   resolved config alone that a schema cannot express. `validate_recipe`
   calls it after the schema pass and reports each message as a step
   error. `format_excel` is the first implementer. Any processor with a
   "this name must match that list" rule should implement it rather than
   check at construction.

Planted-typo proof against `vms_process.yaml`: `--validate` now fails
with one error per affected step and sheet (`step 12 'Format the
corrections report': apply_templates names 'tpl_sizzing' for
['?sheet_001?'], which this step's templates do not declare:
['tpl_header', 'tpl_sizing']`), exit 1; the clean recipe passes.

## Test

`tests/test_format_excel_missing_file_policy.py` (7 checks): default
halts on a missing file; skip passes a missing file and creates nothing;
skip still formats a present file through a template; skip still halts
on a bad config (zoom 999) on a present file; an undeclared template is
refused at construction; the same is reported by `validate_recipe`
through the hook; an unknown policy value is refused by the schema.

# End of file #
