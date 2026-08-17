# Named empty-sheet INFO lines + rolling column-drift alarm (2026-08-17)

dev_notes/NOTES_2026-08-17_named_info_and_drift_alarm.md

SUPERSEDES the earlier same-day expect_empty design (that TGZ should
not be pushed / should be reverted if it was). Ruling and rationale:

## 1. Empty-DataFrame lines: named, demoted to INFO, no opt-in key

The first design added an expect_empty sheet key that SILENCED the
message for acknowledged sheets. Rejected on review for three reasons:

- Writing an empty sheet is a DESIGNED pattern here (formula-spill
  view tabs are exported empty and seeded after), and a WARNING that
  fires on intended behavior trains readers to ignore warnings.
- The acknowledgement did not just hide a log line - it DELETED the
  information: an acknowledged sheet wrote with no trace, so a debug
  session could not confirm from the log that the frame was empty as
  planned.
- The production log already carried the fact at INFO: export_file
  prints the per-sheet "rows x columns" listing immediately before
  the warnings fired - the anonymous WARNING was redundant with
  better information one line above it.

Final shape: FileWriter._validate_dataframe(data, context=None) logs
"Writing empty DataFrame for sheet 'X'" (or the filename on the
single-file path) at INFO, always, for every empty frame. ExcelWriter's
sibling line likewise names its output path at INFO. No new recipe
vocabulary, no signature changes on the write methods, nothing
silenced. Drilled: every empty sheet named, none suppressed.

## 2. diff_data completion log names the movers (unchanged from v1)

"analyzed N rows, identified changes" -> per-status enumeration:
"2 NEW: D, E; 1 DELETED: B" (multi-key rows joined with ' / ', capped
at 10 names per status with +N more), or "no differences". Suite green.

## 3. Rolling column-drift alarm in the VMS recipe (unchanged from v1)

Six-step report-only block after the curated verify_columns step:
profile_sheets (raw import) -> select_columns [Column] -> import
sidecar csv -> diff_data (key: Column) -> export current names over
the sidecar -> free_stages. Membership only; the curated verify owns
hard failure. Sidecar recipe_files/vms_prev_download_columns.csv
({recipe_dir}-relative), seeded from var_expected_download_columns
(66 names). Two-run drill on the real pipeline: run 1 reported
"1 NEW: Fishery Notes; 1 DELETED: Last Moved"; run 2 after rollover
reported "no differences". Recipe: 74 -> 80 steps, four stg_dl_cols_*
stages declared and freed.

## Delivered alongside this TGZ (recipe project files, not in it)

- vms_process.yaml: drift block only - NO expect_empty keys
- vms_prev_download_columns.csv: seed sidecar -> VMS project
  recipe_files/ next to vms_named_lambdas.yaml

# End of file #
