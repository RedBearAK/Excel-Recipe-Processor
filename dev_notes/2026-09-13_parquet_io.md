# Parquet in import_file / export_file

`dev_notes/2026-09-13_parquet_io.md`

Pending since the replica started exporting Parquet for anything too big
for Excel; Kris: a standard format addition, with an option on both
processors to DISCARD types, preserving them by default.

- `.parquet` maps to format 'parquet' in FileReader and FileWriter;
  `format: parquet` is a valid explicit choice on both processors, and
  on the definitions-file readers of group_data / aggregate_data.
- `parquet_types: preserve` (default) - import keeps the file's column
  types; export writes the stage's dtypes into the file. This is the
  reason to use the format at all.
- `parquet_types: text` - import reads every column as text with nulls
  left missing (the way a csv import arrives); export casts every column
  to text first and writes an all-string schema, nulls null - the way a
  raw capture layer keeps it, for a consumer that must not trust types.
- Sheet, encoding, separator, header_row and the NA policy have no
  meaning for Parquet and are ignored for it; only parquet_types applies.
- pyarrow>=14 added to requirements.txt (it is pandas' Parquet engine).

Checked: tests/test_parquet_io.py - a typed frame round-trips with every
dtype intact; text on import; text on export (the FILE's schema is all
strings, nulls null, dates in ISO form); a recipe csv -> parquet (typed)
-> parquet -> xlsx keeps numeric types through the middle; a bad
parquet_types value is refused at validation before any step runs. The
import / export / file reader / file writer / excel_io / schema and
docs-fragment suites all pass.

Also in this archive: dev_notes/bump_version.py, ported from the
replica, keeping ERP's start-at-.0 convention. ERP's version stood at
20250807.0 for a year until Kris set it by hand on 2026-09-10; from now
on every erp_ archive bumps by script and quotes the result.


# End of file #

## Also, v20260913.1: completion lines at real precision

The pulse's two completion lines ("✅ Formatting sheet 1/1: 'Rule_Codes'
- 0s total") printed whole seconds, which is right for the LIVE line
that redraws every second and wrong for a line that stays in the log: a
7 ms phase read as nothing. `log_format.duration()` picks the precision
by scale - 0.007s, 0.43s, 1.4s, 2m 14.2s - and both completion lines use
it; the live counters are unchanged. tests/test_log_format_duration.py.

## Also, v20260913.2: flush_workbooks names its files

Kris: shouldn't there be a "flush THIS workbook, I'm done with it"
directive, rather than a recipe-wide flush that will write whatever
else happens to be open? Yes - a recipe-wide flush is action at a
distance, right today only because nothing else is in the session at
that step, and changed silently by any file operation added earlier.
`flush_workbooks` now takes `target_files`: the named workbooks are
written and closed, the rest stay in the session for the single save at
run end; a named file that is not open is an error (the step is in the
wrong place). Without target_files it still flushes everything, as a
checkpoint. WorkbookSession.flush_paths() beside flush_all().
tests/test_flush_workbooks_named.py: named writes only the named file
and main's later formatting still lands; unnamed still flushes all; a
wrong name errors and lists what IS open; a flushed file touched again
reloads from disk. The VMS recipe uses the named form.
