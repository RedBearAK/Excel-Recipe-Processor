# Large exports: where thirteen minutes went, and where they go now

`dev_notes/2026-09-14_large_exports.md`

The generic parquet_to_xlsx recipe on 728,631 rows x 39 columns took
13m 21s. The log broke it down:

    export_file   write the xlsx                263 s
    format_excel  LOAD the workbook back        233 s
    format_excel  auto_fit_columns              290 s
    run end       save the 85 MB file           204 s

Two of those should not have existed.

## The single-sheet export was not bridged

The workbook session's export bridge - hand the workbook the export
just built to the session instead of serializing it, so the next file
operation edits it in memory - lived only in write_multiple_sheets.
The single-sheet write_file closed its writer (which serializes) and
never adopted. The VMS recipe never noticed because it exports with
sheets_to_create. A plain `sheet_name` export paid 263 s to write a
file the next step spent 233 s reading back byte-identical. Now
write_file delegates to the bridged path under a session (no index, no
extra kwargs - the common case); the workbook is built once in memory
and written once at run end. tests: the format step after a
single-sheet export no longer logs "Loading Excel file".

## Widths measured on the frame, not the sheet

format_excel's auto_fit walks every cell in Python: 28 million reads,
290 s. Kris: limiting the rows scanned could miss critical widths - so
not sampling. The exact answer is cheap at EXPORT time, when the data
is a DataFrame: `series.astype(str).str.len().max()` is vectorized and
renders floats and datetimes the way str(cell.value) does. `export_file:
fit_columns: true` (with fit_min_width / fit_max_width /
fit_for_auto_filter / fit_header_bold) applies the SAME rule and bounds
as auto_fit - longest rendered value, header x1.2 when bold, +4, +3 for
an auto-filter, clamped - and a test proves the two paths give
identical widths column for column on mixed types. 12 s -> 0 s on
1.5 M cells in a comparison; ~220-290 s on the 28 M-cell sheet.

## The bridge's own scan

The bridge normalized pandas' '' cells to None by walking every cell -
another Python pass over 28 million values. The frames say exactly
which cells were null; only those are visited now.

Version 20260914.2. The two generic recipes use fit_columns on the
export and drop auto_fit from their format step.


# End of file #
