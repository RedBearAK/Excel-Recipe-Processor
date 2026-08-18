# WorkbookSession: load once, operate, save once

Expand at the repo root. Whole files - replace, don't merge. pipeline.py
and recipe_pipeline.py also ride in earlier archives, which have been
refreshed to match; apply archives in any order.

## What changed

Named ranges, formula seeding and formatting each paid a full openpyxl
load-and-save on the same output file - on your 51K-row merge, ~90% of
their 168 seconds. They now share ONE live workbook per file path
(core/workbook_session.py), loaded on first touch, saved once after the
last step succeeds. Correctness is identity: the ranges one step writes are
simply present in the object the next step seeds.

Measured at 10,500 rows: 37-40s runs became 23.1s. At 51K-row merge scale,
projected ~80-90s savings.

## Safety design

- DEFERRED MODE IS OPT-IN BY THE PIPELINE. A processor called standalone
  (tests, scripts) keeps legacy save-immediately semantics; nothing outside
  a pipeline changes behavior. (The full suite stays at its 20-failure
  baseline; one early regression in a format test was exactly this, and
  the opt-in design fixed it.)
- On any step failure the session DISCARDS unsaved work; the file on disk
  stays exactly as the export step wrote it, and a rerun regenerates the
  file operations from scratch - no half-modified workbooks.
- Recipes touching multiple files are handled: every file is tracked by
  absolute path and each dirty one saves once at run end.
- The new flush_workbooks processor (the 34th) forces an earlier write when
  an external tool needs the file on disk mid-run.
- Equivalence-gated: a full output snapshot (ranges, seeded formulas,
  formatting, widths, hidden columns, freeze, filter, active sheet) was
  captured BEFORE the change and matches the session-built output
  attribute for attribute.

## format_excel: autofit_scan_rows (same archive)

Auto-fit measures every cell BY DEFAULT - correctness first, since a long
value near the bottom would otherwise get a too-narrow column. Big sheets
can cap the scan to the first N data rows (header always measured):

    autofit_scan_rows: 2000

Verified both directions: full scan catches a row-100 long value, a
50-row cap deliberately does not. Not enabled in the VMS recipe - your
call when merge-scale formatting time matters.

# End of file #
