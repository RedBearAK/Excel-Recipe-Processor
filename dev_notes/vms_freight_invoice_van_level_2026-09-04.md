# VMS download: Freight Invoice is van-level and export-unstable (2026-09-04)

Found while comparing a ship-date download against a process-year download
of the same table, same day.

## What the export does

- Per van, at most one distinct nonzero `Freight Invoice`, on exactly one
  line. Per-van amounts agreed across the two exports on 1,384 of 1,391
  shared vans (the other seven lacked the line on one side).
- WHICH line carries it differs between the exports: the export's own
  line ordering changes with the server-side filter.
- The other lines read 0 or blank, and the same line can be 0 in one
  export and blank in the other.
- `Freight Total` is genuinely per-line (an allocation) and stable.

Compared raw, 2,668 rows differed in this column alone.

## Consequences

- `vms_process.yaml`: zeros blanked right after date coercion
  (`stg_vms_freight_invoice_normalized`), via `Series.mask(== 0)` to keep
  float64 - `replace(0, None)` turns the column object under pandas 3.
  Which line carries the amount is left alone; broadcasting would multiply
  it under any subtotal. The complementary no-Product-ID export stays raw.
- `vms_merge_downloads.yaml`: the conflict twin key uses a derived
  `Freight Invoice (van)` (the van's amount on every line, per source) in
  place of the raw column, which stays in the report for reference.
- Anything that ever compares downloads at line grain must treat this
  column the same way.

## Also learned from the same comparison

A van can carry a previous-year product line next to current-year ones.
The process-year filter drops that line; the ship-date filter keeps the
whole van (116 lines on 111 vans). So the ship-date download is the fuller
one on shared vans and is the merge's primary; the process-year download
contributes the vans with no ship date yet.
