# Two framework fixes surfaced by the ingest recipes

Expand at the repo root. Touches two processor files. Includes the earlier
empty-input change to aggregate_data (same file), so this supersedes
erp_fix_aggregate_empty_input.tgz if that was not yet applied.

## aggregate_data: silent column mislabeling (the serious one)

Renaming aggregation outputs matched by SUBSTRING: with columns
"Major Species" and "Species" both aggregated, the 'Species' rename captured
"Major Species_first" first, so Major Species values landed under the header
"Species" and real Species values stayed as "Species_first". No error - just
wrong labels on right data.

Fix: dict-of-lists aggregation always yields a (column, function) MultiIndex,
so the flattened name is deterministically "column_function"; renames now map
that exact name. Verified: the collision case labels correctly, and multiple
functions on one column (sum + max) name correctly.

## add_calculated_column: new 'row_number' calculation type

    calculation_type: "row_number"
    calculation:
      start: 1

Numbers rows 1..N in current order - a sort anchor for display sheets whose
meaning depends on row order (blanked repeats). Sorting on it restores the
shipped order after any Excel re-sort. The examples file documents it.

## add_calculated_column: new 'constant' calculation type

    calculation_type: "constant"
    calculation:
      value: "current"

Fills the new column with one literal on every row. Was missing entirely -
a marker or category column has no source column to compute from.

Full suite: 20 failures, matching baseline.

# End of file #
