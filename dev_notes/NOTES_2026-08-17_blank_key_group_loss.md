# aggregate_data was silently deleting blank-keyed rows

dev_notes/NOTES_2026-08-17_blank_key_group_loss.md

Found by the summary VIEW disagreeing with its static parent: the CMA
view spilled 406 data rows against the static tab's 395. Set-comparing
the seven-key tuples in the user's pasted-values workbook showed all
11 view-only groups share one trait: blank Carrier Tracking No. The
arithmetic sealed the diagnosis - 395 + 11 = 406 exactly.

## Root cause

pandas groupby defaults to dropna=True, which does not MERGE NaN-keyed
rows - it silently DELETES them from the result. Every booked van
without a tracking number was vanishing from the static export
summary: 13 vans and ~679,341 lbs of net weight (64,231,668.9 in the
view vs 63,552,328.0 in the static tab). Excel's GROUPBY keeps blanks
as "" groups, so the LIVE VIEW was the correct one - the check the
view tabs were built to be, working on their first real comparison.

## Fix (this TGZ)

aggregate_data passes dropna=False - doctrine, not an option: silent
row deletion has no legitimate configuration. When blank-keyed rows
exist, the completion log counts them and NAMES the affected key
columns ("2 row(s) have blank values in group key(s) ['Tracking'] -
kept as blank-keyed groups"). Retention pinned in the suite: NaN-keyed
group present in output, totals conserved. Broad collateral green.

No recipe change needed: on the next run the static tabs gain the 11
groups and converge with the views (modulo the already-ruled
case-collation ordering). The static tab's totals become correct for
the first time.

## Follow-up audit recorded, NOT yet changed

Five more groupby sites carry the same silent default and deserve the
same doctrine pass, each with its own retention test before changing:
  add_subtotals_processor.py:213
  group_data_processor.py:234, 317, 379
  pivot_table_processor.py:245
pivot_table's NaN row_field rows vanishing is the identical class.
Held for a ruling rather than blind-swept in the same delivery.

# End of file #
