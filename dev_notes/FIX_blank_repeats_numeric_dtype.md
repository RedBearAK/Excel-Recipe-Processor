# FIX: blank_repeats on numeric group columns

One-file patch: `excel_recipe_processor/processors/clean_data_processor.py`.
Based on dev_beta as of 2026-08-11.

`clean_data`'s `blank_repeats` action wrote `''` into continuation rows
in-place. Under pandas 3 that raises on any non-text group column:

    Invalid value '' for dtype 'float64'

First hit by the CMA invoices recipe, where SHIP REF is deliberately numeric
(it must sort with real bookings and match the established column type) and
is also the outermost pivot-display group column. The VMS recipe never
tripped this because its group columns are all text.

Fix, inside `_apply_blank_repeats`: before blanking, cast any non-text group
column to object dtype (via the existing `_is_text_column` helper). The
blanks make the column a display artifact anyway - it is the last transform
before export on a sheet meant for reading - so losing the numeric dtype at
that point costs nothing. Values that survive (first row of each group) are
untouched and still export as numbers.

No behavior change for all-text group columns: the cast is skipped and the
original single-statement path is preserved.

Verified: CMA recipe end to end, 64 continuation rows blanked across a
numeric + two text group columns, surviving SHIP REF cells export as ints.

# End of file #
