# 2026-08-29 - fill_data blanks_from_column across dtypes (pandas 3)

`fill_data` with `fill_method: blanks_from_column` raised
`Invalid value '<StringArray>...' for dtype 'float64'` when the target
column was float64 (a Product ID column that is all-blank on import) and
the source was text, and `Invalid value '[30000.0]' for dtype 'float64'`
when both were float64 but the assignment went through an object array.
pandas 3 refuses to place a value of another dtype into a typed column.

Fix (fill_data_processor.py): when the target's dtype differs from the
selected source values' dtype, widen the target to object and cast the
values to object; when they agree, assign the values as they are. Test
`test_blanks_from_column_across_dtypes` added to
`tests/test_fill_data_processor.py`.

Found while wiring the VMS recipe's van-history fill (four
`blanks_from_column` steps from the VMS-Vans-History export).

# End of file #
