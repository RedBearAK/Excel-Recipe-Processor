# More logging in four processors

Built on dev_beta @ 7badc89 plus the double-save and dtype-guard fixes.
`clean_data_processor.py` in this archive supersedes the one in
`fix_clean_data_dtype_guard.tgz` — it contains both changes.

## clean_data — what was cleaned, and what was skipped

```
🧹 Rule 1 'strip_whitespace': cleaned 50 column(s)
   ↳ skipped 16 non-text column(s): Process Year, Van Seq #, Ship Date, Product ID ...
```

The skip line matters. With `columns: "*"` the guard silently passes over
numeric and datetime columns, and previously there was no way to tell whether a
column was skipped by the guard or simply absent. 50 cleaned + 16 skipped = 66,
which reconciles against the import.

## select_columns — which columns it invented

```
➕ Created 5 blank column(s): Test Fresh, Test Cans, Test Dest, Test Carrier, SALE TYPE1
```

Previously reported only "selected 76 column specifications, result: 76 columns",
which could not distinguish a column that arrived in the data from one conjured
out of `columns_to_create`.

## export_file — per-sheet breakdown

```
📄 Writing 5 sheets:
   • VMS Data: 8,680 rows × 76 columns
   • Product_IDs: 1,914 rows × 9 columns
   • Plant_Origin_to_Regions: 18 rows × 2 columns
   • Export_Destinations: 110 rows × 4 columns
   • Carriers: 8 rows × 1 columns
```

Previously a single combined total, which hid a lookup sheet arriving empty or
truncated.

## manage_named_objects — row mode and a summary

```
📐 Written  rng_PID          -> Product_IDs!$C$2:$C$1915   [data]
📐 Written  rng_carrier      -> Carriers!$A$2:$A$1048576   [full_col_no_header]
📐 Named ranges: 9 written, 0 replaced, 0 skipped, across 4 sheet(s)
```

The `[row_mode]` tag makes an unexpectedly short range self-explaining: a `data`
range that stops early means extent detection found the wrong anchor, whereas
`full_col_no_header` is meant to run to the sheet limit.

## Verification

`test_clean_data_dtype_guard` 6/6, `test_manage_named_objects_write` 9/9,
`test_manage_named_objects_processor` passing. 29 processors register.

# End of file #
