# clean_data: dtype guard, and a "*" all-columns form

Built against dev_beta @ 7badc89 plus the earlier double-save fix.

## Why

A blanket "strip whitespace from everything" step is the obvious thing to want
as the first operation on a raw download. It could not be written safely before
this change.

Every text action in `clean_data` routes through `.astype(str)`:

```python
df[column] = df[column].astype(str).str.strip()
```

Applied to a typed column, that silently converts it to text. The damage is
invisible in the logs and only surfaces in the Excel output:

| Column | Before | After blanket strip | In Excel |
|---|---|---|---|
| `Price` | `123.45` (float) | `"123.45"` (str) | text, left-aligned, number format ignored |
| `Product ID` | `10001` (int) | `"10001.0"` (str) | text, with a stray `.0` |
| `Ship Date` | datetime | `"2026-08-01"` (str) | text, not a date |

Verified by writing both frames to `.xlsx` and reading the cell types back.

## Change 1 — text actions skip typed columns

Seven actions coerce via `astype(str)` and are now guarded:

```
uppercase  lowercase  title_case  strip_whitespace
remove_special_chars  remove_invisible_chars  normalize_whitespace
```

A column whose dtype is not object/string is skipped with a debug log line
rather than coerced. Naming a numeric column explicitly skips quietly rather
than raising, since the intent is unambiguous and an error would be unhelpful.

Actions with their own type semantics are untouched: `fix_numeric`, `fix_dates`,
`fill_empty`, `replace`, `standardize_values`, `remove_duplicates`,
`regex_replace`.

## Change 2 — `columns: "*"`

`columns` previously had to be an explicit list. Naming all 66 columns of a
download in every rule defeats the point of a blanket clean, so `"*"` is now
accepted and expands to every column in the frame.

```yaml
- step_description: "Strip whitespace from every text column"
  processor_type: "clean_data"
  source_stage: "stg_vms_import_raw"
  rules:
    - columns: "*"
      action: "strip_whitespace"
  save_to_stage: "stg_vms_cleaned"
```

The two changes are meant to go together: `"*"` is only safe *because* typed
columns are now skipped.

## Verification

`tests/test_clean_data_dtype_guard.py` — 6/6. Covers numeric and datetime
preservation, text columns still being cleaned, nulls not becoming the literal
string `"nan"`, the case actions carrying the same guard, and an explicitly
named numeric column skipping rather than raising.

Existing suites unaffected: `test_clean_data_processor`,
`test_lookup_data_processor`, `test_filter_data_processor`,
`test_group_data_processor`, `test_aggregate_data_processor` all pass.

## Note on pandas 3.0

Under pandas 3.0 the old behaviour did *not* turn nulls into the string `"nan"`,
which it would have under earlier versions. The dtype coercion happened either
way. The tests pin both properties so a pandas downgrade cannot reintroduce the
null problem unnoticed.

# End of file #
