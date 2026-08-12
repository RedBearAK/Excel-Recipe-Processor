# clean_data: nulls turning into the literal string "nan"

Supersedes the `clean_data_processor.py` in every earlier archive. Contains the
dtype guard, the `"*"` all-columns form, the improved logging, and this fix.

## Symptom

After a blanket `strip_whitespace`, several columns in the output workbook
showed the text `nan` in every cell.

## Cause

The dtype guard added earlier stopped numeric and datetime columns being
coerced, but text columns still went through:

```python
df[column] = df[column].astype(str).str.strip()
```

`.astype(str)` converts every null to the four-character string `"nan"`. For a
column that is mostly or entirely empty — `Customer Ref #` is blank on 5,458 of
10,267 rows, and three columns in the download carry a header with no data at
all — that fills the column with text that then survives into Excel as real
content.

## Why it did not show up in testing

**pandas 3 hides it.** Under the pandas 3 string dtype, `astype(str)` preserves
nulls, so the earlier tests passed:

```
pandas 3.0.2:  Series(['Acme', nan, ' Ocean '], dtype=object).astype(str).str.strip()
               -> ['Acme', nan, 'Ocean']        nulls survive

pandas 2.x:    same call
               -> ['Acme', 'nan', 'Ocean']      null became text
```

The test environment was on 3.0.2 and could not reproduce it at all. This looks
like a pandas 2.x environment.

## Change

String operations now apply only to the populated cells:

```python
def _apply_to_text_values(self, series, operation):
    result = series.copy()
    populated = series.notna()
    if not populated.any():
        return result
    result.loc[populated] = operation(series.loc[populated].astype(str).str)
    return result
```

Applied to `strip_whitespace`, `uppercase`, `lowercase`, `title_case`,
`remove_special_chars`, `normalize_whitespace` and `remove_invisible_chars`.
Nulls are never routed through `astype(str)`, so behaviour is identical on
pandas 2 and 3.

## Tests

`test_clean_data_dtype_guard.py` — now 8/8. Two new cases:

- **`test_nulls_never_become_literal_nan`** runs all six text actions against a
  partly empty and a fully empty object column, asserting on the *values* rather
  than the dtype. Value-level assertions catch this on either pandas version;
  the previous dtype-level ones did not.
- **`test_all_empty_object_column`** pins the header-with-no-data case
  specifically.

Full suite: 20 failures before and after, identical to baseline.

## Worth knowing

If your pandas is 2.x, that is also why the earlier `generate_column_config`
`infer_datetime_format` breakage did not appear for you — that parameter was
removed in pandas 3.0. The fix applied there is harmless on 2.x, since the
argument was redundant.

# End of file #
