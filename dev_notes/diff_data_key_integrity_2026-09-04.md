# diff_data: blank and duplicate keys now halt (2026-09-04)

## Why

`diff_data` indexed each side into a dict keyed by the key value. A repeated
key logged a warning and overwrote the earlier row, so every row but the last
with that key vanished before comparison started. Blank keys were worse: NaN
values hash apart, so each blank row surfaced as its own phantom NEW or
DELETED row. Both are configuration mistakes (wrong key column, or a key that
needs a second column to be unique) and neither had a legitimate outcome.

## What changed

`_check_key_integrity()` runs on both sides right after the key columns are
confirmed present, before any indexing. Blank check first (NaN or
whitespace-only), then duplicates via `DataFrame.duplicated(keep=False)`.
Errors name the side, the count, and either the first five Excel row numbers
(blank) or the five most-repeated key values with their row counts
(duplicate). The old warning in `_prepare_data_for_comparison` is now a
defensive raise that should be unreachable.

## Tests

`tests/test_diff_data_processor.py` gained `test_duplicate_keys_halt` and
`test_blank_keys_halt`. Suite: 9/9.

## Related

Membership questions (rows in one file but not the other) belong to the
`only_in_one_file.yaml` recipe / `erp-only-in-one-file` stub, which uses
`filter_data not_in_stage` and tolerates duplicate keys by design.

## Still pending

`_prepare_data_for_comparison` builds a one-row DataFrame per row via
`iterrows`, which is slow and smears dtypes. Untouched here; separate cleanup.
