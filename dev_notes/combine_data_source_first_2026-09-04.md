# combine_data: the source stage is always the first part (2026-09-04)

## The bug

`combine_data` consumed `source_stage` for dependency tracking but stacked
only what `data_sources` listed. The source frame was included only when an
entry named the `current_dataframe` token. Every example in the examples
file omitted the source from `data_sources` and called it the base - the
docs described the intended behavior and the code silently did something
else. First seen live: the VMS merge recipe produced a "merged" file holding
only the appended vans. No existing recipe used the processor.

## Breaking change

- The `source_stage` frame is ALWAYS part one of the result, for both
  `vertical_stack` and `horizontal_concat`; `data_sources` follow in order.
  Column validation and blank-row/column sizing apply to it like any part.
- `insert_from_stage: current_dataframe` is rejected at validation with a
  migration hint (it would double the source).
- A missing source frame raises; nothing starts from an empty stack.
- New optional top-level `retain_source_column_names` (bool), the source
  part's twin of the per-source `retain_column_names`, with the same smart
  default (true under `allow_mismatched_columns`, false under
  `require_matching_columns`).

## Tests

`tests/test_combine_data_processor.py`: every test migrated to pass the
first stage as the execute frame instead of listing it; two new tests
(source leads and a missing frame is refused; `current_dataframe` rejected).
Two assertions were stale since the schema layer (2026-09-03) began
rejecting bad values before the processor's own check - they now accept
either wording. The module also gained a real exit code; it previously
printed a failure banner and exited 0.

## Also in this patch

`deduplicate_data` `keep: none` - see `deduplicate_keep_none_2026-09-04.md`.

## Consumer

`vms_merge_downloads.yaml` / `erp-vms-merge-downloads`, which needs both
changes.
