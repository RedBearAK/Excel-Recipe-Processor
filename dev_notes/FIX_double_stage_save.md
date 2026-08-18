# Fix: aggregate_data and group_data double-saved their output stage

Built against dev_beta @ 7badc89.

## Symptom

Any recipe step using `aggregate_data` or `group_data` with `save_to_stage`
failed, even though the step reported completing:

```
INFO: Saved aggregation results to stage 'stg_x' (205 rows)
INFO: Completed step: 'Count rows per distinct Contracts value'
ERROR: Step 2 failed - Stage 'stg_x' already exists. Use overwrite=true to replace it.
```

## Cause

Both processors wrote the stage themselves and then the base class wrote it
again in `save_output_data()`. The stage manager correctly rejected the second
write.

This is leftover from the stages-only migration. Every other stage-to-stage
processor - `filter_data`, `slice_data`, `clean_data`, `sort_data` - lets the
base class own the write.

## Change

Removed the internal save from both, along with the now-dead helpers
`_save_aggregation_to_stage()` and `_save_grouping_to_stage()` and the config
reads that fed them.

## One test had encoded the outlier behaviour

`test_group_data_processor.py::test_stage_based_grouping` called
`processor.execute(input_data)` and then asserted the output stage existed. That
only passed because of the internal save.

`execute(data)` does not write `save_to_stage` for *any* stage-to-stage
processor - verified against `filter_data`, which returns a DataFrame and writes
nothing. The pipeline path is `execute_stage_to_stage()`. The test now calls
that instead.

## Verification

- `test_group_data_processor.py` 8/8, `test_aggregate_data_processor.py` exit 0
- Full suite run before and after: **byte-identical failure sets**, so zero
  regressions. Note that 20 modules already fail on unmodified `dev_beta` -
  mostly `format_excel`, file readers, and CLI integration. Unrelated to this
  change, but they are there.

# End of file #
