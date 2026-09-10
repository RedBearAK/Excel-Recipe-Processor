# lookup_data: same-named lookup columns no longer overwrite main columns

Date: 2026-09-03

## What was wrong

`_perform_lookup_merge` merged first with suffixes `('', '_FROM_LOOKUP')`
and then, for every suffixed column whose bare name was in
`lookup_columns`, copied the lookup values OVER the main column and
dropped the suffixed one. `prefix`/`suffix` were applied afterwards to
whatever survived under that name - which was the main column's slot.

Net effect for a lookup that pulled a column named like a main column:

- matched rows: main values silently replaced by lookup values;
- unmatched rows: main values replaced by blank;
- with a prefix: the prefix landed on the (already overwritten) main
  column, so the recipe author saw `VMS Van Number` where their own
  `Van Number` used to be, and no second column at all.

Found while building `ims_vms_canned_join.yaml`, which pulls
`Van Number` and `Carrier` from VMS beside the IMS columns of the same
names.

## What changed

- The lookup payload is renamed with prefix/suffix BEFORE the merge.
- A final name that still collides with a main column is a
  `StepProcessorError` naming the columns and the fix (set prefix or
  suffix, or drop the column). No main column is ever replaced.
- The merge key travels under a private label, so the lookup key may
  itself appear in `lookup_columns` as an echo (`VMS Van Seq #` beside
  `Van Sequence #`) without pandas refusing the doubled label.
- `_apply_column_naming` now runs only for the substring-scan mode,
  which builds its columns unprefixed; that mode already refused
  collisions.

## Recipe impact

Any recipe that relied on the overwrite - pulling a lookup column with
the same name as a main column and expecting the main column to be
replaced - now halts at that step with the collision error. Search
recipes for `lookup_columns` entries that name a column already present
in the source stage. The fix is one of: add `prefix`/`suffix`, or drop
the main column first with `select_columns`, or drop the lookup column
if the main one was the intended survivor.

## Tests

- `tests/test_lookup_name_collisions.py` (new, 3 tests)
- `tests/test_lookup_data_processor.py`, `test_lookup_empty_stage.py`,
  `test_lookup_substring_mode.py`, `test_new_yaml_first_integration.py`
  unchanged and passing.

# End of file #
