# deduplicate_data: keep none (2026-09-04)

## Why

The schema declared `keep` choices `first`/`last`/`none` but `__init__`
rejected `none`, so the declared vocabulary and the runtime disagreed.
The VMS merge conflict report needs exactly what `none` means: drop every
row whose key repeats. Keyed on every download column, a stack of the two
files' overlapping vans reduces to the rows without an exact twin - the
symmetric difference - each still carrying its source tag.

## What changed

- `keep: none` accepted; maps to `drop_duplicates(keep=False)`.
- Log line for `none` counts rows whose key appeared once and rows removed.
- Conflict detection is skipped under `none`. Conflict reporting exposes a
  silently picked winner; with `none` nothing is picked, and in the
  symmetric-difference use the non-key columns are source tags that differ
  on every matched pair, so the report would flag precisely the rows that
  matched. Stage/file outputs still emit (empty).
- Examples file and capabilities updated.

## Tests

`tests/test_deduplicate_data.py` gained `test_keep_none_drops_every_repeated_key`
(plain drop-all, and a two-frame symmetric difference). Suite: 7/7.

## Consumer

`vms_merge_downloads.yaml` in the VMS project, via the
`erp-vms-merge-downloads` stub.
