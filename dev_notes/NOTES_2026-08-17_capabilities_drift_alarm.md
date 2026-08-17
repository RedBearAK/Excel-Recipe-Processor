# Capabilities snapshot: regenerated + drift alarm (2026-08-17)

dev_notes/NOTES_2026-08-17_capabilities_drift_alarm.md

Per ruling (option C): current_capabilities.json is a reference
document like the README, kept honest by a test instead of a habit.

## What changed

- current_capabilities.json regenerated: 17 -> 44 processors (the old
  snapshot predated 27 processors and still listed the deleted
  load_stage; committed 7724b50, copy_stage-rename era).
- New tests/test_capabilities_snapshot_drift.py compares the committed
  snapshot against LIVE `--list-capabilities --json` output. On
  divergence it fails loud with an itemized delta (processors
  added/removed, per-processor keys added/removed/changed) and prints
  the exact refresh command. CLI output verified byte-deterministic
  across runs before trusting an equality compare.
- Drill-verified: a doctored snapshot (one processor deleted, one
  mutated, one fabricated) produced all three delta classes by name
  and rc 1; the restored snapshot returned rc 0.

## The refresh ritual (printed by the failing test too)

    PYTHONPATH=. python3 -m excel_recipe_processor \
        --list-capabilities --json > current_capabilities.json

Refreshing is now a deliberate, reviewed act: the test turns red the
moment capabilities move, names what moved, and stays red until the
snapshot is recommitted - the same drift-alarm shape as the VMS
profile diffs, applied to the tool itself.

## Related root-file ruling

add_calc_col_examples.yaml: stray pre-standardization draft from
e6acc80, zero references, already convicted in
NOTES_standardization_sweep.md. Repo-side: git rm add_calc_col_examples.yaml

# End of file #
