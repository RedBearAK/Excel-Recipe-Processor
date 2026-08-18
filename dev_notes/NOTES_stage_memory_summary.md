# NOTES: stage-memory accounting in the run summary (2026-08-16)

The summary block gains one line:

  Stage memory: ~260 MB peak concurrent, ~610 MB allocated, ~592 MB freed during the run

## Why the peak is exact (the part the user offered to forgo)

All stage traffic funnels through StageManager.save_stage and
delete_stage, so a running concurrent total with a high-water mark IS
the literal maximum at the estimation level - no sampling, no
inference. Mid-run freeing keeps that peak far below the
total-allocated figure by design, which is exactly the difference the
number exists to show. The subtle case is OVERWRITE: re-saving an
existing stage releases the old frame's estimate before counting the
new one, or the current/peak figures would double-count (pinned in
the test).

## What the numbers are, honestly

Estimation-level: pandas memory_usage(deep=True) at save time -
DataFrame footprint, NOT process RSS. The OS may compress or swap
independently (the user's own caveat); openpyxl workbook objects,
xlsx serialization buffers, and Python overhead are all outside these
figures. The line answers "how much stage data was in flight", which
is the tunable quantity (free_stages placement), not "how big was the
process".

Display: adaptive precision (one decimal under 10 MB, whole numbers
above) so tiny demo runs do not read "~0 MB". Counters zero at
initialize_stages. The summary line rides mirror_print, so the log
file keeps byte-parity with the terminal - re-verified after wiring.

tests/test_stage_memory_stats.py 1/1 (peak retention, free drop,
totals growth, overwrite release-then-add - which also tripped the
framework's own overwrite-consent guard, working as designed).
