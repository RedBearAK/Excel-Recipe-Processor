# NOTES: millisecond step timings + the frozen run timestamp (2026-08-16)

## Timing precision

Per-operation timings moved from tenths to milliseconds because
"(0.0s)" reads as a broken timer when it is really a fast step. Five
sites changed to :.3f - step completion (recipe_pipeline), processor
completion (base_processor), per-sheet formatting (format_excel),
workbook load and save (workbook_session). The RUN TOTAL stays :.1f
(main.py) - "21.7s" is the right resolution for a whole run, and it
is the one number nobody mistakes for a failed measurement.

## The frozen run timestamp - already true, now pinned

The output workbook ({source_stem}_proc_{hour}{minute}{second}) and
the recipe-triggered log ({output_basename}_log.txt) resolve at
DIFFERENT moments of the run, yet must carry the SAME stamp.
VariableSubstitution already guarantees it: datetime.now() is
captured ONCE at construction (recipe load) and every substitution in
the run reads that frozen clock - so the pair matched from the day
the log_file setting shipped, by design rather than luck. What was
missing was the regression guard: tests/test_frozen_run_timestamp.py
substitutes the stamp twice, 1.1 seconds apart, and requires
identical output, so a future refactor to per-call now() cannot
silently split the workbook/log pair.
