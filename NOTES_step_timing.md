# Per-step and per-phase timing in the logs

Expand at the repo root. Whole files - replace, don't merge. recipe_pipeline
and main also ride in erp_verify_columns_and_repeated_dumps.tgz, which has
been refreshed to match; apply in either order.

Three layers of timing, uniform across every processor:

1. Every step's completion line gains its elapsed:
       Completed step: 'Sort by booking' (0.3s)
       ✅ Step 45 completed successfully (0.3s)
   Implemented in the base class hooks (log_step_start starts the clock,
   log_step_complete reports it), so all 33 processors got it at once.

2. format_excel phase timing - the per-directive level - because the heavy
   end-of-run step deserved decomposition:
       ⏱️  Workbook loaded in 0.4s
       ⏱️  [VMS_Data] formatted in 0.1s     (one line per sheet entry)
       ⏱️  Workbook saved in 0.2s
   On a large output, the LOAD is the dominant cost ("the early
   operations"), and now the log says so with a number.

3. Run total in the completion summary:
       ✓ Recipe completed successfully in 5.8s   (or "in 3m 12.4s")

Full suite: 20 failures, baseline.

# End of file #
