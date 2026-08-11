# free_stages: mid-run stage deletion (the 32nd processor)

Expand at the repo root. New processor + StageManager.delete_stage +
registration in core/pipeline.py (whole file - replace, don't merge; also
carries deduplicate_data and file_metadata registrations if not yet applied)
+ examples + tests (5/5).

    processor_type: "free_stages"
    stages: ["stg_no_longer_needed", ...]
    # on_missing: "skip"   # default "error" - a typo should be loud

Safety: protected stages refuse deletion; an absent stage halts by default;
a stage a LATER step still needs fails loudly at that step, naming it - a
misplaced deletion can only halt a run, never corrupt data. Logs the count
and approximate MB returned.

--dump-stage is UNAFFECTED by deletions: dumps are written the moment a
stage is saved. Verified end to end (dumped a stage that a later free_stages
step deletes; the dump file appeared).

Measured on a 10,500-row VMS run: ~257 MB of frames recycled across five
deletion points. Peak RSS moves modestly at this scale (322 -> 278 MB,
allocator retention dominates); the value is the bounded growth curve for
much larger merged inputs, where the untrimmed plateau would be gigabytes.

# End of file #
