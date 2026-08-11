# Two framework changes: verify_columns + repeated stage dumps

Expand at the repo root. core/pipeline.py, recipe_pipeline.py,
stage_manager.py, stage_inspection.py are WHOLE FILES - replace, don't merge.
pipeline.py carries all recent registrations (deduplicate_data, file_metadata,
free_stages, verify_columns).

## verify_columns (the 33rd processor)

    processor_type: "verify_columns"
    stage: "stg_import_raw"
    expected_columns: "{list:var_expected_download_columns}"
    on_missing_expected: "error"   # default - downstream breaks anyway;
                                   # fail fast with the actual cause
    on_unexpected: "warn"          # default - the recipe survives strangers,
                                   # but the event deserves a loud line

Reads its stage without re-saving (no full-frame duplication). Column ORDER
deliberately unchecked - downstream steps address by name, and a check that
failed on reorder would train people to ignore it. Tests 5/5; all three
outcomes proven end to end on the VMS recipe (clean verify, new-column warn
with completion, missing-column halt naming the column).

## --dump-stage on stage re-use

Previously a requested stage dumped ONCE, its first save. Now every save of
a requested stage dumps, with collision-proof names:

    stg_reused.csv                            first save (unchanged)
    stg_reused_save2_YYYYMMDD_HHMMSS.csv      second save, and so on

Distinct save numbers within a run, distinct timestamps across runs -
repeated dumps can never overwrite one another. Implemented via a per-stage
save counter in StageManager (get_stage_save_count).

## Stage re-use consent fix (stage_manager.py, same file)

Re-saving an UNPROTECTED stage was impossible through any processor: the
guard demanded overwrite=True, which no processor passes. The consent key a
recipe can actually set - confirm_stage_replacement: true - now counts for
unprotected stages too. Discovered while testing repeated dumps, which
require a stage that saves twice.

Full suite: 20 failures, baseline. Discovery: 33/33 clean.

# End of file #
