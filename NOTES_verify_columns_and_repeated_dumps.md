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

## Completion summary fix (stage_manager.py, recipe_pipeline.py, main.py)

"Data stages created" counted stages STILL IN MEMORY at completion, so a
recipe that frees its stages mid-run reported "created: 0" - reading as a
broken run when it was a tidy one. Created now means saved-at-least-once
(sourced from the save counter), and the summary annotates the cleanup:

    Data stages created: 43 (34 freed during the run)

## verify_columns: expected_from_stage (same processor file)

The expectation can come from another stage's columns instead of a literal
list - comparing two FILES is then two imports and one verify step, with
messages naming both stages ("in A not B" / "in B not A"). Exactly one of
expected_columns / expected_from_stage must be given. Tests 7/7.

## file_reader: numeric-string sheets are indexes (core/file_reader.py)

A sheet index routed through a recipe variable arrives as the STRING "1"
and was treated as a sheet NAME. Purely-numeric string sheets now convert
to indexes. (This archive's file_reader.py also carries the earlier
verbatim_text_columns work - it supersedes the copy in
erp_verbatim_text_columns.tgz, which has been refreshed to match.)

Full suite: 20 failures, baseline. Discovery: 33/33 clean.

# End of file #
