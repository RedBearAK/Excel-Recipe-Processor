# file_metadata: a new import processor (the 30th)

Expand at the repo root. New processor + registration in core/pipeline.py +
usage examples + test module (5/5).

Produces a stage describing files rather than reading them: File, Modified
(real datetime, so Excel sorts and formats it), Size (KB). Rows keep the
listed order. on_missing: error (default) / note (MISSING row) / skip -
default is error because a provenance sheet that silently omits a missing
input hides exactly what it exists to surface.

    processor_type: "file_metadata"
    files:
      - "{lookup_dir}/product_ids.xlsx"
    save_to_stage: "stg_source_file_info"

core/pipeline.py in this archive carries only the two registration lines as
its diff, but it is the whole current file - replace, don't merge.

Full suite: 20 failures, matching baseline. Capability discovery: 30/30 clean.

# End of file #
