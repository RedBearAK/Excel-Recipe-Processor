# deduplicate_data: keyed dedupe with conflict evidence (the 31st processor)

Expand at the repo root. New processor + registration in core/pipeline.py
(whole file - replace, don't merge; also carries the file_metadata
registration if not yet applied) + usage examples + test module (6/6).

Collapse rows to one per key_columns combination, keep "first" (default) or
"last". ALL columns are kept - inverting the silent-loss failure of
dedupe-via-aggregation, where a column added upstream but not enumerated
vanishes without a sound.

A "conflict" is a key whose duplicate rows genuinely disagreed on some
column; pure join repetition collapses without comment. Conflicts report
three ways:
- warning log per key: disputed columns, all values, which won
- save_conflicts_to_stage: every row of every conflicted key, annotated
  with 'Dedupe Status' (kept/discarded) and 'Conflicting Columns'
- conflicts_file: same contents, written ONLY when conflicts exist - the
  file's presence is itself the signal

Empty input yields empty output, not an error.

CLI listing description: "Collapse rows to one per key, keeping all columns
and reporting groups whose values conflicted".

Full suite: 20 failures, baseline. Discovery: 31/31 clean.

# End of file #
