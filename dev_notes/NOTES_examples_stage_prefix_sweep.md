# NOTES: stg_ prefix sweep across example YAML files (2026-08-15)

Every stage name in every processor example file now carries the
house stg_ prefix, so recipe authors copying from --get-usage output
inherit the convention that makes editor autocomplete useful (typing
stg_ narrows to exactly the stage names).

## Scope and mechanics

~750 stage values swept across the _examples/*.yaml set, targeting
the FULL stage-key vocabulary discovered by grepping the processors
themselves (the first inventory missed several): source_stage,
save_to_stage, lookup_stage, stage_name, data_source,
expected_from_stage, filtered_stage, output_stage, raw_stage,
reference_stage, save_conflicts_to_stage, filtered_stage_prefix (a
PREFIX that generates stage names, so it is prefixed too), plus
free_stages-style "stages:" item lists. Replacement is LINE-TARGETED
at those keys - a column that happens to share a stage's name is
never dragged along. Values containing {variables} at the head are
untouched; hand-mapped exception: the slice_data example stage
"file_metadata" became "stg_profiled_files" to match the profile_*
family rename rather than the mechanically-correct-but-misleading
stg_file_metadata.

## An incident, kept on the record

The first sweep pass corrupted free_stages' example file: its
"stages:" list segmentation mis-sliced when a list ended at a lower
indent than the head. The per-file yaml.safe_load PARSE-PROOF caught
it BEFORE the write - but the loop died there, leaving a partially
applied sweep (files alphabetically earlier written, later ones
untouched). Recovery: the list handling was rewritten as a line-based
state machine, and the sweep re-run idempotently over everything
(already-prefixed names skip). Lesson reinforced from the recipe
sweeps: parse-proof per file before write, always, and make sweeps
idempotent so recovery is a re-run, not an archaeology dig.

## Proofs

Zero offenders on a full re-scan (keys + lists, all files); the
examples loader loads all 41 processor example files clean; spot test
modules green. Prose/comment mentions of old names inside example
descriptions were deliberately left - functional keys are the
contract, and the descriptions read fine either way.
