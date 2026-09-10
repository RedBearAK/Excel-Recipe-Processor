# auto_free_stages defaults to on; the plan is schema-derived (2026-09-04)

## Default on

The VMS merge recipe, which said nothing about it, ended its run with all
13 stages alive and 149 MB held, against an assumption that freeing was
automatic. Now it is. `settings: auto_free_stages: false` is the opt-out;
`true` is the (now redundant) opt-in the VMS recipe carries.

The model is unchanged: consuming-STEP counting, freeing when a stage's
last consuming step completes (never on load). Undercount fails loud at
the next load; overcount only holds memory longer.

## The flip exposed the scan

The first live run under the default halted at the merge recipe's second
`in_stage` filter: stage not found. The consumer scan was a string-match
heuristic with its own exclusion list, and it excluded `stage_name` -
right for the declaration key under `settings.stages`, which it never
walked, and wrong for the identically spelled rule-reference key inside
steps. Every stage referenced only that way was undercounted. Latent
under opt-in, loud under the default, as designed.

## The fix is not another exclusion

Recipe validation (2026-09-03) already derives each step's reads from the
processor's declared schema - `config_schema.stage_references()` at every
key typed `stage_in`, nested rule lists and variants included - on the
variable-resolved configs. The scan was a second, independent answer to
"what does this step read", and two answers can drift.

Now there is one. `StageManager.plan_auto_free(recipe, registry,
substitute)` runs right after validation passes, on the same configs,
through the same function. `declare_recipe_stages` only reads the
setting. The heuristic and `_NON_REFERENCE_KEYS` are gone. `--validate`
builds the plan too, so its log shows the consumer count.

## Proof it stays fixed

`tests/test_auto_free_default.py`:
- default on / false opts out / true enables
- freeing is at step completion, not on load
- a stage referenced only through a filter rule's `stage_name` counts
- THE AUDIT: for every registered processor, a sentinel at every
  `stage_in` key its schema declares (top level, nested mapping,
  list-of-mappings item, variant), each counted exactly once by the plan.
  46 key paths across 46 processors today; a new processor or a new
  `stage_in` key is covered the moment it is declared, and a processor
  without a schema fails the audit by name.

## Verification note (the process failure behind the process failure)

The first smoke run under the default had already halted the same way,
and it was read through a grep for success lines and reported as passing.
Runs and tests are judged by exit code and an ERROR/failed count from a
captured log, never by a filtered view. Recorded in project memory.
