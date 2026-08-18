# Declaration-wiring test + validator derived from get_minimal_config (2026-08-17)

dev_notes/NOTES_2026-08-17_wiring_and_derived_validator.md

Two rulings executed: validate the @ zip surgery if tests do not, and
advance test_recipe_validator to derive required fields from
get_minimal_config().

## 1. The @ surgery: coverage audit, then the missing wiring test

Audit finding: the surgery ITSELF is thoroughly validated.
test_dynamic_array_metadata pins the byte level (cm="1" markers,
xl/metadata.xml byte-identical to Excel's own, package registrations,
idempotence, scalar formulas untouched, legacy-CSE completion,
fail-loud on unrecognized metadata parts) and
test_declaration_lambda_and_registry pins the flush -> reload -> save
round trip and provenance registry.

The GAP was wiring: nothing proved the session save path applies the
declaration. WorkbookSession._save_workbook declares only when
_declare_dynamic is set (opt-in via recipe settings key
'declare_dynamic_formulas' -> RecipePipeline ->
WorkbookSession.set_declare_dynamic). A refactor dropping that branch
or the flag plumbing would leave every byte-level unit test green
while production output regressed to @-wearing files.

New tests/test_session_declaration_wiring.py (3/3):
- flag ON: a dynamic formula planted through the session and flushed
  lands on disk with cm="1", t="array" ref, the metadata part, and a
  clean audit_legacy_cse - the "file never exists on disk in the form
  that draws the @" guarantee, asserted at the layer users hit
- flag OFF: no declaration appears (the toggle actually toggles)
- the pipeline source still reads the settings key and still calls
  set_declare_dynamic (names the break if either half is renamed)

## 2. Validator: hand table replaced by derivation

The hand-maintained required-fields table in tests/test_recipe_validator.py
(which went stale on lookup_data) is gone. Required fields now derive
from each processor's own get_minimal_config() - the framework's
single source of truth - minus:

- UNIVERSAL_STEP_KEYS (processor_type, step_description, source_stage,
  save_to_stage): identity keys plus stage keys the recipe-structure
  validator already enforces
- OPTIONAL_IN_MINIMAL: keys present in a minimal config for
  ILLUSTRATION but defaulted/conditional in the execute path, each
  entry citing the default that makes it optional (slice_data
  start_row/end_row, fill_data fill_value, debug_breakpoint message,
  filter_terms_detector text_columns, rename_columns shape keys)

Design property: only OPTIONALITY lives in the exception map - key
NAMES always come from the processor - so an entry errs toward
under-flagging, never toward a wrong name. The rename_columns any-of
check (mapping OR pattern OR transform) stays as code: that is
semantics, not vocabulary.

Survey basis: all 44 processors expose get_minimal_config(); the
optionality claims were verified against each processor's
get_config_value defaults before encoding.

Drill-verified: simulating a vocabulary change on lookup_data's
minimal config made the derived requirements track it instantly, and
slice_data derives to just ['slice_type'] with the illustrative row
keys stripped.

Consumers re-verified green: test_recipe_validator,
test_config_validation, test_verify_fixes. Adjacent declaration
modules re-verified green.

## Follow-up flagged, not fixed

tests/test_capabilities_tools.py is a demo module that exits 0
unconditionally, and its own "valid_van_report.yaml" fixture now
reports INVALID - the missing-settings staleness class, predating
today's changes. Candidates when convenient: modernize the fixture to
current doctrine and wire the exit on the demo's real verdicts.

# End of file #
