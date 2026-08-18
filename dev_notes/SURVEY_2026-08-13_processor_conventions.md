# Processor convention survey - 2026-08-13

Programmatic audit of all 39 registered processors (40 modules incl. one
__DEPRECATED__) for basics that should be identical but may have drifted.
Fixed-in-this-pass items are marked; the rest await decisions.

## Fixed in this pass

1. DEAD DUPLICATE CLASS (the big one): core/base_processor.py contained an
   entire stale FormatExcelProcessor (~185 lines) from some early refactor,
   shadowing nothing (no imports anywhere) but confusing every reader and
   grep. Removed; file went 738 -> ~570 lines.

2. NINE processors errored on usage examples (deduplicate_data,
   export_file, file_metadata, flush_workbooks, free_stages, import_file,
   manage_named_objects, verify_columns, + seed_donor via a different
   cause): their YAML files existed but the get_usage_examples METHOD was
   missing, and there was no base default - the pipeline explicitly
   error-reports its absence. Fix: BaseStepProcessor now provides the
   default (load external YAML by processor_type). Side effects: (a) the
   ~28 identical three-line boilerplate methods across other processors
   are now DELETABLE (optional negative-code pass); (b) the test-debt item
   test_usage_examples' "8 missing" claim is likely about these METHODS,
   not files - that test may be repairable rather than stale.

3. seed_donor_formulas_examples.yaml lacked the required top-level
   description key - the one examples file failing the loader's own
   structure validation. Added (noting inject_formulas supersession).

4. Base get_capabilities default was a stale copy-paste of format_excel's
   dict (inside the dead class, it turned out). Honest generic default now
   in BaseStepProcessor.

## Decision items (surveyed, not changed)

5. SHEET-KEY DRIFT - same concept, multiple spellings:
   'sheet' (import_file, conditional_format, verify_data) vs 'sheet_name'
   (export_file sheet entries; also the recipes' export steps use
   sheet_name per entry). Principled variants that are NOT drift: 'sheets'
   (multi-sheet lists), source_sheet/target_sheet pairs (seed_donor,
   generate_column_config), active_sheet (format_excel). Recommendation if
   harmonizing: canonical 'sheet' for "which single tab" (majority +
   nothing else it could mean, unlike the stage/stage_name ambiguity);
   export's per-entry 'sheet_name' -> 'sheet' would touch both production
   recipes' export steps. Sweep-sized, same shape as the stage sweep.

6. [FIXED same day] manage_named_objects: 'import_file'/'export_file'
   both named the YAML definitions file from opposite directions, and the
   processor already had the content-type precedent (vba_file). Both keys
   became ONE: 'yaml_file' - direction follows the operation (export_*
   writes it, import_*/validate_yaml reads it). Guided error on the
   retired keys; examples and the (passing-baseline) test module updated.

7. [PARTLY FIXED same day] Mechanical style debt: docstring path lines
   and end-of-file markers added by script to all 17 lacking modules
   (path inserted after the docstring's summary line; __DEPRECATED__
   module skipped). REMAINING: typing-module imports in 16 modules -
   needs per-file annotation rewrites, its own pass.

## Verified principled (looked like drift, is not)

- File-key semantics: input_file (read), output_file (write new),
  target_file (modify in place), source_file (donor read). Consistent
  within each meaning.
- debug_breakpoint's filename_prefix (different concept).
- requires_* flags: only create_stage sets any now (accurately), after
  the copy_stage repair in the thirteenth pass.

## Sheet-addressing doctrine (vetted 2026-08-14, supersedes item 5's sketch)

Fully vetted across two sessions before any sweep. The design:

- sheet_name (string, always a literal name - "1" means the tab named "1")
  and sheet_index (int, 1-based) are separate, typed keys. Polymorphic
  'sheet' retires; so do both internal dispatch mechanisms (file_reader's
  isdigit coercion, which shadows numeric tab names, and format_excel's
  YAML-type dispatch, which silently no-ops when variable substitution
  stringifies an index - that silent skip is a fail-loud violation to fix
  regardless).
- Plural list operations take sheet_names: plain string lists, names only,
  no mixed forms, no dict items. Grammatical number states capability:
  plural = the operation broadcasts; singular = 1:1 by nature (import,
  export entries, active sheet, donor pairs).
- PRINCIPLE: index addressing exists only at the trust boundary, and
  crossing the boundary converts it to a name - by import (index -> named
  stage) or by a rename step (index -> trusted tab name) for the rare
  in-place-on-foreign-file case. Inside recipe-space, everything speaks
  names. This is rename_columns' doctrine one level up: normalize
  untrusted labels at the boundary, once, visibly.
- Consequence: the mixed name+index list can never be needed, so the
  awkward per-item dictionary form is permanently avoided - the need
  dissolves under the boundary principle rather than being accommodated.
- Lazy escape hatch, build only when a real case appears: a tab-rename
  operation (nothing among the 39 can rename a tab today). Additive,
  never reopens polymorphism.

The sweep itself (sheet_name/sheet_index/sheet_names keys, guided errors,
~27 recipe keys, format entry consolidation for the six lookup tabs) is
designed and vetted but NOT executed - awaiting go-ahead as its own pass.

## Sheet-addressing doctrine v2 (2026-08-14) - SUPERSEDES the typed-key design above

The sheet_name/sheet_index twin-key split is retired before ever being
built, replaced by index PSEUDO-NAMES: ?sheet_001? (case-insensitive,
1-4 digits accepted, 3-digit zero-padded canonical per the 255-sheet soft
limit, up to 9999 supported with a live bounds check). The '?' delimiters
are drawn from Excel's own forbidden character set (\ * ? : / [ ]) and
openpyxl refuses them at creation - verified by probe - so the namespace
is STRUCTURALLY disjoint from every real tab name in every workbook from
every source. Not a convention: a guarantee. (Curly braces were
disqualified - they belong to variable substitution; brackets rejected as
too list-adjacent; renaming foreign tabs rejected as an unwanted side
effect even if renamed back.)

Semantics: no enum, no table - a recognizer interpreted at resolution
time. Substitution runs FIRST, so a variable may expand to a token (the
VMS download_sheet variable becomes '?sheet_001?'). Tokens are valid
anywhere an EXISTING sheet is addressed, including mixed into
sheet_names: lists beside real names - the mixed-list problem is solved
outright, not dissolved. Tokens are rejected loudly in creation contexts
(export entries): they address tabs that exist. Out-of-range fails loud
with the workbook's actual sheet count.

Key consequence: ONE key family - sheet_name / sheet_names ("a name:
real, or a ?sheet_NNN? pseudo-name"); bare 'sheet' still retires;
sheet_index never exists; plural lists stay plain string lists forever.
Implementation commitment: one recognizer - pattern in a _rgx module,
resolver in a shared helper, every processor borrowing the same function
(the anti-drift lesson of this whole thread). The isdigit hack and the
YAML-type dispatch both die; format_excel's silent-skip on unresolvable
sheets becomes a loud failure.

SWEEP EXECUTED 2026-08-14 - see the notes file, sixteenth pass.

# End of file #
