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

# End of file #
