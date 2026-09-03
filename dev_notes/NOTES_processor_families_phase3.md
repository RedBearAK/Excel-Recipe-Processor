# Processor families, Phase 3 (2026-09-03)

Every processor now sits in a family; 33 of 45 declare a schema. The three
recipes in the sibling project validate clean under the strict stage graph.

## Family moves

- 23 processors -> `TransformBaseProcessor` (read one stage, write one).
- `create_stage` -> Import (inline data; `load_data` builds, the family saves).
- `free_stages` -> base family (a stage utility that touches no file); its
  `stages` key is kind `stage_release`, and the graph now errors when a
  released stage is read afterward.
- `verify_columns` -> Transform CHECK: `writes_stage = False`, no
  `save_to_stage`, graph records a read only; `execute_stage_to_stage`
  skips the save for checks. Summary on `check_summary` and in the log.
- `verify_data` SPLIT: `verify_stage_data` (Transform check) and
  `verify_sheet_data` (FileOps: `target_file` + `sheet_name`), one rule
  engine in `_helpers/verify_data_rules.py`. Examples split accordingly.
- `computed_stage_writes(config)` hook on the base: a processor whose
  output stage names are built at run time (`diff_data`'s subset stages)
  declares them to the graph.

## Rulebook applied

- `sort_data`: `ignore_case` -> `case_sensitive` (default false, Excel's
  polarity), including per-criterion form and examples.
- `select_columns`: name lists are strings only by schema. The integer
  position branch in the code is now unreachable - delete in the tail pass.
- `filter_data`: `FILTER_CONDITIONS` is the single list the engine and the
  schema share.

## Schema-less tail (reported at load, stage keys checked only)

aggregate_data, group_data, filter_terms_detector, export_filter_step,
profile_sheets, flush_workbooks, generate_column_config, format_excel,
conditional_format, inject_formulas, excel_data_validation,
manage_named_objects, seed_donor_formulas. Each has a nested vocabulary
that must be read from its code, not guessed - a partial schema would
reject valid recipes. When the last lands: delete `FALLBACK_*` in
`recipe_validation.py` and the `None` default of `config_schema()`.

## What the validator found on its first day

- A stray `filters:` block inside a `clean_data` step of a production
  recipe, silently ignored for weeks.
- Three schema guesses of the author's own that the recipes disproved
  (condition list, a data format, a severity value) - fixed from code.

Tests adapted: test_verify_columns, test_free_stages, test_diff_data_integration
(key_columns as lists), test_verify_stage_sheet_data (from test_verify_data_processor).
Baseline pre-existing failures unchanged: test_basic, test_capabilities_snapshot_drift,
test_excel_range_resolver, test_format_excel_consolidated_cycle,
test_format_excel_whole_column, test_new_comprehensive_test_of_processors,
test_processor_descriptions.

## Delete after extracting (retired by the split; the archive cannot remove files)

    git rm excel_recipe_processor/processors/verify_data_processor.py
    git rm excel_recipe_processor/processors/_examples/verify_data_examples.yaml
    git rm tests/test_verify_data_processor.py

## Examples brought under the schemas (same day)

`tests/test_examples_validate_against_schemas.py` validates every example
recipe step against its processor's schema. First run: 73 problems. Four
were schemas narrower than the code (combine_data, merge_data,
rename_columns case_conversion, strip_formula_caches scope) - widened from
code. The rest were examples teaching keys the processors never supported
(`lookup_source`, `ascending` on sort_data, `mode` and `sheets` on
export_file, `entries` on create_stage, `decimal_places` on fix_numeric,
elided "... rules" steps). All 317 example steps now validate; the
positional select_columns example was retired with the code path.
