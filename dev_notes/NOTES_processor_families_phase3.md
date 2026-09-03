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

## format_excel schema (2026-09-04)

Declared from the module's own option sets: sheet-level options shared by
`templates` and `formatting` entries; `column_formats` rules carry the
`column_names` / `column_refs` pair (at least one) plus style keys;
`cell_formats` rules carry `cells` (a list of A1 refs/ranges) plus style
keys; `cell_ranges` and `row_heights` are open mappings keyed by range /
row number; `pivot_style` and `workbook_theme` are closed mappings.

What it caught: both sibling-project contracts recipes still used
`columns:` inside `column_formats`, a key the helper has refused since the
2026-08-26 rename - they would have failed at their format step on the
next real run. One stale `sheet_name` in the profile_sheets example.

## Baseline test failures cleared (2026-09-04)

The "pre-existing failures" were all stale tests, not defects:
- test_format_excel_consolidated_cycle, test_format_excel_whole_column:
  `columns:` in column_formats rules (retired 08-26); letter refs moved
  to `column_refs`. The resolver's own guided error still said
  "(columns is names only)" - now "(column_names is names only)".
- test_excel_range_resolver: two tests asserted the pre-08-26 union
  (bare 'C' read as a position); rewritten to the current contract.
- test_processor_descriptions: conditional_format's minimal config used
  the retired `columns` key and could not instantiate; fixed. The two
  new verify_* descriptions were over the 80-char cap; shortened.
- test_capabilities_snapshot_drift: intended drift; snapshot refreshed
  (`current_capabilities.json`).
- test_new_comprehensive_test_of_processors: `formula` -> `pandas_formula`
  with {col:} refs; a stage declared for every workflow but written by one
  (the strict graph caught it).
Remaining: test_basic needs pytest installed - an environment matter.

## Construction-time schema check and the test sweep (2026-09-04)

`BaseStepProcessor.__init__` validates the config against
`construction_schema()` - the full schema with the family's stage keys made
optional, since a direct caller may hand a processor a frame with no stages
in play. Unresolved {tokens} are tolerated at construction. This is the
permanent form of "sweep the tests' recipe fragments": every direct
instantiation now goes through the check a recipe goes through at load.

First run under the gate: 45 of 129 test modules failed. Sorted:
- schemas narrower than code (fixed from source): pivot_table index/values
  optional; columns_to_rows / rows_to_columns id_columns optional;
  verify_excel_storage on_violation is halt/warn; merge_data suffixes
  accepts a tuple; split_column and diff_data minimal configs carried
  stale keys.
- stale test keys the processors never read (removed): sheet_specific,
  column_widths mapping, replace_current_data, active in sheets_to_create,
  ignore_case, string key_columns.
- guardrail tests asserting error text now get the schema's wording;
  expectations updated, one construction moved inside its try.
test_basic was a pytest-style scaffold; rewritten in house style.
Result: 129/129 (two modules are slow under 4-way parallel load and
should be run serially for a verdict).
