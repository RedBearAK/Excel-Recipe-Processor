# Processor families and declared step schemas (2026-09-03)

Phase 1 of the schema tree: the core, one processor opted in as proof,
tests around it. No other processor changes shape yet.

## What landed

- `core/config_schema.py`: the vocabulary. `Key` (kind, required, default,
  choices, description), `Schema` (closed mapping, optional discriminated
  variants, at-least-one groups), kinds for scalars / list / mapping /
  open_mapping / list_of_mappings / any / stage_in / stage_out, and the
  three column-selector constructs: `name_list`, `name_ref_pair`,
  `typed_item_list`. Families (`transform`, `import`, `export`, `file_ops`,
  `base`) each contribute their common keys and declare which selector
  constructs they OFFER. `check_processor_schema` merges family + own and
  refuses a redefined family key or an unoffered construct.
- `core/base_processor.py`: `TransformBaseProcessor` (read a stage, return
  a stage; names columns only by header). Every family class carries
  `family`; `config_schema()` is the processor's own declaration,
  `full_schema()` the merged result, checked in `__init_subclass__` - at
  class definition, never at run time.
- `core/recipe_validation.py`: the validation phase. Runs after variables
  resolve and before step 1 on every run; `--validate` stops after it.
  Schema check per step (unknown key with nearest-name suggestion, types,
  required, variants) for processors with a schema; stage keys only for
  the rest, reported once per processor type. Stage graph in step order:
  read-before-write, double write without confirm, declared-never-written,
  declared-never-read are ERRORS (ruling 2026-09-03: strict); undeclared
  stages WARN as before. Exit 1 on errors only.
- `--validate` CLI flag; pipeline `validate_only`; main reports and exits.
- `add_calculated_column` is the proof: now a `TransformBaseProcessor` with
  a schema using flat keys, a nested mapping, a list of mappings and the
  `calculation_type` variant. The three production recipes validate clean
  under it, and a planted `new_colum` / `spill_column` halt at load with
  the right suggestion.

## Temporary scaffolding, to delete as schemas land

`FALLBACK_STAGE_READ_KEYS` / `_NESTED_READ_KEYS` / `_WRITE_KEYS` in
`recipe_validation.py`: how the stage graph reads a schema-less step. Found
one gap already (`insert_from_stage` nested in `data_sources`); each such
find is a key a schema should declare as `stage_in`. When the last
processor has a schema the table goes, and the `None` default for
`config_schema()` with it.

## Audit table (Phase 2 input) - generated from the registry

base column = current parent; xl = touches openpyxl / letter refs;
src/sav = requires_source_stage / requires_save_to_stage.

processor                  base                   xl  src sav  proposed family
add_calculated_column      TransformBaseProcessor -   Y   Y    transform (as is)
add_subtotals              BaseStepProcessor      -   Y   Y    transform
aggregate_data             BaseStepProcessor      -   Y   Y    transform
clean_data                 BaseStepProcessor      -   Y   Y    transform
columns_to_rows            BaseStepProcessor      -   Y   Y    transform
combine_data               BaseStepProcessor      -   Y   Y    transform (multi-read)
conditional_format         FileOpsBaseProcessor   Y   Y   Y    fileops (as is)
copy_stage                 BaseStepProcessor      -   Y   Y    base? or transform
create_stage               BaseStepProcessor      -   -   Y    import (inline)
debug_breakpoint           ExportBaseProcessor    -   Y   Y    export? writes a file from a stage
declare_dynamic_formulas   FileOpsBaseProcessor   -   Y   Y    file_ops? reads yaml writes yaml - RULING
deduplicate_data           BaseStepProcessor      -   Y   Y    transform (+conflicts out)
diff_data                  BaseStepProcessor      -   Y   Y    transform (multi-read/out)
excel_data_validation      FileOpsBaseProcessor   Y   Y   Y    fileops (as is)
export_file                ExportBaseProcessor    Y   Y   Y    export (as is)
export_filter_step         ExportBaseProcessor    -   Y   Y    export
fill_data                  BaseStepProcessor      -   Y   Y    transform
filter_data                BaseStepProcessor      -   Y   Y    transform
filter_terms_detector      BaseStepProcessor      -   Y   Y    transform
flush_workbooks            FileOpsBaseProcessor   -   Y   Y    file_ops (no file key) - RULING
format_excel               FileOpsBaseProcessor   Y   Y   Y    fileops (as is)
free_stages                FileOpsBaseProcessor   Y   Y   Y    base (stage utility, not file_ops) - RULING
generate_column_config     FileOpsBaseProcessor   Y   Y   Y    file_ops (reads xlsx writes yaml)
group_data                 BaseStepProcessor      -   Y   Y    transform
import_file                ImportBaseProcessor    Y   Y   Y    import (as is)
inject_formulas            FileOpsBaseProcessor   Y   Y   Y    fileops (as is)
lookup_data                BaseStepProcessor      -   Y   Y    transform (2 reads)
manage_named_objects       FileOpsBaseProcessor   Y   Y   Y    fileops (as is)
merge_data                 BaseStepProcessor      -   Y   Y    transform (2 reads)
pivot_table                BaseStepProcessor      -   Y   Y    transform
profile_files              ImportBaseProcessor    -   Y   Y    import
profile_named_objects      ImportBaseProcessor    Y   Y   Y    import
profile_sheets             ImportBaseProcessor    Y   Y   Y    import
profile_workbooks          ImportBaseProcessor    Y   Y   Y    import
rename_columns             BaseStepProcessor      -   Y   Y    transform
rows_to_columns            BaseStepProcessor      -   Y   Y    transform
seed_donor_formulas        FileOpsBaseProcessor   Y   Y   Y    fileops (as is)
select_columns             BaseStepProcessor      -   Y   Y    transform
slice_data                 BaseStepProcessor      -   Y   Y    transform
sort_data                  BaseStepProcessor      -   Y   Y    transform
split_column               BaseStepProcessor      -   Y   Y    transform
strip_formula_caches       FileOpsBaseProcessor   Y   Y   Y    fileops (as is)
verify_columns             FileOpsBaseProcessor   -   Y   Y    transform-shaped check, no write - RULING
verify_data                FileOpsBaseProcessor   Y   Y   Y    stage OR sheet mode - RULING (one processor two families?)
verify_excel_storage       FileOpsBaseProcessor   -   Y   Y    fileops (as is)

## Decision points raised by the audit

1. `free_stages` inherits FileOps but touches no file; it releases stages.
   Proposed: `base` family (stage utility), its `stages` key declared as
   a list of stage names the graph treats as neither read nor write.
2. `verify_data` has a stage mode and a sheet mode under one processor.
   Either two processors (verify_stage_data / verify_sheet_data) with
   clean families, or one FileOps processor whose stage mode declares
   `source_stage` explicitly. The name/ref rule favours the split.
3. `verify_columns` reads a stage and writes nothing: a Transform without
   `save_to_stage`, or a `base`-family check. Same question as (2).
4. `debug_breakpoint` is Export-family today but writes a debugging file,
   not a deliverable; fits Export well enough - confirm.
5. `declare_dynamic_formulas` reads and writes YAML (a library file), not
   a workbook; FileOps by medium, but it is the only member not touching
   xlsx. Confirm FileOps.
6. `copy_stage` / `create_stage`: `create_stage` is an Import of inline
   data (no file); `copy_stage` reads one stage and writes another with
   `stage_name` naming the output - Transform if the output key becomes
   `save_to_stage` (it may already; verify), else base.
7. `combine_data`, `lookup_data`, `merge_data`, `diff_data`,
   `deduplicate_data` read or write MORE than one stage. Transform family
   with extra `stage_in` / `stage_out` keys declared per processor - the
   family contribution is the primary pair, extras are the processor's own.

Everything else is Transform (the 20 direct-base processors that read one
stage and write one) or already in its family.

Tests: `tests/test_config_schema.py`, `tests/test_recipe_validation_phase.py`.
