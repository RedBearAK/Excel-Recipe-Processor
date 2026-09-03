# Key migrations and disambiguations - a maintenance ledger

**What this is.** A record of every recipe-vocabulary term that has been
renamed, split, removed, or refused in this project, listing the OLD or
INCORRECT term beside the NEW or CORRECT one. Its purpose is project
maintenance: when checking the health of examples, documentation, tests,
recipes, dev notes, or a model's output, search for the terms in the
left-hand columns. Any hit outside this file is stale and should be fixed
to the right-hand column.

**What this is not.** Not a compatibility table. None of the old terms are
accepted anywhere; the schemas refuse them at recipe load and at processor
construction, usually with a nearest-name suggestion. There is no alias,
no deprecation period, no `renamed_from` in the code - the rulebook is one
key per concept, and renames land with the recipes that use them.

**How to sweep.** From the repo root, for every old term in this file:

```bash
grep -rn --include='*.py' --include='*.yaml' --include='*.md' -e 'OLD_TERM' . | grep -v __pycache__
```

A hit inside a guided error message that names the old term only to refuse
it is acceptable; anything that *uses* the old term is not. Search with
word boundaries or the quoted form (`'new_column_name'`, not
`new_column_name`) - `new_column_names` and `lookup_source_files` are
current vocabulary and will otherwise flood the results. Ignore
`X_broken_tests/`, which is a parking lot for tests known to be stale. The tests
`test_examples_validate_against_schemas.py` and the construction-time
schema check in `BaseStepProcessor.__init__` catch uses in examples and
tests automatically; prose and recipes outside this repo need the grep.

---

## Global terms (apply to every processor)

| Old / incorrect | New / correct | Rule |
|---|---|---|
| `columns` in a **file-addressing** rule (format_excel, conditional_format, manage_named_objects ranges, seed_donor_formulas) | `column_names` (header names) and/or `column_refs` (Excel letters) | In file_ops, a header named `BQ` and a letter `BQ` must be told apart by KEY, never by shape |
| a bare letter like `"C"` in a `column_names` list | move it to `column_refs` | The resolver refuses a letter shape in a name list |
| `force_column_names: true` | removed | The name/ref split made the flag meaningless |
| `columns` in a **data** processor | still `columns` (transform family: names only, never positions) | Data processors have no positional concept |
| an integer position in any name list (`columns_to_keep: [1, "Name"]`) | names only, strings only | An integer header (a year, a plant number) makes `2` ambiguous |
| a single string where a list of names is expected (`key_columns: "ID"`, `group_by: "Region"`) | a list (`key_columns: ["ID"]`) | One shape per key |
| `ignore_case: true` | `case_sensitive: false` (the default everywhere) | One polarity; Excel compares case-insensitively |
| `sheet:` (singular, as a tab address) | `sheet_name:` | 2026-08-14 sheet-addressing doctrine; `?sheet_NNN?` tokens are positional |
| `sheets:` (as a list of tabs to write) | `sheets_to_create:` | Composite keys say what the list is for |
| `stage:` (as the stage a step reads) | `source_stage:` | One key for the primary read; extra reads are named (`lookup_stage`, `reference_stage`, `filtered_stage`) |
| `stage_name:` as a step's OUTPUT (copy_stage, create_stage) | `save_to_stage:` | Every writer uses the same key |
| `formula:` (bare, evaluated) | `pandas_formula:` or `excel_formula:` | An evaluated string never sits under a bare key; the key names its dialect |
| `rules:` / `default:` under `calculation_type: first_match` | `pandas_rules:` / `pandas_default:` | Same rule |
| a camelCase or Excel-dialog spelling as an enum value (`greaterThan`, `containsText`, `notBetween`, `textLength`) | the snake_case ERP name (`greater_than`, `contains`, `not_between`, `text_length`) | A library's spelling is storage, never recipe vocabulary |
| `--variable NAME=VALUE` (CLI) | `--set NAME VALUE` | |
| `pytest`-style or `unittest` tests | standalone modules that run, print, and exit 0/1 | House test style |

### Global concepts that were removed, not renamed

| Removed | Why | Use instead |
|---|---|---|
| alias tables (`CONDITION_ALIASES` in conditional_format, camelCase operators in excel_data_validation) | one spelling per concept | the canonical name; the schema lists the set |
| union lists that auto-detected "name or ref" | ambiguity by design | `column_names` / `column_refs` |
| positional column selection in transforms | ambiguity with integer headers | names |
| `mode: dead` in inject_formulas (formula text written into a STAGE, live on export anyway) | a Transform hiding in a FileOps processor that could not do what its name promised | `mode: text` - the same formulas into the FILE as inert string cells; `awaken` makes them live |
| `predefined_groups` in group_data (a hardcoded table of one company's place names) | data does not belong in the generic tool | `groups`, `groups_source`, or `groups_file` |

---

## Processor-specific terms

### add_calculated_column
| Old / incorrect | New / correct |
|---|---|
| `calculation: {formula: ...}` | `calculation: {pandas_formula: ...}` (or `formula_components`) |
| `calculation: {type: date_extract, date_column:, extract:}` | `pandas_formula: "pd.to_datetime({col:X}).dt.month_name()"` |
| two paired `np.select` steps kept in sync by hand | one step: `calculation_type: first_match` with `spill_columns` |
| `spill_column` | `spill_columns` |

### aggregate_data
| Old / incorrect | New / correct |
|---|---|
| `new_column_name` | `output_name` |
| `group_by: "Col"` | `group_by: ["Col"]` |

### clean_data
| Old / incorrect | New / correct |
|---|---|
| `decimal_places`, `thousands_separator` on `fix_numeric` | not supported; removed from examples |
| `- column: "X"` inside a rule | `- columns: ["X"]` |

### combine_data
| Old / incorrect | New / correct |
|---|---|
| `combine_type: horizontal_stack` | `horizontal_concat` |
| `column_handling: union_columns` / `intersect_columns` / `positional` | `require_matching_columns` or `allow_mismatched_columns` |
| `data_sources: [{stage: ...}]` | `data_sources: [{insert_from_stage: ...}]` |

### conditional_format
| Old / incorrect | New / correct |
|---|---|
| `when_cell: {columns: [...]}` | `when_cell: {column_names: [...]}` |
| `sheet:` | `sheet_name:` |
| `greaterThan`, `containsText`, `beginsWith`, `duplicateValues`, ... | `greater_than`, `contains`, `starts_with`, `duplicates`, ... |
| `when_formula: {formula: ...}` | `when_formula: "<formula string>"` |

### create_stage
| Old / incorrect | New / correct |
|---|---|
| `data: {entries: ...}` | `data: {data: ...}` |
| `format: matrix` | `format: table` |
| `stage_name:` as output | `save_to_stage:` |

### diff_data
| Old / incorrect | New / correct |
|---|---|
| `key_columns: "ID"` | `key_columns: ["ID"]` |

### excel_data_validation
| Old / incorrect | New / correct |
|---|---|
| `range: "B2"` | `apply_to_ranges: ["B2"]` |
| `whole`, `textLength` as `validation_type` | `whole_number`, `text_length` |
| `notBetween`, `greaterThanOrEqual` as `operator` | `not_between`, `greater_than_or_equal` |

### export_file
| Old / incorrect | New / correct |
|---|---|
| `sheets:` / `sheet_names: {Tab: stage}` | `sheets_to_create: [{sheet_name:, data_source:}]` |
| `mode: append` | not supported |
| `active: true` on a sheet entry | not supported |

### filter_data
| Old / incorrect | New / correct |
|---|---|
| `save_to_stage:` inside a filter rule | `stage_name:` + `stage_column:` on `in_stage` / `not_in_stage` rules |

### filter_terms_detector
| Old / incorrect | New / correct |
|---|---|
| `raw_stage:` | `source_stage:` (the raw data; `filtered_stage` is the learned-from result) |

### format_excel
| Old / incorrect | New / correct |
|---|---|
| `column_formats: [{columns: [...]}]` | `column_formats: [{column_names: [...]}]` and/or `column_refs` |
| `sheet_specific: {Tab: {...}}` | one `formatting` entry per `sheet_names` list |
| `column_widths: {A: 15}` | `column_formats` with `width` |
| `cell_formats: [{cells: "B2"}]` | `cells: ["B2"]` (a list) |
| the guided error text "(columns is names only)" | "(column_names is names only)" |

### generate_column_config
| Old / incorrect | New / correct |
|---|---|
| `check_column_data` | removed |

### group_data
| Old / incorrect | New / correct |
|---|---|
| `predefined_groups: <name>` | removed; use `groups` / `groups_source` / `groups_file` |
| `groups_source: {value_column:}` | `values_column:` |
| `groups_source: {file_path:}` | `filename:` |
| `header_row`, `value_start_column`, `lookup_value`, `join_type` in `groups_source` | not supported |

### import_file
| Old / incorrect | New / correct |
|---|---|
| import -> `slice_data` -> promote headers, for exports with title rows | `header_row: N` on the import |
| `replace_current_data` | not supported |
| `create_empty_columns` without `on_missing_file: create_empty` | only legal under that mode |

### inject_formulas
| Old / incorrect | New / correct |
|---|---|
| top-level `sheet_names` + `formulas` in live mode (broadcast) | `sheets_to_receive_formulas: [{sheet_names: [...], formulas: [...]}]` |
| `sheet_name:` (singular) inside an entry | `sheet_names: [...]` |
| `formula:` in a formula entry | `excel_formula:` |
| `mode: dead` with `source_stage` / `save_to_stage` | `mode: text` with `target_file` (inert formula text in the file) |
| `array_formula: true` on a scalar per-row formula | omit (a false declaration; the `@` is cosmetic) |

### lookup_data
| Old / incorrect | New / correct |
|---|---|
| `lookup_source: {type: stage, stage_name:}` | `lookup_stage:` |
| `source_key` / `lookup_key` | `match_col_in_main_data` / `match_col_in_lookup_data` |

### manage_named_objects
| Old / incorrect | New / correct |
|---|---|
| `ranges: [{columns: [...]}]` | `ranges: [{column_names: [...]}]` |
| `sheet:` in a range entry | `sheet_name:` |
| `comment:`, `name_manager_comment:` | `name_mgr_comment:` |
| `force_column_names` | removed |
| `include_local` on an export operation | only meaningful on import / copy_direct |

### merge_data
| Old / incorrect | New / correct |
|---|---|
| `merge_source: {type: stage/file/inline}` | `type:` one of `excel`, `csv`, `tsv`, `dictionary`, `stage` |

### pivot_table
| Old / incorrect | New / correct |
|---|---|
| `values: "Col"` (string) | `values: ["Col"]` |

### rename_columns
| Old / incorrect | New / correct |
|---|---|
| `case_conversion: title_case` | `title` |
| `replace_characters:` | `replace_spaces:` / `strip_characters:` |

### seed_donor_formulas
| Old / incorrect | New / correct |
|---|---|
| `columns: ["C", "Total"]` (mixed) | `column_refs: ["C"]` + `column_names: ["Total"]` |
| `force_column_names` | removed |

### select_columns
| Old / incorrect | New / correct |
|---|---|
| `columns_to_keep: [2, "Name"]` | names only |

### sort_data
| Old / incorrect | New / correct |
|---|---|
| `ascending: [true, false]` | `sort_type: ascending` / `descending` (`custom` with `custom_orders`) |
| `ignore_case` | `case_sensitive` |

### split_column
| Old / incorrect | New / correct |
|---|---|
| `new_columns:` | `new_column_names:` |

### strip_formula_caches
| Old / incorrect | New / correct |
|---|---|
| `scope: "..."` (string) | `scope: [{sheet_names: [...], ...}]` |

### verify_columns
| Old / incorrect | New / correct |
|---|---|
| `stage:` | `source_stage:` |
| a `save_to_stage` on a check | none; a check writes nothing |

### verify_data (split 2026-09-03)
| Old / incorrect | New / correct |
|---|---|
| `processor_type: verify_data` with `source_stage` | `verify_stage_data` |
| `processor_type: verify_data` with `target_file` + `sheet_name` | `verify_sheet_data` |
| `stage:` inside a rule | `stage_name:` (filter_data's rule grammar) |
| `severity: sorta` or anything but `warn` / `halt` | `warn` (default) or `halt` |

### Retired processors
| Old | Replacement |
|---|---|
| `verify_data` | `verify_stage_data`, `verify_sheet_data` |
| `load_stage` (deprecated module) | `import_file` / `create_stage` |

---

## Documentation that was retired rather than repaired

- `docs/processors/*.md` (15 hand-written pages, 8 carrying retired keys):
  superseded by the generated `docs/STEP_SCHEMAS.md` and the validated
  `_examples/*.yaml` (`--get-usage-examples`).
- `docs/recipes/variables.md` and `docs/recipes/yaml-syntax.md`: pre-stage
  era (`output_filename`, no stages); superseded by `--get-settings-examples`
  and the README Quick Start.
- `docs/cli/commands.md`: now generated from `--help`.
- README sections "Available Processors", "Processor Details", "Advanced
  Features", "Recipe Examples", "Recipe Library", "Adding New Processors":
  rewritten 2026-09-04; they named classes that do not exist
  (`ExcelPipeline`, `RecipeValidator`, `PipelineError`) and pre-stage recipes.

## Dates

- 2026-08-13 - `stage_name` -> `save_to_stage` on copy_stage / create_stage
- 2026-08-14 - `sheet` -> `sheet_name`; `?sheet_NNN?` positional tokens
- 2026-08-17 - inject_formulas `sheets_to_receive_formulas` entries replace the broadcast pair
- 2026-08-26 - `formula` -> `excel_formula`; `columns` -> `column_names` / `column_refs` in format_excel; bare letter shapes refused in name lists
- 2026-09-03 - schema declarations; `ignore_case` -> `case_sensitive`; verify_data split; `raw_stage` -> `source_stage`; positional select retired; `new_column_name` -> `output_name`; aliases removed
- 2026-09-04 - remaining file_ops `columns` -> `column_names`; `force_column_names` removed everywhere; `predefined_groups` removed; inject_formulas `mode: dead` removed; every processor declares a schema
