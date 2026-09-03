# Capabilities

What Excel Recipe Processor can do, in prose, for someone deciding whether
it fits a task or looking for the processor that does a particular thing.
For the exact keys each step accepts, see [`STEP_SCHEMAS.md`](STEP_SCHEMAS.md);
for how to add a processor, [`WRITING_A_PROCESSOR.md`](WRITING_A_PROCESSOR.md).

## The idea in one paragraph

A recipe is a YAML file listing steps. Data moves between steps through
named **stages** - in-memory tables that a step reads from and writes to,
declared up front and tracked. Most steps are **transforms** (read a stage,
return a stage). **Import** steps bring files, inline data, or file metadata
into a stage; **export** steps write stages out as workbooks or CSVs.
**File operation** steps then work on the finished workbook itself - formats,
live formulas, named ranges, conditional formatting, data validation - so the
output is a real Excel deliverable, not a data dump. Every step is validated
against its processor's declared schema before anything runs.

## The run

| Capability | What it does |
|---|---|
| Validation phase | Before step 1: every step checked against its schema (unknown keys refused with a nearest-name suggestion), and the stage graph checked in order - read-before-write, double write without confirmation, declared-but-unused stages are errors. `--validate` stops here. |
| Stages | Declared in `settings.stages` with a description and a `protected` flag. Written once (or explicitly replaced), read by any later step. |
| Auto-free | Each stage is released as soon as its last consuming step completes, so memory follows the recipe's shape rather than growing to the end. |
| Variables | `{name}` substitution from `settings.variables`, built-ins (`{date}`, `{recipe_parent_dir}`, ...), and external variables the recipe requires - supplied on the CLI with `--set NAME VALUE` or prompted for. |
| Workbook session | File operations after an export act on the workbook held in memory and write it once; `flush_workbooks` writes earlier when needed. |
| Verification ledger | Every check rule's outcome (pass / warn / halt) is tallied and summarised at run end. |
| Storage audit | After writing, workbooks can be audited for stored-formula grammar and dynamic-array declarations, so a file that Excel would silently repair is caught. |
| Backups | Exports back up a file they replace; how many to keep is configurable. |
| Inspection | `--list-stages`, `--dump-stage NAME[:SPEC]` to CSV, `--stop-after STAGE` to halt early, `--log-file` to mirror the log. |
| Self-description | `--list-capabilities` (text, `--detailed`, `--json`, `--yaml`, `--matrix`), `--export-schemas md|json`, `--get-usage-examples [PROCESSOR]`. |

## Processors by purpose

### Bringing data in (import family)

| Processor | Does |
|---|---|
| `import_file` | Reads xlsx / xls / csv / tsv into a stage; picks a sheet by name, number, or positional token; `header_row` for exports that lead with title lines; can create an empty stage with declared columns when the file is absent. |
| `create_stage` | Builds a stage from inline data in the recipe - a list, a table, or a dictionary. |
| `profile_files` | Per-file metadata (sizes, modification times) as a stage - the basis of a "Sources" tab. |
| `profile_workbooks` | Per-sheet metadata of workbooks: state, tab colour, extents, counts. |
| `profile_sheets` | Per-column metadata of sheets or stages: widths, dtypes, blanks, distinct counts. |
| `profile_named_objects` | Per-name inventory of a workbook's defined names, lambdas, formulas, and tables. |

### Shaping tables (transform family)

| Processor | Does |
|---|---|
| `select_columns` | Keep, drop, reorder, and create columns, by name. |
| `rename_columns` | Rename by mapping, pattern, prefix/suffix, or case conversion. |
| `filter_data` | Keep rows by conditions - equality, text, numeric, membership in a list or in another stage's column - or by a pandas query. |
| `sort_data` | Sort by one or more columns, ascending, descending, or by a custom value order; case-insensitive by default. |
| `deduplicate_data` | One row per key, keeping all columns and reporting value conflicts, optionally to a conflicts stage. |
| `slice_data` | Row ranges, column ranges, or transpose, with header promotion. |
| `split_column` | One column into several by delimiter, fixed width, regex, or position. |
| `fill_data` | Fill missing values with Excel-like strategies, optionally under conditions. |
| `clean_data` | Rule list over columns: replace, regex replace, case, whitespace, numeric and date coercion, standardise values, blank repeated values on continuation rows. |
| `columns_to_rows` / `rows_to_columns` | Wide-to-long and long-to-wide reshapes; the latter refuses silent aggregation. |
| `copy_stage` | Duplicate a stage under another name. |

### Enriching and combining (transform family)

| Processor | Does |
|---|---|
| `add_calculated_column` | New columns from a pandas expression, a first-match rule table (ordered `when`/`then` rules, first true wins, every declared column filled from the winning rule - with `spill_columns` for paired outputs like a value and its audit tag), concatenation, conditionals, math, date, text, constants, row numbers. |
| `lookup_data` | Stage-to-stage lookup by key with key normalisation, defaults for unmatched rows, prefix/suffix on pulled columns, and a match census in the log. An empty lookup stage (an absent optional file) yields blanks rather than halting. |
| `merge_data` | Joins against a stage, a file, or an inline dictionary with left / right / inner / outer semantics. |
| `combine_data` | Stack stages vertically or side by side, with blank spacer rows or columns. |
| `group_data` | Map values into categories from an inline mapping, a stage, a lookup, or a file. |
| `diff_data` | New / changed / unchanged / deleted rows between a stage and a baseline, optionally as separate stages. |

### Summarising (transform family)

| Processor | Does |
|---|---|
| `aggregate_data` | Group by columns and apply sum / count / mean / median / min / max / std / var / nunique. |
| `pivot_table` | Pivot with an aggregation function, margins, and fill values. |
| `add_subtotals` | Subtotal rows inserted before or after each group. |

### Checking (transform checks - read a stage, write nothing)

| Processor | Does |
|---|---|
| `verify_columns` | The stage's columns against an expected list (or another stage's columns), announcing drift; warn or halt per direction. |
| `verify_stage_data` | Row values against rules in `filter_data`'s condition vocabulary, warn or halt per rule, tallied in the verification ledger. |

### Writing files (export family)

| Processor | Does |
|---|---|
| `export_file` | One or many stages to sheets of an xlsx, or to csv / tsv, optionally onto a template workbook; backs up a replaced file. |
| `debug_breakpoint` | Dump a stage to a file and stop the run. |
| `export_filter_step` | Turn reviewed filter terms into a ready-to-paste `filter_data` step (yaml / json). |

### Working on the workbook (file operations family)

| Processor | Does |
|---|---|
| `format_excel` | Presentation on a written workbook: header styles, column formats by header name or letter, widths and auto-fit, freeze panes, filters, banding, outline borders, row heights, hidden columns, tab colour, zoom, sheet state, per-cell formats, reusable templates, pivot styles, workbook themes. |
| `inject_formulas` | Live formulas into cells or ranges, addressed by header name (`{col:Header}`), with fill-down and array declaration; `awaken` turns formula text already in a sheet into live formulas. |
| `manage_named_objects` | Defined names, lambda functions, and tables: create names from columns (by header, resolved on the sheet's own layout), import / export them as YAML or VBA-ready text, copy between workbooks, list, validate. |
| `conditional_format` | Native Excel conditional formatting that stays live in the file: cell conditions, formula rules, colour scales, data bars, targeted at columns, whole rows, or ranges. |
| `excel_data_validation` | Native data validation: dropdown lists (literal, from a defined name, or from a spill range), numeric and date bounds, text length, custom formulas, prompts and alerts. |
| `seed_donor_formulas` | Transplant formulas from a donor workbook's columns into a target's, by header name or letter, with fill-down. |
| `declare_dynamic_formulas` | Mark formulas as dynamic-array era so Excel does not show the implicit-intersection `@`. |
| `strip_formula_caches` | Remove cached formula results Excel stores on save, keeping the formulas, so files do not balloon. |
| `verify_excel_storage` | Audit a workbook's stored formula grammar and declarations. |
| `verify_sheet_data` | Row values of a written sheet against rules (values, not formula results). |
| `generate_column_config` | Compare a source and a template workbook and write a column-configuration YAML. |
| `flush_workbooks` | Write session-held workbooks now. |

### Stage utilities (base family)

| Processor | Does |
|---|---|
| `free_stages` | Release named stages mid-run; a later read of a released stage is an error. |
| `filter_terms_detector` | Learn candidate filter terms by comparing raw data to a hand-filtered result. |

## Conventions worth knowing before writing a recipe

- **Names, not positions.** Data steps name columns by header string only. File operations may use `column_names` or `column_refs` (Excel letters), never a mix in one list.
- **One key per concept.** No aliases; a rename is a breaking change shipped with the recipes. The ledger of past renames is in `dev_notes/KEY_MIGRATIONS_LEDGER.md`.
- **Evaluated text names its dialect.** `pandas_formula`, `pandas_rules`, `pandas_default`, `excel_formula`.
- **Case-insensitive by default** (`case_sensitive: false`), matching Excel.
- **Sheets by name or position token.** `sheet_name: "Data"` or `"?sheet_001?"`; the token uses characters Excel forbids in tab names, so it cannot collide.
- **Fail loud.** Unknown keys, unused stages, empty results where data was expected, and rule violations set to `halt` all stop the run with a located message.
