# Excel Recipe Processor

🚀 **Automated Excel data processing with YAML/JSON recipes**

Transform your Excel automation workflows into reusable, version-controlled recipes. No more manual clicking through Excel - define your data processing steps once and run them repeatedly with consistent results.

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## 🎯 **What It Does**

Excel Recipe Processor automates complex Excel data transformations using simple YAML configuration files. Perfect for:

- **Business Report Automation** - Transform raw data exports into polished reports
- **Data Pipeline Integration** - Reliable, repeatable Excel processing in data workflows  
- **ETL Operations** - Extract, transform, and load Excel data with consistency
- **Report Standardization** - Ensure consistent data processing across teams

## ⚡ **Quick Start**

### Installation

```bash
pip install excel-recipe-processor
```

### Your First Recipe

Data flows between steps through named **stages**. A recipe imports into a
stage, transforms stage to stage, and exports a stage. Create `my_recipe.yaml`:

```yaml
settings:
  description: "Electronics report"
  stages:
    - stage_name: "stg_raw"
      description: "Everything in the download"
      protected: false
    - stage_name: "stg_electronics"
      description: "Electronics rows only"
      protected: false
    - stage_name: "stg_valued"
      description: "With a total value per row"
      protected: false
    - stage_name: "stg_sorted"
      description: "Highest value first"
      protected: false

recipe:
  - step_description: "Import the download"
    processor_type: "import_file"
    input_file: "raw_data.xlsx"
    save_to_stage: "stg_raw"

  - step_description: "Keep electronics"
    processor_type: "filter_data"
    source_stage: "stg_raw"
    filters:
      - column: "Category"
        condition: "equals"
        value: "Electronics"
    save_to_stage: "stg_electronics"

  - step_description: "Total value per row"
    processor_type: "add_calculated_column"
    source_stage: "stg_electronics"
    new_column: "Total_Value"
    calculation:
      pandas_formula: "{col:Quantity} * {col:Price}"
    save_to_stage: "stg_valued"

  - step_description: "Highest value first"
    processor_type: "sort_data"
    source_stage: "stg_valued"
    columns: ["Total_Value"]
    sort_type: "descending"
    save_to_stage: "stg_sorted"

  - step_description: "Write the report"
    processor_type: "export_file"
    source_stage: "stg_sorted"
    output_file: "electronics_report.xlsx"
```

Check it, then run it:

```bash
python -m excel_recipe_processor my_recipe.yaml --validate
python -m excel_recipe_processor my_recipe.yaml
```

Or from Python:

```python
from excel_recipe_processor.core.recipe_pipeline import RecipePipeline

report = RecipePipeline().run_complete_recipe("my_recipe.yaml")
```

## 🔧 **What It Can Do**

46 processors in five families. The one-line version is here; the prose
description of each, with the run-level features (stages, validation,
auto-free, variables, the workbook session, audits), is in
[`docs/CAPABILITIES.md`](docs/CAPABILITIES.md).

| Purpose | Processors |
|---|---|
| Bring data in | `import_file`, `create_stage`, `profile_files`, `profile_workbooks`, `profile_sheets`, `profile_named_objects` |
| Shape tables | `select_columns`, `rename_columns`, `filter_data`, `sort_data`, `deduplicate_data`, `slice_data`, `split_column`, `fill_data`, `clean_data`, `columns_to_rows`, `rows_to_columns`, `copy_stage` |
| Enrich and combine | `add_calculated_column` (expressions and first-match rule tables), `lookup_data`, `merge_data`, `combine_data`, `group_data`, `diff_data` |
| Summarise | `aggregate_data`, `pivot_table`, `add_subtotals` |
| Check | `verify_columns`, `verify_stage_data` (stages); `verify_sheet_data`, `verify_excel_storage` (files) |
| Write files | `export_file`, `debug_breakpoint`, `export_filter_step` |
| Work on the workbook | `format_excel`, `inject_formulas`, `manage_named_objects`, `conditional_format`, `excel_data_validation`, `seed_donor_formulas`, `declare_dynamic_formulas`, `strip_formula_caches`, `generate_column_config`, `flush_workbooks` |
| Stage utilities | `free_stages`, `filter_terms_detector` |

The workbook row is what makes the output a real Excel deliverable rather
than a data dump: live formulas addressed by header name, defined names and
lambdas, native conditional formatting and data validation, formats and
templates - all written into the file the export produced.

## 📋 **Recipe Examples**

Every processor ships worked examples that are validated against its schema
in the test suite, so they cannot drift from the code:

```bash
python -m excel_recipe_processor --get-usage-examples filter_data
python -m excel_recipe_processor --get-usage-examples            # every processor
python -m excel_recipe_processor --get-settings-examples         # the settings block
```

The Quick Start above is a complete, runnable recipe. Settings-block
features (variables, external variables, stages, error policy) are documented
by `--get-settings-examples`; the CLI in [`docs/cli/commands.md`](docs/cli/commands.md).

## 📐 **Start Here: The Declared Schemas**

Every processor declares exactly which keys its recipe step accepts - kinds,
required, defaults, allowed values, nested shapes, and which keys go with
which mode. A recipe is validated against those declarations before any step
runs, and a key the processor does not declare is refused at load with a
nearest-name suggestion. There is no vocabulary outside the declarations.

Two documents let a person or a model learn the whole structure quickly:

| Read this | To |
|---|---|
| [`docs/STEP_SCHEMAS.md`](docs/STEP_SCHEMAS.md) | write or check a **recipe**: every processor, every key, generated from the live declarations |
| [`docs/WRITING_A_PROCESSOR.md`](docs/WRITING_A_PROCESSOR.md) | write a **new processor**: pick its family, declare its schema, follow the naming rules |

Regenerate the schema document from the code any time (this is the source; the
committed file is a convenience):

```bash
python -m excel_recipe_processor --export-schemas md > docs/STEP_SCHEMAS.md
python -m excel_recipe_processor --export-schemas json     # machine form
```

Check a recipe without running it - the same checks run at the start of every
real run, so a recipe that validates will not fail on vocabulary or stage
wiring mid-pipeline:

```bash
python -m excel_recipe_processor recipe.yaml --validate
```

### Processor families

Each processor belongs to a family, set by the base class it inherits, and the
family contributes the step keys it needs and decides how columns may be named:

- **transform** - reads a stage, returns a stage; columns by header name only
  (`source_stage`, `save_to_stage`). A *check* is a transform that writes nothing.
- **import** - creates a stage from a file, inline data, or a profile (`save_to_stage`)
- **export** - consumes a stage into a file (`source_stage`)
- **file_ops** - changes a workbook in place; the only family where a positional
  column ref is legal (`column_names` / `column_refs` pairs)
- **base** - stage utilities that touch no data (`free_stages`)

### Conventions the schemas enforce

- One key per concept, no aliases. Renames are breaking and land with the recipes.
- An evaluated string never sits under a bare key: `pandas_formula`,
  `pandas_rules`, `pandas_default`, `excel_formula` name their dialect.
- Column-name lists are lists of strings, never positions.
- Enum values are snake_case ERP vocabulary; a library's own spelling is storage.
- `case_sensitive: false` by default, everywhere.
- Stage graph is strict: a stage read before it is written, written twice
  without `confirm_stage_replacement`, or declared but never used, is an error.

## 🏗️ **Recipe Structure**

### Basic Recipe Format
```yaml
settings:
  description: "What this recipe produces"
  stages:
    - stage_name: "stg_raw"
      description: "Imported rows"
      protected: false

recipe:
  - step_description: "Human readable step description"
    processor_type: "processor_name"
    # Processor-specific keys: see docs/STEP_SCHEMAS.md
```

### Step Configuration

Each step must have:
- **`step_description`** - Human-readable description
- **`processor_type`** - Which processor to use
- **Family keys** - `source_stage` / `save_to_stage` for transforms, `target_file` for file operations
- **Processor-specific keys** - exactly those the processor declares

## 🔍 **Processor Details**

Every processor's keys, kinds, defaults, and allowed values:
[`docs/STEP_SCHEMAS.md`](docs/STEP_SCHEMAS.md) (generated). Worked examples
for each: `python -m excel_recipe_processor --get-usage-examples <name>`.

## 🛠️ **Beyond the Basics**

- **External variables** - a recipe declares `required_external_vars`; supply
  them with `--set NAME VALUE` (repeatable) or answer the prompt.
- **Inspection** - `--list-stages RECIPE`, `--dump-stage NAME` (to CSV),
  `--stop-after STAGE`, `--log-file PATH`.
- **The workbook layer** - after `export_file`, file-operation steps act on the
  written workbook: `format_excel`, `inject_formulas`, `manage_named_objects`,
  `conditional_format`, `excel_data_validation`. See `docs/CAPABILITIES.md`.
- **Programmatic use** - `RecipePipeline().run_complete_recipe(path, cli_variables)`
  returns a completion report; `RecipePipelineError` is the failure type.

## 📊 **Use Cases**

### Business Intelligence
- **Monthly Sales Reports** - Automate recurring report generation
- **KPI Dashboards** - Transform raw data into dashboard-ready formats
- **Financial Analysis** - Standardize financial data processing

### Data Operations  
- **ETL Pipelines** - Reliable Excel processing in data workflows
- **Data Quality** - Consistent cleaning and validation rules
- **Report Distribution** - Automated report generation and formatting

### Team Collaboration
- **Standardized Processes** - Share processing recipes across teams
- **Version Control** - Track changes to data processing logic
- **Documentation** - Self-documenting data transformation workflows

## 🧪 **Development**

### Running Tests
Tests are standalone modules (no pytest style): each runs on its own, prints
what it checked, and exits 0/1.

```bash
# One module
PYTHONPATH=. python3 tests/test_filter_data_processor.py

# All modules, four at a time (fast map); rerun any non-zero serially for a verdict
ls tests/test_*.py | xargs -P 4 -I{} sh -c 'PYTHONPATH=. python3 {} >/dev/null 2>&1; echo "$? {}"' | grep -v "^0 "
```

Two tests guard the vocabulary itself: `test_examples_validate_against_schemas.py`
(every example step validates against its schema) and `test_schema_export.py`
(the published schema covers every processor).

### Project Structure
```
excel_recipe_processor/
├── config/          # Recipe loading, settings examples
├── core/            # Pipeline, stages, schema vocabulary, validation phase, export
├── processors/      # One module per processor; _helpers/ and _examples/ beside them
├── readers/         # File reading
├── utils/           # Shared utilities
docs/                # STEP_SCHEMAS.md (generated), WRITING_A_PROCESSOR.md, CAPABILITIES.md, cli/
dev_notes/           # Design notes and the key-migration ledger
tests/               # Standalone test modules
```

### Adding New Processors

Read [`docs/WRITING_A_PROCESSOR.md`](docs/WRITING_A_PROCESSOR.md) first: it
has the family decision table, a schema template, and the naming rules. The
short version:

1. Inherit the family base (`TransformBaseProcessor`, `ImportBaseProcessor`,
   `ExportBaseProcessor`, `FileOpsBaseProcessor`) by what the step addresses.
2. Declare `config_schema()` - a processor without one cannot register.
3. Implement `execute()` (transforms), `load_data()` (imports), or
   `perform_file_operation()` (file operations).
4. Add `get_minimal_config()`, an `_examples/<name>_examples.yaml`, and a
   standalone test module.
5. Register it in `core/pipeline.py`.

## 🤝 **Contributing**

Contributions welcome! Please:

1. **Fork the repository**
2. **Create a feature branch** (`git checkout -b feature/amazing-processor`)
3. **Add tests** for new functionality (standalone modules, house style)
4. **Read [`docs/WRITING_A_PROCESSOR.md`](docs/WRITING_A_PROCESSOR.md)** before adding a processor; declare its schema
5. **Submit a pull request**

### Code Guidelines
- Use native Python types (`list`, `dict`) instead of `typing` imports
- Follow existing error handling patterns
- Add comprehensive test coverage
- Include docstrings for public methods

## 📄 **License**

GNU General Public License 3.0 - see LICENSE file for details.

## 🔗 **Links**

- **Documentation:** [Full documentation](https://github.com/your-repo/docs)
- **Issues:** [Report bugs](https://github.com/your-repo/issues)
