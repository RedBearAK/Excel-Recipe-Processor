# `seed_donor_formulas`

**Family:** `file_ops`

Transplant formulas from donor Excel files to seed calculation columns

## Notes

- **column keys**: column_names resolve by header on each side; column_refs are literal letters

## Keys

Generated from the declared schema; keys not listed are refused at recipe load.

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `source_file`: str; REQUIRED
- `source_sheet`: any; REQUIRED
- `target_file`: str; REQUIRED
- `target_sheet`: any; REQUIRED
- `column_names`: list of str - Header names resolved separately in donor and target
- `column_refs`: list of str - Positional Excel refs like A or BQ - never header names
- `start_row`: int; default 2
- `row_count`: int; default 3
- `fill_down`: bool; default false
- `fill_anchor_columns`: list of str - Columns whose extent the fill-down follows
- `on_existing_cell`: str; default "error"; one of error, skip, overwrite
- `array_formula_mode`: str; default "preserve"; one of preserve, convert
- at least one of: `column_names`, `column_refs`

## Examples

Every step below validates against the schema (tests/test_examples_validate_against_schemas.py).

### basic column references

Transplant formulas from template to new file using column letters

```yaml
settings:
  description: "Transplant formulas from template to new file using column letters"

recipe:
  - step_description: "Seed calculation formulas from template"
    processor_type: "seed_donor_formulas"
    source_file: "templates/budget_template.xlsx"
    source_sheet: "Summary"
    target_file: "output/monthly_budget.xlsx"
    target_sheet: "Summary"
    column_refs: ["C", "D", "E"]  # Total, Tax, Net columns
    start_row: 2
    row_count: 3
```

### mixed column types

Mix Excel references and column names for flexibility

```yaml
settings:
  description: "Mix Excel references and column names for flexibility"

recipe:
  - step_description: "Transplant revenue calculation formulas"
    # REQ - Must be "seed_donor_formulas" for this processor type
    processor_type: "seed_donor_formulas"
    # REQ - Donor workbook holding the formulas to transplant
    source_file: "templates/financial_model.xlsx"
    source_sheet: "Calculations"
    target_file: "output/q4_projections.xlsx"
    target_sheet: "Calculations"
    column_refs: ["B", "C"]
    column_names: ["Total_Revenue", "Profit_Margin", "ROI"]
    start_row: 3
    row_count: 5
```

### column names only

Use only column names for more readable configurations

```yaml
settings:
  description: "Use only column names for more readable configurations"

recipe:
  - step_description: "Seed inventory formulas by name"
    processor_type: "seed_donor_formulas"
    source_file: "templates/inventory_tracker.xlsx"
    source_sheet: "Stock"
    target_file: "output/warehouse_A_inventory.xlsx"
    target_sheet: "Stock"
    column_names: ["Units_On_Hand", "Reorder_Point", "Total_Value"]
    start_row: 2
    row_count: 4
```

### workflow integration

Common workflow pattern - create file, then seed formulas

```yaml
settings:
  description: "Export data then seed formulas into the new file"
  stages:
    - stage_name: "stg_inventory_data"
      description: "Inventory rows ready to export"
      protected: false

recipe:
  - step_description: "Export base data to new file"
    processor_type: "export_file"
    source_stage: "stg_inventory_data"
    output_file: "output/location_inventory.xlsx"
    sheet_name: "Current_Stock"

  - step_description: "Seed calculation formulas"
    processor_type: "seed_donor_formulas"
    source_file: "templates/inventory_calculations.xlsx"
    source_sheet: "Current_Stock"
    target_file: "output/location_inventory.xlsx"
    target_sheet: "Current_Stock"
    column_refs: ["D", "E", "F"]  # Value, Reorder, Status columns
    start_row: 2
    row_count: 10
```

### fill down

Seed a few rows, then continue the formulas to the end of the data

```yaml
# Filling every row costs essentially nothing. Measured on 8,681 rows across
# five columns: 43,395 formulas in 0.42 seconds, because loading and saving
# the workbook dominate and both happen either way.
#
# References are adjusted the way Excel's own fill-down does - relative cell
# references move with the row, named ranges and absolute references do not.

settings:
  description: "Transplant formulas and continue them to the last data row"

recipe:
  - step_description: "Seed and fill the formula columns"
    processor_type: "seed_donor_formulas"
    source_file: "donor_workbook.xlsx"
    source_sheet: "Data"
    target_file: "output/report.xlsx"
    target_sheet: "Data"
    column_names: ["Test Fresh", "Test Cans", "Sale Type"]
    start_row: 2
    row_count: 3
    # OPT - Continue the seeded formulas to the last populated row
    # Default value: false
    fill_down: true
    # OPT - Columns used to measure how far the data goes. Defaults to every
    # column, which counts a trailing marker row as data. Naming a column
    # that is populated on every real row and blank on the marker stops the
    # fill exactly one row short of it.
    fill_anchor_columns: ["Van Number"]
```

### array formula

Choose whether an array formula stays one

```yaml
# A donor cell entered with Ctrl-Shift-Enter arrives as an array formula.
# Many such formulas do not need to be arrays at all - if every argument is
# a single-cell comparison and the functions are ordinary scalar ones, it
# was habit rather than necessity.
#
# Converting gives an identical result, and an ordinary formula is what
# Excel fills down and edits natively.

settings:
  description: "Transplant formulas, converting array entry to ordinary"

recipe:
  - step_description: "Seed the formula columns"
    processor_type: "seed_donor_formulas"
    source_file: "donor_workbook.xlsx"
    source_sheet: "Data"
    target_file: "output/report.xlsx"
    target_sheet: "Data"
    column_names: ["Sale Type"]
    start_row: 2
    row_count: 3
    fill_down: true
    # OPT - "preserve" (default) keeps an array formula as an array formula.
    # "convert" writes it as an ordinary formula.
    array_formula_mode: "convert"
```

### occupied cells

Decide what happens when a target cell is not empty

```yaml
# Applies to the seeded rows and the filled rows alike, so the outcome does
# not depend on which row a collision lands in.

settings:
  description: "Fill formulas over a sheet that may already hold values"

recipe:
  - step_description: "Seed and fill, stepping around existing values"
    processor_type: "seed_donor_formulas"
    source_file: "donor_workbook.xlsx"
    source_sheet: "Data"
    target_file: "output/report.xlsx"
    target_sheet: "Data"
    column_names: ["Sale Type"]
    start_row: 2
    row_count: 3
    fill_down: true
    # OPT - "error" (default), "skip", or "overwrite"
    #   error      stop, naming the cell and its current contents
    #   skip       leave it alone, count it, report the total
    #   overwrite  write anyway
    # "skip" is what a trailing marker row wants.
    on_existing_cell: "skip"
```

## Parameter notes

- `source_file` (required): Path to Excel file containing the formula templates
- `source_sheet` (required): Sheet name in source file containing formulas
- `target_file` (required): Path to Excel file where formulas will be planted
- `target_sheet` (required): Sheet name in target file for formula placement
- `column_names`: Header names, each resolved on the donor and the target separately
- `column_refs`: Excel column letters, taken literally on both sides
- `start_row` (default `2`): Starting row number for formula sampling (1-indexed)
- `row_count` (default `3`): Number of rows to process (maximum 10)

