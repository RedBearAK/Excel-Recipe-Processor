# Excel Recipe Processor

Automate complex Excel data processing workflows using simple YAML recipes.

## What It Does

Replace manual Excel operations with automated, repeatable recipes:

```yaml
# Instead of manual: Filter → Find/Replace → Pivot Table → Save
recipe:
  - processor_type: "filter_data"
    filters:
      - column: "Status" 
        condition: "equals"
        value: "Active"
  
  - processor_type: "clean_data"
    rules:
      - column: "Product"
        action: "replace"
        old_value: "FLESH"
        new_value: "CANS"
        condition_column: "Type"
        condition: "contains" 
        condition_value: "Canned"
  
  - processor_type: "pivot_table"
    index: ["Region", "Product"]
    columns: ["Carrier"]
    values: ["Container"]
    aggfunc: "count"
```

## Quick Start

```bash
# Install (assuming package structure)
pip install excel-recipe-processor

# Process a file
python -m excel_recipe_processor data.xlsx --config recipe.yaml

# See what's available  
python -m excel_recipe_processor --list-capabilities --detailed
```

## Core Concepts

**🧾 Recipes**: YAML files defining step-by-step data transformations  
**🔧 Processors**: Several specialized tools for different operations  
**📊 Pipeline**: Automatic execution from raw data to final output  
**🐛 Debug**: Built-in checkpoints for troubleshooting  

## When to Use This

- **Repetitive Excel work**: Same steps every week/month/day
- **SQL export cleanup**: Fix formatting and invisible characters
- **Complex transformations**: Multi-step data processing 
- **Quality control**: Consistent, auditable results
- **Team workflows**: Share recipes instead of manual instructions

## Available Processors

| Processor               | Purpose                     | Example Use                         |
|-------------------------|-----------------------------|-------------------------------------|
| `add_calculated_column` | Create new fields           | Price × Quantity = Total            |
| `add_subtotals`         | Insert subtotal rows        | Regional totals in reports          |
| `aggregate_data`        | Summary statistics          | Total sales by category             |
| `clean_data`            | Fix formatting, replace     | Remove invisible chars from SQL     |
| `create_stage`          | Insert named data stage     | ???   |
| `debug_breakpoint`      | Save intermediate results   | Check data at any step              |
| `export_file`           | Export data to file         | ???   |
| `fill_data`             | Fill missing values         | Replace nulls with 'Unknown'        |
| `filter_data`           | Remove unwanted rows        | Keep only active orders             |
| `group_data`            | Categorize values           | Cities → Regions                    |
| `import_file`           | Import data from file       | ???   |
| `load_stage`            | Load a named data stage     | ???   |
| `lookup_data`           | Enrich with external data   | Add customer details                |
| `merge_data`            | Join with external datasets | Orders + cust. file = Complete data |
| `pivot_table`           | Create cross-tabulations    | Sales by region and product         |
| `rename_columns`        | Standardize headers         | Make column names consistent        |
| `save_stage`            | Save data to named stage    | ???   |
| `sort_data`             | Order records               | Sort by date, priority              |
| `split_column`          | Separate combined data      | "Last, First" → separate columns    |

## Next Steps

- **New users**: [Your First Recipe](getting-started/your-first-recipe.md)
- **Recipe writing**: [YAML Syntax Guide](recipes/yaml-syntax.md)  
- **Specific processor**: [Processor Reference](processors/overview.md)
- **Troubleshooting**: [Common Issues](troubleshooting/common-issues.md)

## Real Example

This recipe processes a typical "van report" - container shipping data:

```yaml
settings:
  output_filename: "{YY}{MMDD}_VanReport.xlsx"

recipe:
  # Fix invisible characters from SQL export
  - processor_type: "clean_data"
    rules:
      - column: "Product Origin"
        action: "normalize_whitespace"
  
  # Filter for specific products  
  - processor_type: "filter_data"
    filters:
      - column: "Component"
        condition: "not_equals"
        value: "CANS"
      - column: "Major Species"
        condition: "contains"
        value: "SALMON"
  
  # Group cities into regions
  - processor_type: "group_data"
    source_column: "Product Origin"
    groups:
      "Bristol Bay": ["Dillingham", "Naknek", "False Pass"]
      "Southeast": ["Ketchikan", "Craig", "Sitka"]
  
  # Create carrier-by-region matrix
  - processor_type: "pivot_table"
    index: ["Region", "Product Origin"]
    columns: ["Carrier"] 
    values: ["Van Number"]
    aggfunc: "count"
    margins: true
```

Result: Automated weekly reports instead of 2+ hours of manual Excel work.
