# Recipe YAML Syntax

Complete guide to writing Excel Recipe Processor YAML files.

## Basic Structure

Every recipe has two main sections:

```yaml
# Settings (optional)
settings:
  output_filename: "results.xlsx"
  create_backup: true
  description: "Monthly sales report processing"

# Processing steps (required)
recipe:
  - step_description: "Clean the input data"
    processor_type: "clean_data"
    # processor-specific configuration...
  
  - step_description: "Filter for active records"
    processor_type: "filter_data"
    # processor-specific configuration...
```

## Settings Section

### Output File Naming
```yaml
settings:
  # Simple filename
  output_filename: "processed_data.xlsx"
  
  # Dynamic filename with variables
  output_filename: "{YY}{MMDD}_SalesReport_{timestamp}.xlsx"
  
  # Custom variables
  variables:
    report_type: "monthly"
    department: "sales"
  output_filename: "{report_type}_{department}_{date}.xlsx"
```

### Available Variables
| Variable | Example | Description |
|----------|---------|-------------|
| `{date}` | `20241221` | YYYYMMDD format |
| `{timestamp}` | `20241221_143022` | Date and time |
| `{YY}` | `24` | Two-digit year |
| `{MMDD}` | `1221` | Month and day |
| `{input_basename}` | `sales_data` | Input filename without extension |

### Other Settings
```yaml
settings:
  create_backup: true              # Backup original files
  description: "Process van data"  # Recipe description
```

## Recipe Steps

### Step Structure
Every step needs these required fields:

```yaml
- step_description: "Human-readable description of what this step does"
  processor_type: "name_of_processor"
  # ... processor-specific configuration
```

### Processor Types
- `clean_data` - Fix data quality issues
- `filter_data` - Remove unwanted rows
- `pivot_table` - Create cross-tabulations
- `aggregate_data` - Summary statistics
- `group_data` - Categorize values
- `lookup_data` - Enrich with external data
- `add_calculated_column` - Create new fields
- `split_column` - Separate combined data
- `rename_columns` - Standardize headers
- `sort_data` - Order records
- `debug_breakpoint` - Save intermediate results

## Common Configuration Patterns

### Lists vs Single Values
```yaml
# Many processors accept both single values and lists
columns: "Product Name"           # Single column
columns: ["Product Name", "SKU"]  # Multiple columns

# Filters always use lists
filters:
  - column: "Status"
    condition: "equals"
    value: "Active"
```

### Column References
```yaml
# Always use exact column names (case-sensitive)
column: "Product Origin"    # ✅ Correct
column: "product origin"    # ❌ Wrong case
column: "Product_Origin"    # ❌ Wrong separator
```

### Boolean Values
```yaml
case_sensitive: true        # or false (lowercase)
margins: true              # or false
dropna: true               # or false
```

## Step Examples

### Data Cleaning
```yaml
- step_description: "Clean and standardize data"
  processor_type: "clean_data"
  rules:
    - column: "Product Name"
      action: "title_case"
    - column: "Status"
      action: "uppercase"
    - column: "Price"
      action: "fix_numeric"
```

### Filtering
```yaml
- step_description: "Filter for recent active orders"
  processor_type: "filter_data"
  filters:
    - column: "Status"
      condition: "equals"
      value: "Active"
    - column: "Order Date"
      condition: "greater_than"
      value: "2024-01-01"
    - column: "Amount"
      condition: "greater_than"
      value: 100
```

### Pivot Tables
```yaml
- step_description: "Create sales summary by region and product"
  processor_type: "pivot_table"
  index: ["Region", "Product"]     # Rows
  columns: ["Quarter"]             # Columns
  values: ["Sales Amount"]         # Values to aggregate
  aggfunc: "sum"                   # How to aggregate
  fill_value: 0                    # Fill empty cells
  margins: true                    # Add totals
```

### Grouping
```yaml
- step_description: "Group cities into regions"
  processor_type: "group_data"
  source_column: "City"
  target_column: "Region"
  groups:
    "West Coast":
      - "Seattle"
      - "Portland"
      - "San Francisco"
    "East Coast":
      - "New York"
      - "Boston"
      - "Miami"
  unmatched_action: "keep_original"
```

## Comments and Documentation

```yaml
# Use comments to explain complex logic
settings:
  output_filename: "van_report.xlsx"

recipe:
  # Step 1: Clean data from SQL export
  - step_description: "Remove invisible characters from SQL export"
    processor_type: "clean_data"
    rules:
      - column: "Product Origin"
        action: "normalize_whitespace"  # Fixes filtering issues
  
  # Step 2: Apply business rules
  # Replace FLESH with CANS only for canned products
  - step_description: "Apply canned product business rule"
    processor_type: "clean_data"
    rules:
      - column: "Component"
        action: "replace"
        old_value: "FLESH"
        new_value: "CANS"
        condition_column: "Product Name"
        condition: "contains"
        condition_value: "Canned"
        case_sensitive: false
```

## Debugging Your Recipe

### Add Debug Checkpoints
```yaml
# Add these anywhere to save intermediate results
- step_description: "Debug: Check data after filtering"
  processor_type: "debug_breakpoint"
  message: "Verify filtering worked correctly"
  show_sample: true
  sample_rows: 5
```

### Test Incrementally
```yaml
# Comment out later steps while testing
recipe:
  - step_description: "Clean data"
    processor_type: "clean_data"
    # ... config
  
  - step_description: "DEBUG: Stop here for testing"
    processor_type: "debug_breakpoint"
    message: "Testing data cleaning only"
  
  # - step_description: "Filter data"
  #   processor_type: "filter_data" 
  #   # ... config (commented out during testing)
```

## Common Mistakes

### YAML Syntax Errors
```yaml
# ❌ Wrong indentation
recipe:
- step_description: "Bad indent"
  processor_type: "clean_data"

# ✅ Correct indentation  
recipe:
  - step_description: "Good indent"
    processor_type: "clean_data"
```

### Missing Required Fields
```yaml
# ❌ Missing step_description
- processor_type: "clean_data"
  rules: []

# ✅ All required fields
- step_description: "Clean the data"
  processor_type: "clean_data"
  rules: []
```

### Wrong Data Types
```yaml
# ❌ String instead of boolean
margins: "true"

# ✅ Actual boolean
margins: true

# ❌ Number as string when number expected
value: "100"

# ✅ Actual number
value: 100
```

## Recipe Validation

```bash
# Check your recipe syntax before running
python -m excel_recipe_processor --validate-recipe recipe.yaml
```

## See Also

- [Variables Guide](variables.md) - Dynamic filenames
- [Debugging Guide](debugging.md) - Troubleshooting recipes
- [Processor Reference](../processors/overview.md) - Individual processor docs
- [Example Recipes](examples/) - Complete working examples
