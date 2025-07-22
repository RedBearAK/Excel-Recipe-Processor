# Pivot Table Processor

Create cross-tabulations and summary matrices from your data.

## Overview

The `pivot_table` processor creates Excel-style pivot tables with rows, columns, and aggregated values. Perfect for business reports, cross-tabulations, and summary analysis.

## Basic Usage

```yaml
- processor_type: "pivot_table"
  index: ["Region"]           # Rows
  columns: ["Product"]        # Columns  
  values: ["Sales"]           # Values to aggregate
  aggfunc: "sum"              # How to aggregate
```

## Van Report Example

This processor was central to creating carrier-by-region matrices:

```yaml
- step_description: "Create carrier matrix by region and origin"
  processor_type: "pivot_table"
  index: ["Region", "Product Origin"]    # Hierarchical rows
  columns: ["Carrier"]                   # Carriers as columns
  values: ["Van Number"]                 # Count van numbers
  aggfunc: "nunique"                     # Count unique vans
  fill_value: 0                          # Empty cells = 0
  margins: true                          # Add total rows/columns
  margins_name: "Grand Total"            # Name for totals
```

## Configuration Options

### Required Fields
```yaml
- processor_type: "pivot_table"
  index: ["Row_Field"]        # Required: what goes in rows
  # columns and values are optional
```

### Complete Configuration
```yaml
- processor_type: "pivot_table"
  index: ["Region", "City"]          # Hierarchical rows
  columns: ["Product_Type"]          # Column headers
  values: ["Sales_Amount"]           # Values to aggregate
  aggfunc: "sum"                     # Aggregation function
  fill_value: 0                      # Fill empty cells
  margins: true                      # Add totals
  margins_name: "Total"              # Name for total rows/columns
  dropna: true                       # Drop rows/columns with all NaN
```

## Index (Rows)

### Single Index
```yaml
index: ["Region"]
# Results in:
# Region     |  Value1  |  Value2
# East       |    100   |    200
# West       |    150   |    250
```

### Hierarchical Index
```yaml
index: ["Region", "City"]
# Results in:
# Region  City      |  Value1  |  Value2
# East    Boston    |    50    |    75
#         New York  |    50    |    125
# West    Seattle   |    75    |    100
#         Portland  |    75    |    150
```

### Multiple Levels (Van Report Style)
```yaml
index: ["Region", "Product Origin"]
# Creates nested grouping like:
# Bristol Bay    DILLINGHAM     |  CMA: 50  |  MSC: 0   |  ONE: 0
#                FALSE PASS     |  CMA: 38  |  MSC: 0   |  ONE: 0
#                NAKNEK         |  CMA: 154 |  MSC: 0   |  ONE: 0
# Kodiak         KODIAK         |  CMA: 0   |  MSC: 0   |  Matson: 9
#                KODIAK WEST    |  CMA: 0   |  MSC: 0   |  Matson: 4
```

## Columns

### Single Column Field
```yaml
columns: ["Product_Type"]
# Creates columns for each unique product type
```

### Multiple Column Fields  
```yaml
columns: ["Quarter", "Product_Type"]
# Creates hierarchical columns: Q1-ProductA, Q1-ProductB, Q2-ProductA, etc.
```

### No Columns (Simple Summary)
```yaml
columns: []
# Creates a simple aggregation without column splitting
```

## Values and Aggregation

### Single Value Field
```yaml
values: ["Sales_Amount"]
aggfunc: "sum"
# Sums sales amount for each row/column combination
```

### Multiple Value Fields
```yaml
values: ["Sales_Amount", "Quantity"]
aggfunc: "sum"  
# Creates columns for both sum of sales and sum of quantity
```

### Different Aggregation Functions
```yaml
values: ["Order_Value"]
aggfunc: "mean"     # Average order value
# or: sum, count, min, max, std, var, first, last, nunique
```

## Aggregation Functions

| Function | Description | Best For |
|----------|-------------|----------|
| `sum` | Add all values | Sales totals, quantities |
| `count` | Count non-null values | Number of records |
| `nunique` | Count unique values | Unique customers, products |
| `mean` | Average value | Average order size |
| `min` / `max` | Minimum/Maximum | Price ranges |
| `first` / `last` | First/Last value | Latest status |

### Count vs NUnique
```yaml
# Count: number of records
values: ["Customer_ID"]
aggfunc: "count"
# Result: total number of orders per region

# NUnique: number of unique values  
values: ["Customer_ID"]
aggfunc: "nunique"
# Result: number of unique customers per region
```

## Real-World Examples

### Sales by Region and Product
```yaml
- step_description: "Create sales summary matrix"
  processor_type: "pivot_table"
  index: ["Sales_Region"]
  columns: ["Product_Category"]
  values: ["Revenue"]
  aggfunc: "sum"
  fill_value: 0
  margins: true
```

### Customer Analysis
```yaml
- step_description: "Customer count by region and quarter"
  processor_type: "pivot_table"
  index: ["Region"]
  columns: ["Quarter"]
  values: ["Customer_ID"]
  aggfunc: "nunique"
  fill_value: 0
  margins: true
  margins_name: "Total"
```

### Order Frequency Analysis
```yaml
- step_description: "Order frequency by customer type and month"
  processor_type: "pivot_table"
  index: ["Customer_Type"]
  columns: ["Order_Month"]
  values: ["Order_ID"]
  aggfunc: "count"
  fill_value: 0
```

### Inventory Summary
```yaml
- step_description: "Stock levels by warehouse and product"
  processor_type: "pivot_table"
  index: ["Warehouse", "Product_SKU"]
  columns: ["Stock_Status"]
  values: ["Quantity"]
  aggfunc: "sum"
  fill_value: 0
  dropna: true
```

## Advanced Features

### Margins (Totals)
```yaml
margins: true               # Adds total rows and columns
margins_name: "Grand Total" # Custom name for totals
```

Results in:
```
Region     Product_A  Product_B  Grand Total
East           100        200         300
West           150        250         400
Grand Total    250        450         700
```

### Fill Values
```yaml
fill_value: 0    # Replace empty cells with 0
fill_value: ""   # Replace with empty string
fill_value: null # Leave as null/blank
```

### Drop Empty Rows/Columns
```yaml
dropna: true   # Remove rows/columns that are all NaN
dropna: false  # Keep all rows/columns
```

## Common Patterns

### Simple Count Matrix
```yaml
# Count occurrences
- processor_type: "pivot_table"
  index: ["Category"]
  columns: ["Status"]
  values: []              # Empty = count rows
  aggfunc: "count"
  fill_value: 0
```

### Financial Summary
```yaml
# Revenue by region and month
- processor_type: "pivot_table"
  index: ["Sales_Region"]
  columns: ["Month"]
  values: ["Revenue"]
  aggfunc: "sum"
  fill_value: 0
  margins: true
  margins_name: "Total"
```

### Performance Dashboard
```yaml
# Multiple metrics in one table
- processor_type: "pivot_table"
  index: ["Department"]
  columns: ["Metric_Type"]
  values: ["Metric_Value"]
  aggfunc: "sum"
  fill_value: 0
```

## Troubleshooting

### "Grouper not 1-dimensional" Error
**Problem**: Using same column in `index` and `values`

**Wrong**:
```yaml
index: ["Product", "Van_Number"]    # Van_Number here
values: ["Van_Number"]              # AND here - causes conflict
```

**Solution**: Use different column for values
```yaml
index: ["Product", "Van_Number"]
values: ["Order_ID"]                # Different column
aggfunc: "count"
```

### Too Many Columns
**Problem**: Empty `values: []` creates columns for every remaining field

**Solution**: Specify one column for counting
```yaml
values: ["Primary_Key"]   # Use any single column
aggfunc: "count"          # Count occurrences
```

### Empty Results
**Problem**: All cells are empty or zero

**Debug**: Check your data before pivoting
```yaml
- processor_type: "debug_breakpoint"
  message: "Check data before pivot - do the index/column values exist?"

- processor_type: "pivot_table"
  # ... your config
```

### Column Names Too Complex
**Problem**: Hierarchical column names are hard to read

**Solution**: Use column renaming after pivot
```yaml
- processor_type: "pivot_table"
  # ... config

- processor_type: "rename_columns"
  rename_type: "mapping"
  mapping:
    "Van Number_CMA": "CMA"
    "Van Number_MSC": "MSC"
    "Van Number_Matson": "Matson"
```

## Performance Tips

### Filter Before Pivoting
Reduce data size before creating pivot tables:

```yaml
# Filter first to reduce processing load
- processor_type: "filter_data"
  filters:
    - column: "Date"
      condition: "greater_than"
      value: "2024-01-01"

# Then pivot smaller dataset
- processor_type: "pivot_table"
  # ... config
```

### Use Appropriate Aggregation
```yaml
# Counting records: use "count"
aggfunc: "count"

# Counting unique values: use "nunique"  
aggfunc: "nunique"

# Summing numbers: use "sum"
aggfunc: "sum"
```

## Integration with Other Processors

### Typical Pivot Workflow
```yaml
# 1. Clean and prepare data
- processor_type: "clean_data"
  rules:
    - column: "Region"
      action: "title_case"

# 2. Add groupings if needed
- processor_type: "group_data"
  source_column: "City"
  target_column: "Region"
  groups:
    "West": ["Seattle", "Portland"]
    "East": ["Boston", "New York"]

# 3. Filter for relevant data
- processor_type: "filter_data"
  filters:
    - column: "Status"
      condition: "equals"
      value: "Active"

# 4. Create pivot table
- processor_type: "pivot_table"
  index: ["Region"]
  columns: ["Product_Type"]
  values: ["Sales"]
  aggfunc: "sum"
  margins: true

# 5. Clean up column names
- processor_type: "rename_columns"
  rename_type: "transform"
  transformation: "title_case"
```

## See Also

- [Group Data](group-data.md) - Create categories before pivoting
- [Aggregate Data](aggregate-data.md) - Alternative for simple summaries
- [Filter Data](filter-data.md) - Reduce data before pivoting
- [Debug Breakpoint](debug-breakpoint.md) - Troubleshoot pivot results
