# Add Subtotals Processor

Insert subtotal rows into grouped data with calculated aggregations.

## Overview

The `add_subtotals` processor inserts subtotal rows at group boundaries within your data, providing intermediate summaries before grand totals. Perfect for business reports, hierarchical summaries, and enhancing pivot table results with group-level calculations.

## Basic Usage

```yaml
- processor_type: "add_subtotals"
  group_by: ["Region"]           # Group data by region
  subtotal_columns: ["Sales"]    # Calculate subtotals for sales
  subtotal_functions: ["sum"]    # Use sum aggregation
  subtotal_label: "Regional Total"
```

## Van Report Enhancement

Perfect for adding regional subtotals to pivot table results:

```yaml
# Step 1: Create pivot table
- processor_type: "pivot_table"
  index: ["Region", "Product Origin"]
  columns: ["Carrier"]
  values: ["Van Number"]
  aggfunc: "nunique"
  margins: true                  # Grand totals

# Step 2: Add regional subtotals
- processor_type: "add_subtotals"
  group_by: ["Region"]
  subtotal_columns: ["CMA", "MSC", "Matson"]
  subtotal_functions: ["sum", "sum", "sum"]
  subtotal_label: "Regional Total"
  preserve_totals: true          # Keep existing grand totals
```

## Configuration Options

### Required Fields
```yaml
- processor_type: "add_subtotals"
  group_by: ["Group_Column"]     # Required: columns to group by
  subtotal_columns: ["Value"]    # Required: columns to calculate subtotals for
```

### Complete Configuration
```yaml
- processor_type: "add_subtotals"
  group_by: ["Division", "Region"]        # Hierarchical grouping
  subtotal_columns: ["Sales", "Units"]    # Multiple columns
  subtotal_functions: ["sum", "count"]    # Different functions per column
  subtotal_label: "Subtotal"             # Label for subtotal rows
  position: "after_group"                # Where to place subtotals
  preserve_totals: true                  # Keep existing grand totals
```

## Grouping Strategies

### Single-Level Grouping
```yaml
group_by: ["Region"]
```

**Result structure:**
```
East Region Data...
Regional Total: East    [calculated values]
West Region Data...
Regional Total: West    [calculated values]
```

### Multi-Level Hierarchical
```yaml
group_by: ["Division", "Region", "Territory"]
```

**Result structure:**
```
North Division Data...
Territory Total: North  [calculated values]
South Division Data...
Territory Total: South  [calculated values]
```

### Strategic Grouping
Choose grouping levels based on your reporting needs:

- **Financial Reports**: `["Department", "Cost_Center"]`
- **Sales Analysis**: `["Region", "Territory", "Rep"]`
- **Inventory Reports**: `["Warehouse", "Category", "SKU"]`
- **Project Tracking**: `["Project", "Phase", "Task"]`

## Aggregation Functions

### Available Functions
```yaml
subtotal_functions: ["sum"]      # Add values
subtotal_functions: ["count"]    # Count records
subtotal_functions: ["mean"]     # Average values
subtotal_functions: ["min"]      # Minimum value
subtotal_functions: ["max"]      # Maximum value
subtotal_functions: ["nunique"]  # Count unique values
subtotal_functions: ["std"]      # Standard deviation
subtotal_functions: ["var"]      # Variance
```

### Multiple Columns, Different Functions
```yaml
subtotal_columns: ["Sales", "Quantity", "Orders"]
subtotal_functions: ["sum", "mean", "count"]
# Sales gets sum, Quantity gets mean, Orders gets count
```

### Same Function for All Columns
```yaml
subtotal_columns: ["Q1", "Q2", "Q3", "Q4"]
subtotal_functions: ["sum"]
# All quarters get sum function
```

## Positioning Options

### After Group (Default)
```yaml
position: "after_group"
```

**Result:**
```
Region Data Rows...
Subtotal: Region Name
Next Region Data...
Subtotal: Next Region
```

**Best for:** Traditional business reports, financial statements

### Before Group
```yaml
position: "before_group"
```

**Result:**
```
Subtotal: Region Name
Region Data Rows...
Subtotal: Next Region
Next Region Data...
```

**Best for:** Summary-first reports, executive dashboards

## Grand Total Preservation

### Automatic Detection
The processor automatically detects and preserves existing grand totals:

```yaml
preserve_totals: true    # Default: true
```

**Detected total indicators:**
- "Grand Total"
- "Total"
- "All"
- "TOTAL"

### Integration with Pivot Tables
```yaml
# Perfect workflow for enhanced pivot reports
- processor_type: "pivot_table"
  index: ["Region"]
  columns: ["Product"]
  values: ["Sales"]
  margins: true              # Creates grand totals

- processor_type: "add_subtotals"
  group_by: ["Region"]
  subtotal_columns: ["Product_A", "Product_B"]
  preserve_totals: true      # Keeps pivot grand totals intact
```

## Real-World Examples

### Sales Report Enhancement
```yaml
- step_description: "Add quarterly subtotals to sales report"
  processor_type: "add_subtotals"
  group_by: ["Sales_Region"]
  subtotal_columns: ["Q1_Sales", "Q2_Sales", "Q3_Sales", "Q4_Sales"]
  subtotal_functions: ["sum", "sum", "sum", "sum"]
  subtotal_label: "Regional Total"
```

### Multi-Level Territory Analysis
```yaml
- step_description: "Add division and territory subtotals"
  processor_type: "add_subtotals"
  group_by: ["Division", "Territory"]
  subtotal_columns: ["Revenue", "Units_Sold", "Order_Count"]
  subtotal_functions: ["sum", "sum", "count"]
  subtotal_label: "Territory Total"
  position: "after_group"
```

### Inventory Summary
```yaml
- step_description: "Add warehouse subtotals to inventory"
  processor_type: "add_subtotals"
  group_by: ["Warehouse", "Category"]
  subtotal_columns: ["Units_On_Hand", "Total_Value"]
  subtotal_functions: ["sum", "sum"]
  subtotal_label: "Category Total"
```

### Customer Analysis
```yaml
- step_description: "Add customer tier subtotals"
  processor_type: "add_subtotals"
  group_by: ["Customer_Tier"]
  subtotal_columns: ["Annual_Revenue", "Order_Count", "Avg_Order_Size"]
  subtotal_functions: ["sum", "count", "mean"]
  subtotal_label: "Tier Summary"
```

### Project Reporting
```yaml
- step_description: "Add project phase subtotals"
  processor_type: "add_subtotals"
  group_by: ["Project", "Phase"]
  subtotal_columns: ["Hours_Logged", "Cost_Incurred", "Tasks_Complete"]
  subtotal_functions: ["sum", "sum", "count"]
  subtotal_label: "Phase Total"
```

## Advanced Features

### Custom Labels
```yaml
subtotal_label: "Division Summary"     # Custom subtotal text
subtotal_label: "Regional Breakdown"   # Descriptive labels
subtotal_label: "Quarterly Total"      # Context-specific names
```

### Selective Column Processing
Only calculate subtotals for specific columns while preserving others:

```yaml
# Only subtotal numeric columns, leave text columns unchanged
subtotal_columns: ["Sales", "Quantity"]  # Skip "Product_Name", "Customer_Name"
```

### Pre-Sorting for Proper Grouping
```yaml
# Ensure data is sorted before adding subtotals
- processor_type: "sort_data"
  columns: ["Region", "Territory", "Rep_Name"]
  ascending: [true, true, true]

- processor_type: "add_subtotals"
  group_by: ["Region", "Territory"]
  subtotal_columns: ["Sales"]
  subtotal_functions: ["sum"]
```

## Integration Workflows

### Complete Business Report Pipeline
```yaml
# 1. Clean and prepare data
- processor_type: "clean_data"
  rules:
    - column: "Sales_Amount"
      action: "fix_numeric"

# 2. Group related data
- processor_type: "group_data"
  source_column: "State"
  target_column: "Region"
  groups:
    "West": ["CA", "OR", "WA"]
    "East": ["NY", "MA", "CT"]

# 3. Create pivot analysis
- processor_type: "pivot_table"
  index: ["Region", "Territory"]
  columns: ["Product_Line"]
  values: ["Sales_Amount"]
  aggfunc: "sum"
  margins: true

# 4. Add subtotals to pivot result
- processor_type: "add_subtotals"
  group_by: ["Region"]
  subtotal_columns: ["Electronics", "Software", "Services"]
  subtotal_functions: ["sum", "sum", "sum"]
  subtotal_label: "Regional Total"
  preserve_totals: true

# 5. Clean up final formatting
- processor_type: "rename_columns"
  rename_type: "transform"
  case_conversion: "title_case"
```

### Hierarchical Financial Reporting
```yaml
# Multi-level subtotals for financial statements
- processor_type: "add_subtotals"
  group_by: ["Division"]
  subtotal_columns: ["Revenue", "Expenses", "Profit"]
  subtotal_functions: ["sum", "sum", "sum"]
  subtotal_label: "Division Total"

- processor_type: "add_subtotals"
  group_by: ["Division", "Department"]
  subtotal_columns: ["Revenue", "Expenses", "Profit"]
  subtotal_functions: ["sum", "sum", "sum"]
  subtotal_label: "Department Total"
```

### Post-Pivot Enhancement
```yaml
# Enhance existing pivot table with subtotals
- processor_type: "pivot_table"
  index: ["Year", "Quarter"]
  columns: ["Product_Category"]
  values: ["Sales"]
  aggfunc: "sum"
  margins: true

- processor_type: "add_subtotals"
  group_by: ["Year"]
  subtotal_columns: ["Electronics", "Software", "Hardware"]
  subtotal_label: "Annual Total"
  preserve_totals: true
```

## Troubleshooting

### "No subtotals created" Issue
**Problem**: Subtotal rows aren't appearing in results

**Solutions:**
```yaml
# 1. Check data is grouped properly
- processor_type: "debug_breakpoint"
  message: "Check grouping before subtotals"

# 2. Ensure group columns exist
group_by: ["Region"]  # Verify "Region" column exists

# 3. Sort data before grouping
- processor_type: "sort_data"
  columns: ["Region"]
  ascending: true

- processor_type: "add_subtotals"
  group_by: ["Region"]
  # ... rest of config
```

### "Column not found" Error
**Problem**: Grouping or subtotal columns don't exist

```
Group column 'Territory' not found. Available columns: ['Region', 'Sales']
```

**Solution**: Check column names match exactly
```yaml
# Wrong
group_by: ["Territory"]

# Correct (check actual column names)
group_by: ["Sales_Territory"]
```

### Subtotals Calculated Incorrectly
**Problem**: Subtotal values don't match expected calculations

**Debug approach:**
```yaml
# Check data before subtotaling
- processor_type: "debug_breakpoint"
  message: "Verify data before subtotals"
  show_sample: true
  sample_rows: 20

- processor_type: "add_subtotals"
  group_by: ["Region"]
  subtotal_columns: ["Sales"]
  subtotal_functions: ["sum"]  # Verify function is correct

# Check results after subtotaling  
- processor_type: "debug_breakpoint"
  message: "Verify subtotal calculations"
```

### Grand Totals Disappearing
**Problem**: Existing grand totals are lost after adding subtotals

**Solution**: Enable total preservation
```yaml
preserve_totals: true  # Explicitly preserve existing totals
```

### Performance with Large Datasets
**Problem**: Slow processing with large amounts of data

**Solutions:**
```yaml
# 1. Filter data first
- processor_type: "filter_data"
  filters:
    - column: "Date"
      condition: "greater_than"
      value: "2024-01-01"

# 2. Then add subtotals to smaller dataset
- processor_type: "add_subtotals"
  group_by: ["Region"]
  subtotal_columns: ["Sales"]
```

## Performance Tips

### Optimal Grouping
- **Group by low-cardinality columns first** (fewer unique values)
- **Avoid grouping by high-cardinality columns** (like customer IDs)

```yaml
# Good: Low cardinality
group_by: ["Region", "Product_Category"]  # ~12 unique combinations

# Avoid: High cardinality  
group_by: ["Customer_ID", "Order_ID"]     # Thousands of combinations
```

### Function Selection
- **Use `sum` for financial calculations** (fastest)
- **Use `count` for record counting** (efficient)
- **Avoid `std` and `var` on large datasets** (computationally expensive)

### Data Preparation
```yaml
# Sort data once before subtotaling
- processor_type: "sort_data"
  columns: ["Division", "Region", "Territory"]
  ascending: [true, true, true]

# Then group by same columns for optimal performance
- processor_type: "add_subtotals"
  group_by: ["Division", "Region"]
  subtotal_columns: ["Sales"]
```

## Utility Functions

### Shared Logic Access
Use `SubtotalUtils` for programmatic access:

```python
from excel_recipe_processor.processors.add_subtotals_processor import SubtotalUtils

# Add subtotals to any DataFrame
config = {
    'group_by': ['Region'],
    'subtotal_columns': ['Sales'],
    'subtotal_functions': ['sum']
}

result = SubtotalUtils.add_subtotals_to_dataframe(df, config)
```

### Configuration Validation
```python
# Validate config before execution
SubtotalUtils.validate_subtotal_config(config)

# Get default configuration template
default_config = SubtotalUtils.get_default_subtotal_config()
```

## Future Integration

The subtotal processor is designed to integrate with enhanced pivot table functionality:

```yaml
# Future: Excel-like pivot tables with built-in subtotals
- processor_type: "pivot_table"
  index: ["Region", "Product Origin"]
  columns: ["Carrier"] 
  values: ["Van Number"]
  subtotals: ["Region"]        # Built-in subtotal support
  margins: true                # Grand totals
```

This uses the same `SubtotalUtils` logic internally, providing both standalone flexibility and integrated Excel-like experience.

## See Also

- [Pivot Table](pivot-table.md) - Create cross-tabulations with margins
- [Aggregate Data](aggregate-data.md) - Simple group-by summaries
- [Group Data](group-data.md) - Categorize values before subtotaling
- [Sort Data](sort-data.md) - Prepare data for proper grouping
- [Debug Breakpoint](debug-breakpoint.md) - Troubleshoot subtotal results
