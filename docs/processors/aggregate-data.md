# Aggregate Data Processor

Group data and calculate summary statistics.

## Overview

The `aggregate_data` processor groups rows by one or more columns and calculates summary statistics like sums, averages, counts, etc. Simpler than pivot tables for basic aggregations.

## Basic Usage

```yaml
- processor_type: "aggregate_data"
  group_by: ["Region"]
  aggregations:
    - column: "Sales"
      function: "sum"
      new_column_name: "Total_Sales"
```

## When to Use vs Pivot Table

### Use Aggregate Data For:
- Simple summaries (total sales by region)
- Multiple statistics per group (count, sum, average)
- Linear output (one row per group)

### Use Pivot Table For:
- Cross-tabulations (regions vs products)
- Matrix format output
- Hierarchical groupings with subtotals

## Configuration

### Required Fields
```yaml
- processor_type: "aggregate_data"
  group_by: ["Region"]              # Required: grouping columns
  aggregations:                     # Required: what to calculate
    - column: "Sales"
      function: "sum"
```

### Complete Configuration
```yaml
- processor_type: "aggregate_data"
  group_by: ["Region", "Product_Type"]    # Multiple grouping columns
  aggregations:
    - column: "Sales_Amount"
      function: "sum"
      new_column_name: "Total_Sales"
    - column: "Order_Count"
      function: "count"
      new_column_name: "Number_of_Orders"
    - column: "Customer_ID"
      function: "nunique"
      new_column_name: "Unique_Customers"
  keep_group_columns: true          # Include grouping columns in output
  sort_by_groups: true              # Sort by grouping columns
  reset_index: true                 # Reset index after grouping
```

## Available Functions

| Function | Description | Best For |
|----------|-------------|----------|
| `sum` | Add all values | Sales totals, quantities |
| `mean` | Average value | Average order size, ratings |
| `median` | Middle value | Typical values with outliers |
| `count` | Count non-null values | Number of records |
| `nunique` | Count unique values | Unique customers, products |
| `min` / `max` | Minimum/Maximum | Price ranges, date ranges |
| `std` / `var` | Standard deviation/Variance | Data spread, consistency |
| `first` / `last` | First/Last value | Latest status, initial value |
| `size` | Count all rows (including nulls) | Total records per group |

## Real-World Examples

### Sales Summary by Region
```yaml
- step_description: "Calculate regional sales totals"
  processor_type: "aggregate_data"
  group_by: ["Sales_Region"]
  aggregations:
    - column: "Revenue"
      function: "sum"
      new_column_name: "Total_Revenue"
    - column: "Order_ID"
      function: "count"
      new_column_name: "Order_Count"
    - column: "Customer_ID"
      function: "nunique"
      new_column_name: "Unique_Customers"
    - column: "Revenue"
      function: "mean"
      new_column_name: "Average_Order_Value"
```

### Customer Analysis
```yaml
- step_description: "Customer behavior by segment"
  processor_type: "aggregate_data"
  group_by: ["Customer_Segment", "Registration_Year"]
  aggregations:
    - column: "Total_Spent"
      function: "sum"
      new_column_name: "Segment_Revenue"
    - column: "Total_Spent"
      function: "mean"
      new_column_name: "Average_Customer_Value"
    - column: "Customer_ID"
      function: "size"
      new_column_name: "Customer_Count"
    - column: "Last_Order_Date"
      function: "max"
      new_column_name: "Most_Recent_Order"
```

### Product Performance
```yaml
- step_description: "Product performance metrics"
  processor_type: "aggregate_data"
  group_by: ["Product_Category", "Brand"]
  aggregations:
    - column: "Units_Sold"
      function: "sum"
      new_column_name: "Total_Units"
    - column: "Revenue"
      function: "sum"
      new_column_name: "Total_Revenue"
    - column: "Rating"
      function: "mean"
      new_column_name: "Average_Rating"
    - column: "Stock_Level"
      function: "min"
      new_column_name: "Minimum_Stock"
```

### Van Report Aggregation
```yaml
# Alternative to pivot table for simple container counts
- step_description: "Container counts by region and carrier"
  processor_type: "aggregate_data"
  group_by: ["Region", "Carrier"]
  aggregations:
    - column: "Van_Number"
      function: "nunique"
      new_column_name: "Unique_Containers"
    - column: "Shipment_Date"
      function: "count"
      new_column_name: "Total_Shipments"
    - column: "Container_Value"
      function: "sum"
      new_column_name: "Total_Value"
```

## Multiple Aggregations

### Same Column, Different Functions
```yaml
aggregations:
  - column: "Sales_Amount"
    function: "sum"
    new_column_name: "Total_Sales"
  - column: "Sales_Amount"
    function: "mean"
    new_column_name: "Average_Sale"
  - column: "Sales_Amount"
    function: "count"
    new_column_name: "Number_of_Sales"
```

### Different Columns
```yaml
aggregations:
  - column: "Revenue"
    function: "sum"
    new_column_name: "Total_Revenue"
  - column: "Quantity"
    function: "sum"
    new_column_name: "Total_Quantity"
  - column: "Customer_ID"
    function: "nunique"
    new_column_name: "Unique_Customers"
  - column: "Order_Date"
    function: "max"
    new_column_name: "Latest_Order"
```

## Grouping Options

### Single Column Grouping
```yaml
group_by: ["Region"]
# Groups all data by region
```

### Multiple Column Grouping
```yaml
group_by: ["Region", "Product_Type", "Quarter"]
# Creates groups for each unique combination
# Example: West-Electronics-Q1, West-Electronics-Q2, etc.
```

### Hierarchical Analysis
```yaml
# First group by major category
- processor_type: "aggregate_data"
  group_by: ["Region"]
  aggregations:
    - column: "Sales"
      function: "sum"
      new_column_name: "Regional_Total"

# Then group by subcategory within regions
- processor_type: "aggregate_data"
  group_by: ["Region", "City"]
  aggregations:
    - column: "Sales"
      function: "sum"
      new_column_name: "City_Total"
```

## Output Options

### Column Management
```yaml
keep_group_columns: true    # Include grouping columns in output (default)
keep_group_columns: false   # Remove grouping columns, keep only aggregations
```

### Sorting and Index
```yaml
sort_by_groups: true        # Sort by grouping columns (default)
sort_by_groups: false       # Keep original order
reset_index: true           # Make grouping columns regular columns (default)
reset_index: false          # Keep as index
```

## Advanced Examples

### Time Series Analysis
```yaml
- step_description: "Monthly sales trends"
  processor_type: "aggregate_data"
  group_by: ["Year", "Month"]
  aggregations:
    - column: "Revenue"
      function: "sum"
      new_column_name: "Monthly_Revenue"
    - column: "Revenue"
      function: "mean"
      new_column_name: "Average_Daily_Revenue"
    - column: "Customer_ID"
      function: "nunique"
      new_column_name: "Active_Customers"
  sort_by_groups: true
```

### Geographic Analysis
```yaml
- step_description: "Performance by geographic hierarchy"
  processor_type: "aggregate_data"
  group_by: ["Country", "State", "City"]
  aggregations:
    - column: "Store_Count"
      function: "sum"
      new_column_name: "Total_Stores"
    - column: "Revenue"
      function: "sum"
      new_column_name: "Total_Revenue"
    - column: "Population"
      function: "first"  # Population is same for all stores in city
      new_column_name: "City_Population"
    - column: "Revenue"
      function: "std"
      new_column_name: "Revenue_Variability"
```

### Business Intelligence Summary
```yaml
- step_description: "Executive dashboard metrics"
  processor_type: "aggregate_data"
  group_by: ["Business_Unit", "Quarter"]
  aggregations:
    # Revenue metrics
    - column: "Revenue"
      function: "sum"
      new_column_name: "Total_Revenue"
    - column: "Revenue"
      function: "mean"
      new_column_name: "Average_Deal_Size"
    
    # Customer metrics
    - column: "Customer_ID"
      function: "nunique"
      new_column_name: "Unique_Customers"
    - column: "New_Customer_Flag"
      function: "sum"
      new_column_name: "New_Customers"
    
    # Operational metrics
    - column: "Days_to_Close"
      function: "mean"
      new_column_name: "Average_Sales_Cycle"
    - column: "Opportunity_Count"
      function: "sum"
      new_column_name: "Total_Opportunities"
```

## Performance Tips

### Efficient Grouping
```yaml
# Group by high-cardinality columns first
group_by: ["Specific_Product", "General_Category"]  # Product has more unique values

# Better than:
group_by: ["General_Category", "Specific_Product"]
```

### Choose Right Functions
```yaml
# For counting records: use "count" or "size"
function: "count"   # Excludes nulls
function: "size"    # Includes nulls

# For counting unique values: use "nunique"
function: "nunique"

# For numeric summaries: use appropriate function
function: "sum"     # Totals
function: "mean"    # Averages
function: "median"  # Typical values (less affected by outliers)
```

## Troubleshooting

### Empty Results
**Problem**: No output or unexpected groupings

**Debug**: Check grouping column values
```yaml
- processor_type: "debug_breakpoint"
  message: "Check unique values in grouping columns"

- processor_type: "aggregate_data"
  # ... your config
```

### Wrong Data Types
**Problem**: Numeric functions fail

**Solution**: Fix data types first
```yaml
- processor_type: "clean_data"
  rules:
    - column: "Sales_Amount"
      action: "fix_numeric"

- processor_type: "aggregate_data"
  group_by: ["Region"]
  aggregations:
    - column: "Sales_Amount"
      function: "sum"
```

### Too Many Groups
**Problem**: Output has thousands of rows

**Solution**: Use higher-level grouping or filter first
```yaml
# Instead of grouping by individual customer
group_by: ["Customer_ID"]  # ❌ Too many groups

# Group by customer segment
group_by: ["Customer_Segment"]  # ✅ Manageable groups
```

## Integration with Other Processors

### Typical Workflow
```yaml
# 1. Clean and prepare data
- processor_type: "clean_data"
  rules:
    - column: "Sales_Amount"
      action: "fix_numeric"

# 2. Add categories if needed
- processor_type: "group_data"
  source_column: "City"
  target_column: "Region"
  groups:
    "West": ["Seattle", "Portland"]
    "East": ["Boston", "New York"]

# 3. Filter for relevant data
- processor_type: "filter_data"
  filters:
    - column: "Order_Date"
      condition: "greater_than"
      value: "2024-01-01"

# 4. Create aggregated summary
- processor_type: "aggregate_data"
  group_by: ["Region", "Product_Type"]
  aggregations:
    - column: "Sales_Amount"
      function: "sum"
      new_column_name: "Total_Sales"
```

### After Aggregation
```yaml
# Aggregate first
- processor_type: "aggregate_data"
  group_by: ["Region"]
  aggregations:
    - column: "Sales"
      function: "sum"
      new_column_name: "Total_Sales"

# Then sort by results
- processor_type: "sort_data"
  columns: ["Total_Sales"]
  ascending: false  # Highest sales first

# Or add calculated columns
- processor_type: "add_calculated_column"
  new_column: "Sales_Percentage"
  calculation:
    type: "expression"
    expression: "Total_Sales / sum(Total_Sales) * 100"
```

## See Also

- [Pivot Table](pivot-table.md) - For cross-tabulations and matrix output
- [Group Data](group-data.md) - Create categories before aggregating  
- [Sort Data](sort-data.md) - Order aggregated results
- [Clean Data](clean-data.md) - Fix data types before aggregating
