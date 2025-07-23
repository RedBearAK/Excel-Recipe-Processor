# Fill Data Processor

Fill missing values and blanks using Excel-equivalent strategies.

## Overview

The `fill_data` processor handles missing values using methods familiar from Excel, including fill down/up, statistical fills, and conditional filling. Essential for data cleaning and preparation, especially before merge operations.

## Basic Usage

```yaml
- processor_type: "fill_data"
  columns: ["Customer_Name"]
  fill_method: "constant"
  fill_value: "Unknown"
```

This fills all blank/null values in the Customer_Name column with "Unknown".

## Fill Methods

### Constant Fill (Most Common)
Fill with a specific value:

```yaml
- processor_type: "fill_data"
  columns: ["Status", "Priority"]
  fill_method: "constant"
  fill_value: "Pending"
```

### Forward Fill (Fill Down)
Excel's "Fill Down" equivalent - carry forward last known value:

```yaml
- processor_type: "fill_data"
  columns: ["Last_Known_Status"]
  fill_method: "forward_fill"
  limit: 10  # Don't fill more than 10 consecutive blanks
```

### Backward Fill (Fill Up)
Excel's "Fill Up" equivalent - carry backward next known value:

```yaml
- processor_type: "fill_data"
  columns: ["Next_Due_Date"]
  fill_method: "backward_fill"
```

### Statistical Fills
Fill with calculated values:

```yaml
# Fill with average
- processor_type: "fill_data"
  columns: ["Order_Amount"]
  fill_method: "mean"

# Fill with most common value
- processor_type: "fill_data"
  columns: ["Customer_Type"]
  fill_method: "mode"

# Fill with middle value
- processor_type: "fill_data"
  columns: ["Rating"]
  fill_method: "median"
```

## Configuration Options

### Required Fields
```yaml
- processor_type: "fill_data"
  columns: ["Column_Name"]      # Required: columns to fill
  fill_method: "constant"       # Required: fill strategy
```

### Complete Configuration
```yaml
- processor_type: "fill_data"
  columns: ["Col1", "Col2"]           # Columns to process
  fill_method: "forward_fill"         # Fill strategy  
  fill_value: "Default"               # Value for constant/replace methods
  limit: 5                            # Max consecutive fills
  inplace: false                      # Modify original data
  conditions: [...]                   # Conditional fill rules
```

## Real-World Examples

### Van Report Data Cleanup
```yaml
- step_description: "Fill missing container information"
  processor_type: "fill_data"
  columns: ["Container_Status"]
  fill_method: "forward_fill"
  limit: 3
  # Carry forward last known status for up to 3 missing records

- step_description: "Set default values for missing data"
  processor_type: "fill_data"
  columns: ["Origin_Port", "Destination"]
  fill_method: "constant"
  fill_value: "Unknown"
```

### Customer Data Standardization
```yaml
- step_description: "Fill missing customer information"
  processor_type: "fill_data"
  columns: ["Customer_Type"]
  fill_method: "mode"
  # Fill with most common customer type

- step_description: "Set default contact preferences"
  processor_type: "fill_data"
  columns: ["Email_Preference", "Phone_Preference"]
  fill_method: "constant"
  fill_value: "Yes"
```

### Sales Data Processing
```yaml
- step_description: "Handle missing sales amounts"
  processor_type: "fill_data"
  columns: ["Order_Amount"]
  fill_method: "mean"
  # Fill with average order amount

- step_description: "Forward fill sales rep assignments"
  processor_type: "fill_data"
  columns: ["Assigned_Rep"]
  fill_method: "forward_fill"
  # Carry forward rep assignment through territory
```

## Conditional Filling

Fill based on values in other columns:

```yaml
- step_description: "Department-specific salary defaults"
  processor_type: "fill_data"
  columns: ["Starting_Salary"]
  fill_method: "constant"
  conditions:
    - condition_column: "Department"
      condition_type: "equals"
      condition_value: "Sales"
      fill_value: 45000
    - condition_column: "Department"
      condition_type: "equals"
      condition_value: "Engineering"
      fill_value: 65000
    - condition_column: "Department"
      condition_type: "equals"
      condition_value: "Marketing"
      fill_value: 50000
```

### Advanced Conditional Examples

#### Regional Defaults
```yaml
- step_description: "Region-based shipping defaults"
  processor_type: "fill_data"
  columns: ["Shipping_Method"]
  fill_method: "constant"
  conditions:
    - condition_column: "Region"
      condition_type: "in_list"
      condition_value: ["Alaska", "Hawaii"]
      fill_value: "Air Express"
    - condition_column: "Region"
      condition_type: "contains"
      condition_value: "International"
      fill_value: "Ocean Freight"
```

#### Value-Based Rules
```yaml
- step_description: "Priority based on order value"
  processor_type: "fill_data"
  columns: ["Priority"]
  fill_method: "constant"
  conditions:
    - condition_column: "Order_Value"
      condition_type: "greater_than"
      condition_value: 10000
      fill_value: "High"
    - condition_column: "Order_Value"
      condition_type: "less_than"
      condition_value: 1000
      fill_value: "Low"
```

## Specialized Fill Operations

### Replace Specific Values
```yaml
- step_description: "Standardize status codes"
  processor_type: "fill_data"
  columns: ["Status_Code"]
  fill_method: "replace"
  old_value: "N/A"
  fill_value: "Unknown"
```

### Zero Fill for Calculations
```yaml
- step_description: "Fill missing quantities with zero"
  processor_type: "fill_data"
  columns: ["Quantity", "Backorder_Amount"]
  fill_method: "zero"
```

### Empty String Fill
```yaml
- step_description: "Fill missing text fields"
  processor_type: "fill_data"
  columns: ["Notes", "Comments"]
  fill_method: "empty_string"
```

### Interpolation for Time Series
```yaml
- step_description: "Interpolate missing measurement values"
  processor_type: "fill_data"
  columns: ["Temperature", "Pressure"]
  fill_method: "interpolate"
  # Creates smooth progression between known values
```

## Integration with Other Processors

### Complete Data Preparation Workflow
```yaml
# 1. Split combined data
- processor_type: "split_column"
  source_column: "Full_Name"
  split_type: "delimiter"
  delimiter: ", "
  new_column_names: ["Last_Name", "First_Name"]

# 2. Clean the split data
- processor_type: "clean_data"
  rules:
    - column: "Last_Name"
      action: "title_case"

# 3. Fill missing names
- step_description: "Fill missing customer names"
  processor_type: "fill_data"
  columns: ["First_Name", "Last_Name"]
  fill_method: "constant"
  fill_value: "Unknown"

# 4. Group customers by region
- processor_type: "group_data"
  source_column: "City"
  target_column: "Region"
  groups:
    "West": ["Seattle", "Portland"]
    "East": ["Boston", "New York"]

# 5. Fill missing regions
- processor_type: "fill_data"
  columns: ["Region"]
  fill_method: "constant"
  fill_value: "Other"
```

### Post-Lookup Cleanup
```yaml
# 1. Lookup customer details
- processor_type: "lookup_data"
  lookup_source: "customers.xlsx"
  lookup_key: "Customer_ID"
  source_key: "Customer_Code"
  lookup_columns: ["Customer_Name", "Credit_Rating"]
  join_type: "left"  # Creates nulls for non-matches

# 2. Fill missing lookup results
- step_description: "Handle customers not found in lookup"
  processor_type: "fill_data"
  columns: ["Customer_Name"]
  fill_method: "constant"
  fill_value: "New Customer"

- step_description: "Set default credit rating"
  processor_type: "fill_data"
  columns: ["Credit_Rating"]
  fill_method: "constant"
  fill_value: "Under Review"
```

### Pre-Merge Data Preparation
```yaml
# Prepare data before merging to avoid null issues
- step_description: "Fill key fields before merge"
  processor_type: "fill_data"
  columns: ["Merge_Key", "Category"]
  fill_method: "constant"
  fill_value: "Unknown"

# Later: merge operation (future processor)
# - processor_type: "merge_data"
#   ...
```

## Condition Types

### Comparison Operators
```yaml
condition_type: "equals"           # Exact match
condition_type: "not_equals"       # Not equal to
condition_type: "greater_than"     # Numeric >
condition_type: "less_than"        # Numeric <
```

### Text Operations
```yaml
condition_type: "contains"         # Text contains substring
condition_type: "not_contains"     # Text doesn't contain
```

### Null Checks
```yaml
condition_type: "is_null"          # Field is null/blank
condition_type: "not_null"         # Field has value
```

### List Operations
```yaml
condition_type: "in_list"          # Value in specified list
condition_type: "not_in_list"      # Value not in list
condition_value: ["A", "B", "C"]   # List of values
```

## Advanced Features

### Limit Consecutive Fills
Prevent over-filling by limiting consecutive blanks filled:

```yaml
- processor_type: "fill_data"
  columns: ["Sensor_Reading"]
  fill_method: "forward_fill"
  limit: 3  # Only fill up to 3 consecutive missing values
```

### Multiple Column Processing
Process several columns with different strategies:

```yaml
- step_description: "Multi-column fill strategy"
  processor_type: "fill_data"
  columns: ["Name", "Phone", "Email", "Age", "Score"]
  fill_method: "constant"
  fill_value: "Unknown"
  # All columns get same fill value

# For different strategies per column, use multiple steps:
- processor_type: "fill_data"
  columns: ["Age"]
  fill_method: "mean"

- processor_type: "fill_data"
  columns: ["Score"]
  fill_method: "median"
```

### In-Place Modification
Modify original data directly (use cautiously):

```yaml
- processor_type: "fill_data"
  columns: ["Status"]
  fill_method: "constant"
  fill_value: "Active"
  inplace: true  # Modifies original DataFrame
```

## Error Handling & Troubleshooting

### Common Issues

#### Statistical Fill on Non-Numeric Data
**Problem**: `Cannot calculate mean for non-numeric column`

**Solution**: Use appropriate fill method for data type
```yaml
# ❌ Mean on text column
- processor_type: "fill_data"
  columns: ["Customer_Name"]
  fill_method: "mean"

# ✅ Use mode for text columns
- processor_type: "fill_data"
  columns: ["Customer_Name"]  
  fill_method: "mode"
```

#### Missing Fill Value
**Problem**: `Fill method 'constant' requires 'fill_value' parameter`

**Solution**: Always specify fill_value for constant fills
```yaml
- processor_type: "fill_data"
  columns: ["Status"]
  fill_method: "constant"
  fill_value: "Unknown"  # Required
```

#### Column Not Found
**Problem**: `Column 'Name' not found`

**Solution**: Check column names before processing
```yaml
- processor_type: "debug_breakpoint"
  message: "Check column names"

- processor_type: "fill_data"
  columns: ["Exact_Column_Name"]
  fill_method: "constant"
  fill_value: "Unknown"
```

### Debugging Fill Operations
```yaml
# Before filling: check missing data patterns
- processor_type: "debug_breakpoint"
  message: "Check missing data before fill"

# After filling: verify results
- processor_type: "fill_data"
  columns: ["Customer_Type"]
  fill_method: "mode"

- processor_type: "debug_breakpoint"
  message: "Verify fill operation results"
  show_sample: true
```

## Best Practices

### Choose Appropriate Fill Methods
```yaml
# ✅ Logical choices
Text_Fields: "mode" or "constant"
Numeric_Fields: "mean", "median", or "zero"
Time_Series: "forward_fill" or "interpolate"
Categories: "mode" or "constant"

# ❌ Poor choices
Text_Fields: "mean"  # Can't calculate mean of text
Categories: "interpolate"  # Doesn't make sense
```

### Handle Edge Cases
```yaml
# ✅ Use limits to prevent over-filling
- processor_type: "fill_data"
  columns: ["Measurement"]
  fill_method: "forward_fill"
  limit: 5  # Don't fill more than 5 consecutive blanks

# ✅ Set sensible defaults
fill_value: "Unknown"    # Better than leaving blank
fill_value: 0           # For quantities, counts
fill_value: "TBD"       # For future assignments
```

### Order Operations Logically
```yaml
# ✅ Good order
1. split_column     # Separate combined data
2. clean_data       # Fix data quality
3. fill_data        # Fill missing values
4. group_data       # Categorize clean data
5. lookup_data      # Enrich complete data

# ❌ Poor order - filling before cleaning
1. fill_data        # May fill bad data
2. clean_data       # Cleaning after filling
```

### Use Conditional Fills Wisely
```yaml
# ✅ Business logic
- processor_type: "fill_data"
  columns: ["Shipping_Cost"]
  fill_method: "constant"
  conditions:
    - condition_column: "Weight"
      condition_type: "less_than"
      condition_value: 5
      fill_value: 15.00  # Light packages

# ✅ Default after conditionals
- processor_type: "fill_data"
  columns: ["Shipping_Cost"]
  fill_method: "constant"
  fill_value: 25.00  # Catch remaining nulls
```

## Performance Tips

- **Fill early**: Handle missing data before complex operations
- **Use appropriate methods**: Statistical fills are slower than constants
- **Batch similar fills**: Process multiple columns together when possible  
- **Limit forward fills**: Use limits to prevent excessive consecutive fills

## Excel Equivalencies

| Excel Feature | Recipe Equivalent |
|---------------|-------------------|
| Fill Down | `fill_method: "forward_fill"` |
| Fill Up | `fill_method: "backward_fill"` |
| Fill Series | `fill_method: "interpolate"` |
| Find & Replace | `fill_method: "replace"` |
| AVERAGE() | `fill_method: "mean"` |
| MODE() | `fill_method: "mode"` |

## See Also

- [Clean Data](clean-data.md) - Fix data before filling
- [Lookup Data](lookup-data.md) - Often creates nulls that need filling
- [Split Column](split-column.md) - May create incomplete data
- [Debug Breakpoint](debug-breakpoint.md) - Analyze missing data patterns
