# Filter Data Processor

Remove unwanted rows based on specified conditions.

## Overview

The `filter_data` processor removes rows that don't meet your criteria. Essential for focusing on specific data subsets. **Important**: Clean your data first if it comes from SQL exports.

## Basic Usage

```yaml
- processor_type: "filter_data"
  filters:
    - column: "Status"
      condition: "equals"
      value: "Active"
```

## Van Report Pattern

This processor was central to the van report workflow:

```yaml
- step_description: "Filter for salmon records, exclude cans"
  processor_type: "filter_data"
  filters:
    - column: "Component"
      condition: "not_equals" 
      value: "CANS"
    - column: "Major Species"
      condition: "contains"
      value: "SALMON"
```

## Available Conditions

### Exact Matching
```yaml
# Exact equality
- column: "Status"
  condition: "equals"
  value: "Active"

# Not equal
- column: "Status"
  condition: "not_equals"
  value: "Cancelled"
```

### Text Matching
```yaml
# Contains text (case-sensitive)
- column: "Product Name"
  condition: "contains"
  value: "Canned"

# Does not contain
- column: "Product Name"
  condition: "not_contains"
  value: "Expired"
```

### Numeric Comparisons
```yaml
# Greater than
- column: "Price"
  condition: "greater_than"
  value: 100

# Less than or equal
- column: "Quantity"
  condition: "less_equal"
  value: 50

# Available: greater_than, less_than, greater_equal, less_equal
```

### Empty/Null Checks
```yaml
# Remove empty rows
- column: "Customer Name"
  condition: "not_empty"

# Keep only empty rows
- column: "Optional Field"
  condition: "is_empty"
```

### List Membership
```yaml
# Value in list
- column: "Region"
  condition: "in_list"
  value: ["West", "East", "Central"]

# Value not in list
- column: "Status"
  condition: "not_in_list"
  value: ["Cancelled", "Expired", "Invalid"]
```

## Multiple Filters

Filters are applied in sequence (AND logic):

```yaml
- step_description: "Filter for high-value active orders"
  processor_type: "filter_data"
  filters:
    # Must be active
    - column: "Status"
      condition: "equals"
      value: "Active"
    
    # AND must be high value
    - column: "Order Value"
      condition: "greater_than"
      value: 1000
    
    # AND must be recent
    - column: "Order Date"
      condition: "greater_than"
      value: "2024-01-01"
```

## Real-World Examples

### Sales Data Filtering
```yaml
- step_description: "Filter for current quarter sales"
  processor_type: "filter_data"
  filters:
    - column: "Sale Date"
      condition: "greater_equal"
      value: "2024-10-01"
    - column: "Sale Date"
      condition: "less_than"
      value: "2025-01-01"
    - column: "Amount"
      condition: "greater_than"
      value: 0
    - column: "Status"
      condition: "not_equals"
      value: "Refunded"
```

### Product Catalog Cleanup
```yaml
- step_description: "Filter for active products only"
  processor_type: "filter_data"
  filters:
    - column: "Status"
      condition: "in_list"
      value: ["Active", "Featured", "On Sale"]
    - column: "Price"
      condition: "greater_than"
      value: 0
    - column: "Stock"
      condition: "greater_than"
      value: 0
    - column: "Product Name"
      condition: "not_empty"
```

### Customer Data Processing
```yaml
- step_description: "Filter for valid customer records"
  processor_type: "filter_data"
  filters:
    - column: "Email"
      condition: "not_empty"
    - column: "Email"
      condition: "contains"
      value: "@"
    - column: "Customer Type"
      condition: "not_equals"
      value: "Test"
    - column: "Registration Date"
      condition: "greater_than"
      value: "2020-01-01"
```

## Date Filtering Patterns

### Recent Records Only
```yaml
- column: "Created Date"
  condition: "greater_than"
  value: "2024-01-01"
```

### Date Range
```yaml
# Records from 2024
- column: "Date"
  condition: "greater_equal"
  value: "2024-01-01"
- column: "Date"
  condition: "less_than"
  value: "2025-01-01"
```

### This Year Only
```yaml
# Use current year dynamically
- column: "Order Date"
  condition: "greater_equal"
  value: "2024-01-01"  # Update yearly or use variables
```

## Text Filtering Tips

### Case Sensitivity
Conditions are **case-sensitive** by default:

```yaml
# These are different:
condition: "equals"
value: "Active"     # Matches: Active
value: "ACTIVE"     # Matches: ACTIVE  
value: "active"     # Matches: active
```

### Partial Matching
```yaml
# Find canned products (flexible)
- column: "Product Name"
  condition: "contains"
  value: "Canned"     # Matches: "Canned Beans", "Canned Corn"

# Exact matching (strict)
- column: "Category"
  condition: "equals"
  value: "Canned Goods"  # Must match exactly
```

## Common Filtering Mistakes

### Invisible Characters Issue
**Problem**: Filter doesn't work even though values look identical

**Cause**: Invisible Unicode characters from SQL exports

**Solution**: Clean data first
```yaml
# ALWAYS clean before filtering SQL export data
- processor_type: "clean_data"
  rules:
    - column: "Status"
      action: "normalize_whitespace"

# NOW filtering will work
- processor_type: "filter_data"
  filters:
    - column: "Status"
      condition: "equals"
      value: "Active"
```

### Over-Filtering
**Problem**: All rows removed unexpectedly

**Debug**: Add checkpoints
```yaml
- processor_type: "filter_data"
  filters:
    - column: "Status"
      condition: "equals"
      value: "Active"

# Check what's left
- processor_type: "debug_breakpoint"
  message: "Rows after status filter"
  show_sample: true
```

### Wrong Data Types
**Problem**: Numeric filters don't work

**Solution**: Fix data types first
```yaml
- processor_type: "clean_data"
  rules:
    - column: "Price"
      action: "fix_numeric"

- processor_type: "filter_data"
  filters:
    - column: "Price"
      condition: "greater_than"
      value: 100
```

## Performance Tips

### Filter Early
Place filters early in your recipe to reduce data size:

```yaml
recipe:
  # 1. Clean first
  - processor_type: "clean_data"
    # ...
  
  # 2. Filter early (reduces processing load)
  - processor_type: "filter_data"
    # ...
  
  # 3. Then do expensive operations on smaller dataset
  - processor_type: "pivot_table"
    # ...
```

### Efficient Filter Order
Put most selective filters first:

```yaml
filters:
  # Most selective first (removes most rows)
  - column: "Region"
    condition: "equals"
    value: "West Coast"
  
  # Less selective filters after
  - column: "Status"
    condition: "not_equals"
    value: "Cancelled"
```

## Condition Reference

| Condition | Description | Value Type | Example |
|-----------|-------------|------------|---------|
| `equals` | Exact match | Any | `"Active"`, `100`, `"2024-01-01"` |
| `not_equals` | Not equal | Any | `"Cancelled"` |
| `contains` | Text contains | String | `"Canned"`, `"@gmail"` |
| `not_contains` | Text doesn't contain | String | `"Test"`, `"Invalid"` |
| `greater_than` | Numeric > | Number/Date | `100`, `"2024-01-01"` |
| `less_than` | Numeric < | Number/Date | `1000`, `"2024-12-31"` |
| `greater_equal` | Numeric >= | Number/Date | `0`, `"2024-01-01"` |
| `less_equal` | Numeric <= | Number/Date | `999`, `"2024-12-31"` |
| `not_empty` | Has value | None | (no value needed) |
| `is_empty` | No value/null | None | (no value needed) |
| `in_list` | Value in list | List | `["A", "B", "C"]` |
| `not_in_list` | Value not in list | List | `["X", "Y", "Z"]` |

## Integration with Other Processors

### Typical Filter Workflow
```yaml
# 1. Clean data (fix invisible characters)
- processor_type: "clean_data"
  rules:
    - column: "Status"
      action: "normalize_whitespace"

# 2. Filter for relevant records
- processor_type: "filter_data"
  filters:
    - column: "Status"
      condition: "equals"
      value: "Active"

# 3. Process filtered data
- processor_type: "aggregate_data"
  # Only processes active records
```

## See Also

- [Clean Data](clean-data.md) - Clean before filtering
- [Debug Breakpoint](debug-breakpoint.md) - Troubleshoot filter results
- [Common Issues](../troubleshooting/common-issues.md) - Filter troubleshooting
