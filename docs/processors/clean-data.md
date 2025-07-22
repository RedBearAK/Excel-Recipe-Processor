# Clean Data Processor

Fix data quality issues and standardize values.

## Overview

The `clean_data` processor handles common data quality problems from SQL exports, Excel imports, and manual data entry. Most recipes start with data cleaning.

## Common Problems It Solves

- **Invisible characters** that break exact filtering
- **Inconsistent text case** (CANS vs Cans vs cans)
- **Extra whitespace** around values
- **Wrong data types** (numbers stored as text)
- **Inconsistent values** (Active vs ACTIVE vs active)

## Basic Usage

```yaml
- processor_type: "clean_data"
  rules:
    - column: "Product Name"
      action: "strip_whitespace"
    - column: "Status"
      action: "uppercase"
```

## Available Actions

### Text Cleaning
```yaml
# Remove leading/trailing spaces
- action: "strip_whitespace"

# Fix invisible Unicode characters (SQL export issues)
- action: "normalize_whitespace"

# Remove invisible characters only
- action: "remove_invisible_chars"
```

### Case Conversion
```yaml
- action: "uppercase"        # HELLO WORLD
- action: "lowercase"        # hello world  
- action: "title_case"       # Hello World
```

### Find and Replace
```yaml
# Basic replacement
- column: "Component"
  action: "replace"
  old_value: "FLESH"
  new_value: "CANS"

# Case-insensitive
- action: "replace"
  old_value: "active"
  new_value: "Active"
  case_sensitive: false
```

### Data Type Fixes
```yaml
# Fix numbers with $ signs, commas
- column: "Price"
  action: "fix_numeric"
  fill_na: 0.0

# Fix date formats
- column: "Order Date"  
  action: "fix_dates"
  format: "%Y-%m-%d"
```

### Advanced Operations
```yaml
# Regex replacement
- action: "regex_replace"
  pattern: "([A-Z]+)-([0-9]+)"
  replacement: "\\1_\\2"

# Value standardization
- action: "standardize_values"
  mapping:
    "active": "Active"
    "ACTIVE": "Active"
    "pending": "Pending"
```

## Conditional Replacement

Replace values only when conditions are met:

```yaml
# Replace FLESH with CANS only for canned products
- column: "Component"
  action: "replace"
  old_value: "FLESH"
  new_value: "CANS"
  condition_column: "Product Name"
  condition: "contains"
  condition_value: "Canned"
  case_sensitive: false
```

### Supported Conditions
- `equals` / `not_equals`
- `contains` / `not_contains`  
- `greater_than` / `less_than`
- `is_null` / `not_null`

## Real-World Examples

### SQL Export Cleanup
```yaml
# Fix common SQL export issues
- step_description: "Clean SQL export data"
  processor_type: "clean_data"
  rules:
    # Remove invisible characters that break filtering
    - column: "Product Origin"
      action: "normalize_whitespace"
    
    # Standardize case
    - column: "Status"
      action: "uppercase"
    
    # Fix price formatting  
    - column: "Unit Price"
      action: "fix_numeric"
```

### Standardize Customer Data
```yaml
# Clean up customer information
- step_description: "Standardize customer data"
  processor_type: "clean_data"
  rules:
    # Title case for names
    - column: "Customer Name"
      action: "title_case"
    
    # Remove extra spaces
    - column: "Address"
      action: "strip_whitespace"
    
    # Standardize phone formats
    - column: "Phone"
      action: "regex_replace"
      pattern: "[^0-9]"
      replacement: ""
```

### Conditional Business Logic
```yaml
# Apply business rules conditionally
- step_description: "Apply product categorization"
  processor_type: "clean_data" 
  rules:
    # Mark high-value items as priority
    - column: "Priority"
      action: "replace"
      old_value: "Standard"
      new_value: "High"
      condition_column: "Value"
      condition: "greater_than"
      condition_value: 1000
    
    # Special handling for certain regions
    - column: "Shipping Method"
      action: "replace"
      old_value: "Standard"
      new_value: "Express"
      condition_column: "Region"
      condition: "in_list"
      condition_value: ["Alaska", "Hawaii"]
```

## Troubleshooting

### Filters Not Working?
If exact filtering fails, try cleaning first:

```yaml
# BEFORE filtering, clean the data
- processor_type: "clean_data"
  rules:
    - column: "Status"
      action: "normalize_whitespace"

# NOW filtering will work reliably  
- processor_type: "filter_data"
  filters:
    - column: "Status"
      condition: "equals"
      value: "Active"
```

### Common Mistakes
- **Not handling case**: "ACTIVE" ≠ "Active" ≠ "active"
- **Invisible characters**: SQL exports often contain zero-width spaces
- **Wrong column names**: Check exact spelling and case
- **Missing conditions**: Partial conditional configs cause errors

## Performance Tips

- Clean data early in your recipe
- Use `normalize_whitespace` for SQL exports
- Group multiple rules in one step
- Use `standardize_values` for complex mappings

## See Also

- [Filter Data](filter-data.md) - Usually follows cleaning
- [Troubleshooting SQL Exports](../troubleshooting/sql-export-problems.md)
- [Recipe Examples](../recipes/examples/)
