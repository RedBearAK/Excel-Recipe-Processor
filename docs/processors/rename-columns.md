# Rename Columns Processor

Standardize and rename DataFrame columns with flexible transformation options.

## Overview

The `rename_columns` processor cleans up and standardizes column names for professional output. Perfect as a final step to make reports presentation-ready with consistent, readable column headers.

## Basic Usage

```yaml
- processor_type: "rename_columns"
  rename_type: "mapping"
  mapping:
    "Product Origin": "Origin Location"
    "Van Number": "Container ID"
    "Major Species": "Species Type"
```

This directly renames specific columns to more business-friendly names.

## Rename Types

### Direct Mapping
Map specific columns to new names:

```yaml
- processor_type: "rename_columns"
  rename_type: "mapping"
  mapping:
    "Old_Column_Name": "New Column Name"
    "SKU": "Product Code"
    "qty": "Quantity"
```

### Pattern Replacement
Use regex patterns to transform names:

```yaml
- processor_type: "rename_columns"
  rename_type: "pattern"
  pattern: "_temp$"           # Remove "_temp" suffix
  replacement: ""
```

### Systematic Transformation
Apply consistent transformations to all columns:

```yaml
- processor_type: "rename_columns"
  rename_type: "transform"
  case_conversion: "snake_case"
  replace_spaces: "_"
  strip_characters: "!@#$%"
```

## Configuration Options

### Required Fields
```yaml
- processor_type: "rename_columns"
  rename_type: "mapping"        # Required: type of renaming
  # Additional fields depend on rename_type
```

### Complete Configuration (Transform Type)
```yaml
- processor_type: "rename_columns"
  rename_type: "transform"            # Rename type
  case_conversion: "snake_case"       # Case conversion option
  add_prefix: "final_"               # Prefix to add
  add_suffix: "_clean"               # Suffix to add  
  strip_characters: " !@#$%"         # Characters to remove
  replace_spaces: "_"                # Replace spaces with this
```

## Real-World Examples

### Van Report Presentation
```yaml
- step_description: "Clean column names for van report"
  processor_type: "rename_columns"
  rename_type: "mapping"
  mapping:
    "Product Origin": "Origin Port"
    "Van Number": "Container ID"
    "Major Species": "Fish Species"
    "Component": "Product Type"
    "Regional Group": "Alaska Region"
    "Days_In_Transit": "Transit Days"
    "Container_Value": "Value ($)"
```

### Business Report Standardization
```yaml
- step_description: "Standardize business report columns"
  processor_type: "rename_columns" 
  rename_type: "mapping"
  mapping:
    "Customer_ID": "Customer Code"
    "Total_Orders_Value": "Total Order Value ($)"
    "Customer_LTV": "Customer Lifetime Value"
    "Performance_Flag": "Performance Status"
    "Territory_Manager": "Sales Manager"
    "Commission_Rate": "Commission (%)"
```

### Data Source Cleanup
```yaml
- step_description: "Clean messy column names from SQL export"
  processor_type: "rename_columns"
  rename_type: "transform"
  case_conversion: "title"
  strip_characters: "[]()!@#$%^&*"
  replace_spaces: " "
  # Converts: "[customer_id!]" → "Customer Id"
```

### API Response Normalization
```yaml
- step_description: "Normalize API column names"
  processor_type: "rename_columns"
  rename_type: "transform"
  case_conversion: "snake_case"
  # Converts: "customerFirstName" → "customer_first_name"
```

## Case Conversion Options

### Snake Case (Recommended for Data)
```yaml
case_conversion: "snake_case"
# "Customer Name" → "customer_name"
# "Product-SKU" → "product_sku"
# "OrderTotal" → "order_total"
```

### Title Case (Best for Reports)
```yaml
case_conversion: "title"
# "customer_name" → "Customer Name"
# "order_total" → "Order Total"
```

### Camel Case (API Style)
```yaml
case_conversion: "camel_case"
# "customer_name" → "customerName"
# "order_total" → "orderTotal"
```

### Upper/Lower Case
```yaml
case_conversion: "upper"     # "name" → "NAME"
case_conversion: "lower"     # "NAME" → "name"
```

## Pattern-Based Renaming

### Remove Prefixes/Suffixes
```yaml
# Remove "temp_" prefix
- processor_type: "rename_columns"
  rename_type: "pattern"
  pattern: "^temp_"
  replacement: ""

# Remove "_backup" suffix  
- processor_type: "rename_columns"
  rename_type: "pattern"
  pattern: "_backup$"
  replacement: ""
```

### Replace Characters
```yaml
# Replace dots with underscores
- processor_type: "rename_columns"
  rename_type: "pattern"
  pattern: "\\."
  replacement: "_"

# Replace multiple spaces with single space
- processor_type: "rename_columns"
  rename_type: "pattern"
  pattern: "\\s+"
  replacement: " "
```

### Complex Pattern Transformations
```yaml
# Convert "Column.1", "Column.2" to "Column_1", "Column_2"
- processor_type: "rename_columns"
  rename_type: "pattern"
  pattern: "(\w+)\.(\d+)"
  replacement: "\\1_\\2"
```

## Systematic Transformations

### Professional Report Headers
```yaml
- step_description: "Create professional column headers"
  processor_type: "rename_columns"
  rename_type: "transform"
  case_conversion: "title"
  replace_spaces: " "
  strip_characters: "_"
  # "customer_lifetime_value" → "Customer Lifetime Value"
```

### Database-Ready Names
```yaml
- step_description: "Create database-compatible names"
  processor_type: "rename_columns"
  rename_type: "transform"
  case_conversion: "snake_case"
  replace_spaces: "_"
  strip_characters: "!@#$%^&*()+-=[]{}|;:'\",.<>?/~`"
  # "Customer Name (Primary)" → "customer_name_primary"
```

### Add Metadata to Columns
```yaml
- step_description: "Add prefix for data lineage"
  processor_type: "rename_columns"
  rename_type: "transform"
  add_prefix: "src_"
  case_conversion: "lower"
  # "Customer_Name" → "src_customer_name"
```

## Integration with Other Processors

### Complete Workflow (Final Polish)
```yaml
# 1. Group locations into regions
- processor_type: "group_data"
  source_column: "Product Origin"
  target_column: "Region"
  groups:
    "Bristol Bay": ["Dillingham", "Naknek"]
    "Kodiak": ["Kodiak", "Kodiak West"]

# 2. Lookup additional details
- processor_type: "lookup_data"
  lookup_source: "regional_data.csv"
  lookup_key: "Region_Code"
  source_key: "Region"
  lookup_columns: ["Manager", "Zone"]

# 3. Calculate business metrics
- processor_type: "add_calculated_column"
  new_column: "Total_Value"
  calculation:
    type: "math"
    operation: "multiply"
    column1: "Unit_Price"
    column2: "Quantity"

# 4. Clean pivot table results
- processor_type: "pivot_table"
  index: ["Region"]
  columns: ["Carrier"]
  values: ["Total_Value"]
  aggfunc: "sum"

# 5. Final presentation cleanup
- step_description: "Final column cleanup for presentation"
  processor_type: "rename_columns"
  rename_type: "mapping"
  mapping:
    "Region": "Alaska Region"
    "CMA": "CMA Containers"
    "MSC": "MSC Containers"
    "Matson": "Matson Containers"
    "Grand Total": "Total All Carriers"
```

### Post-Pivot Table Cleanup
```yaml
# After pivot table creation, clean complex column names
- processor_type: "pivot_table"
  index: ["Product"]
  columns: ["Quarter", "Region"]
  values: ["Sales"]
  aggfunc: "sum"

# Clean up hierarchical column names
- processor_type: "rename_columns"
  rename_type: "pattern"
  pattern: "\\('([^']+)',\\s*'([^']+)'\\)"
  replacement: "\\1 \\2"
  # "('Q1', 'West')" → "Q1 West"

# Then apply business names
- processor_type: "rename_columns"
  rename_type: "mapping"
  mapping:
    "Q1 West": "Q1 West Coast"
    "Q1 East": "Q1 East Coast"
    "Q2 West": "Q2 West Coast"
    "Q2 East": "Q2 East Coast"
```

### Multi-Stage Cleanup
```yaml
# Stage 1: Technical cleanup
- step_description: "Remove technical artifacts"
  processor_type: "rename_columns"
  rename_type: "pattern"
  pattern: "^(raw_|temp_|calc_)"
  replacement: ""

# Stage 2: Standardize format
- step_description: "Standardize naming format"
  processor_type: "rename_columns"
  rename_type: "transform"
  case_conversion: "snake_case"
  strip_characters: "[](){}!@#$%"
  replace_spaces: "_"

# Stage 3: Business naming
- step_description: "Apply business-friendly names"
  processor_type: "rename_columns"
  rename_type: "mapping"
  mapping:
    "customer_identifier": "Customer Code"
    "total_order_amount": "Order Total ($)"
    "days_since_last_order": "Days Since Last Order"
```

## Advanced Features

### Conditional Renaming (Using Multiple Steps)
```yaml
# First: Standardize technical columns
- processor_type: "rename_columns"
  rename_type: "transform"
  case_conversion: "snake_case"

# Then: Apply business names to specific columns
- processor_type: "rename_columns"
  rename_type: "mapping"
  mapping:
    "customer_id": "Customer Code"
    "order_date": "Order Date"
    # Leave other snake_case columns as-is
```

### Dynamic Prefix/Suffix Based on Data Source
```yaml
# Add source identifier
- step_description: "Tag columns with data source"
  processor_type: "rename_columns"
  rename_type: "transform"
  add_prefix: "salesforce_"
  case_conversion: "snake_case"

# Later in pipeline, distinguish multiple sources
- step_description: "Tag ERP columns"
  processor_type: "rename_columns"
  rename_type: "transform" 
  add_prefix: "erp_"
  case_conversion: "snake_case"
```

## Special Use Cases

### Excel Export Preparation
```yaml
- step_description: "Prepare columns for Excel export"
  processor_type: "rename_columns"
  rename_type: "transform"
  case_conversion: "title"
  replace_spaces: " "
  strip_characters: "_"
  # Perfect for Excel headers that need to be readable
```

### Database Table Creation
```yaml
- step_description: "Prepare for database import"
  processor_type: "rename_columns"
  rename_type: "transform"
  case_conversion: "snake_case"
  strip_characters: " !@#$%^&*()+-=[]{}|;:'\",.<>?/~`"
  replace_spaces: "_"
  # Ensures valid database column names
```

### API Response Formatting
```yaml
- step_description: "Format for JSON API response"
  processor_type: "rename_columns"
  rename_type: "transform"
  case_conversion: "camel_case"
  strip_characters: " _-"
  # "customer_name" → "customerName"
```

## Error Handling & Troubleshooting

### Common Issues

#### Column Not Found Error
**Problem**: `Column 'Old_Name' not found for renaming`

**Solution**: Check exact column names before renaming
```yaml
- processor_type: "debug_breakpoint"
  message: "Check current column names"
  show_sample: true

# Use exact names from debug output
- processor_type: "rename_columns"
  rename_type: "mapping"
  mapping:
    "Exact_Column_Name": "New Name"
```

#### Duplicate Column Names
**Problem**: `Duplicate new column names not allowed`

**Solution**: Ensure unique target names
```yaml
# ❌ Creates duplicates
mapping:
  "Col1": "Name"
  "Col2": "Name"  # Duplicate

# ✅ Unique names
mapping:
  "Col1": "First_Name"
  "Col2": "Last_Name"
```

#### Invalid Regex Pattern
**Problem**: `Invalid regex pattern`

**Solution**: Test regex patterns carefully
```yaml
# ❌ Invalid regex
pattern: "[invalid"

# ✅ Valid regex (escape special characters)
pattern: "\\[column\\]"
```

## Best Practices

### Naming Conventions
```yaml
# ✅ Descriptive business names
mapping:
  "cust_id": "Customer Code"
  "ord_val": "Order Value ($)"
  "ship_dt": "Shipping Date"

# ❌ Cryptic abbreviations
mapping:
  "cust_id": "CID" 
  "ord_val": "OV"
```

### Consistent Formatting
```yaml
# ✅ Consistent case and spacing
case_conversion: "title"
replace_spaces: " "

# Results: "Customer Name", "Order Value", "Ship Date"
```

### Logical Processing Order
```yaml
# ✅ Clean first, then apply business names
# Step 1: Technical cleanup
- processor_type: "rename_columns"
  rename_type: "transform"
  strip_characters: "[]()!@#$%"
  case_conversion: "snake_case"

# Step 2: Business naming
- processor_type: "rename_columns"  
  rename_type: "mapping"
  mapping:
    "clean_column": "Business Friendly Name"
```

### Final Report Polish
```yaml
# Last step: Perfect presentation
- step_description: "Final report formatting"
  processor_type: "rename_columns"
  rename_type: "mapping"
  mapping:
    "total_revenue": "Total Revenue ($)"
    "customer_count": "Number of Customers"
    "avg_order_value": "Average Order Value ($)"
    "growth_rate": "Growth Rate (%)"
```

## Performance Tips

- **Order matters**: Clean technical issues first, then apply business names
- **Use mapping for specific renames**: More efficient than transforms for exact changes  
- **Batch transforms**: Use single transform step rather than multiple pattern steps
- **Test patterns**: Validate regex patterns before applying to large datasets

## See Also

- [Pivot Table](pivot-table.md) - Often needs column cleanup afterward
- [Add Calculated Column](add-calculated-column.md) - Creates columns that need renaming
- [Aggregate Data](aggregate-data.md) - Generates columns that need business names
- [Debug Breakpoint](debug-breakpoint.md) - Check column names before renaming
