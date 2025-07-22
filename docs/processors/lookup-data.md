# Lookup Data Processor

Enrich data with VLOOKUP/XLOOKUP-equivalent operations from external sources.

## Overview

The `lookup_data` processor enriches your data by looking up values from reference tables, similar to Excel's VLOOKUP, XLOOKUP, or INDEX-MATCH functions. It supports multiple data sources, join types, and advanced matching options.

## Basic Usage (VLOOKUP Equivalent)

```yaml
- processor_type: "lookup_data"
  lookup_source:
    data:
      "CUST001": {"Customer_Name": "Acme Corp", "Region": "West"}
      "CUST002": {"Customer_Name": "Beta Inc", "Region": "East"}
  lookup_key: "Customer_ID"
  source_key: "Customer_ID"
  lookup_columns: ["Customer_Name", "Region"]
```

This adds `Customer_Name` and `Region` columns by looking up `Customer_ID` values.

## Van Report Pattern

The processor can enrich van data with regional information:

```yaml
- step_description: "Add regional details to origin locations"
  processor_type: "lookup_data"
  lookup_source:
    type: "dictionary"
    data:
      "Cordova": {"Region": "PWS", "State": "Alaska", "Zone": "Coastal"}
      "Naknek": {"Region": "Bristol Bay", "State": "Alaska", "Zone": "Inland"}
      "Kodiak": {"Region": "Kodiak", "State": "Alaska", "Zone": "Island"}
  lookup_key: "Origin_Location"
  source_key: "Product Origin"
  lookup_columns: ["Region", "State", "Zone"]
  join_type: "left"
  default_values:
    "Region": "Unknown"
    "State": "Unknown"
    "Zone": "Unknown"
```

## Configuration Options

### Required Fields
```yaml
- processor_type: "lookup_data"
  lookup_source: {...}              # Required: data source
  lookup_key: "Key_Column"          # Required: key in lookup data
  source_key: "Source_Column"       # Required: key in main data  
  lookup_columns: ["Col1", "Col2"]  # Required: columns to lookup
```

### Complete Configuration
```yaml
- processor_type: "lookup_data"
  lookup_source: {...}                    # Data source (see below)
  lookup_key: "Reference_ID"              # Key column in lookup data
  source_key: "Main_ID"                   # Key column in main data
  lookup_columns: ["Name", "Category"]    # Columns to add from lookup
  join_type: "left"                       # Join type: left/inner/outer
  handle_duplicates: "first"              # Duplicate handling: first/last/error
  case_sensitive: true                    # Case-sensitive matching
  default_values:                         # Default values for non-matches
    "Name": "Unknown"
    "Category": "Uncategorized"
  add_prefix: "lookup_"                   # Prefix for new columns
  add_suffix: "_ref"                      # Suffix for new columns
```

## Data Sources

### Dictionary/Inline Data
```yaml
lookup_source:
  type: "dictionary"
  data:
    "KEY001": {"Name": "Product A", "Price": 100}
    "KEY002": {"Name": "Product B", "Price": 200}
```

### List of Records
```yaml
lookup_source:
  type: "inline"
  data:
    - {"Product_Code": "A001", "Product_Name": "Widget", "Category": "Hardware"}
    - {"Product_Code": "B002", "Product_Name": "Gadget", "Category": "Electronics"}
```

### Excel File
```yaml
lookup_source: "reference_data.xlsx"
lookup_sheet: "Products"  # Optional: sheet name or index
```

### CSV File
```yaml
lookup_source: "customer_data.csv"
```

### DataFrame (Programmatic)
```yaml
# When called from Python code
lookup_source: dataframe_variable
```

## Join Types

### Left Join (Default)
```yaml
join_type: "left"
# Keeps all main data rows
# Adds lookup columns where matches found
# Non-matches get null/default values
```

### Inner Join
```yaml
join_type: "inner" 
# Only keeps rows that have matches in lookup data
# Filters out non-matching main data rows
```

### Outer Join
```yaml
join_type: "outer"
# Keeps all rows from both main and lookup data
# Useful for completeness checking
```

## Real-World Examples

### Customer Enrichment
```yaml
- step_description: "Add customer details to orders"
  processor_type: "lookup_data"
  lookup_source: "customer_master.xlsx"
  lookup_sheet: "Customers"
  lookup_key: "Customer_ID"
  source_key: "Customer_Code"
  lookup_columns: ["Company_Name", "Industry", "Sales_Rep", "Credit_Limit"]
  join_type: "left"
  default_values:
    "Company_Name": "Unknown Customer"
    "Industry": "Unclassified"
    "Credit_Limit": 0
```

### Product Information Lookup
```yaml
- step_description: "Enrich order lines with product details"
  processor_type: "lookup_data"
  lookup_source:
    type: "inline"
    data:
      - {"SKU": "WIDGET-001", "Product_Name": "Premium Widget", "Category": "Hardware", "Unit_Cost": 25.50}
      - {"SKU": "GADGET-002", "Product_Name": "Smart Gadget", "Category": "Electronics", "Unit_Cost": 89.99}
      - {"SKU": "TOOL-003", "Product_Name": "Power Tool", "Category": "Tools", "Unit_Cost": 150.00}
  lookup_key: "SKU"
  source_key: "Product_Code"
  lookup_columns: ["Product_Name", "Category", "Unit_Cost"]
  handle_duplicates: "error"  # Ensure unique SKUs
```

### Geographic Data Enhancement
```yaml
- step_description: "Add geographic details to shipping addresses"
  processor_type: "lookup_data"
  lookup_source: "zip_code_database.csv"
  lookup_key: "ZIP_Code"
  source_key: "Zip"
  lookup_columns: ["City", "State", "County", "Timezone"]
  case_sensitive: false
  default_values:
    "State": "Unknown"
    "Timezone": "UTC"
```

### Sales Territory Assignment
```yaml
- step_description: "Assign sales territories to accounts"
  processor_type: "lookup_data"
  lookup_source:
    type: "dictionary" 
    data:
      "CA": {"Territory": "West", "Sales_Manager": "Alice Johnson", "Commission_Rate": 0.05}
      "TX": {"Territory": "Central", "Sales_Manager": "Bob Smith", "Commission_Rate": 0.04}
      "NY": {"Territory": "East", "Sales_Manager": "Carol Davis", "Commission_Rate": 0.045}
  lookup_key: "State_Code"
  source_key: "State"
  lookup_columns: ["Territory", "Sales_Manager", "Commission_Rate"]
  add_prefix: "assigned_"
```

### Multi-File Lookup Chain
```yaml
# First lookup: Add customer details
- step_description: "Add customer information"
  processor_type: "lookup_data"
  lookup_source: "customers.xlsx"
  lookup_key: "Customer_ID"
  source_key: "Cust_ID"
  lookup_columns: ["Customer_Name", "Industry"]

# Second lookup: Add industry details  
- step_description: "Add industry information"
  processor_type: "lookup_data"
  lookup_source: "industries.csv"
  lookup_key: "Industry_Code"
  source_key: "Industry"  # From previous lookup
  lookup_columns: ["Industry_Description", "Risk_Category"]
```

## Duplicate Handling

### Keep First Match
```yaml
handle_duplicates: "first"
# Uses first occurrence of duplicate keys
# Safe default for most cases
```

### Keep Last Match
```yaml
handle_duplicates: "last"
# Uses last occurrence of duplicate keys
# Useful when lookup data has updates at the end
```

### Error on Duplicates
```yaml
handle_duplicates: "error"
# Stops processing if duplicate keys found
# Ensures data quality in lookup source
```

## Case Sensitivity

### Case Sensitive (Default)
```yaml
case_sensitive: true
# "Customer001" ≠ "customer001"
# Exact matching required
```

### Case Insensitive
```yaml
case_sensitive: false
# "Customer001" = "customer001" = "CUSTOMER001"
# More flexible matching for messy data
```

## Column Naming Options

### Add Prefix
```yaml
add_prefix: "customer_"
lookup_columns: ["Name", "Type"]
# Creates: customer_Name, customer_Type
```

### Add Suffix  
```yaml
add_suffix: "_info"
lookup_columns: ["Name", "Type"]
# Creates: Name_info, Type_info
```

### Both Prefix and Suffix
```yaml
add_prefix: "ref_"
add_suffix: "_data"
lookup_columns: ["Name", "Type"]  
# Creates: ref_Name_data, ref_Type_data
```

## Advanced Features

### Multi-Column Lookup Keys
```yaml
# Lookup using multiple columns (composite key)
- processor_type: "lookup_data"
  lookup_source: "product_pricing.xlsx"
  lookup_key: ["Product_Code", "Region"]     # Composite key
  source_key: ["SKU", "Sales_Region"]        # Must match order
  lookup_columns: ["Regional_Price", "Discount"]
```

### Complex Default Values
```yaml
default_values:
  "Customer_Name": "Unknown Customer"
  "Credit_Rating": "D"
  "Account_Manager": "Unassigned"
  "Last_Order_Date": "1900-01-01"
```

### Lookup with Validation
```yaml
- step_description: "Validate and enrich customer data"
  processor_type: "lookup_data"
  lookup_source: "valid_customers.xlsx"
  lookup_key: "Customer_ID"
  source_key: "Customer_Code"
  lookup_columns: ["Validation_Status", "Customer_Name"]
  join_type: "inner"  # Only keep valid customers
```

## Error Handling & Troubleshooting

### Common Issues

#### Column Not Found Error
**Problem**: `Lookup key column 'Customer_ID' not found in lookup data`

**Debug**: Check exact column names in your lookup source
```yaml
- processor_type: "debug_breakpoint"
  message: "Check lookup data structure"
  
# Add this before your lookup to see available columns
```

#### No Matches Found
**Problem**: All lookup values are null/default

**Solution**: Check key formats and case sensitivity
```yaml
# Try case insensitive first
case_sensitive: false

# Check for leading/trailing spaces
# Clean data before lookup if needed
```

#### Duplicate Key Error
**Problem**: `Duplicate keys found in lookup data`

**Solution**: Choose duplicate handling strategy
```yaml
handle_duplicates: "first"  # or "last", or fix source data
```

### Debugging Lookups

```yaml
# Before lookup: check source keys
- processor_type: "debug_breakpoint"
  message: "Check source key values"
  show_sample: true
  sample_rows: 10

# After lookup: verify results
- processor_type: "lookup_data"
  # ... your config
  
- processor_type: "debug_breakpoint"
  message: "Check lookup results"
  show_sample: true
```

## Performance Tips

### Efficient Lookups
```yaml
# Use specific columns only
lookup_columns: ["Name", "Category"]  # Only what you need

# Handle duplicates efficiently
handle_duplicates: "first"  # Faster than "error" checking

# Use appropriate join type
join_type: "left"  # Usually sufficient and fastest
```

### Large Lookup Tables
```yaml
# For very large lookup files, consider pre-filtering
# or using database connections instead of Excel files

# Use CSV for better performance with large datasets
lookup_source: "large_reference.csv"  # vs .xlsx
```

## Integration with Other Processors

### Complete Enrichment Workflow
```yaml
# 1. Clean main data keys
- processor_type: "clean_data"
  rules:
    - column: "Customer_Code"
      action: "strip_whitespace"
    - column: "Customer_Code"
      action: "uppercase"

# 2. First lookup: customer details
- processor_type: "lookup_data"
  lookup_source: "customers.xlsx"
  lookup_key: "Customer_ID" 
  source_key: "Customer_Code"
  lookup_columns: ["Customer_Name", "Industry", "Region"]
  case_sensitive: false

# 3. Second lookup: regional information
- processor_type: "lookup_data"
  lookup_source:
    type: "dictionary"
    data:
      "West": {"Territory_Manager": "Alice", "Tax_Rate": 0.08}
      "East": {"Territory_Manager": "Bob", "Tax_Rate": 0.06}
  lookup_key: "Region_Code"
  source_key: "Region"  # From previous lookup
  lookup_columns: ["Territory_Manager", "Tax_Rate"]

# 4. Use enriched data for analysis
- processor_type: "pivot_table"
  index: ["Territory_Manager"]
  columns: ["Industry"]
  values: ["Order_Value"]
  aggfunc: "sum"
```

### Van Report Integration
```yaml
# After regional grouping, add detailed location data
- processor_type: "group_data"
  source_column: "Product Origin"
  target_column: "Region"
  groups:
    "Bristol Bay": ["Dillingham", "Naknek", "False Pass"]
    # ... other groups

# Then enrich with additional regional details
- processor_type: "lookup_data"
  lookup_source:
    type: "dictionary"
    data:
      "Bristol Bay": {"Zone": "Inland", "Manager": "John Smith", "Transport_Cost": 150}
      "Kodiak": {"Zone": "Island", "Manager": "Jane Doe", "Transport_Cost": 200}
      "PWS": {"Zone": "Coastal", "Manager": "Bob Jones", "Transport_Cost": 175}
  lookup_key: "Region_Name"
  source_key: "Region"
  lookup_columns: ["Zone", "Manager", "Transport_Cost"]
```

## VLOOKUP Equivalency Guide

### Excel VLOOKUP
```excel
=VLOOKUP(A2, Table1, 3, FALSE)
```

### Recipe Equivalent
```yaml
- processor_type: "lookup_data"
  lookup_source: "Table1.xlsx"  # or inline data
  lookup_key: "Column1"         # First column of Table1
  source_key: "Column_A"        # Column A in main data
  lookup_columns: ["Column3"]   # 3rd column of Table1
  join_type: "left"            # FALSE = exact match
```

### Excel XLOOKUP
```excel
=XLOOKUP(A2, Table1[Key], Table1[Values], "Not Found")
```

### Recipe Equivalent  
```yaml
- processor_type: "lookup_data"
  lookup_source: "Table1.xlsx"
  lookup_key: "Key"
  source_key: "Column_A"
  lookup_columns: ["Values"]
  default_values:
    "Values": "Not Found"
```

## See Also

- [Group Data](group-data.md) - Categorize before lookup operations
- [Clean Data](clean-data.md) - Standardize keys before lookups
- [Filter Data](filter-data.md) - Filter enriched data
- [Debug Breakpoint](debug-breakpoint.md) - Troubleshoot lookup results
