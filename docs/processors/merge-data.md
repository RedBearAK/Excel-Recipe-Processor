# Merge Data Processor

Combine your DataFrame with external data sources using various join strategies.

## Overview

The `merge_data` processor enriches your data by merging it with external sources like Excel files, CSV files, or configuration dictionaries. Perfect for adding customer details, product information, or any lookup data to your main dataset.

## Basic Usage

```yaml
- processor_type: "merge_data"
  merge_source:
    type: "excel"
    path: "customers.xlsx"
  left_key: "Customer_ID"      # Column in your main data
  right_key: "Customer_ID"     # Column in the external source
  join_type: "left"            # How to combine the data
```

## Real-World Example

Add customer and product details to order data:

```yaml
# Step 1: Add customer information
- step_description: "Enrich orders with customer data"
  processor_type: "merge_data"
  merge_source:
    type: "excel"
    path: "customer_master.xlsx"
    sheet: "Active_Customers"
  left_key: "Customer_ID"
  right_key: "Customer_ID"
  join_type: "left"

# Step 2: Add product information  
- step_description: "Add product details from catalog"
  processor_type: "merge_data"
  merge_source:
    type: "csv"
    path: "product_catalog.csv"
  left_key: "Product_SKU"
  right_key: "SKU"
  join_type: "left"
```

## Configuration Options

### Required Fields
```yaml
- processor_type: "merge_data"
  merge_source: {...}          # Required: data source configuration
  left_key: "ID_Column"        # Required: key column in main data
  right_key: "ID_Column"       # Required: key column in external data
```

### Complete Configuration
```yaml
- processor_type: "merge_data"
  merge_source:
    type: "excel"              # Data source type
    path: "data.xlsx"          # File path
    sheet: 0                   # Sheet name/index (Excel only)
  left_key: "Customer_ID"      # Main data key column
  right_key: "Cust_ID"         # External data key column
  join_type: "left"            # Join strategy
  suffixes: ["_main", "_ext"]  # Handle duplicate column names
  drop_duplicate_keys: true    # Remove duplicate key columns
```

## Data Sources

### Excel Files
```yaml
merge_source:
  type: "excel"
  path: "customer_data.xlsx"
  sheet: "Customers"           # Sheet name or index (default: 0)
```

**Supported formats:** `.xlsx`, `.xls`

### CSV Files
```yaml
merge_source:
  type: "csv"
  path: "product_catalog.csv"
  encoding: "utf-8"            # File encoding (default: utf-8)
  separator: ","               # Field separator (default: ,)
```

**Encoding options:** `utf-8`, `latin-1`, `cp1252`
**Separators:** `,`, `;`, `\t`, `|`

### Dictionary Configuration
Perfect for lookup mappings and small reference data:

```yaml
merge_source:
  type: "dictionary"
  data:
    "CUST001": 
      Customer_Name: "TechCorp Inc"
      Region: "West Coast"
      Tier: "Premium"
    "CUST002":
      Customer_Name: "DataSys Ltd" 
      Region: "East Coast"
      Tier: "Standard"
```

The dictionary keys become the merge key column (`right_key`).

## Join Types

### Left Join (Default)
**Keep all rows from main data, add matching external data**

```yaml
join_type: "left"
```

**Result:** All original rows preserved, missing external data becomes null.
**Use when:** You want to keep all your original data and add external details where available.

### Inner Join
**Keep only rows that match in both datasets**

```yaml
join_type: "inner"
```

**Result:** Only rows with matching keys in both datasets.
**Use when:** You only want records that have complete information from both sources.

### Outer Join
**Keep all rows from both datasets**

```yaml
join_type: "outer"
```

**Result:** All rows from both datasets, missing data becomes null.
**Use when:** You want comprehensive data from both sources.

### Right Join
**Keep all rows from external data, add matching main data**

```yaml
join_type: "right"  
```

**Result:** All external rows preserved, missing main data becomes null.
**Use when:** External source is your master list.

## Key Columns

### Same Column Names
```yaml
left_key: "Customer_ID"
right_key: "Customer_ID"
```

### Different Column Names
```yaml
left_key: "Cust_ID"          # In your main data
right_key: "Customer_Code"    # In external source
```

### Data Type Handling
The processor automatically handles common data type mismatches:

```yaml
# Main data has numeric IDs: [1, 2, 3]
# External source has string IDs: ["1", "2", "3"]
# Processor converts automatically
```

## Column Conflicts

### Duplicate Column Names
When both datasets have columns with the same name:

```yaml
suffixes: ["_orders", "_customers"]
```

**Before merge:**
- Main data: `Name`, `Value`
- External data: `Name`, `Value`

**After merge:**
- Result: `Name_orders`, `Value_orders`, `Name_customers`, `Value_customers`

### Drop Duplicate Keys
```yaml
drop_duplicate_keys: true    # Default: true
```

Removes duplicate key columns (e.g., removes `Customer_ID_y` when you already have `Customer_ID_x`).

## Common Patterns

### Customer Enrichment
```yaml
- step_description: "Add customer details to orders"
  processor_type: "merge_data"
  merge_source:
    type: "excel"
    path: "customer_master.xlsx"
  left_key: "Customer_ID"
  right_key: "Customer_ID"
  join_type: "left"
```

### Product Lookup
```yaml
- step_description: "Add product names and categories"
  processor_type: "merge_data"
  merge_source:
    type: "csv"
    path: "product_catalog.csv"
  left_key: "Product_SKU"
  right_key: "SKU"
  join_type: "left"
```

### Region Mapping
```yaml
- step_description: "Map cities to sales regions"
  processor_type: "merge_data"
  merge_source:
    type: "dictionary"
    data:
      "Seattle": {"Region": "West", "Territory": "Northwest"}
      "Portland": {"Region": "West", "Territory": "Northwest"}
      "Boston": {"Region": "East", "Territory": "Northeast"}
      "Atlanta": {"Region": "East", "Territory": "Southeast"}
  left_key: "City"
  right_key: "key"
  join_type: "left"
```

### Multi-Source Enrichment
```yaml
# Step 1: Add customer data
- processor_type: "merge_data"
  merge_source:
    type: "excel"
    path: "customers.xlsx"
  left_key: "Customer_ID"
  right_key: "Customer_ID"
  join_type: "left"

# Step 2: Add product data
- processor_type: "merge_data"
  merge_source:
    type: "csv"
    path: "products.csv"
  left_key: "Product_Code"
  right_key: "Product_Code"
  join_type: "left"

# Step 3: Add territory mappings
- processor_type: "merge_data"
  merge_source:
    type: "dictionary"
    data:
      "West": {"Territory_Manager": "Alice Johnson"}
      "East": {"Territory_Manager": "Bob Smith"}
  left_key: "Region"
  right_key: "key"
  join_type: "left"
```

## Advanced Features

### File Path Variables
Use variables in file paths for dynamic merging:

```yaml
merge_source:
  type: "excel"
  path: "data/{year}/customers_{month}.xlsx"
```

### Conditional Merging
Use filters before merging to limit external data:

```yaml
# Filter external data first, then merge
- processor_type: "filter_data"
  filters:
    - column: "Status"
      condition: "equals"
      value: "Active"

- processor_type: "merge_data"
  merge_source:
    type: "excel"
    path: "customers.xlsx"
  left_key: "Customer_ID"
  right_key: "Customer_ID"
  join_type: "inner"  # Only merge with active customers
```

## Troubleshooting

### "Column not found" Error
**Problem**: Key column doesn't exist in one of the datasets

```
Left key column 'Cust_ID' not found. Available columns: ['Customer_ID', 'Order_Date']
```

**Solution**: Check column names in both datasets
```yaml
# Wrong
left_key: "Cust_ID"

# Correct  
left_key: "Customer_ID"
```

### "File not found" Error
**Problem**: External file path is incorrect

**Solution**: Use full paths or verify relative paths
```yaml
# Use full path
merge_source:
  type: "excel"
  path: "/full/path/to/data.xlsx"

# Or verify relative path from where you run the recipe
merge_source:
  type: "excel"
  path: "./data/customers.xlsx"
```

### No Matching Records
**Problem**: Join produces no matches

**Debug approach:**
```yaml
# Check your data before merging
- processor_type: "debug_breakpoint"
  message: "Check key column values before merge"

- processor_type: "merge_data"
  # ... your merge config
  join_type: "outer"  # Use outer join to see all data

- processor_type: "debug_breakpoint"
  message: "Check merge results - are keys matching?"
```

**Common causes:**
- Data type mismatch (the processor handles most, but not all)
- Leading/trailing spaces in key values
- Different case (uppercase vs lowercase)
- Missing data in key columns

### Too Many Columns
**Problem**: Result has duplicate columns with confusing names

**Solution**: Use custom suffixes and drop duplicate keys
```yaml
merge_source:
  # ... your config
suffixes: ["_main", "_lookup"]
drop_duplicate_keys: true
```

### Memory Issues with Large Files
**Problem**: Large Excel/CSV files cause memory problems

**Solutions:**
1. **Filter external data first** (separate step)
2. **Use CSV instead of Excel** for large files
3. **Split large files** into smaller chunks
4. **Use database connections** (future enhancement)

## Performance Tips

### File Format Choice
```yaml
# Fastest to slowest:
# 1. Dictionary (in-memory)
# 2. CSV files
# 3. Excel files
```

### Large Dataset Strategies
```yaml
# 1. Filter main data first to reduce merge size
- processor_type: "filter_data"
  filters:
    - column: "Date"
      condition: "greater_than"
      value: "2024-01-01"

# 2. Then merge smaller dataset
- processor_type: "merge_data"
  # ... merge config
```

### Key Column Optimization
- Use **simple data types** (strings, integers) for key columns
- Avoid **floating-point numbers** as keys
- Ensure **no null values** in key columns

## Integration with Other Processors

### Typical Merge Workflow
```yaml
# 1. Clean main data first
- processor_type: "clean_data"
  rules:
    - column: "Customer_ID"
      action: "strip_whitespace"

# 2. Merge with external data
- processor_type: "merge_data"
  merge_source:
    type: "excel"
    path: "customers.xlsx"
  left_key: "Customer_ID"
  right_key: "Customer_ID"
  join_type: "left"

# 3. Handle missing data from merge
- processor_type: "fill_data"
  columns: ["Customer_Name"]
  fill_value: "Unknown Customer"

# 4. Create analysis
- processor_type: "aggregate_data"
  group_by: ["Region", "Customer_Type"]
  aggregations:
    - column: "Order_Value"
      function: "sum"
```

### Post-Merge Processing
```yaml
# After merging, you often want to:

# Clean up column names
- processor_type: "rename_columns"
  rename_type: "mapping"
  mapping:
    "Customer_Name_x": "Customer_Name"
    "Region_y": "Sales_Region"

# Add calculated columns based on merged data
- processor_type: "add_calculated_column"
  new_column: "Customer_Category"
  calculation:
    type: "conditional"
    conditions:
      - condition: "Customer_Type == 'Premium'"
        value: "High Value"
      - condition: "Order_Value > 1000"
        value: "Large Order"
    default_value: "Standard"
```

## See Also

- [Lookup Data](lookup-data.md) - Alternative approach for simple lookups
- [Clean Data](clean-data.md) - Prepare data before merging
- [Filter Data](filter-data.md) - Reduce dataset size before merging
- [Fill Data](fill-data.md) - Handle missing data after merging
- [Debug Breakpoint](debug-breakpoint.md) - Troubleshoot merge results
