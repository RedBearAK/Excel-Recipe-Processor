# Group Data Processor

Categorize individual values into groups using mapping rules.

## Overview

The `group_data` processor transforms individual values into category groups using predefined mappings. Perfect for converting cities to regions, products to categories, or any many-to-one classification.

## Basic Usage

```yaml
- processor_type: "group_data"
  source_column: "City"
  groups:
    "West Coast":
      - "Seattle"
      - "Portland" 
      - "San Francisco"
    "East Coast":
      - "New York"
      - "Boston"
      - "Miami"
```

## Van Report Pattern

This processor was designed for the van report regional grouping:

```yaml
- step_description: "Group origins into Alaska regions"
  processor_type: "group_data"
  source_column: "Product Origin"
  target_column: "Region"
  groups:
    "Bristol Bay":
      - "Dillingham"
      - "False Pass"
      - "Naknek"
      - "Naknek West"
      - "Wood River"
    "Kodiak":
      - "Kodiak"
      - "Kodiak West"
    "PWS":
      - "Cordova"
      - "Seward"
      - "Valdez"
    "SE":
      - "Craig"
      - "Ketchikan"
      - "Petersburg"
      - "Sitka"
  unmatched_action: "keep_original"
```

## Configuration Options

### Required Fields
```yaml
- processor_type: "group_data"
  source_column: "Column_Name"     # Required: column to group
  groups:                          # Required: mapping dictionary
    "Group1": ["value1", "value2"]
    "Group2": ["value3", "value4"]
```

### Complete Configuration
```yaml
- processor_type: "group_data"
  source_column: "Product Origin"        # Column to group
  target_column: "Region"               # New column name (default: {source}_Group)
  groups:                               # Mapping dictionary
    "Group Name":
      - "Value 1"
      - "Value 2"
  replace_source: false                 # Replace source column (default: false)
  unmatched_action: "keep_original"     # How to handle unmatched values
  unmatched_value: "Other"              # Default for unmatched (if action is 'set_default')
  case_sensitive: true                  # Case-sensitive matching (default: true)
```

## Unmatched Value Handling

### Keep Original Values
```yaml
unmatched_action: "keep_original"
# Unmatched values remain unchanged
# Input: "Unknown City" → Output: "Unknown City"
```

### Set Default Value
```yaml
unmatched_action: "set_default"
unmatched_value: "Other Region"
# All unmatched values become the default
# Input: "Unknown City" → Output: "Other Region"
```

### Error on Unmatched
```yaml
unmatched_action: "error"
# Processing stops if any unmatched values found
# Useful for ensuring complete mapping
```

## Real-World Examples

### Customer Segmentation
```yaml
- step_description: "Group customers by company size"
  processor_type: "group_data"
  source_column: "Company Name"
  target_column: "Company Size"
  groups:
    "Enterprise":
      - "Microsoft"
      - "Google"
      - "Amazon"
      - "Apple"
    "Mid-Market":
      - "Slack Technologies"
      - "Zoom Video"
      - "DocuSign"
    "Small Business":
      - "Local Restaurant"
      - "Corner Store"
  unmatched_action: "set_default"
  unmatched_value: "Unknown Size"
```

### Product Categorization
```yaml
- step_description: "Group products into categories"
  processor_type: "group_data"
  source_column: "Product SKU"
  target_column: "Product Category"
  groups:
    "Electronics":
      - "SKU-PHONE-001"
      - "SKU-LAPTOP-002"
      - "SKU-TABLET-003"
    "Clothing":
      - "SKU-SHIRT-001"
      - "SKU-PANTS-002"
    "Books":
      - "SKU-BOOK-FICTION-001"
      - "SKU-BOOK-TECH-002"
  case_sensitive: false
  unmatched_action: "keep_original"
```

### Sales Territory Mapping
```yaml
- step_description: "Map states to sales territories"
  processor_type: "group_data"
  source_column: "State"
  target_column: "Sales Territory"
  groups:
    "West Territory":
      - "CA"
      - "OR" 
      - "WA"
      - "NV"
      - "AZ"
    "Central Territory":
      - "TX"
      - "OK"
      - "KS"
      - "CO"
    "East Territory":
      - "NY"
      - "NJ"
      - "PA"
      - "FL"
  unmatched_action: "error"  # Ensure all states are mapped
```

### Status Standardization
```yaml
- step_description: "Standardize various status values"
  processor_type: "group_data"
  source_column: "Order Status"
  target_column: "Standard Status"
  groups:
    "Active":
      - "active"
      - "ACTIVE"
      - "Active"
      - "in_progress"
      - "processing"
    "Completed":
      - "complete"
      - "COMPLETE"
      - "finished"
      - "done"
    "Cancelled":
      - "cancel"
      - "cancelled"
      - "CANCELLED"
      - "terminated"
  case_sensitive: false
  unmatched_action: "set_default"
  unmatched_value: "Unknown"
```

## Case Sensitivity Options

### Case Sensitive (Default)
```yaml
case_sensitive: true
# "Seattle" ≠ "seattle" ≠ "SEATTLE"
# Each variation needs explicit mapping
```

### Case Insensitive
```yaml
case_sensitive: false
# "Seattle" = "seattle" = "SEATTLE"  
# Single mapping works for all variations
groups:
  "West":
    - "seattle"    # Matches Seattle, SEATTLE, seattle
    - "portland"   # Matches Portland, PORTLAND, portland
```

## Advanced Features

### Replace Source Column
```yaml
- processor_type: "group_data"
  source_column: "Detailed Location"
  target_column: "Region"
  groups:
    "North": ["City A", "City B"]
    "South": ["City C", "City D"]
  replace_source: true    # Remove "Detailed Location", keep only "Region"
```

### Multi-Level Grouping
```yaml
# First level: Group cities into regions
- processor_type: "group_data"
  source_column: "City"
  target_column: "Region" 
  groups:
    "West": ["Seattle", "Portland"]
    "East": ["Boston", "New York"]

# Second level: Group regions into zones
- processor_type: "group_data"
  source_column: "Region"
  target_column: "Zone"
  groups:
    "Coastal": ["West", "East"]
    "Inland": ["Central", "Mountain"]
```

### Complex Business Rules
```yaml
- step_description: "Group customers by value tier"
  processor_type: "group_data"
  source_column: "Customer ID"
  target_column: "Value Tier"
  groups:
    "Platinum":
      - "CUST-00001"    # High-value customers
      - "CUST-00007"
      - "CUST-00023"
    "Gold":
      - "CUST-00002"    # Medium-value customers
      - "CUST-00008"
    "Silver":
      - "CUST-00003"    # Standard customers
      - "CUST-00009"
  unmatched_action: "set_default"
  unmatched_value: "Bronze"    # New customers default to Bronze
```

## Validation Features

### Duplicate Detection
The processor automatically detects values that appear in multiple groups:

```yaml
# ❌ This will cause an error
groups:
  "Group A": ["Seattle", "Portland"]
  "Group B": ["Portland", "Tacoma"]    # Portland appears twice
```

Error: `Value 'Portland' appears in both group 'Group A' and group 'Group B'`

### Column Validation
```yaml
# ❌ This will cause an error if column doesn't exist
source_column: "NonExistent Column"
```

Error: `Source column 'NonExistent Column' not found. Available columns: [...]`

## Performance Tips

### Efficient Grouping
```yaml
# ✅ Good: Specific, manageable groups
groups:
  "Major Cities": ["New York", "Los Angeles", "Chicago"]
  "Secondary": ["Seattle", "Boston", "Denver"]

# ❌ Avoid: Thousands of individual mappings  
groups:
  "Individual": ["Customer1", "Customer2", ...]  # Too many
```

### Case Sensitivity Strategy
```yaml
# For cleaner data: use case sensitive (faster)
case_sensitive: true

# For messy data: use case insensitive (more flexible)
case_sensitive: false
```

## Troubleshooting

### Empty Groups Error
**Problem**: `Group 'GroupName' cannot have empty values list`

**Solution**: Ensure all groups have at least one value
```yaml
groups:
  "Valid Group": ["value1", "value2"]    # ✅ Has values
  "Empty Group": []                       # ❌ Empty
```

### No Matches Found
**Problem**: All values end up as unmatched

**Debug**: Check case sensitivity and exact spelling
```yaml
- processor_type: "debug_breakpoint"
  message: "Check unique values in source column"

- processor_type: "group_data"
  source_column: "City"
  case_sensitive: false    # Try case insensitive first
  # ... rest of config
```

### Unexpected Duplicates
**Problem**: Same value mapped to multiple groups

**Solution**: Review your group mappings for overlaps
```yaml
# Check for typos or genuine duplicates
groups:
  "West": ["Seattle", "Seatle"]    # ❌ Typo
  "East": ["Seattle"]              # ❌ Duplicate (if typo fixed)
```

## Integration with Other Processors

### Typical Grouping Workflow
```yaml
# 1. Clean source data first
- processor_type: "clean_data"
  rules:
    - column: "City"
      action: "title_case"    # Standardize case

# 2. Group into categories  
- processor_type: "group_data"
  source_column: "City"
  target_column: "Region"
  groups:
    "West": ["Seattle", "Portland"]
    "East": ["Boston", "New York"]

# 3. Use grouped data for analysis
- processor_type: "pivot_table"
  index: ["Region"]    # Use new grouped column
  columns: ["Product"]
  values: ["Sales"]
  aggfunc: "sum"
```

### After Grouping
```yaml
# Group first
- processor_type: "group_data"
  source_column: "Product Origin"
  target_column: "Region"
  groups:
    "Bristol Bay": ["Dillingham", "Naknek"]
    "Kodiak": ["Kodiak", "Kodiak West"]

# Then filter by groups
- processor_type: "filter_data"
  filters:
    - column: "Region"
      condition: "in_list"
      value: ["Bristol Bay", "Kodiak"]

# Or aggregate by groups
- processor_type: "aggregate_data"
  group_by: ["Region"]
  aggregations:
    - column: "Container Count"
      function: "sum"
```

## Van Report Integration

### Complete Van Report Grouping
```yaml
# This is the standard van report regional grouping
- step_description: "Create Alaska regional groups"
  processor_type: "group_data"
  source_column: "Product Origin"
  target_column: "Region"
  groups:
    "Bristol Bay":
      - "Dillingham"
      - "False Pass" 
      - "Naknek"
      - "Naknek West"
      - "Wood River"
    "Kodiak":
      - "Kodiak"
      - "Kodiak West"
    "PWS":  # Prince William Sound
      - "Cordova"
      - "Seward"
      - "Valdez"
    "SE":   # Southeast
      - "Craig"
      - "Ketchikan"
      - "Petersburg"
      - "Sitka"
  unmatched_action: "keep_original"
  case_sensitive: true
```

### Use with Pivot Tables
```yaml
# After regional grouping, create carrier matrix
- processor_type: "pivot_table"
  index: ["Region", "Product Origin"]    # Hierarchical rows
  columns: ["Carrier"]                   # CMA, MSC, Matson, etc.
  values: ["Van Number"]
  aggfunc: "nunique"                     # Count unique containers
  margins: true
```

## See Also

- [Pivot Table](pivot-table.md) - Use grouped data for cross-tabulations
- [Filter Data](filter-data.md) - Filter by group categories
- [Aggregate Data](aggregate-data.md) - Summarize by groups
- [Clean Data](clean-data.md) - Standardize data before grouping
