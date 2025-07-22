# Split Column Processor

Separate single columns into multiple columns using various splitting methods.

## Overview

The `split_column` processor breaks apart combined data into separate columns for better analysis and processing. Essential for normalizing data from sources that combine multiple values in single fields.

## Basic Usage

```yaml
- processor_type: "split_column"
  source_column: "Customer_Name"
  split_type: "delimiter"
  delimiter: ", "
  new_column_names: ["Last_Name", "First_Name"]
```

This splits "Smith, John" into separate Last_Name and First_Name columns.

## Split Types

### Delimiter-Based Splitting
Split on specific characters or strings:

```yaml
- processor_type: "split_column"
  source_column: "Full_Address"
  split_type: "delimiter"
  delimiter: "|"
  new_column_names: ["Street", "City", "State", "Zip"]
  max_splits: 3
```

### Fixed-Width Splitting
Split at specific character positions:

```yaml
- processor_type: "split_column"
  source_column: "Product_Code"
  split_type: "fixed_width"
  widths: [3, 4, 2]  # 3 chars, 4 chars, 2 chars
  new_column_names: ["Category", "Item_ID", "Variant"]
```

### Regex Pattern Splitting
Use regular expressions for complex patterns:

```yaml
- processor_type: "split_column"
  source_column: "Transaction_ID"
  split_type: "regex"
  pattern: "[-_]"  # Split on hyphens or underscores
  new_column_names: ["Prefix", "Number", "Suffix"]
```

### Position-Based Splitting
Split at exact character positions:

```yaml
- processor_type: "split_column"
  source_column: "Date_Time"
  split_type: "position"
  positions: [10, 19]  # Split at positions 10 and 19
  new_column_names: ["Date", "Time", "Timezone"]
```

## Configuration Options

### Required Fields
```yaml
- processor_type: "split_column"
  source_column: "Column_Name"     # Required: column to split
  split_type: "delimiter"          # Required: splitting method
  # Additional fields depend on split_type
```

### Complete Configuration
```yaml
- processor_type: "split_column"
  source_column: "Combined_Data"         # Column to split
  split_type: "delimiter"               # Split method
  delimiter: ","                        # Delimiter (for delimiter type)
  new_column_names: ["Part1", "Part2"]  # Names for new columns
  max_splits: 2                         # Maximum number of splits
  remove_original: false                # Remove source column
  fill_missing: ""                      # Value for missing parts
  strip_whitespace: true                # Remove leading/trailing spaces
```

## Real-World Examples

### Customer Name Normalization
```yaml
- step_description: "Split customer names into components"
  processor_type: "split_column"
  source_column: "Customer_Name"
  split_type: "delimiter"
  delimiter: ", "
  new_column_names: ["Last_Name", "First_Name"]
  max_splits: 1
  strip_whitespace: true
  # "Smith, John" → Last_Name: "Smith", First_Name: "John"
```

### Address Parsing
```yaml
- step_description: "Parse combined address field"
  processor_type: "split_column"
  source_column: "Full_Address"
  split_type: "delimiter"
  delimiter: " | "
  new_column_names: ["Street_Address", "City", "State", "ZIP_Code"]
  fill_missing: "Unknown"
  # "123 Main St | Seattle | WA | 98101" → separate components
```

### Product Code Decomposition
```yaml
- step_description: "Break down structured product codes"
  processor_type: "split_column"
  source_column: "SKU"
  split_type: "fixed_width"
  widths: [2, 4, 3, 2]
  new_column_names: ["Category", "Product_ID", "Size_Code", "Color"]
  strip_whitespace: true
  # "EL1234MED01" → Category: "EL", Product_ID: "1234", Size_Code: "MED", Color: "01"
```

### Van Report Container Parsing
```yaml
- step_description: "Split container information"
  processor_type: "split_column"
  source_column: "Container_Info"
  split_type: "delimiter"
  delimiter: "-"
  new_column_names: ["Container_Type", "Container_Number", "Check_Digit"]
  max_splits: 2
  # "MSCU-123456-7" → separate components
```

### Email Domain Extraction
```yaml
- step_description: "Extract email components"
  processor_type: "split_column"
  source_column: "Email_Address"
  split_type: "delimiter"
  delimiter: "@"
  new_column_names: ["Username", "Domain"]
  max_splits: 1
  # "john.doe@company.com" → Username: "john.doe", Domain: "company.com"
```

## Advanced Splitting Methods

### Complex Regex Patterns
```yaml
# Split on multiple delimiters
- processor_type: "split_column"
  source_column: "Mixed_Format"
  split_type: "regex"
  pattern: "[,;|]"  # Split on comma, semicolon, or pipe
  new_column_names: ["Item1", "Item2", "Item3"]

# Extract parts with capturing groups
- processor_type: "split_column"
  source_column: "Phone_Number"
  split_type: "regex"
  pattern: "\\((\\d{3})\\)\\s?(\\d{3})-(\\d{4})"
  new_column_names: ["Area_Code", "Exchange", "Number"]
  # "(555) 123-4567" → Area_Code: "555", Exchange: "123", Number: "4567"
```

### Multi-Character Delimiters
```yaml
- processor_type: "split_column"
  source_column: "Concatenated_Fields"
  split_type: "delimiter"
  delimiter: " :: "
  new_column_names: ["Field_A", "Field_B", "Field_C"]
  # "ValueA :: ValueB :: ValueC" → separate fields
```

### Fixed-Width with Varying Lengths
```yaml
- processor_type: "split_column"
  source_column: "Fixed_Format_Record"
  split_type: "fixed_width"
  widths: [8, 15, 10, 12]
  new_column_names: ["Account_ID", "Account_Name", "Balance", "Last_Activity"]
  strip_whitespace: true
  fill_missing: "N/A"
```

## Integration with Other Processors

### Complete Data Normalization Workflow
```yaml
# 1. Split combined customer data
- step_description: "Split customer name and contact info"
  processor_type: "split_column"
  source_column: "Customer_Info"
  split_type: "delimiter"
  delimiter: " | "
  new_column_names: ["Customer_Name", "Phone", "Email"]

# 2. Further split customer names
- step_description: "Split customer names"
  processor_type: "split_column"
  source_column: "Customer_Name"
  split_type: "delimiter"
  delimiter: ", "
  new_column_names: ["Last_Name", "First_Name"]

# 3. Clean the split data
- processor_type: "clean_data"
  rules:
    - column: "First_Name"
      action: "title_case"
    - column: "Last_Name"
      action: "title_case"
    - column: "Email"
      action: "lowercase"

# 4. Group customers by region (using lookup)
- processor_type: "lookup_data"
  lookup_source: "customer_regions.csv"
  lookup_key: "Customer_ID"
  source_key: "Customer_ID"
  lookup_columns: ["Region", "Territory"]

# 5. Calculate customer metrics
- processor_type: "add_calculated_column"
  new_column: "Full_Name"
  calculation:
    type: "concat"
    columns: ["First_Name", "Last_Name"]
    separator: " "

# 6. Final presentation formatting
- processor_type: "rename_columns"
  rename_type: "mapping"
  mapping:
    "First_Name": "First Name"
    "Last_Name": "Last Name"
    "Full_Name": "Customer Name"

# 7. Sort for final report
- processor_type: "sort_data"
  columns: ["Region", "Customer Name"]
  ascending: [true, true]
```

### Van Report Data Preparation
```yaml
# 1. Split container identifiers
- step_description: "Parse container codes"
  processor_type: "split_column"
  source_column: "Container_Code"
  split_type: "delimiter"
  delimiter: "-"
  new_column_names: ["Carrier_Code", "Container_Number"]

# 2. Split location information  
- step_description: "Parse origin location details"
  processor_type: "split_column"
  source_column: "Origin_Detail"
  split_type: "delimiter"
  delimiter: " / "
  new_column_names: ["Port", "Facility", "Berth"]

# 3. Clean and standardize
- processor_type: "clean_data"
  rules:
    - column: "Port"
      action: "title_case"
    - column: "Carrier_Code"
      action: "uppercase"

# 4. Group into regions
- processor_type: "group_data"
  source_column: "Port"
  target_column: "Region"
  groups:
    "Bristol Bay": ["Dillingham", "Naknek", "False Pass"]
    "Kodiak": ["Kodiak", "Kodiak West"]

# 5. Continue with analysis...
```

## Handling Edge Cases

### Missing Values and Short Strings
```yaml
- processor_type: "split_column"
  source_column: "Variable_Length_Data"
  split_type: "delimiter"
  delimiter: ","
  new_column_names: ["Part1", "Part2", "Part3"]
  fill_missing: "N/A"  # Fill missing parts
  max_splits: 2        # Limit splits to avoid too many columns
```

### Inconsistent Formatting
```yaml
# Handle both "Last, First" and "First Last" formats
- step_description: "Split names with mixed formats"
  processor_type: "split_column"
  source_column: "Name"
  split_type: "regex"
  pattern: "[, ]+"  # Split on comma or spaces
  new_column_names: ["Name_Part1", "Name_Part2"]
  max_splits: 1

# Then use conditional logic to standardize
- processor_type: "add_calculated_column"
  new_column: "Contains_Comma"
  calculation:
    type: "conditional"
    condition_column: "Name"
    condition: "contains"
    condition_value: ","
    value_if_true: true
    value_if_false: false

# Apply business logic based on format detected
```

### Preserving Original Data
```yaml
- processor_type: "split_column"
  source_column: "Important_Combined_Field"
  split_type: "delimiter"
  delimiter: "|"
  new_column_names: ["Component_A", "Component_B"]
  remove_original: false  # Keep original for reference
```

## Split Type Details

### Delimiter Options
```yaml
# Common delimiters
delimiter: ","      # Comma-separated
delimiter: ";"      # Semicolon-separated  
delimiter: "|"      # Pipe-separated
delimiter: "\t"     # Tab-separated
delimiter: " - "    # Dash with spaces
delimiter: "::"     # Double colon

# Special characters (need escaping in regex)
delimiter: "."      # Period (careful with regex)
delimiter: "("      # Parenthesis (careful with regex)
```

### Fixed Width Specifications
```yaml
# Sequential widths
widths: [3, 5, 2, 4]  # 3 chars, then 5 chars, then 2 chars, then 4 chars

# For formats like: "ABCDEFGHIJK123WXYZ"
# Results: "ABC", "DEFGH", "IJ", "K123"
```

### Position Specifications
```yaml
# Split at specific positions
positions: [5, 12, 20]  # Split at characters 5, 12, and 20

# Creates columns for:
# - Characters 0-4
# - Characters 5-11  
# - Characters 12-19
# - Characters 20-end
```

## Error Handling & Troubleshooting

### Common Issues

#### Column Not Found Error
**Problem**: `Source column 'Name' not found`

**Solution**: Check exact column names
```yaml
- processor_type: "debug_breakpoint"
  message: "Check column names before splitting"

- processor_type: "split_column"
  source_column: "Exact_Column_Name"  # Use exact name from debug
```

#### Inconsistent Split Results
**Problem**: Some rows have fewer parts than expected

**Solution**: Use `fill_missing` parameter
```yaml
- processor_type: "split_column"
  source_column: "Variable_Data"
  split_type: "delimiter"
  delimiter: ","
  new_column_names: ["Part1", "Part2", "Part3"]
  fill_missing: "Not Available"  # Handle short records
```

#### Regex Pattern Errors
**Problem**: `Invalid regex pattern`

**Solution**: Test and escape special characters
```yaml
# ❌ Unescaped special characters
pattern: "()"

# ✅ Properly escaped
pattern: "\\(\\)"

# ✅ Use simple alternatives when possible
pattern: "[()]"  # Character class is safer
```

### Debugging Split Operations
```yaml
# Before splitting: examine the data
- processor_type: "debug_breakpoint"
  message: "Check source data format"
  show_sample: true
  sample_rows: 10

# After splitting: verify results
- processor_type: "split_column"
  # ... your split configuration

- processor_type: "debug_breakpoint"
  message: "Check split results"
  show_sample: true
```

## Best Practices

### Plan Your Splits
```yaml
# ✅ Analyze data first
# Look at sample values to understand format
# Use debug breakpoints to examine patterns

# ✅ Use descriptive column names
new_column_names: ["Customer_First_Name", "Customer_Last_Name"]

# ❌ Generic names
new_column_names: ["Part1", "Part2"]
```

### Handle Edge Cases
```yaml
# ✅ Plan for missing data
fill_missing: "Unknown"

# ✅ Limit splits to avoid too many columns
max_splits: 3

# ✅ Keep original data when uncertain
remove_original: false
```

### Choose Right Split Method
```yaml
# ✅ For consistent delimiters
split_type: "delimiter"

# ✅ For fixed-format data (like mainframe exports)
split_type: "fixed_width"

# ✅ For complex patterns
split_type: "regex"

# ✅ For simple position-based cuts
split_type: "position"
```

## Performance Tips

- **Split early**: Normalize data structure early in your workflow
- **Use simple methods**: Delimiter splitting is faster than regex
- **Limit splits**: Use `max_splits` to avoid creating too many columns
- **Clean after splitting**: Apply text cleaning to split results

## Common Splitting Patterns

### Name Formats
```yaml
# "Last, First" format
delimiter: ", "
max_splits: 1

# "First Last" format  
delimiter: " "
max_splits: 1

# "First Middle Last" format
delimiter: " "
max_splits: 2
```

### Address Formats
```yaml
# Standard delimited addresses
delimiter: " | "
new_column_names: ["Street", "City", "State", "ZIP"]

# CSV-style addresses
delimiter: ","
strip_whitespace: true
```

### Product Codes
```yaml
# Fixed-width codes: "AB1234CD"
split_type: "fixed_width"
widths: [2, 4, 2]
new_column_names: ["Category", "ID", "Suffix"]

# Delimited codes: "CAT-12345-RED"
split_type: "delimiter"
delimiter: "-"
```

## See Also

- [Clean Data](clean-data.md) - Clean split results
- [Add Calculated Column](add-calculated-column.md) - Recombine split data  
- [Group Data](group-data.md) - Categorize split components
- [Rename Columns](rename-columns.md) - Give split columns business-friendly names
