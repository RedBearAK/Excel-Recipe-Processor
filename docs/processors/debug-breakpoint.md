# Debug Breakpoint Processor

Stop recipe execution and save current data state for inspection.

## Overview

The `debug_breakpoint` processor is your primary troubleshooting tool. It saves the current data to an Excel file and stops recipe execution, letting you inspect intermediate results during recipe development.

## Basic Usage

```yaml
- processor_type: "debug_breakpoint"
  message: "Check data after filtering"
```

This creates a timestamped Excel file in `./debug_outputs/` and stops execution.

## Why Use Debug Breakpoints?

### Recipe Development
Test steps incrementally instead of running the entire recipe:

```yaml
recipe:
  - step_description: "Clean data"
    processor_type: "clean_data"
    # ... config
  
  - step_description: "DEBUG: Check cleaning results"
    processor_type: "debug_breakpoint"
    message: "Verify data cleaning worked"
  
  # Comment out remaining steps during development
  # - step_description: "Filter data"
  #   processor_type: "filter_data"
```

### Troubleshooting Failures
When a step fails, add a breakpoint before it:

```yaml
- step_description: "Filter for active records"
  processor_type: "filter_data"
  filters:
    - column: "Status"
      condition: "equals"
      value: "Active"

# This step is failing - let's see the data before it
- step_description: "DEBUG: Check data before pivot"
  processor_type: "debug_breakpoint"
  message: "What does the data look like before pivot table?"

- step_description: "Create summary pivot"
  processor_type: "pivot_table"
  # ... failing config
```

### Data Quality Validation
Verify transformations at key points:

```yaml
- step_description: "Apply business rules"
  processor_type: "clean_data"
  # ... conditional replacements

- step_description: "VALIDATE: Business rules applied correctly"
  processor_type: "debug_breakpoint"
  message: "Check that FLESH->CANS replacement worked for canned products"
  show_sample: true
  sample_rows: 10
```

## Configuration Options

### Basic Configuration
```yaml
- processor_type: "debug_breakpoint"
  message: "Simple checkpoint"
```

### Full Configuration
```yaml
- processor_type: "debug_breakpoint"
  message: "Detailed checkpoint with custom settings"
  output_path: "./my_debug_folder/"
  filename_prefix: "step_2_results"
  include_timestamp: true
  show_sample: true
  sample_rows: 15
```

### Available Options

| Option | Default | Description |
|--------|---------|-------------|
| `message` | `"Debug checkpoint"` | Description shown in console |
| `output_path` | `"./debug_outputs/"` | Where to save files |
| `filename_prefix` | `"debug_breakpoint"` | Start of filename |
| `include_timestamp` | `true` | Add timestamp to filename |
| `show_sample` | `true` | Display sample rows in console |
| `sample_rows` | `5` | Number of sample rows to show |

## Real-World Examples

### Van Report Development
```yaml
recipe:
  # Clean SQL export data
  - processor_type: "clean_data"
    rules:
      - column: "Component"
        action: "normalize_whitespace"
  
  - step_description: "CHECK: Invisible characters removed"
    processor_type: "debug_breakpoint"
    message: "Verify Component column is clean for filtering"
    output_path: "./van_debug/"
    filename_prefix: "after_cleaning"
  
  # Apply business logic
  - processor_type: "clean_data"
    rules:
      - column: "Component"
        action: "replace"
        old_value: "FLESH"
        new_value: "CANS"
        condition_column: "Product Name"
        condition: "contains"
        condition_value: "Canned"
  
  - step_description: "CHECK: Business rules applied"
    processor_type: "debug_breakpoint"
    message: "Verify FLESH->CANS replacement for canned products"
    show_sample: true
    sample_rows: 20
```

### Filter Troubleshooting
```yaml
# Problem: Filter is removing all rows
- step_description: "Filter for SALMON, exclude CANS"
  processor_type: "filter_data"
  filters:
    - column: "Component"
      condition: "not_equals"
      value: "CANS"
    - column: "Major Species"
      condition: "contains"
      value: "SALMON"

# Add breakpoint to see what's happening
- step_description: "DEBUG: Why are all rows removed?"
  processor_type: "debug_breakpoint"
  message: "Check data after filtering - should have SALMON records"
  filename_prefix: "filter_results"
  show_sample: true
```

### Pivot Table Validation
```yaml
- step_description: "Create regional summary"
  processor_type: "pivot_table"
  index: ["Region", "Product Origin"]
  columns: ["Carrier"]
  values: ["Van Number"]
  aggfunc: "nunique"

- step_description: "VALIDATE: Pivot table structure"
  processor_type: "debug_breakpoint"
  message: "Check pivot table - should have regions as rows, carriers as columns"
  output_path: "./pivot_validation/"
  include_timestamp: false  # Easier to find the file
```

## Console Output

When a breakpoint is reached, you'll see:

```
============================================================
🔍 DEBUG BREAKPOINT REACHED
============================================================
Message: Check data after filtering
Data saved: ./debug_outputs/debug_breakpoint_20241221_143052.xlsx
Data shape: 847 rows, 8 columns
Columns: ['Product Origin', 'Van Number', 'Carrier', 'Destination', 'Component', 'Major Species', 'Order Date', 'Amount']
============================================================

First 5 rows:
  Product Origin Van Number    Carrier     Destination Component Major Species
0       CORDOVA     V001        MSC      Le Havre       FLESH      SALMON
1       NAKNEK      V002        CMA      Qingdao        FLESH      SALMON  
2       KODIAK      V003      Matson     Sendai         FLESH      SALMON
...
```

## Development Workflow

### Phase 1: Build Recipe Step-by-Step
```yaml
recipe:
  # Step 1: Load and clean
  - processor_type: "clean_data"
    # ... config
  
  - step_description: "PHASE 1 COMPLETE: Data cleaning"
    processor_type: "debug_breakpoint"
    message: "Phase 1 - verify data cleaning"

  # Add more steps one at a time, with breakpoints
```

### Phase 2: Remove Intermediate Breakpoints
```yaml
recipe:
  - processor_type: "clean_data"
    # ... config
  
  # Comment out intermediate breakpoints
  # - processor_type: "debug_breakpoint"
  #   message: "Phase 1 - verify data cleaning"
  
  - processor_type: "filter_data"
    # ... config
  
  - step_description: "FINAL VALIDATION"
    processor_type: "debug_breakpoint"
    message: "Final check before production"
```

### Phase 3: Production Ready
```yaml
recipe:
  - processor_type: "clean_data"
    # ... config
  
  - processor_type: "filter_data"
    # ... config
  
  - processor_type: "pivot_table"
    # ... config
  
  # No debug breakpoints in production
```

## Troubleshooting Patterns

### "Empty DataFrame" Error
```yaml
# Step causing the error
- processor_type: "filter_data"
  filters:
    - column: "Status"
      condition: "equals"
      value: "Active"

# Add breakpoint BEFORE the problematic step
- step_description: "DEBUG: Check data before filtering"
  processor_type: "debug_breakpoint"
  message: "Is Status column clean? Are there any 'Active' values?"
  show_sample: true
```

### "Column Not Found" Error
```yaml
# Step causing the error
- processor_type: "rename_columns"
  mapping:
    "Old Name": "New Name"

# Check actual column names
- step_description: "DEBUG: What are the actual column names?"
  processor_type: "debug_breakpoint"
  message: "Check exact column names and spelling"
```

### "Unexpected Results"
```yaml
# Your transformation
- processor_type: "clean_data"
  rules:
    - column: "Component" 
      action: "replace"
      old_value: "FLESH"
      new_value: "CANS"
      condition_column: "Product Name"
      condition: "contains"
      condition_value: "Canned"

# Verify it worked
- step_description: "DEBUG: Did conditional replacement work?"
  processor_type: "debug_breakpoint"
  message: "Check: FLESH should become CANS only for canned products"
  show_sample: true
  sample_rows: 20
```

## File Management

### Organized Debug Output
```yaml
# Use descriptive paths and prefixes
- processor_type: "debug_breakpoint"
  message: "After data cleaning"
  output_path: "./debug/step_1_cleaning/"
  filename_prefix: "cleaned_data"

# Later in recipe
- processor_type: "debug_breakpoint"
  message: "After filtering"
  output_path: "./debug/step_2_filtering/"
  filename_prefix: "filtered_data"
```

### No Timestamp for Easy Access
```yaml
# Easier to find the file when testing repeatedly
- processor_type: "debug_breakpoint"
  message: "Current test results"
  filename_prefix: "current_test"
  include_timestamp: false
  # Always saves as: current_test.xlsx
```

## Integration Tips

### With Other Processors
```yaml
# Before complex operations
- processor_type: "debug_breakpoint"
  message: "Before pivot table creation"

- processor_type: "pivot_table"
  # ... complex config

# After complex operations  
- processor_type: "debug_breakpoint"
  message: "After pivot table - verify structure"
```

### Error Recovery
```yaml
recipe:
  - processor_type: "clean_data"
    # ... config
  
  # Checkpoint: Known good state
  - processor_type: "debug_breakpoint"
    message: "CHECKPOINT: Data cleaned successfully"
  
  # Risky operation
  - processor_type: "complex_transformation"
    # ... might fail
```

## Best Practices

### Use Descriptive Messages
```yaml
# ✅ Good - explains what to check
message: "Verify SALMON filtering worked - should be ~800 rows"

# ❌ Poor - not helpful
message: "Debug checkpoint"
```

### Check Key Metrics
```yaml
message: "After regional grouping - should have 4 regions: Bristol Bay, Kodiak, PWS, SE"
```

### Remove Before Production
```yaml
# Mark debug breakpoints clearly
- step_description: "DEBUG: REMOVE BEFORE PRODUCTION"
  processor_type: "debug_breakpoint"
  message: "Development checkpoint only"
```

## See Also

- [Recipe Development](../recipes/debugging.md) - Systematic debugging approach
- [Common Issues](../troubleshooting/common-issues.md) - What to look for in debug output  
- [YAML Syntax](../recipes/yaml-syntax.md) - Recipe structure and commenting
