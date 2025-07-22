# Common Issues & Solutions

Quick fixes for the most frequent problems.

## Filtering Problems

### "Filter removes all rows" or "Filter doesn't work"

**Symptoms**: 
- Filter step removes all data when it shouldn't
- `condition: "equals"` finds no matches for values you can see

**Cause**: Invisible Unicode characters from SQL exports

**Solution**: Clean data first
```yaml
# BEFORE any filtering steps
- step_description: "Clean invisible characters"
  processor_type: "clean_data"
  rules:
    - column: "Product Origin"
      action: "normalize_whitespace"
    - column: "Status"
      action: "normalize_whitespace"

# NOW filtering will work
- step_description: "Filter for active records"
  processor_type: "filter_data"
  filters:
    - column: "Status"
      condition: "equals"
      value: "Active"
```

**Why this happens**: SQL exports often contain zero-width spaces, non-breaking spaces, and other invisible characters that make "Active" ≠ "Active " (note the invisible space).

---

## Pivot Table Problems

### "Grouper not 1-dimensional" Error

**Symptoms**: 
```
Error: Grouper for 'Van Number' not 1-dimensional
```

**Cause**: Using the same column in both `index` and `values`

**Wrong**:
```yaml
- processor_type: "pivot_table"
  index: ["Region", "Van Number"]    # Van Number used here
  values: ["Van Number"]             # AND here - causes conflict
  aggfunc: "count"
```

**Solution**: Use different column for values or leave empty
```yaml
- processor_type: "pivot_table"
  index: ["Region", "Van Number"]
  values: ["Product Name"]           # Different column
  aggfunc: "count"

# OR leave values empty for simple counting
- processor_type: "pivot_table"
  index: ["Region", "Van Number"]
  values: []                         # Empty for row counting
  aggfunc: "count"
```

### Too Many Pivot Columns

**Symptoms**: Pivot table has 60+ columns when you expected 4

**Cause**: Empty `values: []` counts all remaining columns

**Solution**: Specify one column for counting
```yaml
- processor_type: "pivot_table"
  index: ["Region", "Product"]
  columns: ["Carrier"]
  values: ["Container_ID"]           # Use any single column
  aggfunc: "nunique"                 # Count unique values
```

---

## Recipe Syntax Errors

### YAML Indentation Errors

**Symptoms**:
```
Error: could not determine a constructor for the tag
```

**Cause**: Wrong YAML indentation

**Wrong**:
```yaml
recipe:
- step_description: "Bad indent"
  processor_type: "clean_data"
```

**Correct**:
```yaml
recipe:
  - step_description: "Good indent"    # Note the 2-space indent
    processor_type: "clean_data"
```

### Missing Required Fields

**Symptoms**:
```
Error: missing required field: step_description
```

**Solution**: Every step needs both fields
```yaml
- step_description: "What this step does"  # Required
  processor_type: "clean_data"             # Required
  # ... processor-specific config
```

---

## Column Name Issues

### "Column not found" Errors

**Symptoms**:
```
Error: Column 'product name' not found
```

**Cause**: Case-sensitive column names or typos

**Debug**: Check exact column names
```yaml
# Add a debug step to see actual column names
- processor_type: "debug_breakpoint"
  message: "Check column names"
  show_sample: true
```

**Solution**: Use exact case and spelling
```yaml
column: "Product Name"     # ✅ Correct
column: "product name"     # ❌ Wrong case
column: "Product_Name"     # ❌ Wrong separator
```

---

## Data Type Problems

### Numeric Operations Fail

**Symptoms**: Math operations return NaN or errors

**Cause**: Numbers stored as text with formatting

**Solution**: Fix numeric format first
```yaml
- processor_type: "clean_data"
  rules:
    - column: "Price"
      action: "fix_numeric"    # Removes $, commas, converts to number
    - column: "Quantity"
      action: "fix_numeric"

# NOW calculations will work
- processor_type: "add_calculated_column"
  new_column: "Total"
  calculation:
    type: "expression"
    expression: "Price * Quantity"
```

---

## Empty Results

### "Received empty DataFrame" Error

**Symptoms**: Processing stops with empty data error

**Cause**: Filtering removed all rows

**Debug**: Add checkpoint before the problematic step
```yaml
- processor_type: "filter_data"
  # ... your filter config

# Add this to check what's left
- processor_type: "debug_breakpoint"
  message: "Check data after filtering"
  show_sample: true
```

**Solution**: Adjust filter conditions or check data quality

---

## Performance Issues

### Processing Very Slow

**Common causes**:
- Large datasets (>100k rows)
- Complex regex patterns
- Multiple lookups on large tables

**Solutions**:
- Filter data early to reduce size
- Use simpler text operations when possible
- Consider breaking into smaller chunks

### Memory Errors

**Solution**: Process in smaller batches or filter earlier
```yaml
# Filter early to reduce memory usage
- processor_type: "filter_data"
  filters:
    - column: "Date"
      condition: "greater_than"
      value: "2024-01-01"    # Only recent data

# THEN do expensive operations on smaller dataset
- processor_type: "pivot_table"
  # ... config
```

---

## Recipe Development Tips

### Test Incrementally
```yaml
recipe:
  - step_description: "Step 1"
    processor_type: "clean_data"
    # ... config
  
  - step_description: "DEBUG: Stop here for testing"
    processor_type: "debug_breakpoint"
    message: "Testing step 1 only"
  
  # Comment out remaining steps during testing
  # - step_description: "Step 2"
  #   processor_type: "filter_data"
```

### Use Verbose Mode
```bash
python -m excel_recipe_processor data.xlsx --config recipe.yaml --verbose
```

### Validate Before Running
```bash
python -m excel_recipe_processor --validate-recipe recipe.yaml
```

---

## Getting Help

### Check System Capabilities
```bash
# See what's available
python -m excel_recipe_processor --list-capabilities --detailed

# Get machine-readable info
python -m excel_recipe_processor --list-capabilities --json
```

### Common Debug Pattern
```yaml
# Standard debugging approach
recipe:
  # 1. Clean data first
  - processor_type: "clean_data"
    rules:
      - column: "all_text_columns"
        action: "normalize_whitespace"
  
  # 2. Add checkpoint
  - processor_type: "debug_breakpoint"
    message: "After cleaning"
  
  # 3. Continue with processing
  # ... rest of recipe
```

### Error Message Patterns

| Error Contains | Likely Cause | Quick Fix |
|----------------|--------------|-----------|
| "not found" | Column name typo | Check exact column names |
| "1-dimensional" | Pivot table config error | Don't use same column in index and values |
| "empty DataFrame" | Over-filtering | Add debug checkpoint before filters |
| "yaml" or "indent" | YAML syntax | Check indentation and colons |
| "capabilities" | Processor doesn't exist | Check spelling, run `--list-capabilities` |

---

## See Also

- [SQL Export Problems](sql-export-problems.md) - Invisible character details
- [Recipe Debugging](../recipes/debugging.md) - Systematic debugging approach  
- [Processor Reference](../processors/overview.md) - Individual processor troubleshooting
