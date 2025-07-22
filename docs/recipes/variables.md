# Variable Substitution

Dynamic filename generation using variables and templates.

## Overview

Variable substitution allows you to create dynamic output filenames based on dates, input files, and custom values. Perfect for automated reports that need unique names.

## Basic Usage

```yaml
settings:
  output_filename: "{YY}{MMDD}_SalesReport.xlsx"
  # Generates: 241221_SalesReport.xlsx (for Dec 21, 2024)
```

## Available Variables

### Date & Time Variables

| Variable | Example | Description |
|----------|---------|-------------|
| `{year}` | `2024` | Four-digit year |
| `{month}` | `12` | Two-digit month |
| `{day}` | `21` | Two-digit day |
| `{hour}` | `14` | Two-digit hour (24h) |
| `{minute}` | `30` | Two-digit minute |
| `{second}` | `45` | Two-digit second |
| `{date}` | `20241221` | YYYYMMDD format |
| `{time}` | `143045` | HHMMSS format |
| `{timestamp}` | `20241221_143045` | Full timestamp |

### Short Date Variables

| Variable | Example | Description |
|----------|---------|-------------|
| `{YY}` | `24` | Two-digit year |
| `{MM}` | `12` | Two-digit month |
| `{DD}` | `21` | Two-digit day |
| `{HH}` | `14` | Two-digit hour |
| `{MMDD}` | `1221` | Month and day |
| `{HHMM}` | `1430` | Hour and minute |

### File-Based Variables

| Variable | Example | Description |
|----------|---------|-------------|
| `{input_filename}` | `sales_data.xlsx` | Full input filename |
| `{input_basename}` | `sales_data` | Input name without extension |
| `{input_extension}` | `.xlsx` | Input file extension |
| `{recipe_filename}` | `monthly_report.yaml` | Recipe filename |
| `{recipe_basename}` | `monthly_report` | Recipe name without extension |

## Real-World Examples

### Van Report Pattern
```yaml
settings:
  output_filename: "{YY}{MMDD}_ExportContainersUpdate.xlsx"
  # Generates: 241221_ExportContainersUpdate.xlsx
```

### Timestamped Reports
```yaml
settings:
  output_filename: "DailyReport_{timestamp}.xlsx"
  # Generates: DailyReport_20241221_143045.xlsx
```

### Input-Based Naming
```yaml
settings:
  output_filename: "Processed_{input_basename}_{date}.xlsx"
  # Input: sales_raw.xlsx → Output: Processed_sales_raw_20241221.xlsx
```

### Department Reports
```yaml
settings:
  variables:
    department: "finance"
    report_type: "quarterly"
  output_filename: "{department}_{report_type}_{YY}Q4.xlsx"
  # Generates: finance_quarterly_24Q4.xlsx
```

## Advanced Formatting

### Formatted Variables
Use `{variable:format}` syntax for custom formatting:

```yaml
settings:
  # Custom date formats
  output_filename: "Report_{date:YYYY-MM-DD}.xlsx"
  # Generates: Report_2024-12-21.xlsx
  
  output_filename: "Summary_{date:MonthDay}.xlsx"  
  # Generates: Summary_Dec21.xlsx
```

### Available Date Formats

| Format | Example | Description |
|--------|---------|-------------|
| `{date:YYYY}` | `2024` | Four-digit year |
| `{date:YY}` | `24` | Two-digit year |
| `{date:MM}` | `12` | Two-digit month |
| `{date:DD}` | `21` | Two-digit day |
| `{date:MMDD}` | `1221` | Month and day |
| `{date:YYYYMMDD}` | `20241221` | Full date |
| `{date:MonthDay}` | `Dec21` | Abbreviated month |
| `{date:Month}` | `Dec` | Month abbreviation |
| `{date:MonthName}` | `December` | Full month name |

### Time Formats

| Format | Example | Description |
|--------|---------|-------------|
| `{time:HH}` | `14` | Hour (24h) |
| `{time:MM}` | `30` | Minute |
| `{time:SS}` | `45` | Second |
| `{time:HHMM}` | `1430` | Hour and minute |
| `{time:HHMMSS}` | `143045` | Full time |

## Custom Variables

Define your own variables in the settings section:

```yaml
settings:
  variables:
    region: "west_coast"
    period: "Q4_2024"
    version: "v2"
  
  output_filename: "{region}_{period}_report_{version}.xlsx"
  # Generates: west_coast_Q4_2024_report_v2.xlsx
```

### Complex Custom Variables
```yaml
settings:
  variables:
    # Business variables
    fiscal_year: "FY25"
    department: "operations"
    classification: "confidential"
    
    # Processing metadata
    source_system: "ERP"
    extract_type: "full"
  
  output_filename: "{department}_{fiscal_year}_{extract_type}_{source_system}_{date}.xlsx"
  # Generates: operations_FY25_full_ERP_20241221.xlsx
```

## Common Patterns

### Daily Reports
```yaml
# Pattern: YYMMDD_ReportName.xlsx
output_filename: "{YY}{MMDD}_DailySales.xlsx"
# 241221_DailySales.xlsx
```

### Monthly Reports
```yaml
# Pattern: YYYY-MM_ReportName.xlsx  
output_filename: "{year}-{month}_MonthlySummary.xlsx"
# 2024-12_MonthlySummary.xlsx
```

### Timestamped Backups
```yaml
# Pattern: Original_YYYYMMDD_HHMMSS.xlsx
output_filename: "{input_basename}_{timestamp}.xlsx"
# sales_data_20241221_143045.xlsx
```

### Versioned Reports
```yaml
settings:
  variables:
    version: "1.2"
  output_filename: "Report_v{version}_{date}.xlsx"
  # Report_v1.2_20241221.xlsx
```

## Debugging Variables

### Check Variable Values
Add a debug step to see what variables resolve to:

```yaml
settings:
  output_filename: "{YY}{MMDD}_Debug_{input_basename}.xlsx"

recipe:
  - step_description: "Debug: Check filename variables"
    processor_type: "debug_breakpoint"
    message: "Output will be: {YY}{MMDD}_Debug_{input_basename}.xlsx"
```

### Test Variable Expansion
Use the CLI to validate:

```bash
# Run with --verbose to see variable substitution
python -m excel_recipe_processor data.xlsx --config recipe.yaml --verbose
```

## Error Handling

### Missing Variable Error
```yaml
# ❌ This will fail if 'unknown_var' is not defined
output_filename: "{unknown_var}_report.xlsx"

# Error: Unknown variable 'unknown_var' in filename template
```

### Safe Patterns
```yaml
# ✅ Use only built-in variables for reliability
output_filename: "{YY}{MMDD}_SafeReport.xlsx"

# ✅ Or define all custom variables
settings:
  variables:
    report_name: "weekly_summary"
  output_filename: "{report_name}_{date}.xlsx"
```

## Best Practices

### Use Descriptive Names
```yaml
# ✅ Clear and descriptive
output_filename: "{YY}{MMDD}_VanReport_Export.xlsx"

# ❌ Too cryptic  
output_filename: "{YY}{MMDD}_VR_E.xlsx"
```

### Include Dates for Time Series
```yaml
# ✅ Good for automated reports
output_filename: "WeeklySales_{YY}{MMDD}.xlsx"

# ❌ Will overwrite previous runs
output_filename: "WeeklySales.xlsx"
```

### Separate with Underscores
```yaml
# ✅ Easy to read and parse
output_filename: "dept_{department}_period_{YY}Q4_final.xlsx"

# ❌ Hard to read
output_filename: "dept{department}period{YY}Q4final.xlsx"
```

### Keep Extensions Simple
```yaml
# ✅ Standard Excel extension
output_filename: "report_{date}.xlsx"

# ❌ Avoid complex extensions
output_filename: "report_{date}.{input_extension}"
```

## Variable Reference

### Complete List of Built-in Variables
```yaml
# Date/Time (current execution time)
{year}, {month}, {day}, {hour}, {minute}, {second}
{date}, {time}, {timestamp}
{YY}, {MM}, {DD}, {HH}, {MMDD}, {HHMM}

# Input file (from command line)
{input_filename}, {input_basename}, {input_extension}

# Recipe file  
{recipe_filename}, {recipe_basename}

# Custom (defined in settings.variables)
{your_custom_variable}
```

### Advanced Date Formats
```yaml
# Standard formats
{date:YYYY}, {date:MM}, {date:DD}, {date:MMDD}
{date:YYYYMMDD}, {date:MonthDay}, {date:Month}

# Time formats
{time:HH}, {time:MM}, {time:HHMM}, {time:HHMMSS}

# Custom strftime formats (advanced)
{date:%Y-%m-%d}, {time:%H:%M:%S}
```

## See Also

- [YAML Syntax](yaml-syntax.md) - Complete recipe structure
- [CLI Commands](../cli/commands.md) - Running recipes with variables
- [Example Recipes](examples/) - Real variable usage patterns
