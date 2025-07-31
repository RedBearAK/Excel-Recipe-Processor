# CLI Commands Reference

Complete command-line interface guide.

## Basic Usage

```bash
python -m excel_recipe_processor [input_file] --config [recipe.yaml] [options]
```

## Process Files

### Simple Processing
```bash
# Basic file processing
python -m excel_recipe_processor data.xlsx --config recipe.yaml

# Specify output file
python -m excel_recipe_processor data.xlsx --config recipe.yaml --output results.xlsx

# Process specific sheet
python -m excel_recipe_processor data.xlsx --config recipe.yaml --sheet "Sheet2"

# Verbose output for debugging
python -m excel_recipe_processor data.xlsx --config recipe.yaml --verbose
```

### Advanced Options
```bash
# Custom output sheet name
python -m excel_recipe_processor data.xlsx \
  --config recipe.yaml \
  --output results.xlsx \
  --output-sheet "Processed Data"

# Process by sheet index (1-based)
python -m excel_recipe_processor data.xlsx --config recipe.yaml --sheet 1

# Process by sheet name
python -m excel_recipe_processor data.xlsx --config recipe.yaml --sheet "Raw Data"
```

## System Information

### List Available Processors
```bash
# Basic list
python -m excel_recipe_processor --list-capabilities

# Detailed information
python -m excel_recipe_processor --list-capabilities --detailed

# JSON output (for scripts/automation)
python -m excel_recipe_processor --list-capabilities --json

# YAML output (cleaner than JSON)
python -m excel_recipe_processor --list-capabilities --yaml

# Detailed with YAML capability listings
python -m excel_recipe_processor --list-capabilities --detailed-yaml

# Feature comparison matrix
python -m excel_recipe_processor --list-capabilities --matrix
```

### Save Capabilities to File
```bash
# Save JSON for documentation or automation
python -m excel_recipe_processor --list-capabilities --json > capabilities.json

# Save YAML for cleaner documentation
python -m excel_recipe_processor --list-capabilities --yaml > capabilities.yaml

# Save detailed report
python -m excel_recipe_processor --list-capabilities --detailed > system-info.txt

# Save detailed YAML report (hybrid format)
python -m excel_recipe_processor --list-capabilities --detailed-yaml > detailed-capabilities.yaml
```

## Recipe Validation

### Validate Recipe Files
```bash
# Check recipe syntax and processor availability
python -m excel_recipe_processor --validate-recipe recipe.yaml

# Validate multiple recipes
python -m excel_recipe_processor --validate-recipe sales-report.yaml
python -m excel_recipe_processor --validate-recipe van-report.yaml
```

## Help and Version

```bash
# Show help
python -m excel_recipe_processor --help

# Show version
python -m excel_recipe_processor --version
```

## Command Reference

### Required Arguments

| Argument | Description | Example |
|----------|-------------|---------|
| `input_file` | Excel file to process | `data.xlsx` |

### Core Options

| Option | Short | Description | Example |
|--------|-------|-------------|---------|
| `--config` | `-c` | Recipe YAML file | `--config recipe.yaml` |
| `--output` | `-o` | Output Excel file | `--output results.xlsx` |
| `--sheet` | `-s` | Input sheet name/index | `--sheet "Raw Data"` |
| `--verbose` | `-v` | Detailed logging | `--verbose` |

### System Commands

| Option | Description | Output |
|--------|-------------|--------|
| `--list-capabilities` | Show available processors | Text list |
| `--list-capabilities --detailed` | Detailed processor info | Formatted report |
| `--list-capabilities --json` | Machine-readable capabilities | JSON object |
| `--list-capabilities --yaml` | Clean machine-readable capabilities | YAML format |
| `--list-capabilities --detailed-yaml` | Detailed with YAML capability listings | Hybrid format |
| `--list-capabilities --matrix` | Feature comparison | Table format |
| `--validate-recipe` | Check recipe syntax | Validation report |

### Additional Options

| Option | Description | Default |
|--------|-------------|---------|
| `--output-sheet` | Output sheet name | `ProcessedData` |
| `--help` | Show help message | - |
| `--version` | Show version info | - |

## Exit Codes

| Code | Meaning | Common Causes |
|------|---------|---------------|
| `0` | Success | Processing completed normally |
| `1` | Error | File not found, recipe error, processing failure |
| `2` | Validation Warning | Recipe valid but has warnings |

## Examples by Use Case

### Development Workflow
```bash
# 1. Check system capabilities with clean YAML output
python -m excel_recipe_processor --list-capabilities --yaml

# 2. Get detailed view with structured YAML listings
python -m excel_recipe_processor --list-capabilities --detailed-yaml

# 3. Validate recipe during development
python -m excel_recipe_processor --validate-recipe recipe.yaml

# 4. Test with verbose output
python -m excel_recipe_processor test-data.xlsx --config recipe.yaml --verbose

# 5. Production run
python -m excel_recipe_processor data.xlsx --config recipe.yaml
```

### Automation Scripts
```bash
#!/bin/bash
# Automated processing script

# Get system info for logging (YAML is cleaner for parsing)
python -m excel_recipe_processor --list-capabilities --yaml > system-info.yaml

# Validate recipe
if python -m excel_recipe_processor --validate-recipe monthly-report.yaml; then
    echo "Recipe is valid, processing..."
    
    # Process all files in directory
    for file in *.xlsx; do
        python -m excel_recipe_processor "$file" --config monthly-report.yaml
    done
else
    echo "Recipe validation failed!"
    exit 1
fi
```

### Documentation Generation
```bash
# Generate comprehensive capability documentation
python -m excel_recipe_processor --list-capabilities --detailed-yaml > docs/system-capabilities.md

# Generate machine-readable capability index
python -m excel_recipe_processor --list-capabilities --yaml > config/system-capabilities.yaml

# Generate feature compatibility matrix
python -m excel_recipe_processor --list-capabilities --matrix > docs/feature-matrix.txt
```

### Troubleshooting
```bash
# Debug a failing recipe
python -m excel_recipe_processor data.xlsx \
  --config recipe.yaml \
  --verbose \
  --output debug-output.xlsx

# Check if specific processor is available (YAML is easier to parse)
python -m excel_recipe_processor --list-capabilities --yaml | grep "pivot_table"

# Get detailed capability info for debugging
python -m excel_recipe_processor --list-capabilities --detailed-yaml | grep -A 20 "PIVOT"

# Validate recipe step by step
python -m excel_recipe_processor --validate-recipe recipe.yaml
```

## Integration with Other Tools

### PowerShell (Windows)
```powershell
# Process multiple files
Get-ChildItem *.xlsx | ForEach-Object {
    python -m excel_recipe_processor $_.Name --config recipe.yaml
}

# Export system capabilities for documentation
python -m excel_recipe_processor --list-capabilities --yaml | Out-File -Encoding UTF8 capabilities.yaml
```

### Bash Scripting
```bash
# Check if processing succeeded
if python -m excel_recipe_processor data.xlsx --config recipe.yaml; then
    echo "Processing successful"
    # Move processed file
    mv processed_data.xlsx /output/directory/
else
    echo "Processing failed"
    exit 1
fi

# Parse YAML capabilities in scripts (requires yq)
TOTAL_PROCESSORS=$(python -m excel_recipe_processor --list-capabilities --yaml | yq '.system_info.total_processors')
echo "System has $TOTAL_PROCESSORS processors available"
```

### Scheduled Tasks
```bash
# Cron job for daily processing with capability check
# 0 6 * * * /usr/bin/python -m excel_recipe_processor --list-capabilities --yaml > /logs/capabilities.yaml && /usr/bin/python -m excel_recipe_processor /data/daily.xlsx --config /scripts/daily-recipe.yaml
```

## Output Format Comparison

| Format | Use Case | Pros | Cons |
|--------|----------|------|------|
| Basic (`--list-capabilities`) | Quick overview | Fast, human-readable | Limited detail |
| Detailed (`--detailed`) | Manual inspection | Rich formatting, examples | Not machine-parseable |
| JSON (`--json`) | Automation/scripts | Standard format, all data | Verbose, harder to read |
| **YAML (`--yaml`)** | Clean automation | Less cluttered than JSON, human-readable | Requires PyYAML |
| **Detailed YAML (`--detailed-yaml`)** | Documentation | Structured + complete listings | Longer output |
| Matrix (`--matrix`) | Feature comparison | Visual compatibility | Limited to features only |

## Performance Tips

- Use `--verbose` only during development/debugging
- Validate recipes before running in production
- Process smaller files first to test recipes
- Use specific sheet names rather than indexes when possible
- Use YAML output for cleaner automation scripts
- Use detailed-yaml for comprehensive documentation

## See Also

- [System Capabilities](capabilities.md) - Understanding `--list-capabilities`
- [Recipe Validation](../recipes/debugging.md) - Using `--validate-recipe`
- [Troubleshooting](../troubleshooting/common-issues.md) - Solving common problems