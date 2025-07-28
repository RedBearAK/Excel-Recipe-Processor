# Usage Examples Formatting Standards

## Formatting Rules for `get_usage_examples()` Methods

### 1. **Comment Placement**
- Always place comments **above** the line they describe
- Never use inline comments (no `# comment` at end of lines)
- Leave blank line between comment groups for readability

```yaml
# ✅ GOOD
# REQ - Processor type identifier
processor_type: "export_file"
# OPT - Backup existing file
# Default value: false
create_backup: true

# ❌ BAD
processor_type: "export_file"                    # required: processor type
create_backup: true                              # optional: backup file
```

### 2. **Required vs Optional Marking**
- Always start parameter comments with `# REQ` or `# OPT`
- Use separate lines for additional information like defaults and examples
- Be generous with helpful details across multiple comment lines

```yaml
# REQ - Must be "processor_name" for this processor type
processor_type: "processor_name"
# OPT - Human-readable step description
step_description: "Process the data"
# OPT - Backup existing files
# Default value: false
create_backup: true
# REQ - Input file path with variable support
# Valid examples: "data.xlsx", "files/{date}.csv", "{batch_id}_input.xlsx"
input_file: "data/monthly_sales.xlsx"
```

### 3. **Multi-line Comments for Complex Features**
- Use multiple comment lines generously for complex parameters
- Include examples, valid values, defaults, and detailed explanations
- Group related information together for clarity

```yaml
# REQ - Output file path with variable substitution support
# Built-in variables: {date}, {timestamp}, {company}, {batch_id}
# Valid extensions: .xlsx, .csv, .tsv
# Variable examples: company="AcmeCorp", batch_id="B001"
output_file: "reports/{company}_{date}.xlsx"
# OPT - Text encoding for CSV files (ignored for Excel files)
# Valid values: "utf-8", "latin1", "cp1252", "ascii"
# Default value: "utf-8"
encoding: "utf-8"
```

### 4. **List and Object Documentation**
- Document list items and object properties clearly
- Show the structure with proper indentation
- Explain each nested parameter with OPT/REQ marking

```yaml
# OPT - List of sheet configurations for multi-sheet export
# If omitted, exports current pipeline data to single sheet
sheets:
  # First sheet configuration
  - # REQ - Name of the Excel sheet tab
    sheet_name: "Summary"
    # OPT - Data source for this sheet
    # Valid values: "current", or any saved stage name  
    # Default value: "current"
    data_source: "current"
    # OPT - Make this the active sheet when file opens
    # Default value: false
    active: true
```

### 5. **Line Length and Readability**
- Keep lines under 80 characters when possible
- Break long comments into multiple lines
- Use consistent indentation (2 spaces)

### 6. **Example Structure**
Each example should follow this pattern:

```yaml
'example_name': {
    'description': 'Brief description of what this example demonstrates',
    'yaml': '''# Context comment explaining the overall purpose
- # OPT - Step description
  step_description: "Clear description of what this step does"
  # REQ - Processor type
  processor_type: "processor_name"
  # REQ - Main parameter with clear explanation
  # Valid examples: value1, value2, value3
  main_param: "example_value"
  # OPT - Additional parameter with helpful info
  # Default value: default_setting
  # Special notes: any important usage notes
  optional_param: "value"'''
}
```

## Complete Template

```python
def get_usage_examples(self) -> dict:
    """Get complete usage examples for the [processor_name] processor."""
    return {
        'description': 'Brief description of what this processor does',
        
        'basic_example': {
            'description': 'Simple, common use case',
            'yaml': '''# Brief context about this example
- # OPT - Human-readable description
  step_description: "Simple operation"
  # REQ - Processor type identifier
  processor_type: "processor_name"
  # REQ - Main required parameter
  # Valid examples: example1, example2, example3
  required_param: "example_value"
  # OPT - Optional parameter
  # Default value: default_setting
  optional_param: "example"'''
        },
        
        'advanced_example': {
            'description': 'Complex use case showing all features',
            'yaml': '''# Complex example demonstrating advanced features
- # OPT - Step description
  step_description: "Advanced operation with all features"
  # REQ - Processor type
  processor_type: "processor_name"
  # REQ - Complex parameter with multiple options
  # Valid values: option1, option2, option3
  # Variable examples: var1="value1", var2="value2"
  complex_param: "advanced_value"
  # OPT - Nested configuration object
  nested_config:
    # REQ - Nested required parameter
    nested_required: "value"
    # OPT - Nested optional parameter
    # Default value: default_value
    # Special handling: important usage notes
    nested_optional: "custom_value"'''
        },
        
        'parameter_details': {
            'required_param': {
                'type': 'string',
                'required': True,
                'description': 'Clear description of what this parameter does',
                'examples': ['example1', 'example2']
            },
            'optional_param': {
                'type': 'string',
                'required': False,
                'default': 'default_value',
                'description': 'Description with default value noted'
            }
        }
    }
```

## Benefits of This Format

### ✅ **Highly Scannable**
- `OPT` and `REQ` tags stand out immediately
- No visual clutter from unnecessary punctuation
- Quick identification of required vs optional parameters

### ✅ **Maximum Information Density**
- Multi-line comments provide comprehensive guidance
- Room for examples, defaults, valid values, and edge cases
- No space wasted on inline comment limitations

### ✅ **Developer Friendly**
- Easy to scan and understand at a glance
- Copy-paste ready examples with full context
- Clear guidance on proper usage and common pitfalls

### ✅ **Professional Appearance**
- Clean, consistent formatting across all processors
- Logical information hierarchy
- Proper indentation and spacing

## Implementation Notes

1. **Always test examples** - Make sure the YAML actually works
2. **Keep examples realistic** - Use meaningful variable names and values
3. **Show progression** - Basic → Advanced → Specialized use cases
4. **Document edge cases** - Include common gotchas and limitations
5. **Reference integration** - Show how processor works with others

This formatting standard ensures all processors provide consistent, professional, and highly usable documentation.
