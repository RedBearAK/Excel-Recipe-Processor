# Usage Example File Creation Rules

## File Structure Requirements

### 1. **File Naming**
- Format: `{processor_name}_examples.yaml`
- Location: `excel_recipe_processor/processors/_examples/`
- Examples: `filter_data_examples.yaml`, `export_file_examples.yaml`

### 2. **Required File Header**
```yaml
# Revision date: 2025-07-30

description: "Brief description of what this processor does with stage-based data flow"
```

### 3. **Required Sections**
Every file must contain:
- **Main description** - What the processor does
- **Multiple examples** - At least `basic_example`, ideally 4-6 examples
- **parameter_details** - Complete parameter documentation

## Example Structure Requirements

### 4. **Example Naming Pattern**
```yaml
basic_example:
  description: "Simple common use case"
  yaml: |
    # Example content here

advanced_example:
  description: "Complex use case showing all features" 
  yaml: |
    # Example content here

{specific}_example:
  description: "Specific scenario description"
  yaml: |
    # Example content here
```

### 5. **Example Progression**
- **Basic** → **Advanced** → **Specialized** scenarios
- Each example should be **complete and functional**
- Show **realistic use cases**, not toy examples

## Settings Section Requirements

### 6. **Required Description** ⚠️ **HARD REQUIREMENT**
Every `settings:` section must include a description:
```yaml
settings:
  description: "Brief description of what this recipe does"
  # ... other settings
```

### 7. **Stage Declarations**
All stages used in examples must be declared:
```yaml
settings:
  description: "Process customer data with filtering"
  stages:
    - stage_name: "raw_data"
      description: "Raw imported data"
      protected: false
    - stage_name: "filtered_data"
      description: "Filtered results"
      protected: false
```

### 8. **Variables (when used)**
```yaml
settings:
  description: "Dynamic processing with variables"
  variables:
    region: "west"
    min_amount: "500"
  # ... stages
```

## Recipe Step Requirements

### 9. **Stage-Based Architecture** ⚠️ **REQUIRED**
Every processing step must use stages:
```yaml
recipe:
  - step_description: "Process the data"
    processor_type: "filter_data"
    source_stage: "raw_data"      # REQ for processing steps
    save_to_stage: "filtered_data" # REQ for processing steps
    # ... processor parameters
```

**Exceptions:**
- `import_file`: Only needs `save_to_stage`
- `export_file`: Only needs `source_stage`
- `debug_breakpoint`: Neither required

### 10. **Parameter Documentation** ⚠️ **REQUIRED**
Every parameter must have proper comments:
```yaml
# REQ - Must be "filter_data" for this processor type
processor_type: "filter_data"
# OPT - Human-readable step description
# Default value: "Unnamed filter_data step"
step_description: "Filter for active customers"
# REQ - Stage to read data from (must be declared in settings.stages)
source_stage: "raw_data"
```

## Comment Format Requirements

### 11. **OPT/REQ Markers** ⚠️ **REQUIRED**
- Start with `# REQ -` for required parameters
- Start with `# OPT -` for optional parameters
- Always place comments **above** the parameter line

### 12. **Default Value Documentation** ⚠️ **REQUIRED**
Optional parameters must document defaults:
```yaml
# OPT - Create backup before overwriting
# Default value: false
create_backup: true
```

### 13. **Multi-line Documentation**
Use multiple comment lines for complex parameters:
```yaml
# REQ - Output file path with variable substitution support
# Built-in variables: {date}, {timestamp}, {YYYY}, {MM}, {DD}
# Custom variables: {department}, {batch_id} - defined in recipe settings
# Variable examples: department="sales", batch_id="B001"
output_file: "reports/{department}_{date}.xlsx"
```

## Parameter Details Section

### 14. **Complete Parameter Documentation**
```yaml
parameter_details:
  source_stage:
    type: string
    required: true
    description: "Stage name to read data from (must be declared in settings.stages)"
    examples:
      - "raw_data"
      - "imported_customers"
    note: "Stage must exist and contain data before processing step"
  
  optional_param:
    type: boolean
    required: false
    default: false
    description: "Clear description of what this parameter does"
```

## Quality Standards

### 15. **Complete Working Examples**
- Every example should be **copy-paste ready**
- Include **realistic data scenarios**
- Show **proper error handling patterns**

### 16. **Progressive Complexity**
- Start with **minimal configuration**
- Add **common options** in middle examples
- Show **advanced features** in later examples

### 17. **Consistent Formatting**
- **2-space indentation**
- **Blank lines** between major sections
- **Consistent variable naming** (snake_case for stages, realistic names)

## Validation Checks

### 18. **Files Must Pass Tests**
- **Revision date check** - Must have current date
- **Settings description check** - Every settings must have description
- **OPT/REQ markers check** - All parameters must be marked
- **Default value check** - Optional parameters must document defaults
- **YAML syntax check** - Must be valid YAML

### 19. **Breaking Changes**
These requirements are **enforced by validation**:
- Missing settings descriptions = **Recipe processing stops**
- Missing OPT/REQ markers = **Quality test failure**
- Missing stage declarations = **Runtime warnings**

## Common Anti-Patterns to Avoid

### ❌ **Don't Do This:**
```yaml
# Bad: Empty settings
settings:

# Bad: Missing description
settings:
  stages: [...]

# Bad: Inline comments
processor_type: "filter_data"  # required

# Bad: No default documentation
# OPT - Optional parameter
create_backup: true

# Bad: Non-stage architecture
processor_type: "filter_data"
filters: [...]  # Missing source_stage/save_to_stage
```

### ✅ **Do This:**
```yaml
# Good: Complete settings
settings:
  description: "Filter customer data for active accounts"
  stages:
    - stage_name: "raw_data"
      description: "Raw customer data"
      protected: false

# Good: Proper parameter documentation
# REQ - Must be "filter_data" for this processor type
processor_type: "filter_data"
# REQ - Stage to read data from
source_stage: "raw_data"
# REQ - Stage to save filtered results
save_to_stage: "active_customers"
# OPT - Create backup before processing
# Default value: false
create_backup: true
```

## Summary Checklist

✅ **File Structure:**
- [ ] Revision date comment at top
- [ ] Main description
- [ ] Multiple progressive examples
- [ ] Complete parameter_details section

✅ **Settings Requirements:**
- [ ] Every settings has description (**HARD REQUIREMENT**)
- [ ] All used stages are declared
- [ ] Variables defined when used

✅ **Recipe Requirements:**
- [ ] Stage-based architecture (source_stage/save_to_stage)
- [ ] All parameters have OPT/REQ markers
- [ ] Optional parameters document defaults
- [ ] Comments above parameters, not inline

✅ **Quality Standards:**
- [ ] Complete working examples
- [ ] Realistic scenarios
- [ ] Progressive complexity
- [ ] Consistent formatting

Following these rules ensures your usage example file will pass all validation tests and provide excellent documentation for users.
