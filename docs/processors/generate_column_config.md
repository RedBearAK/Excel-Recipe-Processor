# `generate_column_config`

**Family:** `file_ops`

Generate YAML column configuration by comparing source and template files

## Keys

Generated from the declared schema; keys not listed are refused at recipe load.

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `source_file`: str; REQUIRED
- `source_sheet`: any
- `template_file`: str; REQUIRED
- `template_sheet`: any
- `output_file`: str; REQUIRED
- `header_row`: int; default 1
- `max_rows`: int; default 1000
- `sample_rows`: int; default 5
- `similarity_threshold`: number; default 0.8
- `include_recipe_section`: bool; default false

## Examples

Every step below validates against the schema (tests/test_examples_validate_against_schemas.py).

### basic file

Compare two files and generate column configuration

```yaml
settings:
  description: "Generate column config from raw download and desired format files"
  
recipe:
  - # Generate column configuration by comparing files directly
    step_description: "Generate column configuration for recipe sync"
    # REQ - Must be "generate_column_config" for this processor type
    processor_type: "generate_column_config"
    # REQ - File with original/raw column names
    source_file: "downloads/raw_export.xlsx"
    # REQ - File with desired column names in final order
    template_file: "templates/desired_format.xlsx"
    # REQ - Output file path for generated YAML configuration
    output_file: "configs/column_config.yaml"
```

### excel sheet selection

Generate config from specific Excel sheets

```yaml
settings:
  description: "Generate column config with specific sheet selection"
  variables:
    config_date: "{YYYY-MM-DD}"
    raw_file: "exports/database_export_{YYYYMMDD}.xlsx"
    template_file: "templates/cash_flow_format.xlsx"
    
recipe:
  - # Generate column configuration with sheet selection
    step_description: "Generate column config from specific Excel sheets"
    processor_type: "generate_column_config"
    # REQ - Source Excel file path
    source_file: "{str:raw_file}"
    # REQ - Template Excel file path  
    template_file: "{str:template_file}"
    # OPT - Specific sheet in source file (name or 1-based number)
    source_sheet: "Database Export"
    # OPT - Specific sheet in template file (name or 1-based number)
    template_sheet: 2
    # REQ - Output configuration file
    output_file: "configs/cash_flow_column_config_{str:config_date}.yaml"
    # OPT - Include ready-to-use recipe section
    include_recipe_section: true
```

### advanced configuration

Advanced configuration with custom similarity threshold

```yaml
settings:
  description: "Generate column config with custom analysis settings"
  
recipe:
  - # Advanced column configuration generation
    step_description: "Generate column config with custom similarity matching"
    processor_type: "generate_column_config"
    source_file: "data/legacy_system_export.csv"
    template_file: "templates/business_format.xlsx"
    template_sheet: "Column Template"
    output_file: "configs/legacy_to_business_mapping.yaml"
    # OPT - Lower threshold to catch more potential renames
    # Values: 0.0 (match anything) to 1.0 (exact match only)
    # Default value: 0.8
    similarity_threshold: 0.5
    # OPT - Which row contains headers (1-based for Excel)
    # Default value: 1
    header_row: 3
    # OPT - Number of sample rows to analyze for empty columns
    # Default value: 5 (keep low for performance)
    sample_rows: 10
    # OPT - Whether to check for empty columns in data
    # OPT - Include recipe for immediate use
    include_recipe_section: true
```

### mixed file formats

Compare CSV source with Excel template

```yaml
settings:
  description: "Generate config comparing different file formats"
  
recipe:
  - # Mixed file format comparison
    step_description: "Compare CSV export with Excel template"
    processor_type: "generate_column_config"
    # Source can be CSV, Excel, etc.
    source_file: "downloads/daily_export.csv"
    # Template can be different format
    template_file: "templates/report_format.xlsx"
    template_sheet: "Report Layout"
    output_file: "configs/daily_export_mapping.yaml"
    # CSV files ignore sheet parameters (no error)
    source_sheet: null  # Explicitly null for clarity
    include_recipe_section: false  # Just generate variables
```

## Parameter notes

- `processor_type` (required): Must be 'generate_column_config' for this processor type
- `step_description` (default `Unnamed generate_column_config step`): Human-readable description of what this configuration generation does
- `source_file` (required): Path to file containing original/raw column names
- `template_file` (required): Path to file containing desired column names in final order
- `output_file` (required): Path where the generated YAML configuration file will be written
- `source_sheet` (default `None`): Specific sheet in source Excel file (ignored for CSV files)
- `template_sheet` (default `None`): Specific sheet in template Excel file (ignored for CSV files)
- `header_row` (default `1`): Row number containing column headers (1-based for Excel)
- `sample_rows` (default `True`): Whether to analyze data rows to detect truly empty columns
- `similarity_threshold` (default `0.8`): Minimum similarity score for automatic column name matching
- `include_recipe_section` (default `False`): Whether to include a complete recipe section with import/rename/select steps

