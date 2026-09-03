# `select_columns`

**Family:** `transform`

Select and reorder DataFrame columns with flexible inclusion/exclusion patterns

## Keys

Generated from the declared schema; keys not listed are refused at recipe load.

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `source_stage`: stage_in; REQUIRED - Stage to read
- `save_to_stage`: stage_out; REQUIRED - Stage to write
- `confirm_stage_replacement`: bool; default false - Required true to overwrite an existing stage
- `columns_to_keep`: list of str - Names, in output order
- `columns_to_drop`: list of str - Column names
- `columns_to_create`: list of str - Created blank when absent from columns_to_keep
- `default_value`: any - Fill for created columns
- `strict_mode`: bool; default true
- `allow_duplicates`: bool; default true
- at least one of: `columns_to_keep`, `columns_to_drop`

## Examples

Every step below validates against the schema (tests/test_examples_validate_against_schemas.py).

### basic selection

Keep only essential columns from a wide dataset

```yaml
# Simple column selection example

settings:
  description: "Select key business columns from wide export data"
  stages:
    - stage_name: "stg_raw_export"
      description: "Wide export data with many columns"
      protected: false
    - stage_name: "stg_essential_data"
      description: "Key business columns only"
      protected: false

recipe:
  # Previous step would populate raw_export stage
  - # OPT - Human-readable step description
    # Default value: "Unnamed select_columns step"
    step_description: "Keep only essential business columns"
    # REQ - Must be "select_columns" for this processor type
    processor_type: "select_columns"
    # REQ - Stage to read data from (must be declared in settings.stages)
    source_stage: "stg_raw_export"
    # REQ - List of columns to keep (in this exact order)
    columns_to_keep: ["Customer_ID", "Product_Name", "Price", "Quantity", "Status"]
    # REQ - Stage to save selected columns
    save_to_stage: "stg_essential_data"
```

### reordering

Reorder columns by specifying them in desired sequence

```yaml
# Column reordering example

settings:
  description: "Reorder columns for better readability and workflow"
  stages:
    - stage_name: "stg_messy_order"
      description: "Data with columns in poor order"
      protected: false
    - stage_name: "stg_ordered_data"
      description: "Columns in logical business order"
      protected: false

recipe:
  - # OPT - Human-readable step description
    step_description: "Reorder columns for business logic flow"
    # REQ - Must be "select_columns" for this processor type
    processor_type: "select_columns"
    # REQ - Stage to read data from
    source_stage: "stg_messy_order"
    # REQ - Columns in desired order (Price first, then ID, then details)
    columns_to_keep: ["Price", "Customer_ID", "Product_Name", "Description", "Category"]
    # OPT - Allow duplicate columns if same column listed multiple times
    # Default value: true
    allow_duplicates: true
    # REQ - Stage to save reordered data
    save_to_stage: "stg_ordered_data"
```

### flexible selection

Flexible selection with error handling for missing columns

```yaml
# Flexible selection with missing column handling

settings:
  description: "Select columns that may or may not exist in import data"
  stages:
    - stage_name: "stg_variable_import"
      description: "Import data with varying column structure"
      protected: false
    - stage_name: "stg_standardized_output"
      description: "Standardized output with available columns"
      protected: false

recipe:
  - # OPT - Human-readable step description
    step_description: "Select standard columns, skip missing ones"
    # REQ - Must be "select_columns" for this processor type
    processor_type: "select_columns"
    # REQ - Stage to read data from
    source_stage: "stg_variable_import"
    # REQ - Preferred columns (some may not exist in all imports)
    columns_to_keep: ["ID", "Name", "Email", "Phone", "Address", "Notes", "Created_Date"]
    # OPT - Don't fail if some columns are missing, just skip them
    # Default value: true (fail on missing)
    strict_mode: false
    # OPT - No duplicates needed for this case
    # Default value: true
    allow_duplicates: false
    # REQ - Stage to save available columns
    save_to_stage: "stg_standardized_output"
```

### exclusion

Drop unwanted columns while keeping everything else

```yaml
# Column exclusion example

settings:
  description: "Remove internal/temporary columns from final export"
  stages:
    - stage_name: "stg_working_data"
      description: "Data with internal working columns"
      protected: false
    - stage_name: "stg_clean_export"
      description: "Clean data ready for external use"
      protected: false

recipe:
  - # OPT - Human-readable step description
    step_description: "Remove internal columns before export"
    # REQ - Must be "select_columns" for this processor type
    processor_type: "select_columns"
    # REQ - Stage to read data from
    source_stage: "stg_working_data"
    # REQ - List of columns to remove (everything else is kept)
    columns_to_drop: ["_temp_calc", "_internal_id", "debug_info", "processing_notes"]
    # OPT - Fail if drop columns don't exist vs skip missing ones
    # Default value: true (fail on missing)
    strict_mode: false
    # REQ - Stage to save cleaned data
    save_to_stage: "stg_clean_export"
```

### duplication

Duplicate columns for comparison or special formatting

```yaml
# Column duplication example

settings:
  description: "Duplicate price column for comparison analysis"
  stages:
    - stage_name: "stg_pricing_data"
      description: "Product pricing information"
      protected: false
    - stage_name: "stg_comparison_format"
      description: "Data formatted for price comparison"
      protected: false

recipe:
  - # OPT - Human-readable step description
    step_description: "Create comparison format with duplicate price columns"
    # REQ - Must be "select_columns" for this processor type
    processor_type: "select_columns"
    # REQ - Stage to read data from
    source_stage: "stg_pricing_data"
    # REQ - Include Price column multiple times for comparison
    # The same column can appear multiple times in the list
    columns_to_keep: ["Product_ID", "Product_Name", "Price", "Price", "Category"]
    # OPT - Allow duplicate columns (required for this to work)
    # Default value: true
    allow_duplicates: true
    # REQ - Stage to save duplicated format
    save_to_stage: "stg_comparison_format"
```

### column creation

Create new empty columns alongside existing ones

```yaml
# Column creation example

settings:
  description: "Add template columns to existing data for future data entry"
  stages:
    - stage_name: "stg_basic_data"
      description: "Basic imported data"
      protected: false
    - stage_name: "stg_template_ready"
      description: "Data with additional empty columns for manual entry"
      protected: false

recipe:
  - # OPT - Human-readable step description
    step_description: "Add empty columns for manual data entry"
    # REQ - Must be "select_columns" for this processor type
    processor_type: "select_columns"
    # REQ - Stage to read data from
    source_stage: "stg_basic_data"
    # REQ - Include existing columns plus new ones to be created
    columns_to_keep: ["Customer_ID", "Product_Name", "Price", "Notes", "Follow_Up_Date", "Status_Override"]
    # REQ - Explicitly declare which columns should be created (not missing/typos)
    columns_to_create: ["Notes", "Follow_Up_Date", "Status_Override"]
    # OPT - Default value for created columns
    # Default value: pd.NA
    default_value: ""
    # OPT - Still catch typos in existing column names
    # Default value: true
    strict_mode: true
    # REQ - Stage to save template-ready data
    save_to_stage: "stg_template_ready"
```

## Parameter notes

- `processor_type` (required): Must be 'select_columns' for this processor type
- `step_description` (default `Unnamed select_columns step`): Human-readable description of what this selection does
- `source_stage` (default `None`): Stage to read data from (must be declared in settings.stages). If omitted, uses current pipeline data
- `save_to_stage` (default `None`): Stage to save selected data (must be declared in settings.stages)
- `columns_to_keep` (default `None`): List of columns to keep by name (string) or position (1-based integer). Cannot use with columns_to_drop
- `columns_to_drop` (default `None`): List of column names to exclude from result. Cannot use with columns_to_keep
- `allow_duplicates` (default `True`): Whether to allow the same column name multiple times in columns_to_keep
- `strict_mode` (default `True`): Whether to fail when specified columns don't exist, or skip missing columns
- `columns_to_create` (default `[]`): List of column names to create if they don't exist in source data. Can only be used with columns_to_keep
- `default_value` (default `pd.NA`): Default value to use when creating new columns

