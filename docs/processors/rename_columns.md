# `rename_columns`

**Family:** `transform`

Rename DataFrame columns with flexible transformation options

## Keys

Generated from the declared schema; keys not listed are refused at recipe load.

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `source_stage`: stage_in; REQUIRED - Stage to read
- `save_to_stage`: stage_out; REQUIRED - Stage to write
- `confirm_stage_replacement`: bool; default false - Required true to overwrite an existing stage
- `rename_type`: str; default "mapping"; one of mapping, pattern, transform
- `mapping`: open_mapping - old name -> new name
- `pattern`: str
- `replacement`: str; default ""
- `add_prefix`: str
- `add_suffix`: str
- `case_conversion`: str; one of upper, lower, title, snake_case, camel_case
- `replace_spaces`: str
- `strip_characters`: str

## Examples

Every step below validates against the schema (tests/test_examples_validate_against_schemas.py).

### basic

Simple direct mapping to rename specific columns

```yaml
settings:
  description: "Rename columns for business-friendly report output"
  stages:
    - stage_name: "stg_raw_data"
      description: "Data with technical column names"
      protected: false
    - stage_name: "stg_renamed_data"
      description: "Data with business-friendly column names"
      protected: false

recipe:
  - # Step 1: Import data with technical names
    step_description: "Import raw data file"
    processor_type: "import_file"
    input_file: "data/monthly_sales.xlsx"
    save_to_stage: "stg_raw_data"

  - # Step 2: Rename columns for presentation
    # OPT - Human-readable step description
    # Default value: "Unnamed rename_columns step"
    step_description: "Apply business-friendly column names"
    # REQ - Must be "rename_columns" for this processor type
    processor_type: "rename_columns"
    # REQ - Stage to read data from (must be declared in settings.stages)
    source_stage: "stg_raw_data"
    # REQ - Type of renaming operation
    # Valid values: "mapping", "pattern", "transform"
    rename_type: "mapping"
    # REQ - Direct mapping of old names to new names (when rename_type is "mapping")
    mapping:
      "cust_id": "Customer Code"
      "ord_dt": "Order Date"
      "prod_sku": "Product SKU"
      "qty": "Quantity"
      "amt_usd": "Amount (USD)"
      "ship_st": "Shipping Status"
    # REQ - Stage to save renamed data
    save_to_stage: "stg_renamed_data"
```

### pattern rename

Use regex patterns to systematically rename columns

```yaml
settings:
  description: "Clean up column names with pattern-based renaming"
  stages:
    - stage_name: "stg_export_data"
      description: "Data exported from legacy system"
      protected: false
    - stage_name: "stg_cleaned_columns"
      description: "Data with cleaned column names"
      protected: false

recipe:
  - # Step 1: Import legacy system export
    step_description: "Import data with prefixed column names"
    processor_type: "import_file"
    input_file: "exports/legacy_system_export.csv"
    save_to_stage: "stg_export_data"

  - # Step 2: Remove system prefixes using pattern
    step_description: "Remove 'EXPORT_' prefix from all columns"
    processor_type: "rename_columns"
    source_stage: "stg_export_data"
    # REQ - Use pattern-based renaming
    rename_type: "pattern"
    # REQ - Regex pattern to match (when rename_type is "pattern")
    # This example removes "EXPORT_" from the beginning of column names
    pattern: "^EXPORT_"
    # OPT - Replacement string (when rename_type is "pattern")
    # Default value: ""
    replacement: ""
    save_to_stage: "stg_cleaned_prefixes"

  - # Step 3: Clean date column suffixes
    step_description: "Remove date suffixes from column names"
    processor_type: "rename_columns"
    source_stage: "stg_cleaned_prefixes"
    rename_type: "pattern"
    # REQ - Pattern to match date suffixes like "_20240731"
    pattern: "_\\d{8}$"
    replacement: ""
    save_to_stage: "stg_cleaned_columns"
```

### transform

Apply systematic transformations to all column names

```yaml
settings:
  description: "Standardize messy column names from multiple sources"
  stages:
    - stage_name: "stg_merged_data"
      description: "Data merged from multiple sources with inconsistent naming"
      protected: false
    - stage_name: "stg_standardized_columns"
      description: "Data with standardized column names"
      protected: false

recipe:
  - # Step 1: Import merged data with messy columns
    step_description: "Import data with inconsistent column naming"
    processor_type: "import_file"
    input_file: "data/merged_sources.xlsx"
    save_to_stage: "stg_merged_data"

  - # Step 2: Standardize all column names
    step_description: "Apply consistent naming convention to all columns"
    processor_type: "rename_columns"
    source_stage: "stg_merged_data"
    # REQ - Use transformation-based renaming
    rename_type: "transform"
    # OPT - Case conversion to apply
    # Default value: null (no conversion)
    # Valid values: "upper", "lower", "title", "snake_case", "camel_case"
    case_conversion: "snake_case"
    # OPT - Characters to strip from column names
    # Default value: null (no stripping)
    strip_characters: " !@#$%^&*()[]{}|;:'\",.<>?"
    # OPT - Character to replace spaces with
    # Default value: null (no replacement)
    replace_spaces: "_"
    save_to_stage: "stg_standardized_columns"
```

### multi step cleanup

Complex column cleanup using multiple rename steps

```yaml
settings:
  description: "Clean and standardize column names from SQL export"
  variables:
    report_date: "2024-07-31"
  stages:
    - stage_name: "stg_sql_export"
      description: "Raw SQL export with technical column names"
      protected: false
    - stage_name: "stg_technical_cleanup"
      description: "Columns with technical artifacts removed"
      protected: false
    - stage_name: "stg_standardized"
      description: "Columns in standard format"
      protected: false
    - stage_name: "stg_business_names"
      description: "Final business-friendly column names"
      protected: false

recipe:
  - # Step 1: Import SQL export
    step_description: "Import database export file"
    processor_type: "import_file"
    input_file: "exports/database_export_{report_date}.csv"
    save_to_stage: "stg_sql_export"

  - # Step 2: Remove technical prefixes and suffixes
    step_description: "Remove table prefixes from column names"
    processor_type: "rename_columns"
    source_stage: "stg_sql_export"
    rename_type: "pattern"
    # Remove patterns like "TBL_CUSTOMER_" or "VW_ORDER_"
    pattern: "^(TBL_|VW_|SP_)[A-Z]+_"
    replacement: ""
    save_to_stage: "stg_technical_cleanup"

  - # Step 3: Standardize format
    step_description: "Convert to consistent snake_case format"
    processor_type: "rename_columns"
    source_stage: "stg_technical_cleanup"
    rename_type: "transform"
    case_conversion: "snake_case"
    strip_characters: " -"
    save_to_stage: "stg_standardized"

  - # Step 4: Apply business names to key columns
    step_description: "Apply final business-friendly names"
    processor_type: "rename_columns"
    source_stage: "stg_standardized"
    rename_type: "mapping"
    mapping:
      "customer_identifier": "Customer ID"
      "order_timestamp": "Order Date & Time"
      "product_stock_keeping_unit": "Product SKU"
      "total_amount_usd": "Total Amount ($)"
      "shipping_status_code": "Shipping Status"
    save_to_stage: "stg_business_names"
```

### prefix suffix

Add prefixes or suffixes to column names for data source identification

```yaml
settings:
  description: "Tag columns with data source for multi-source analysis"
  stages:
    - stage_name: "stg_salesforce_data"
      description: "Customer data from Salesforce"
      protected: false
    - stage_name: "stg_erp_data"
      description: "Order data from ERP system"
      protected: false
    - stage_name: "stg_tagged_salesforce"
      description: "Salesforce data with source prefix"
      protected: false
    - stage_name: "stg_tagged_erp"
      description: "ERP data with source prefix"
      protected: false

recipe:
  - # Step 1: Import Salesforce data
    step_description: "Import customer data from Salesforce"
    processor_type: "import_file"
    input_file: "integrations/salesforce_customers.csv"
    save_to_stage: "stg_salesforce_data"

  - # Step 2: Import ERP data
    step_description: "Import order data from ERP"
    processor_type: "import_file"
    input_file: "integrations/erp_orders.csv"
    save_to_stage: "stg_erp_data"

  - # Step 3: Tag Salesforce columns
    step_description: "Add SF_ prefix to Salesforce columns"
    processor_type: "rename_columns"
    source_stage: "stg_salesforce_data"
    rename_type: "transform"
    # OPT - Prefix to add to all column names
    # Default value: null (no prefix)
    add_prefix: "SF_"
    # OPT - Also standardize the format
    case_conversion: "upper"
    save_to_stage: "stg_tagged_salesforce"

  - # Step 4: Tag ERP columns
    step_description: "Add ERP_ prefix to ERP columns"
    processor_type: "rename_columns"
    source_stage: "stg_erp_data"
    rename_type: "transform"
    add_prefix: "ERP_"
    case_conversion: "upper"
    # OPT - Suffix to add to all column names
    # Default value: null (no suffix)
    add_suffix: "_FIELD"
    save_to_stage: "stg_tagged_erp"
```

### pivot cleanup

Clean up complex column names after pivot table operations

```yaml
settings:
  description: "Clean hierarchical column names from pivot table"
  stages:
    - stage_name: "stg_pivot_result"
      description: "Pivot table with complex column names"
      protected: false
    - stage_name: "stg_cleaned_pivot"
      description: "Pivot table with clean column names"
      protected: false
    - stage_name: "stg_final_report"
      description: "Report-ready pivot table"
      protected: false

recipe:
  - # Step 1: Create pivot table (creates complex column names)
    step_description: "Generate quarterly sales pivot"
    processor_type: "pivot_table"
    source_stage: "stg_sales_data"
    index: ["Region"]
    columns: ["Quarter", "Product_Category"]
    values: ["Sales_Amount"]
    aggfunc: "sum"
    margins: true
    save_to_stage: "stg_pivot_result"

  - # Step 2: Clean pivot column names
    step_description: "Clean up hierarchical pivot column names"
    processor_type: "rename_columns"
    source_stage: "stg_pivot_result"
    rename_type: "pattern"
    # Pattern to clean names like "('Q1', 'Electronics')" to "Q1_Electronics"
    pattern: "\\('([^']+)',\\s*'([^']+)'\\)"
    replacement: "\\1_\\2"
    save_to_stage: "stg_cleaned_pivot"

  - # Step 3: Apply final presentation names
    step_description: "Apply business-friendly names for report"
    processor_type: "rename_columns"
    source_stage: "stg_cleaned_pivot"
    rename_type: "mapping"
    mapping:
      "Q1_Electronics": "Q1 Electronics Sales"
      "Q1_Clothing": "Q1 Clothing Sales"
      "Q2_Electronics": "Q2 Electronics Sales"
      "Q2_Clothing": "Q2 Clothing Sales"
      "Q3_Electronics": "Q3 Electronics Sales"
      "Q3_Clothing": "Q3 Clothing Sales"
      "Q4_Electronics": "Q4 Electronics Sales"
      "Q4_Clothing": "Q4 Clothing Sales"
      "All": "Grand Total"
    save_to_stage: "stg_final_report"
```

### case conversion

Demonstrate all available case conversion options

```yaml
settings:
  description: "Show different case conversion options for column names"
  stages:
    - stage_name: "stg_mixed_case_data"
      description: "Data with mixed case column names"
      protected: false
    - stage_name: "stg_upper_case"
      description: "All uppercase columns"
      protected: false
    - stage_name: "stg_snake_case"
      description: "Snake case columns"
      protected: false
    - stage_name: "stg_camel_case"
      description: "Camel case columns"
      protected: false

recipe:
  - # Step 1: Import data with mixed case columns
    step_description: "Import data with inconsistent column casing"
    processor_type: "import_file"
    input_file: "data/mixed_case_columns.xlsx"
    save_to_stage: "stg_mixed_case_data"

  - # Step 2: Convert to UPPERCASE (for database export)
    step_description: "Convert all columns to uppercase"
    processor_type: "rename_columns"
    source_stage: "stg_mixed_case_data"
    rename_type: "transform"
    # Convert to: "CUSTOMER_NAME", "ORDER_DATE", etc.
    case_conversion: "upper"
    save_to_stage: "stg_upper_case"

  - # Step 3: Convert to snake_case (for programming)
    step_description: "Convert all columns to snake_case"
    processor_type: "rename_columns"
    source_stage: "stg_mixed_case_data"
    rename_type: "transform"
    # Convert to: "customer_name", "order_date", etc.
    case_conversion: "snake_case"
    save_to_stage: "stg_snake_case"

  - # Step 4: Convert to camelCase (for JSON/API)
    step_description: "Convert all columns to camelCase"
    processor_type: "rename_columns"
    source_stage: "stg_mixed_case_data"
    rename_type: "transform"
    # Convert to: "customerName", "orderDate", etc.
    case_conversion: "camel_case"
    save_to_stage: "stg_camel_case"
```

## Parameter notes

- `processor_type` (required): Must be 'rename_columns' for this processor type
- `step_description` (default `Unnamed rename_columns step`): Human-readable description of what this renaming operation does
- `source_stage` (required): Stage to read data from (must be declared in settings.stages)
- `save_to_stage` (required): Stage to save renamed data (must be declared in settings.stages)
- `rename_type` (required): Type of renaming operation to perform
- `mapping` (default `{}`): Dictionary mapping old column names to new names (required when rename_type is 'mapping')
- `pattern` (default `None`): Regex pattern to match in column names (required when rename_type is 'pattern')
- `replacement` (default ``): Replacement string for pattern matches (used with rename_type 'pattern')
- `case_conversion` (default `None`): Case conversion to apply to all column names (used with rename_type 'transform')
- `add_prefix` (default `None`): Prefix to add to all column names (used with rename_type 'transform')
- `add_suffix` (default `None`): Suffix to add to all column names (used with rename_type 'transform')
- `strip_characters` (default `None`): Characters to remove from beginning/end of column names (used with rename_type 'transform')
- `replace_spaces` (default `None`): Character to replace spaces with in column names (used with rename_type 'transform')

