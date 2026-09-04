# `import_file`

**Family:** `import`

Import Excel, CSV, or TSV files into stages, with sheet selection

## Notes

- **fast excel reader**: when the optional python-calamine wheel is installed, Excel imports use it automatically (several times faster on large files, values identical); without it the openpyxl path serves unchanged - no configuration
- **header row**: 1-based row holding the column headers (default 1); rows above it are discarded
- **missing file policy**: on_missing_file: 'error' (default) or 'create_empty' with declared create_empty_columns for fail-safe imports of files that may not exist yet

## Keys

Generated from the declared schema; keys not listed are refused at recipe load.

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `save_to_stage`: stage_out; REQUIRED - Stage to write
- `confirm_stage_replacement`: bool; default false - Required true to overwrite an existing stage
- `input_file`: str; REQUIRED
- `sheet_name`: any; default "?sheet_001?" - Tab name, 1-based number, or ?sheet_NNN? token
- `header_row`: int; default 1
- `encoding`: str; default "utf-8"
- `separator`: str; default ","
- `format`: str; one of xlsx, xls, csv, tsv
- `verbatim_text_columns`: list of str - Column names
- `on_missing_file`: str; default "error"; one of error, create_empty
- when `on_missing_file` = `error`:
- when `on_missing_file` = `create_empty`:
  - `create_empty_columns`: list of str; REQUIRED - Column names

## Examples

Every step below validates against the schema (tests/test_examples_validate_against_schemas.py).

### basic

Simple file import saving to a stage

```yaml
# Complete recipe with import_file processor

settings:
  description: "Simple Excel file import saving customer data to a stage"
  stages:
    - stage_name: "stg_customer_data"
      description: "Imported customer data from Excel"
      protected: false

recipe:
  - # OPT - Step description
    step_description: "Import customer data from Excel"
    # REQ - Processor type
    processor_type: "import_file"
    # REQ - File path (supports variable substitution)
    input_file: "customer_data.xlsx"
    # REQ - Stage to save imported data
    save_to_stage: "stg_customer_data"
```

### excel sheet

Import specific sheet from multi-sheet Excel file

```yaml
# Import specific sheet from Excel workbook

settings:
  description: "Import specific sheet from multi-sheet Excel workbook"
  stages:
    - stage_name: "stg_q4_sales"
      description: "Q4 sales data from specific sheet"
      protected: false

recipe:
  - # OPT - Step description
    step_description: "Import quarterly sales from specific sheet"
    # REQ - Processor type
    processor_type: "import_file"
    # REQ - Excel file path
    input_file: "reports/quarterly_sales.xlsx"
    # REQ - Stage to save imported data
    save_to_stage: "stg_q4_sales"
    # OPT - Sheet name or index to import
    # Can be sheet name (string) or sheet index (integer, 1-based)
    # Default value: 1 (first sheet)
    sheet_name: "Q4 Sales Data"
```

### csv import

Import CSV file with custom encoding and separator

```yaml
# Import CSV file with custom formatting options

settings:
  description: "Import CSV file with custom encoding and separator options"
  stages:
    - stage_name: "stg_system_data"
      description: "System data imported from CSV"
      protected: false

recipe:
  - # OPT - Step description
    step_description: "Import CSV data with custom separator"
    # REQ - Processor type
    processor_type: "import_file"
    # REQ - CSV file path
    input_file: "exports/system_data.csv"
    # REQ - Stage to save imported data
    save_to_stage: "stg_system_data"
    # OPT - Text encoding for CSV/TSV files (ignored for Excel files)
    # Valid examples: "utf-8", "latin1", "cp1252", "ascii"
    # Default value: "utf-8"
    encoding: "utf-8"
    # OPT - Column separator for CSV files
    # Valid examples: ",", ";", "|", "\t"
    # Default value: ","
    separator: ";"
```

### variable substitution

Import with dynamic file paths using variables

```yaml
# Import file with variable substitution in path

settings:
  description: "Import daily data files using dynamic variable-based file paths"
  variables:
    department: "sales"
  stages:
    - stage_name: "stg_daily_data"
      description: "Daily data imported with variables"
      protected: false

recipe:
  - # OPT - Step description
    step_description: "Import daily data using date variables"
    # REQ - Processor type
    processor_type: "import_file"
    # REQ - File path with variable substitution
    # Built-in date variables: {date}, {timestamp}, {YYYY}, {MM}, {DD}, {YY}, {MMDD}
    # Custom variables: {department}, {batch_id} - defined in recipe settings or CLI
    # Variable examples: department="sales", batch_id="B001"
    input_file: "daily_data/{department}_{YYYY}{MM}{DD}.xlsx"
    # REQ - Stage to save imported data
    save_to_stage: "stg_daily_data"
    # OPT - Import specific sheet by index (starts at 1, converts to 0-based internally)
    sheet_name: 1
```

### multi import workflow

Complete workflow importing multiple files to different stages

```yaml
# Import multiple files saving each to different stages for complex processing

settings:
  description: "Complete workflow importing multiple data sources to separate stages for complex processing"
  variables:
    processing_date: "20250729"
  stages:
    - stage_name: "stg_customer_master"
      description: "Master customer data"
      protected: false
    - stage_name: "stg_product_catalog"
      description: "Current product catalog"
      protected: false
    - stage_name: "stg_orders_data"
      description: "Daily orders for processing"
      protected: false

recipe:
  # Import customer master data
  - step_description: "Import customer master data"
    processor_type: "import_file"
    input_file: "master/customers.xlsx"
    save_to_stage: "stg_customer_master"
    
  # Import product catalog
  - step_description: "Import product catalog"
    processor_type: "import_file"
    input_file: "master/products.xlsx"
    save_to_stage: "stg_product_catalog"
    
  # Import daily orders with variable substitution
  - step_description: "Import daily orders"
    processor_type: "import_file"
    input_file: "daily/orders_{processing_date}.xlsx"
    save_to_stage: "stg_orders_data"
```

### verbatim text

Protect literal N/A-style text in designated columns

```yaml
# pandas normally coerces strings like "N/A", "NA", "NULL" to missing
# values at import - reasonable for numbers, wrong for a text column
# where "N/A" is something a person deliberately typed and a filter
# needs to match. Listing a column here keeps its text verbatim, while
# a genuinely EMPTY cell still imports as missing, so blank and "N/A"
# stay distinguishable. Every other column behaves exactly as a normal
# import. Use this for any column whose literal N/A-like entries matter.

settings:
  description: "Import with a protected text column"
  stages:
    - stage_name: "stg_import_raw"
      description: "Download with Customer Ref # text preserved"
      protected: false

recipe:
  - step_description: "Import, keeping ref text verbatim"
    processor_type: "import_file"
    input_file: "download.xlsx"
    sheet_name: "Van Detail Report"
    # OPT - Columns whose literal text must survive import
    verbatim_text_columns: ["Customer Ref #"]
    save_to_stage: "stg_import_raw"
```

## Parameter notes

- `input_file` (required): Path to input file (Excel, CSV, or TSV)
- `save_to_stage` (required): Stage name to save imported data (must be declared in settings.stages)
- `sheet_name` (default `1`): Sheet name or index to import from Excel files
- `encoding` (default `utf-8`): Text encoding for CSV/TSV files (ignored for Excel files)
- `separator` (default `,`): Column separator for CSV files
- `format`: Explicit format override (usually auto-detected from extension)

