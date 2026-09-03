# `slice_data`

**Family:** `transform`

Extract portions of DataFrames with flexible slicing options

## Notes

- **indexing**: 1-based for user configuration (converted internally)
- **transpose**: slice_type: transpose turns the label column's values into the header row and the old headers into a new first column (Excel Paste Special > Transpose semantics); fails loud on duplicate or blank labels

## Keys

Generated from the declared schema; keys not listed are refused at recipe load.

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `source_stage`: stage_in; REQUIRED - Stage to read
- `save_to_stage`: stage_out; REQUIRED - Stage to write
- `confirm_stage_replacement`: bool; default false - Required true to overwrite an existing stage
- `slice_type`: str; REQUIRED; one of row_range, column_range, transpose
- `slice_result_contains_headers`: bool; default false
- `header_column`: str
- `old_headers_column_name`: str; default "Field"
- when `slice_type` = `row_range`:
  - `start_row`: int; default 1
  - `end_row`: int
- when `slice_type` = `column_range`:
  - `start_col`: any
  - `end_col`: any
- when `slice_type` = `transpose`:

## Examples

Every step below validates against the schema (tests/test_examples_validate_against_schemas.py).

### basic

Extract specific rows from a DataFrame

```yaml
settings:
  description: "Import Excel file and extract metadata rows"
  stages:
    - stage_name: "stg_raw_data"
      description: "Complete imported Excel file"
      protected: false
    - stage_name: "stg_profiled_files"
      description: "Header information from Excel file"
      protected: false

recipe:
  - # OPT - Human-readable step description
    step_description: "Import Excel file"
    # REQ - Must be "import_file" for file import
    processor_type: "import_file"
    # REQ - Path to input Excel file
    input_file: "data/monthly_report.xlsx"
    # REQ - Stage to save imported data (import_file only needs save_to_stage)
    save_to_stage: "stg_raw_data"

  - # OPT - Human-readable step description
    # Default value: "Unnamed slice_data step"
    step_description: "Extract file metadata section"
    # REQ - Must be "slice_data" for this processor type
    processor_type: "slice_data"
    # REQ - Stage to read data from (must be declared in settings.stages)
    source_stage: "stg_raw_data"
    # REQ - Type of slice operation to perform
    # Valid values: "row_range", "column_range"
    slice_type: "row_range"
    # REQ - Starting row number (1-based indexing, matches Excel)
    # Row 1 is the first row in the data
    start_row: 1
    # OPT - Ending row number (1-based, inclusive)
    # If omitted, extracts from start_row to end of data
    end_row: 3
    # REQ - Stage to save extracted rows
    save_to_stage: "stg_profiled_files"
```

### data extraction

Extract data section with automatic header promotion

```yaml
settings:
  description: "Import file and extract clean data with proper column headers"
  stages:
    - stage_name: "stg_imported_file"
      description: "Complete imported Excel file"
      protected: false
    - stage_name: "stg_clean_data"
      description: "Data section with promoted headers"
      protected: false

recipe:
  - # OPT - Human-readable step description
    step_description: "Import Excel file with mixed content"
    # REQ - Must be "import_file" for file import
    processor_type: "import_file"
    # REQ - Path to input Excel file
    input_file: "data/sales_report.xlsx"
    # REQ - Stage to save imported data (import_file only needs save_to_stage)
    save_to_stage: "stg_imported_file"

  - # OPT - Human-readable step description
    step_description: "Extract data with header promotion"
    # REQ - Must be "slice_data" for this processor type
    processor_type: "slice_data"
    # REQ - Stage to read data from (must be declared in settings.stages)
    source_stage: "stg_imported_file"
    # REQ - Type of slice operation
    slice_type: "row_range"
    # REQ - Row where data section starts (1-based)
    # This should be the row containing column headers
    start_row: 4
    # OPT - Ending row (1-based, inclusive)
    # Omit to get all remaining rows from start_row
    end_row: 10
    # OPT - Promote first row of slice to column headers
    # Default value: false
    # When true, first row becomes DataFrame column names
    slice_result_contains_headers: true
    # REQ - Stage to save the clean data
    save_to_stage: "stg_clean_data"
```

### column slicing

Extract specific columns using different reference methods

```yaml
settings:
  description: "Import wide dataset and extract key columns"
  stages:
    - stage_name: "stg_wide_dataset"
      description: "Complete imported dataset with many columns"
      protected: false
    - stage_name: "stg_key_columns"
      description: "Important columns only"
      protected: false
    - stage_name: "stg_first_three_columns"
      description: "First three columns by number"
      protected: false

recipe:
  - # OPT - Human-readable step description
    step_description: "Import wide Excel dataset"
    # REQ - Must be "import_file" for file import
    processor_type: "import_file"
    # REQ - Path to input Excel file
    input_file: "data/wide_dataset.xlsx"
    # REQ - Stage to save imported data (import_file only needs save_to_stage)
    save_to_stage: "stg_wide_dataset"

  - # OPT - Human-readable step description
    step_description: "Extract key columns using Excel references"
    # REQ - Must be "slice_data" for this processor type
    processor_type: "slice_data"
    # REQ - Stage to read data from (must be declared in settings.stages)
    source_stage: "stg_wide_dataset"
    # REQ - Type of slice operation
    slice_type: "column_range"
    # REQ - Starting column reference (Excel letter format)
    # Valid formats: "A", "B", "C", etc.
    start_col: "A"
    # OPT - Ending column reference (Excel letter format)
    # If omitted, extracts from start_col to last column
    end_col: "D"
    # REQ - Stage to save column subset
    save_to_stage: "stg_key_columns"

  - # Alternative example using 1-based column numbers
    step_description: "Extract first 3 columns by number"
    processor_type: "slice_data"
    # REQ - Stage to read data from (must be declared in settings.stages)
    source_stage: "stg_wide_dataset"
    slice_type: "column_range"
    # REQ - Starting column number (1-based indexing)
    # Column 1 is the first column (Excel column A)
    start_col: 1
    # OPT - Ending column number (1-based, inclusive)
    end_col: 3
    # REQ - Stage to save column subset
    save_to_stage: "stg_first_three_columns"
```

### stage workflow

Process data from saved stage instead of pipeline input

```yaml
settings:
  description: "Multi-step workflow using stage-based processing"
  stages:
    - stage_name: "stg_raw_excel_data"
      description: "Complete imported Excel file"
      protected: false
    - stage_name: "stg_extracted_metadata"
      description: "File metadata section"
      protected: false
    - stage_name: "stg_clean_dataset"
      description: "Data section with headers"
      protected: false

recipe:
  - # Step 1: Import the entire Excel file
    step_description: "Import complete Excel file"
    processor_type: "import_file"
    # REQ - Path to input file
    input_file: "reports/monthly_data.xlsx"
    # REQ - Stage to save imported data
    save_to_stage: "stg_raw_excel_data"

  - # Step 2: Extract metadata from saved stage
    step_description: "Extract file metadata from saved data"
    processor_type: "slice_data"
    slice_type: "row_range"
    # REQ - Load data from this stage instead of pipeline input
    # Stage must exist and contain data
    source_stage: "stg_raw_excel_data"
    start_row: 1
    end_row: 2
    save_to_stage: "stg_extracted_metadata"

  - # Step 3: Extract data section from same saved stage
    step_description: "Extract data with headers from saved data"
    processor_type: "slice_data"
    slice_type: "row_range"
    source_stage: "stg_raw_excel_data"
    start_row: 4
    slice_result_contains_headers: true
    save_to_stage: "stg_clean_dataset"
```

### advanced

Complex workflow with column name references and validation

```yaml
settings:
  description: "Advanced slicing with column name references and data validation"
  variables:
    report_date: "2024-03"
    department: "sales"
  stages:
    - stage_name: "stg_imported_report"
      description: "Raw monthly sales report"
      protected: false
    - stage_name: "stg_core_data"
      description: "Essential columns for analysis"
      protected: false
    - stage_name: "stg_validation_data"
      description: "Data subset for quality checks"
      protected: false

recipe:
  - # Import with variable substitution
    step_description: "Import monthly sales report"
    processor_type: "import_file"
    # REQ - Input file path with variable support
    # Variables: {report_date}, {department} defined in settings
    input_file: "reports/{department}_{report_date}.xlsx"
    save_to_stage: "stg_imported_report"

  - # Extract core columns by name reference
    step_description: "Extract essential columns by name"
    processor_type: "slice_data"
    slice_type: "column_range"
    source_stage: "stg_imported_report"
    # REQ - Starting column name (exact match required)
    # Must match existing column header exactly
    start_col: "Product ID"
    # OPT - Ending column name (exact match required)
    end_col: "Total Sales"
    save_to_stage: "stg_core_data"

  - # Extract validation subset
    step_description: "Extract data for quality validation"
    processor_type: "slice_data"
    slice_type: "row_range"
    source_stage: "stg_core_data"
    start_row: 1
    # OPT - Limit to first 100 rows for validation
    end_row: 100
    slice_result_contains_headers: true
    save_to_stage: "stg_validation_data"
```

### transpose

Turn a metrics-down/months-across table into months-down/metrics-across

```yaml
settings:
  description: "Transpose the metrics table for month-per-row processing"
  stages:
    - stage_name: "stg_metrics_wide"
      description: "Metric per row, one column per month"
      protected: false
    - stage_name: "stg_metrics_by_month"
      description: "Month per row, one column per metric"
      protected: false

recipe:
  - # Headers-aware, like Excel's Paste Special > Transpose on a table
    # with row labels: the label column's VALUES become the new header
    # row, and the old headers become a new first column. Fails loud if
    # the label column has duplicate or blank values - either would
    # produce unaddressable or colliding column names downstream.
    # Mixed-type rows come back as object columns; re-coerce with
    # clean_data where dtypes matter.
    step_description: "Transpose metrics table to month-per-row"
    processor_type: "slice_data"
    source_stage: "stg_metrics_wide"
    slice_type: "transpose"
    # OPT - Column whose values become the new headers
    # Default value: the first column
    header_column: "Metric"
    # OPT - Name for the new first column holding the previous headers.
    # The source has no name for what its own headers represent, so say
    # what you mean. Default value: "Field"
    old_headers_column_name: "Month"
    save_to_stage: "stg_metrics_by_month"
```

## Parameter notes

- `processor_type` (required): Must be 'slice_data' for this processor type
- `step_description` (default `Unnamed slice_data step`): Human-readable description of what this step does
- `slice_type` (required): Type of slicing operation to perform
- `start_row` (required): Starting row number using 1-based indexing (row 1 = first row)
- `end_row` (default `None`): Ending row number (1-based, inclusive). If omitted, extracts to end of data
- `start_col` (required): Starting column reference. Supports Excel letters (A, B), 1-based numbers (1, 2), or exact column names
- `end_col` (default `None`): Ending column reference. Same format as start_col. If omitted, extracts to last column
- `slice_result_contains_headers` (default `False`): When true, promotes first row of slice result to DataFrame column headers
- `source_stage` (default `None`): Load data from this saved stage instead of pipeline input. Stage must exist and contain data
- `save_to_stage` (default `None`): Save slice results to this stage name for later use in pipeline

