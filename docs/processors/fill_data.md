# `fill_data`

**Family:** `transform`

Fill missing values using Excel-like fill strategies

## Keys

Generated from the declared schema; keys not listed are refused at recipe load.

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `source_stage`: stage_in; REQUIRED - Stage to read
- `save_to_stage`: stage_out; REQUIRED - Stage to write
- `confirm_stage_replacement`: bool; default false - Required true to overwrite an existing stage
- `columns`: list of str; REQUIRED - Column names
- `fill_method`: str; REQUIRED
- `fill_value`: any
- `source_column`: str
- `old_value`: any
- `limit`: int
- `inplace`: bool; default false
- `conditions`: list_of_mappings
  - `condition_column`: str; REQUIRED
  - `condition_type`: str; REQUIRED
  - `condition_value`: any
  - `fill_value`: any

## Examples

Every step below validates against the schema (tests/test_examples_validate_against_schemas.py).

### basic

Simple constant fill for missing customer information

```yaml
settings:
  description: "Clean customer data by filling missing values"
  stages:
    - stage_name: "stg_customer_data"
      description: "Raw customer data with missing values"
      protected: false
    - stage_name: "stg_cleaned_customers"
      description: "Customer data with filled missing values"
      protected: false

recipe:
  - # Step 1: Import customer data
    step_description: "Import customer database"
    # REQ - Must be "import_file" for file import
    processor_type: "import_file"
    # REQ - Path to input Excel file
    input_file: "data/customer_database.xlsx"
    # REQ - Stage to save imported data (import_file only needs save_to_stage)
    save_to_stage: "stg_customer_data"

  - # Step 2: Fill missing customer names
    # OPT - Human-readable step description
    # Default value: "Unnamed fill_data step"
    step_description: "Fill missing customer names with Unknown"
    # REQ - Must be "fill_data" for this processor type
    processor_type: "fill_data"
    # REQ - Stage to read data from (must be declared in settings.stages)
    source_stage: "stg_customer_data"
    # REQ - Columns to fill missing values in
    # Can be single column string or list of columns
    columns: ["Customer_Name", "Contact_Person"]
    # REQ - Method to use for filling missing values
    # Valid values: constant, forward_fill, backward_fill, mean, median, mode, interpolate, replace, zero, empty_string
    fill_method: "constant"
    # REQ - Value to fill with (required for constant and replace methods)
    fill_value: "Unknown"
    # REQ - Stage to save filled data
    save_to_stage: "stg_cleaned_customers"
```

### statistical fill

Use statistical methods to fill numeric and categorical data

```yaml
settings:
  description: "Fill missing values using statistical methods for better data quality"
  stages:
    - stage_name: "stg_sales_data"
      description: "Sales data with missing values"
      protected: false
    - stage_name: "stg_amounts_filled"
      description: "Sales data with amounts filled using mean"
      protected: false
    - stage_name: "stg_categories_filled"
      description: "Sales data with categories filled using mode"
      protected: false

recipe:
  - # Step 1: Import sales data
    step_description: "Import monthly sales report"
    processor_type: "import_file"
    input_file: "data/sales_march_2024.xlsx"
    save_to_stage: "stg_sales_data"

  - # Step 2: Fill missing sales amounts with average
    step_description: "Fill missing sales amounts with mean value"
    processor_type: "fill_data"
    source_stage: "stg_sales_data"
    # REQ - Numeric columns for statistical filling
    columns: ["Sales_Amount", "Commission", "Discount"]
    # REQ - Use mean for numeric data
    # Calculates average of existing values and fills missing with that
    fill_method: "mean"
    save_to_stage: "stg_amounts_filled"

  - # Step 3: Fill missing categories with most common value
    step_description: "Fill missing customer types with most common type"
    processor_type: "fill_data"
    source_stage: "stg_amounts_filled"
    # REQ - Categorical columns for mode filling
    columns: ["Customer_Type", "Sales_Region"]
    # REQ - Use mode for categorical data
    # Finds most frequently occurring value and fills missing with that
    fill_method: "mode"
    save_to_stage: "stg_categories_filled"
```

### forward backward fill

Excel-equivalent forward fill (fill down) and backward fill (fill up)

```yaml
settings:
  description: "Use Excel-style fill operations for time series and sequential data"
  stages:
    - stage_name: "stg_time_series_data"
      description: "Time series data with gaps"
      protected: false
    - stage_name: "stg_forward_filled"
      description: "Data with forward fill applied"
      protected: false
    - stage_name: "stg_backward_filled"
      description: "Data with backward fill applied"
      protected: false

recipe:
  - # Step 1: Import time series data
    step_description: "Import sensor readings with missing values"
    processor_type: "import_file"
    input_file: "data/sensor_readings.xlsx"
    save_to_stage: "stg_time_series_data"

  - # Step 2: Forward fill (Excel Fill Down equivalent)
    step_description: "Carry forward last known status values"
    processor_type: "fill_data"
    source_stage: "stg_time_series_data"
    # REQ - Columns suitable for forward filling
    columns: ["Equipment_Status", "Operator_Name"]
    # REQ - Forward fill method (same as Excel Fill Down)
    # Copies the last known value forward to fill gaps
    fill_method: "forward_fill"
    # OPT - Limit consecutive fills to prevent over-filling
    # Default value: None (no limit)
    # Maximum number of consecutive missing values to fill
    limit: 5
    save_to_stage: "stg_forward_filled"

  - # Step 3: Backward fill (Excel Fill Up equivalent)
    step_description: "Fill maintenance dates backward from next known date"
    processor_type: "fill_data"
    source_stage: "stg_forward_filled"
    # REQ - Columns suitable for backward filling
    columns: ["Next_Maintenance_Date"]
    # REQ - Backward fill method (same as Excel Fill Up)
    # Copies next known value backward to fill gaps
    fill_method: "backward_fill"
    # OPT - Limit consecutive fills
    limit: 3
    save_to_stage: "stg_backward_filled"
```

### conditional fill

Fill values based on conditions in other columns

```yaml
settings:
  description: "Apply business rules for filling missing values based on context"
  stages:
    - stage_name: "stg_employee_data"
      description: "Employee data with missing information"
      protected: false
    - stage_name: "stg_salary_filled"
      description: "Employee data with department-based salary defaults"
      protected: false
    - stage_name: "stg_benefits_filled"
      description: "Employee data with region-based benefit defaults"
      protected: false

recipe:
  - # Step 1: Import employee data
    step_description: "Import HR employee database"
    processor_type: "import_file"
    input_file: "data/employee_database.xlsx"
    save_to_stage: "stg_employee_data"

  - # Step 2: Fill missing salaries based on department
    step_description: "Set department-specific salary defaults"
    processor_type: "fill_data"
    source_stage: "stg_employee_data"
    # REQ - Column to fill
    columns: ["Starting_Salary"]
    # REQ - Constant fill method for conditional filling
    fill_method: "constant"
    # OPT - Conditional filling rules based on other column values
    # Default value: [] (no conditions, fill all missing values)
    conditions:
      # First condition: Sales department
      - # REQ - Column to check for condition
        condition_column: "Department"
        # REQ - Type of condition to apply
        # Valid values: equals, not_equals, greater_than, less_than, contains, not_contains, is_null, not_null, in_list, not_in_list
        condition_type: "equals"
        # REQ - Value to compare against
        condition_value: "Sales"
        # REQ - Value to fill with when condition is met
        fill_value: 45000
      
      # Second condition: Engineering department
      - condition_column: "Department"
        condition_type: "equals"
        condition_value: "Engineering"
        fill_value: 65000
      
      # Third condition: Marketing department
      - condition_column: "Department"
        condition_type: "equals"
        condition_value: "Marketing"
        fill_value: 50000
    save_to_stage: "stg_salary_filled"

  - # Step 3: Fill missing benefits based on region
    step_description: "Set region-specific benefit defaults"
    processor_type: "fill_data"
    source_stage: "stg_salary_filled"
    columns: ["Health_Plan", "Retirement_Match"]
    fill_method: "constant"
    conditions:
      # High-cost regions get premium benefits
      - condition_column: "Work_Location"
        condition_type: "in_list"
        condition_value: ["San Francisco", "New York", "Boston"]
        fill_value: "Premium"
      
      # Other regions get standard benefits
      - condition_column: "Work_Location"
        condition_type: "not_in_list"
        condition_value: ["San Francisco", "New York", "Boston"]
        fill_value: "Standard"
    save_to_stage: "stg_benefits_filled"
```

### advanced workflow

Complex data preparation workflow with multiple fill strategies

```yaml
settings:
  description: "Complete data cleaning workflow with various fill methods and business logic"
  variables:
    default_region: "Other"
    minimum_order: "0"
  stages:
    - stage_name: "stg_raw_orders"
      description: "Raw order data from import"
      protected: false
    - stage_name: "stg_basic_cleanup"
      description: "Orders with basic missing values filled"
      protected: false
    - stage_name: "stg_advanced_fills"
      description: "Orders with statistical and conditional fills"
      protected: false
    - stage_name: "stg_final_dataset"
      description: "Complete cleaned order dataset"
      protected: false

recipe:
  - # Step 1: Import order data
    step_description: "Import quarterly order data"
    processor_type: "import_file"
    input_file: "data/q1_orders.xlsx"
    save_to_stage: "stg_raw_orders"

  - # Step 2: Fill missing quantities with zero
    step_description: "Fill missing quantities and amounts with zero for calculations"
    processor_type: "fill_data"
    source_stage: "stg_raw_orders"
    # REQ - Numeric columns that should default to zero
    columns: ["Quantity", "Backorder_Amount", "Discount_Amount"]
    # REQ - Zero fill method for numeric defaults
    # Sets missing numeric values to 0 for calculations
    fill_method: "zero"
    save_to_stage: "stg_basic_cleanup"

  - # Step 3: Fill missing order amounts with median
    step_description: "Fill missing order amounts with median value"
    processor_type: "fill_data"
    source_stage: "stg_basic_cleanup"
    columns: ["Order_Amount"]
    # REQ - Median fill for robust statistical filling
    # Less sensitive to outliers than mean
    fill_method: "median"
    save_to_stage: "stg_advanced_fills"

  - # Step 4: Interpolate missing dates
    step_description: "Interpolate missing ship dates in time series"
    processor_type: "fill_data"
    source_stage: "stg_advanced_fills"
    columns: ["Ship_Date", "Expected_Delivery"]
    # REQ - Interpolate method for smooth progression
    # Creates gradual progression between known values
    fill_method: "interpolate"
    # OPT - Limit interpolation to prevent unrealistic fills
    limit: 10
    save_to_stage: "stg_final_dataset"

  - # Step 5: Replace specific values
    step_description: "Replace outdated status codes with current ones"
    processor_type: "fill_data"
    source_stage: "stg_final_dataset"
    columns: ["Order_Status"]
    # REQ - Replace method for value substitution
    # Replaces specific old values with new ones
    fill_method: "replace"
    # REQ - Old value to find and replace (required for replace method)
    old_value: "PROC"
    # REQ - New value to replace with
    fill_value: "Processing"
    save_to_stage: "stg_final_dataset"
```

### specialized methods

Specialized fill methods for specific data types and use cases

```yaml
settings:
  description: "Demonstrate specialized fill methods for different data scenarios"
  stages:
    - stage_name: "stg_mixed_data"
      description: "Dataset with various data types"
      protected: false
    - stage_name: "stg_text_filled"
      description: "Data with text fields filled"
      protected: false
    - stage_name: "stg_numeric_filled"
      description: "Data with numeric fields filled"
      protected: false

recipe:
  - # Step 1: Import mixed data types
    step_description: "Import dataset with various data types"
    processor_type: "import_file"
    input_file: "data/mixed_data_types.xlsx"
    save_to_stage: "stg_mixed_data"

  - # Step 2: Fill text fields with empty strings
    step_description: "Fill missing comments and notes with empty strings"
    processor_type: "fill_data"
    source_stage: "stg_mixed_data"
    # REQ - Text columns that should be empty rather than null
    columns: ["Comments", "Notes", "Additional_Info"]
    # REQ - Empty string fill method
    # Sets missing text values to empty string instead of null
    fill_method: "empty_string"
    save_to_stage: "stg_text_filled"

  - # Step 3: Fill missing measurements with interpolation
    step_description: "Interpolate missing sensor measurements"
    processor_type: "fill_data"
    source_stage: "stg_text_filled"
    # REQ - Numeric measurement columns
    columns: ["Temperature", "Pressure", "Flow_Rate"]
    # REQ - Interpolate for smooth measurement progression
    fill_method: "interpolate"
    save_to_stage: "stg_numeric_filled"

  - # Step 4: Fill missing IDs with forward fill
    step_description: "Carry forward batch IDs through related records"
    processor_type: "fill_data"
    source_stage: "stg_numeric_filled"
    columns: ["Batch_ID", "Operator_ID"]
    fill_method: "forward_fill"
    # OPT - Prevent excessive forward filling
    limit: 20
    save_to_stage: "stg_final_clean_data"
```

## Parameter notes

- `processor_type` (required): Must be 'fill_data' for this processor type
- `step_description` (default `Unnamed fill_data step`): Human-readable description of what this fill operation does
- `source_stage` (required): Stage to read data from (must be declared in settings.stages)
- `save_to_stage` (required): Stage to save filled data (must be declared in settings.stages)
- `columns` (required): Column(s) to fill missing values in. Can be single column name or list of column names
- `fill_method` (required): Method to use for filling missing values
- `fill_value` (default `None`): Value to fill with (required for 'constant' and 'replace' methods)
- `old_value` (default `None`): Value to find and replace (required for 'replace' method only)
- `limit` (default `None`): Maximum number of consecutive missing values to fill (prevents over-filling)
- `conditions` (default `[]`): Conditional filling rules based on values in other columns

