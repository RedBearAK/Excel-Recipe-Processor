# `aggregate_data`

**Family:** `transform`

Group data and calculate summary statistics

## Keys

Generated from the declared schema; keys not listed are refused at recipe load.

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `source_stage`: stage_in; REQUIRED - Stage to read
- `save_to_stage`: stage_out; REQUIRED - Stage to write
- `confirm_stage_replacement`: bool; default false - Required true to overwrite an existing stage
- `group_by`: list of str - Column names
- `aggregations`: list_of_mappings
  - `column`: str; REQUIRED
  - `function`: str; REQUIRED; one of sum, count, mean, median, min, max, std, var, nunique
  - `output_name`: str - Default: column_function
- `aggregation_source`: mapping
  - `type`: str; REQUIRED; one of file, stage, lookup, table
  - `filename`: str
  - `sheet`: any
  - `format`: str
  - `encoding`: str
  - `separator`: str
  - `stage_name`: stage_in
  - `lookup_stage`: stage_in
  - `lookup_key`: str
  - `data_key`: str
  - `group_by_column`: str
  - `aggregations_column`: str
  - `filter_condition`: any
- `keep_group_columns`: bool; default true
- `reset_index`: bool; default true
- `sort_by_groups`: bool; default true
- at least one of: `aggregations`, `aggregation_source`
- at least one of: `group_by`, `aggregation_source`

## Examples

Every step below validates against the schema (tests/test_examples_validate_against_schemas.py).

### basic

Simple grouping with single aggregation function

```yaml
# Complete recipe with aggregate_data processor

settings:
  description: "Calculate regional sales totals"
  stages:
    - stage_name: "stg_sales_data"
      description: "Raw sales data from import"
      protected: false
    - stage_name: "stg_regional_summary"
      description: "Sales summarized by region"
      protected: false

recipe:
  # Previous step would populate 'sales_data' stage
  - step_description: "Calculate total sales by region"
    # REQ - Must be "aggregate_data" for this processor type
    processor_type: "aggregate_data"
    # REQ - Stage to read data from (must be declared in settings.stages)
    source_stage: "stg_sales_data"
    # REQ - Stage to save aggregated results
    save_to_stage: "stg_regional_summary"
    # REQ - List of columns to group by
    group_by: ["Sales_Region"]
    # REQ - List of aggregation operations to perform
    aggregations:
      - # REQ - Column name to aggregate
        column: "Revenue"
        # REQ - Aggregation function to apply
        function: "sum"
        # OPT - Name for the new aggregated column
        # Default value: "{column}_{function}" (e.g., "Revenue_sum")
        output_name: "Total_Revenue"
```

### multiple aggregations

Multiple grouping columns with different aggregation functions

```yaml
# Complex aggregation with multiple functions and grouping levels

settings:
  description: "Customer analysis by segment and registration year"
  variables:
    analysis_year: "2024"
  stages:
    - stage_name: "stg_customer_data"
      description: "Customer transaction data"
      protected: false
    - stage_name: "stg_customer_metrics"
      description: "Customer behavior metrics by segment"
      protected: false

recipe:
  # Previous steps populate customer_data
  - step_description: "Calculate customer metrics by segment and year"
    processor_type: "aggregate_data"
    source_stage: "stg_customer_data"
    save_to_stage: "stg_customer_metrics"
    # REQ - Multiple grouping columns for hierarchical analysis
    group_by: ["Customer_Segment", "Registration_Year"]
    # REQ - Multiple aggregations with different functions
    aggregations:
      # Revenue aggregations
      - column: "Total_Spent"
        function: "sum"
        output_name: "Segment_Revenue"
      - column: "Total_Spent"
        function: "mean"
        output_name: "Average_Customer_Value"
      
      # Customer count metrics
      - column: "Customer_ID"
        function: "nunique"
        output_name: "Unique_Customers"
      - column: "Customer_ID"
        function: "count"
        output_name: "Total_Transactions"
      
      # Order behavior metrics
      - column: "Order_Count"
        function: "sum"
        output_name: "Total_Orders"
      - column: "Order_Count"
        function: "mean"
        output_name: "Avg_Orders_Per_Customer"
      
      # Date analysis
      - column: "Last_Order_Date"
        function: "max"
        output_name: "Most_Recent_Order"
      - column: "Last_Order_Date"
        function: "min"
        output_name: "First_Order_Date"
```

### business intelligence

Executive dashboard metrics with advanced aggregations

```yaml
# Comprehensive business intelligence aggregation for executive reporting

settings:
  description: "Executive dashboard metrics by business unit and quarter"
  variables:
    current_year: "2024"
    company_name: "AcmeCorp"
  stages:
    - stage_name: "stg_sales_transactions"
      description: "All sales transaction data"
      protected: false
    - stage_name: "stg_executive_metrics"
      description: "Executive-level KPIs"
      protected: false

recipe:
  # Previous steps populate sales_transactions
  - step_description: "Generate executive dashboard KPIs"
    processor_type: "aggregate_data"
    source_stage: "stg_sales_transactions"
    save_to_stage: "stg_executive_metrics"
    # REQ - Group by business dimensions
    group_by: ["Business_Unit", "Quarter"]
    # REQ - Comprehensive business metrics
    aggregations:
      # Revenue metrics
      - column: "Revenue"
        function: "sum"
        output_name: "Total_Revenue"
      - column: "Revenue"
        function: "mean"
        output_name: "Average_Deal_Size"
      - column: "Revenue"
        function: "std"
        output_name: "Revenue_Variability"
      
      # Customer metrics
      - column: "Customer_ID"
        function: "nunique"
        output_name: "Unique_Customers"
      - column: "New_Customer_Flag"
        function: "sum"
        output_name: "New_Customers_Acquired"
      
      # Operational metrics
      - column: "Days_to_Close"
        function: "mean"
        output_name: "Average_Sales_Cycle"
      - column: "Opportunity_Count"
        function: "sum"
        output_name: "Total_Opportunities"
      - column: "Win_Rate"
        function: "mean"
        output_name: "Average_Win_Rate"
      
      # Product metrics
      - column: "Product_Line"
        function: "nunique"
        output_name: "Product_Lines_Sold"
      - column: "Units_Sold"
        function: "sum"
        output_name: "Total_Units_Sold"
```

## Parameter notes

- `group_by` (required): List of column names to group data by - creates unique combinations
- `aggregations` (required): List of aggregation operations to perform on grouped data
- `column` (required): Name of the column to aggregate - must exist in source data
- `function` (required): Aggregation function to apply to the column
- `output_name` (default `Uses format: {column}_{function} (e.g., Revenue_sum, Customer_ID_nunique)`): Custom name for the aggregated column
- `keep_group_columns` (default `True`): Whether to include grouping columns in the output DataFrame
- `sort_by_groups` (default `True`): Whether to sort the result by grouping columns
- `reset_index` (default `True`): Whether to reset index after grouping (makes group columns regular columns)

