# `merge_data`

**Family:** `transform`

Merge DataFrames with external data sources using various join strategies

## Keys

Generated from the declared schema; keys not listed are refused at recipe load.

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `source_stage`: stage_in; REQUIRED - Stage to read
- `save_to_stage`: stage_out; REQUIRED - Stage to write
- `confirm_stage_replacement`: bool; default false - Required true to overwrite an existing stage
- `merge_source`: mapping; REQUIRED
  - `type`: str; REQUIRED; one of excel, csv, tsv, dictionary, stage
  - `stage_name`: stage_in
  - `path`: str
  - `sheet`: any
  - `encoding`: str
  - `separator`: str
  - `format`: str
  - `data`: any
  - `columns_to_prefix`: list of str
- `left_key`: any; REQUIRED
- `right_key`: any; REQUIRED
- `join_type`: str; default "left"; one of left, right, inner, outer
- `column_prefix`: str; default ""
- `suffixes`: any - Two suffixes for overlapping names, list or tuple
- `drop_duplicate_keys`: bool; default true

## Examples

Every step below validates against the schema (tests/test_examples_validate_against_schemas.py).

### basic

Simple left join with Excel file to enrich main data

```yaml
# Complete recipe with merge_data processor

settings:
  description: "Enrich order data with customer information"
  stages:
    - stage_name: "stg_order_data"
      description: "Raw order data from import"
      protected: false
    - stage_name: "stg_enriched_orders"
      description: "Orders with customer details added"
      protected: false

recipe:
  # Previous step would populate 'order_data' stage
  - step_description: "Add customer details to orders"
    # REQ - Must be "merge_data" for this processor type
    processor_type: "merge_data"
    # REQ - Stage to read data from (must be declared in settings.stages)
    source_stage: "stg_order_data"
    # REQ - Stage to save merged results
    save_to_stage: "stg_enriched_orders"
    # REQ - External data source configuration
    merge_source:
      # REQ - Type of data source
      type: "excel"
      # REQ - Path to external file (supports variable substitution)
      path: "customer_master.xlsx"
      # OPT - Sheet name or index for Excel files
      # Can be string (sheet name) or integer (1-based index)
      # Default value: 1 (first sheet)
      sheet: "Customers"
    # REQ - Column in main data to use as merge key
    left_key: "Customer_ID"
    # REQ - Column in external data to use as merge key
    right_key: "Customer_ID"
    # OPT - Type of join operation to perform
    # Default value: "left"
    join_type: "left"
```

### multi source

Chain multiple merges to enrich from different sources

```yaml
# Multi-step enrichment from Excel, CSV, and dictionary sources

settings:
  description: "Comprehensive data enrichment from multiple sources"
  variables:
    data_year: "2024"
    region: "west"
  stages:
    - stage_name: "stg_raw_orders"
      description: "Raw order data"
      protected: false
    - stage_name: "stg_customer_enriched"
      description: "Orders with customer data"
      protected: false
    - stage_name: "stg_product_enriched"
      description: "Orders with customer and product data"
      protected: false
    - stage_name: "stg_territory_enriched"
      description: "Fully enriched order data"
      protected: false

recipe:
  # Step 1: Add customer information from Excel
  - step_description: "Enrich with customer master data"
    processor_type: "merge_data"
    source_stage: "stg_raw_orders"
    save_to_stage: "stg_customer_enriched"
    merge_source:
      type: "excel"
      path: "reference_data/customers_{data_year}.xlsx"
      sheet: "Active_Customers"
      # OPT - Text encoding for files (ignored for Excel)
      # Default value: "utf-8"
      encoding: "utf-8"
    left_key: "Customer_ID"
    right_key: "Customer_ID"
    join_type: "left"
    # OPT - Handle duplicate column names
    # Default value: ["_x", "_y"]
    suffixes: ["_order", "_customer"]
  
  # Step 2: Add product information from CSV
  - step_description: "Add product catalog details"
    processor_type: "merge_data"
    source_stage: "stg_customer_enriched"
    save_to_stage: "stg_product_enriched"
    merge_source:
      type: "csv"
      path: "product_catalog.csv"
      # OPT - Field separator for CSV files
      # Default value: ","
      separator: ","
      encoding: "utf-8"
    left_key: "Product_SKU"
    right_key: "SKU"
    join_type: "left"
  
  # Step 3: Add territory mapping from configuration
  - step_description: "Map regions to sales territories"
    processor_type: "merge_data"
    source_stage: "stg_product_enriched"
    save_to_stage: "stg_territory_enriched"
    merge_source:
      # REQ - Dictionary-based lookup data
      type: "dictionary"
      # REQ - Inline mapping data
      data:
        "West": 
          Territory: "Pacific"
          Territory_Manager: "Alice Johnson"
          Commission_Rate: 0.05
        "East":
          Territory: "Atlantic"
          Territory_Manager: "Bob Smith"
          Commission_Rate: 0.045
        "Central":
          Territory: "Continental"
          Territory_Manager: "Carol Davis"
          Commission_Rate: 0.04
    left_key: "Customer_Region"
    # REQ - For dictionary sources, right_key must be "key"
    right_key: "key"
    join_type: "left"
```

### join types

Demonstrate different join types and their use cases

```yaml
# Show how different join types affect the results

settings:
  description: "Compare join type behaviors for data validation"
  stages:
    - stage_name: "stg_main_data"
      description: "Main dataset"
      protected: false
    - stage_name: "stg_left_joined"
      description: "Left join result - keeps all main data"
      protected: false
    - stage_name: "stg_inner_joined"
      description: "Inner join result - only matching records"
      protected: false
    - stage_name: "stg_outer_joined"
      description: "Outer join result - all records from both sources"
      protected: false

recipe:
  # Left join: Keep all main data, add external where available
  - step_description: "Left join - preserve all main data rows"
    processor_type: "merge_data"
    source_stage: "stg_main_data"
    save_to_stage: "stg_left_joined"
    merge_source:
      type: "excel"
      path: "reference_data.xlsx"
    left_key: "ID"
    right_key: "Reference_ID"
    # OPT - Left join preserves all rows from main data
    join_type: "left"
  
  # Inner join: Only keep rows that match in both datasets
  - step_description: "Inner join - only complete records"
    processor_type: "merge_data"
    source_stage: "stg_main_data"
    save_to_stage: "stg_inner_joined"
    merge_source:
      type: "excel"
      path: "reference_data.xlsx"
    left_key: "ID"
    right_key: "Reference_ID"
    # OPT - Inner join filters out non-matching rows
    join_type: "inner"
  
  # Outer join: Keep all rows from both datasets
  - step_description: "Outer join - comprehensive data view"
    processor_type: "merge_data"
    source_stage: "stg_main_data"
    save_to_stage: "stg_outer_joined"
    merge_source:
      type: "excel"
      path: "reference_data.xlsx"
    left_key: "ID"
    right_key: "Reference_ID"
    # OPT - Outer join includes all data from both sources
    join_type: "outer"
```

### stage merge

Merge with data from another stage instead of external file

```yaml
# Use previously processed data as merge source

settings:
  description: "Merge with results from previous processing stages"
  stages:
    - stage_name: "stg_customer_orders"
      description: "Customer order data"
      protected: false
    - stage_name: "stg_customer_segments"
      description: "Customer segmentation results"
      protected: false
    - stage_name: "stg_segmented_orders"
      description: "Orders enriched with segmentation data"
      protected: false

recipe:
  # Previous steps would populate both customer_orders and customer_segments
  - step_description: "Add customer segmentation to orders"
    processor_type: "merge_data"
    source_stage: "stg_customer_orders"
    save_to_stage: "stg_segmented_orders"
    merge_source:
      # REQ - Stage-based data source
      type: "stage"
      # REQ - Name of previously saved stage
      stage_name: "stg_customer_segments"
    left_key: "Customer_ID"
    right_key: "Customer_ID"
    # OPT - Inner join to only include customers that were segmented
    join_type: "inner"
```

## Parameter notes

- `merge_source` (required): Configuration for the external data source to merge with main data
- `merge_source_type` (required): Type of external data source
- `merge_source_path` (required): File path to external data source (supports variable substitution)
- `merge_source_sheet` (default `1`): Sheet name or 1-based index to read from Excel file
- `merge_source_data` (required): Inline dictionary mapping data where keys become the merge column
- `merge_source_stage_name` (required): Name of previously saved stage to use as merge source
- `left_key` (required): Column name in main data to use as merge key
- `right_key` (required): Column name in external data to use as merge key
- `join_type` (default `left`): Type of join operation to perform between datasets
- `suffixes` (default `['_x', '_y']`): Suffixes to add to duplicate column names from left and right datasets
- `drop_duplicate_keys` (default `True`): Whether to remove duplicate key columns after merge
- `encoding` (default `utf-8`): Text encoding for reading CSV/TSV files (ignored for Excel files)
- `separator` (default `,`): Field separator character for CSV files

