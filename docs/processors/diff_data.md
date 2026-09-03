# `diff_data`

**Family:** `transform`

Compare two datasets and identify new, changed, unchanged, and deleted rows

## Keys

Generated from the declared schema; keys not listed are refused at recipe load.

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `source_stage`: stage_in; REQUIRED - Stage to read
- `save_to_stage`: stage_out; REQUIRED - Stage to write
- `confirm_stage_replacement`: bool; default false - Required true to overwrite an existing stage
- `reference_stage`: stage_in; REQUIRED
- `key_columns`: list of str; REQUIRED - Column names
- `exclude_columns`: list of str - Column names
- `handle_deleted_rows`: str; default "include"; one of include, exclude
- `include_json_details`: bool; default false
- `create_filtered_stages`: bool; default false
- `filtered_stage_prefix`: str; default "stg_diff" - Prefix for the filtered stages created when create_filtered_stages is true

## Examples

Every step below validates against the schema (tests/test_examples_validate_against_schemas.py).

### basic

Simple comparison between current and baseline data using composite key

```yaml
settings:
  description: "Compare customer data against baseline to identify changes"
  stages:
    - stage_name: "stg_current_data_standardized_and_keyed"
      description: "Current customer data with composite keys"
      protected: false
    - stage_name: "stg_baseline_data_standardized_and_keyed"
      description: "Baseline customer data for comparison"
      protected: false
    - stage_name: "stg_diff_complete_analysis_with_metadata"
      description: "Complete diff analysis with change metadata"
      protected: false

recipe:
  - # OPT - Human-readable step description
    # Default value: "Unnamed diff_data step"
    step_description: "Compare current data against baseline to identify all changes"
    # REQ - Must be "diff_data" for this processor type
    processor_type: "diff_data"
    # REQ - Stage containing updated/current data
    source_stage: "stg_current_data_standardized_and_keyed"
    # REQ - Stage containing baseline/reference data for comparison
    reference_stage: "stg_baseline_data_standardized_and_keyed"
    # REQ - Column(s) used to match rows between datasets
    key_columns: ["composite_key"]
    # REQ - Main output stage with all data plus change metadata
    save_to_stage: "stg_diff_complete_analysis_with_metadata"
    # OPT - Columns to exclude from change detection
    # Default value: [] (compare all columns)
    exclude_columns: ["composite_key", "last_modified_timestamp"]
```

### advanced

Comprehensive diff analysis with multiple key columns and filtered outputs

```yaml
settings:
  description: "Advanced diff analysis with separated change type outputs"
  stages:
    - stage_name: "stg_current_data_ready_for_comparison"
      description: "Current data ready for comparison"
      protected: false
    - stage_name: "stg_baseline_data_ready_for_comparison"
      description: "Baseline data ready for comparison"
      protected: false
    - stage_name: "stg_diff_complete_analysis_with_metadata"
      description: "Complete analysis with all rows and metadata"
      protected: false
    - stage_name: "stg_diff_analysis_new_rows_subset"
      description: "Only new rows subset"
      protected: false
    - stage_name: "stg_diff_analysis_changed_rows_subset"
      description: "Only changed rows subset"
      protected: false
    - stage_name: "stg_diff_analysis_unchanged_rows_subset"
      description: "Only unchanged rows subset"
      protected: false
    - stage_name: "stg_diff_analysis_deleted_rows_subset"
      description: "Only deleted rows subset"
      protected: false

recipe:
  - step_description: "Comprehensive diff analysis with separated outputs"
    processor_type: "diff_data"
    source_stage: "stg_current_data_ready_for_comparison"
    reference_stage: "stg_baseline_data_ready_for_comparison"
    # REQ - Multiple columns can be used as composite key
    key_columns: ["customer_id", "product_code"]
    save_to_stage: "stg_diff_complete_analysis_with_metadata"
    exclude_columns: ["customer_id", "product_code", "created_date", "internal_id"]
    # OPT - Generate separate convenience stages for each change type
    # Default value: false (only create main stage)
    create_filtered_stages: true
    # OPT - Stage naming pattern for filtered outputs
    # Default value: "stg_diff_{change_type}_rows_subset"
    filtered_stage_prefix: "stg_diff_analysis"
    # OPT - Include JSON format of change details
    # Default value: false (only pipe-separated format)
    include_json_details: true
    # OPT - How to handle rows that exist in baseline but not in current
    # Default value: "include" | Options: "include", "exclude", "separate_stage"
    handle_deleted_rows: "include"
```

### single key

Simple comparison using single column as unique key

```yaml
settings:
  description: "Compare product data using simple product ID key"
  stages:
    - stage_name: "stg_current_products_imported"
      description: "Current product catalog"
      protected: false
    - stage_name: "stg_baseline_products_archived"
      description: "Previous product catalog baseline"
      protected: false
    - stage_name: "stg_product_changes_identified"
      description: "Product changes with metadata"
      protected: false

recipe:
  - step_description: "Identify product catalog changes"
    processor_type: "diff_data"
    source_stage: "stg_current_products_imported"
    reference_stage: "stg_baseline_products_archived"
    # REQ - Single column name when key is unique
    key_columns: ["product_id"]
    save_to_stage: "stg_product_changes_identified"
    # OPT - Exclude timestamp fields from change detection
    exclude_columns: ["last_updated", "sync_timestamp"]
```

### exclude deleted

Focus only on current data changes, ignoring deleted items

```yaml
settings:
  description: "Analyze changes in active data without tracking deletions"
  stages:
    - stage_name: "stg_current_active_customers"
      description: "Currently active customer records"
      protected: false
    - stage_name: "stg_baseline_customer_snapshot"
      description: "Previous customer snapshot"
      protected: false
    - stage_name: "stg_active_customer_changes_only"
      description: "Changes in active customers only"
      protected: false

recipe:
  - step_description: "Track changes in active customers only"
    processor_type: "diff_data"
    source_stage: "stg_current_active_customers"
    reference_stage: "stg_baseline_customer_snapshot"
    key_columns: ["customer_id"]
    save_to_stage: "stg_active_customer_changes_only"
    exclude_columns: ["customer_id", "last_login_date"]
    # OPT - Exclude deleted rows from output entirely
    handle_deleted_rows: "exclude"
```

### filtered stages with custom prefix

Create filtered stages with custom naming pattern

```yaml
settings:
  description: "Generate change analysis with custom stage naming"
  variables:
    analysis_date: "2024-12-15"
    department: "sales"
  stages:
    - stage_name: "stg_current_sales_data_processed"
      description: "Current sales data"
      protected: false
    - stage_name: "stg_baseline_sales_data_archived"
      description: "Baseline sales data"
      protected: false
    - stage_name: "stg_sales_diff_complete_analysis_with_metadata"
      description: "Complete sales diff analysis"
      protected: false
    - stage_name: "stg_sales_breakdown_new_rows_subset"
      description: "New sales records"
      protected: false
    - stage_name: "stg_sales_breakdown_changed_rows_subset"
      description: "Modified sales records"
      protected: false
    - stage_name: "stg_sales_breakdown_unchanged_rows_subset"
      description: "Unchanged sales records"
      protected: false
    - stage_name: "stg_sales_breakdown_deleted_rows_subset"
      description: "Deleted sales records"
      protected: false

recipe:
  - step_description: "Analyze sales data changes with custom stage naming"
    processor_type: "diff_data"
    source_stage: "stg_current_sales_data_processed"
    reference_stage: "stg_baseline_sales_data_archived"
    key_columns: ["sale_id", "customer_id"]
    save_to_stage: "stg_sales_diff_complete_analysis_with_metadata"
    exclude_columns: ["sale_id", "customer_id", "created_timestamp"]
    create_filtered_stages: true
    # OPT - Custom prefix for filtered stage names
    filtered_stage_prefix: "stg_sales_breakdown"
    include_json_details: false
```

### json details

Include JSON format change details for machine processing

```yaml
settings:
  description: "Generate diff analysis with JSON change details for automation"
  stages:
    - stage_name: "stg_current_inventory_standardized"
      description: "Current inventory data"
      protected: false
    - stage_name: "stg_baseline_inventory_snapshot"
      description: "Baseline inventory snapshot"
      protected: false
    - stage_name: "stg_inventory_changes_with_json_details"
      description: "Inventory changes with JSON metadata"
      protected: false

recipe:
  - step_description: "Track inventory changes with machine-readable details"
    processor_type: "diff_data"
    source_stage: "stg_current_inventory_standardized"
    reference_stage: "stg_baseline_inventory_snapshot"
    key_columns: ["item_code", "location_code"]
    save_to_stage: "stg_inventory_changes_with_json_details"
    exclude_columns: ["item_code", "location_code", "last_count_date"]
    # OPT - Add JSON format column for programmatic access
    include_json_details: true
```

## Parameter notes

- `source_stage` (required): Stage containing current/updated data to compare
- `reference_stage` (required): Stage containing baseline/reference data for comparison
- `key_columns` (required): Column(s) used to match rows between datasets
- `save_to_stage` (required): Main output stage containing all rows with change metadata
- `exclude_columns` (default `[]`): Columns to exclude from change detection analysis
- `create_filtered_stages` (default `False`): Generate separate stages for each change type (NEW, CHANGED, UNCHANGED, DELETED)
- `filtered_stage_prefix` (default `stg_diff`): Prefix for filtered stage names when create_filtered_stages is true
- `include_json_details` (default `False`): Add Change_Details_JSON column with machine-readable change format
- `handle_deleted_rows` (default `include`): How to handle rows that exist in baseline but not in current data

