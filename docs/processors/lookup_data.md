# `lookup_data`

**Family:** `transform`

Stage-to-stage lookups with smart key normalization and detailed logging

## Keys

Generated from the declared schema; keys not listed are refused at recipe load.

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `source_stage`: stage_in; REQUIRED - Stage to read
- `save_to_stage`: stage_out; REQUIRED - Stage to write
- `confirm_stage_replacement`: bool; default false - Required true to overwrite an existing stage
- `lookup_stage`: stage_in; REQUIRED
- `match_col_in_main_data`: str; REQUIRED
- `match_col_in_lookup_data`: str; REQUIRED
- `lookup_columns`: list of str; REQUIRED - Column names
- `join_type`: str; default "left"; one of left, inner
- `handle_duplicates`: str; default "first"; one of first, last, error
- `default_values`: open_mapping - lookup column -> value when unmatched
- `normalize_keys`: bool; default true
- `low_match_warning`: bool; default true
- `match_mode`: str; default "exact_key_equality"; one of exact_key_equality, lookup_value_within_main_text
- `prefix`: str; default ""
- `suffix`: str; default ""

## Examples

Every step below validates against the schema (tests/test_examples_validate_against_schemas.py).

### basic

Simple customer lookup from stage data

```yaml
# Basic lookup workflow: import data -> lookup -> result

settings:
  description: "Add customer details to order data"
  stages:
    - stage_name: "stg_raw_orders"
      description: "Imported order data"
      protected: false
    - stage_name: "stg_customer_data" 
      description: "Customer reference data"
      protected: false
    - stage_name: "stg_enriched_orders"
      description: "Orders with customer details"
      protected: false

recipe:
  # Step 1: Import order data
  - step_description: "Import order transactions"
    processor_type: "import_file"
    input_file: "orders.xlsx"
    save_to_stage: "stg_raw_orders"
  
  # Step 2: Import customer reference data
  - step_description: "Import customer master data"
    processor_type: "import_file"
    input_file: "customers.xlsx"
    save_to_stage: "stg_customer_data"
  
  # Step 3: Lookup customer details
  - step_description: "Add customer details to orders"
    processor_type: "lookup_data"
    source_stage: "stg_raw_orders"
    save_to_stage: "stg_enriched_orders"
    # REQ - Stage containing lookup data
    lookup_stage: "stg_customer_data"
    # REQ - Key column in lookup data
    match_col_in_lookup_data: "Customer_ID"
    # REQ - Key column in main data  
    match_col_in_main_data: "Customer_ID"
    # REQ - Columns to bring from lookup data
    lookup_columns: ["Customer_Name", "Region", "Tier"]
```

### key normalization

Handle type mismatches and format issues automatically

```yaml
# Demonstrates smart key normalization for common data issues

settings:
  description: "Handle mixed data types in lookup keys"
  stages:
    - stage_name: "stg_sales_data"
      description: "Sales with mixed ID formats"
      protected: false
    - stage_name: "stg_product_catalog"
      description: "Product reference with clean IDs"
      protected: false
    - stage_name: "stg_normalized_sales"
      description: "Sales with product details"
      protected: false

recipe:
  - step_description: "Import sales data (IDs may be: 1001, '1002.0', ' 1003 ')"
    processor_type: "import_file"
    input_file: "sales_data.xlsx"
    save_to_stage: "stg_sales_data"
  
  - step_description: "Import product catalog (clean string IDs: '1001', '1002')"
    processor_type: "import_file"
    input_file: "product_catalog.xlsx"
    save_to_stage: "stg_product_catalog"
  
  - step_description: "Lookup with automatic key normalization"
    processor_type: "lookup_data"
    source_stage: "stg_sales_data"
    save_to_stage: "stg_normalized_sales"
    lookup_stage: "stg_product_catalog"
    match_col_in_lookup_data: "Product_ID"
    match_col_in_main_data: "Product_ID"
    lookup_columns: ["Product_Name", "Category", "Unit_Price"]
    # OPT - Enable smart normalization (default: true)
    # Handles: numeric vs string, trailing .0, whitespace, null values
    normalize_keys: true
```

### column naming

Use prefix/suffix to avoid column name conflicts

```yaml
# Handle situations where lookup columns conflict with existing columns

settings:
  description: "Add product and customer status without conflicts"
  stages:
    - stage_name: "stg_order_data"
      description: "Orders with existing Status column"
      protected: false
    - stage_name: "stg_customer_lookup"
      description: "Customer data with Status column"
      protected: false
    - stage_name: "stg_product_lookup"
      description: "Product data with Status column"
      protected: false
    - stage_name: "stg_fully_enriched"
      description: "Orders with all details"
      protected: false

recipe:
  # Multiple lookups with different prefixes to avoid conflicts
  - step_description: "Add customer details with prefix"
    processor_type: "lookup_data"
    source_stage: "stg_order_data"
    save_to_stage: "stg_customer_enriched"
    lookup_stage: "stg_customer_lookup"
    match_col_in_lookup_data: "Customer_ID"
    match_col_in_main_data: "Customer_ID"
    lookup_columns: ["Name", "Status", "Region"]
    # OPT - Add prefix to avoid conflicts with existing Status column
    prefix: "Customer_"
    # Result: Customer_Name, Customer_Status, Customer_Region
  
  - step_description: "Add product details with prefix"
    processor_type: "lookup_data"
    source_stage: "stg_customer_enriched"
    save_to_stage: "stg_fully_enriched"
    lookup_stage: "stg_product_lookup"
    match_col_in_lookup_data: "Product_SKU"
    match_col_in_main_data: "Product_SKU"
    lookup_columns: ["Name", "Status", "Category"]
    prefix: "Product_"
    # Result: Product_Name, Product_Status, Product_Category
```

### default values

Provide defaults for missing lookup matches

```yaml
# Handle missing matches with sensible defaults

settings:
  description: "Lookup with fallback values for missing data"
  stages:
    - stage_name: "stg_transaction_data"
      description: "All transactions including unknown customers"
      protected: false
    - stage_name: "stg_known_customers"
      description: "Partial customer database"
      protected: false  
    - stage_name: "stg_complete_transactions"
      description: "Transactions with customer info or defaults"
      protected: false

recipe:
  - step_description: "Lookup customer details with defaults for unknowns"
    processor_type: "lookup_data"
    source_stage: "stg_transaction_data"
    save_to_stage: "stg_complete_transactions"
    lookup_stage: "stg_known_customers"
    match_col_in_lookup_data: "Customer_ID"
    match_col_in_main_data: "Customer_ID" 
    lookup_columns: ["Customer_Name", "Region", "Tier", "Credit_Limit"]
    # OPT - Provide defaults for missing matches
    default_values:
      Customer_Name: "Unknown Customer"
      Region: "Unassigned"
      Tier: "Standard"
      Credit_Limit: 1000.0
```

### duplicate handling

Control how duplicate lookup keys are handled

```yaml
# Handle duplicate keys in lookup data

settings:
  description: "Lookup with duplicate key resolution"
  stages:
    - stage_name: "stg_order_data"
      description: "Orders to enrich"
      protected: false
    - stage_name: "stg_price_history"
      description: "Product prices with historical entries"
      protected: false
    - stage_name: "stg_current_orders"
      description: "Orders with current prices"
      protected: false

recipe:
  - step_description: "Get current prices (use most recent for duplicates)"
    processor_type: "lookup_data"
    source_stage: "stg_order_data"
    save_to_stage: "stg_current_orders"
    lookup_stage: "stg_price_history"
    match_col_in_lookup_data: "Product_SKU"
    match_col_in_main_data: "Product_SKU"
    lookup_columns: ["Current_Price", "Effective_Date"]
    # OPT - Handle duplicate Product_SKU entries
    # Valid values: "first", "last", "error"
    # Default: "first"
    handle_duplicates: "last"  # Use most recent price
```

### advanced workflow

Multi-stage lookup workflow with different join types

```yaml
# Complex workflow with multiple lookups and join strategies

settings:
  description: "Multi-stage enrichment with validation"
  stages:
    - stage_name: "stg_raw_sales"
      description: "Raw sales data"
      protected: false
    - stage_name: "stg_valid_customers"
      description: "Validated customer list"
      protected: false
    - stage_name: "stg_product_catalog"
      description: "Current product catalog"
      protected: false
    - stage_name: "stg_validated_sales"
      description: "Sales with valid customers only"
      protected: false
    - stage_name: "stg_complete_sales"
      description: "Validated sales with product details"
      protected: false

recipe:
  # Step 1: Validate customers (inner join - only keep valid customers)
  - step_description: "Filter for valid customers only"
    processor_type: "lookup_data"
    source_stage: "stg_raw_sales"
    save_to_stage: "stg_validated_sales"
    lookup_stage: "stg_valid_customers"
    match_col_in_lookup_data: "Customer_ID"
    match_col_in_main_data: "Customer_ID"
    lookup_columns: ["Customer_Name", "Account_Status"]
    # REQ - Inner join removes rows with invalid customers
    join_type: "inner"
  
  # Step 2: Add product details (left join - keep all validated sales)
  - step_description: "Add product information"
    processor_type: "lookup_data"
    source_stage: "stg_validated_sales"
    save_to_stage: "stg_complete_sales"
    lookup_stage: "stg_product_catalog"
    match_col_in_lookup_data: "Product_SKU"
    match_col_in_main_data: "Product_SKU"
    lookup_columns: ["Product_Name", "Category", "Unit_Cost"]
    join_type: "left"
    default_values:
      Product_Name: "Unknown Product"
      Category: "Uncategorized"
```

## Parameter notes

- `lookup_stage` (required): Name of stage containing lookup data (must be declared in settings.stages)
- `match_col_in_lookup_data` (required): Column name in lookup data to match against
- `match_col_in_main_data` (required): Column name in main data to match with match_col_in_lookup_data
- `lookup_columns` (required): List of column names to retrieve from lookup data
- `join_type` (default `left`): Type of join operation
- `prefix` (default ``): Prefix to add to lookup column names to avoid conflicts
- `suffix` (default ``): Suffix to add to lookup column names to avoid conflicts
- `default_values`: Default values for lookup columns when no match is found
- `normalize_keys` (default `True`): Enable automatic key normalization to handle format mismatches
- `handle_duplicates` (default `first`): How to handle duplicate keys in lookup data

