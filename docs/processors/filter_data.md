# `filter_data`

**Family:** `transform`

Filter rows by conditions, including stage-based comparisons

## Keys

Generated from the declared schema; keys not listed are refused at recipe load.

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `source_stage`: stage_in; REQUIRED - Stage to read
- `save_to_stage`: stage_out; REQUIRED - Stage to write
- `confirm_stage_replacement`: bool; default false - Required true to overwrite an existing stage
- `filters`: list_of_mappings
  - `column`: str; REQUIRED
  - `condition`: str; REQUIRED; one of contains, contains_all_in_list, contains_any_in_list, ends_with, ends_with_any_in_list, equals, equals_any_in_list, greater_equal, greater_equal_max_in_list, greater_equal_min_in_list, greater_than, greater_than_max_in_list, greater_than_min_in_list, in_list, in_stage, is_empty, less_equal, less_equal_max_in_list, less_equal_min_in_list, less_than, less_than_max_in_list, less_than_min_in_list, not_contains, not_contains_any_in_list, not_empty, not_ends_with, not_equals, not_equals_any_in_list, not_in_list, not_in_stage, not_starts_with, stage_comparison, starts_with, starts_with_any_in_list
  - `value`: any
  - `case_sensitive`: bool; default false
  - `comparison_operator`: str
  - `key_column`: str
  - `stage_name`: stage_in - For in_stage / not_in_stage
  - `stage_column`: str
  - `stage_key_column`: str
  - `stage_value_column`: str
- `pandas_expression`: str - pandas query text; alternative to filters
- at least one of: `filters`, `pandas_expression`

## Examples

Every step below validates against the schema (tests/test_examples_validate_against_schemas.py).

### basic

Simple equals filter with case-insensitive matching (default behavior)

```yaml
# Complete recipe with basic filter processor

settings:
  description: "Filter for active customers using case-insensitive equality condition"
  stages:
    - stage_name: "stg_raw_data"
      description: "Raw imported customer data"
      protected: false
    - stage_name: "stg_active_customers"
      description: "Filtered active customers only"
      protected: false

recipe:
  # Previous step would populate raw_data stage
  - # OPT - Human-readable step description
    # Default value: "Unnamed filter_data step"
    step_description: "Filter for active customers only"
    # REQ - Must be "filter_data" for this processor type
    processor_type: "filter_data"
    # REQ - Stage to read data from (must be declared in settings.stages)
    source_stage: "stg_raw_data"
    # REQ - Stage to save filtered results (must be declared in settings.stages)
    save_to_stage: "stg_active_customers"
    # REQ - List of filter conditions (AND logic applied)
    filters:
      - # REQ - Column name to filter on
        column: "Status"
        # REQ - Filter condition type
        condition: "equals"
        # REQ - Value to compare against
        value: "Active"
        # OPT - Case sensitivity control for text comparisons
        # Default value: false (case-insensitive matching)
        case_sensitive: false
```

### case sensitivity

Case sensitivity control for precise text matching

```yaml
# Control case sensitivity in text comparisons

settings:
  description: "Filter products with precise case-sensitive and case-insensitive matching"
  stages:
    - stage_name: "stg_all_products"
      description: "Complete product catalog"
      protected: false
    - stage_name: "stg_exact_products"
      description: "Products with exact case matching"
      protected: false

recipe:
  - # OPT - Human-readable step description
    # Default value: "Unnamed filter_data step"
    step_description: "Filter with case-sensitive product matching"
    # REQ - Must be "filter_data" for this processor type
    processor_type: "filter_data"
    # REQ - Stage to read data from
    source_stage: "stg_all_products"
    # REQ - Stage to save filtered results
    save_to_stage: "stg_exact_products"
    # REQ - List of filter conditions
    filters:
      # Case-insensitive by default (matches Active, ACTIVE, active)
      - # REQ - Column name to filter on
        column: "Status"
        # REQ - Filter condition type
        condition: "equals"
        # REQ - Value to compare against
        value: "Active"
      # Case-sensitive matching (only matches exact "Premium")
      - # REQ - Column name to filter on
        column: "Tier"
        # REQ - Filter condition type
        condition: "equals"
        # REQ - Value to compare against
        value: "Premium"
        # OPT - Enable case-sensitive matching
        # Default value: false
        case_sensitive: true
```

### pattern matching

Enhanced pattern matching with starts_with, ends_with, and negation

```yaml
# Advanced text pattern matching conditions

settings:
  description: "Filter data using advanced text pattern matching and exclusions"
  stages:
    - stage_name: "stg_raw_inventory"
      description: "Raw inventory data"
      protected: false
    - stage_name: "stg_filtered_inventory"
      description: "Inventory matching specific patterns"
      protected: false

recipe:
  - # OPT - Human-readable step description
    # Default value: "Unnamed filter_data step"
    step_description: "Filter inventory using pattern matching"
    # REQ - Must be "filter_data" for this processor type
    processor_type: "filter_data"
    # REQ - Stage to read data from
    source_stage: "stg_raw_inventory"
    # REQ - Stage to save filtered results
    save_to_stage: "stg_filtered_inventory"
    # REQ - List of filter conditions
    filters:
      # Include products starting with "PROD-"
      - # REQ - Column name to filter on
        column: "Product_Code"
        # REQ - Pattern matching condition
        condition: "starts_with"
        # REQ - Text pattern to match (case-insensitive by default)
        value: "PROD-"
      # Include Excel files only
      - # REQ - Column name to filter on
        column: "Source_File"
        # REQ - Pattern matching condition
        condition: "ends_with"
        # REQ - File extension pattern
        value: ".xlsx"
      # Exclude test products
      - # REQ - Column name to filter on
        column: "Product_Code"
        # REQ - Negated pattern matching condition
        condition: "not_starts_with"
        # REQ - Pattern to exclude
        value: "TEST-"
```

### enhanced list conditions

Advanced list operations with contains_any_in_list and contains_all_in_list

```yaml
# Enhanced list-based filtering with flexible pattern matching

settings:
  description: "Filter products using advanced list conditions and pattern containment"
  stages:
    - stage_name: "stg_product_catalog"
      description: "Complete product catalog"
      protected: false
    - stage_name: "stg_targeted_products"
      description: "Products matching complex criteria"
      protected: false

recipe:
  - # OPT - Human-readable step description
    # Default value: "Unnamed filter_data step"
    step_description: "Apply advanced list-based filtering"
    # REQ - Must be "filter_data" for this processor type
    processor_type: "filter_data"
    # REQ - Stage to read data from
    source_stage: "stg_product_catalog"
    # REQ - Stage to save filtered results
    save_to_stage: "stg_targeted_products"
    # REQ - List of filter conditions
    filters:
      # Include if description contains ANY of these terms
      - # REQ - Column name to filter on
        column: "Description"
        # REQ - List condition for ANY match (OR logic)
        condition: "contains_any_in_list"
        # REQ - List of patterns to search for
        value: ["premium", "wireless", "bluetooth"]
      # Include only if tags contain ALL required terms
      - # REQ - Column name to filter on
        column: "Tags"
        # REQ - List condition for ALL matches (AND logic)
        condition: "contains_all_in_list"
        # REQ - List of patterns that must all be present
        value: ["certified", "warranty"]
      # Exclude products with unwanted terms
      - # REQ - Column name to filter on
        column: "Description"
        # REQ - Exclusion condition for ANY match
        condition: "not_contains_any_in_list"
        # REQ - List of patterns to exclude
        value: ["discontinued", "legacy", "deprecated"]
```

### numeric list conditions

Numeric list operations with min/max comparisons

```yaml
# Numeric filtering using list-based minimum and maximum values

settings:
  description: "Filter orders using numeric list conditions for flexible range checking"
  stages:
    - stage_name: "stg_all_orders"
      description: "Complete order history"
      protected: false
    - stage_name: "stg_qualifying_orders"
      description: "Orders meeting numeric criteria"
      protected: false

recipe:
  - # OPT - Human-readable step description
    # Default value: "Unnamed filter_data step"
    step_description: "Filter orders using numeric list conditions"
    # REQ - Must be "filter_data" for this processor type
    processor_type: "filter_data"
    # REQ - Stage to read data from
    source_stage: "stg_all_orders"
    # REQ - Stage to save filtered results
    save_to_stage: "stg_qualifying_orders"
    # REQ - List of filter conditions
    filters:
      # Orders above the minimum threshold in any region
      - # REQ - Column name to filter on
        column: "Order_Amount"
        # REQ - Greater than minimum value in list
        condition: "greater_than_min_in_list"
        # REQ - List of threshold values (uses minimum: 500)
        value: [500, 1000, 1500]
      # Orders below the maximum limit for any tier
      - # REQ - Column name to filter on
        column: "Order_Amount"
        # REQ - Less than maximum value in list
        condition: "less_than_max_in_list"
        # REQ - List of limit values (uses maximum: 5000)
        value: [2000, 3500, 5000]
      # High-value orders exceeding all regional targets
      - # REQ - Column name to filter on
        column: "Order_Amount"
        # REQ - Greater than maximum value in list
        condition: "greater_than_max_in_list"
        # REQ - List of target values (uses maximum: 10000)
        value: [5000, 7500, 10000]
```

### stage based filtering

Stage-based filtering with cross-stage data validation

```yaml
# Advanced stage-based filtering with case sensitivity and comparisons

settings:
  description: "Filter orders using stage-based validation and cross-stage comparisons"
  stages:
    - stage_name: "stg_all_orders"
      description: "Complete order dataset"
      protected: false
    - stage_name: "stg_approved_customers"
      description: "Pre-approved customer list"
      protected: false
    - stage_name: "stg_price_history"
      description: "Historical pricing data"
      protected: false
    - stage_name: "stg_validated_orders"
      description: "Orders from approved customers with price validation"
      protected: false

recipe:
  # Previous steps would populate approved_customers and price_history stages
  - # OPT - Human-readable step description
    # Default value: "Unnamed filter_data step"
    step_description: "Filter orders using stage-based validation"
    # REQ - Must be "filter_data" for this processor type
    processor_type: "filter_data"
    # REQ - Stage to read data from
    source_stage: "stg_all_orders"
    # REQ - Stage to save filtered results
    save_to_stage: "stg_validated_orders"
    # REQ - List of filter conditions
    filters:
      # Include only orders from approved customers
      - # REQ - Column name in current data
        column: "Customer_ID"
        # REQ - Stage-based inclusion condition
        condition: "in_stage"
        # REQ - Stage name containing valid values
        stage_name: "stg_approved_customers"
        # REQ - Column name in referenced stage
        stage_column: "Customer_ID"
        # OPT - Case sensitivity for stage comparison
        # Default value: false
        case_sensitive: false
      # Include orders with prices below historical maximum
      - # REQ - Current data column for comparison key
        column: "Product_ID"
        # REQ - Stage comparison condition
        condition: "stage_comparison"
        # REQ - Stage containing comparison data
        stage_name: "stg_price_history"
        # REQ - Key column in current data
        key_column: "Product_ID"
        # REQ - Key column in stage data
        stage_key_column: "Product_ID"
        # REQ - Value column in stage for comparison
        stage_value_column: "Max_Price"
        # REQ - Comparison operator to apply
        comparison_operator: "less_than"
```

### multiple filters

Multiple filter conditions with different operators and logic

```yaml
# Apply multiple filters in sequence (AND logic between all conditions)

settings:
  description: "Apply multiple filter conditions to extract premium recent orders"
  stages:
    - stage_name: "stg_all_orders"
      description: "All order data"
      protected: false
    - stage_name: "stg_premium_orders"
      description: "High-value recent orders"
      protected: false

recipe:
  # Previous step populates all_orders
  - # OPT - Human-readable step description
    # Default value: "Unnamed filter_data step"
    step_description: "Filter for premium recent orders"
    # REQ - Must be "filter_data" for this processor type
    processor_type: "filter_data"
    # REQ - Stage to read data from
    source_stage: "stg_all_orders"
    # REQ - Stage to save filtered results
    save_to_stage: "stg_premium_orders"
    # REQ - List of filter conditions (all must be true - AND logic)
    filters:
      # Filter 1: Amount greater than $1000
      - # REQ - Column name to filter on
        column: "Order_Amount"
        # REQ - Numeric comparison condition
        condition: "greater_than"
        # REQ - Numeric value for comparison
        value: 1000
      # Filter 2: Premium product categories
      - # REQ - Column name to filter on
        column: "Product_Category"
        # REQ - Enhanced list condition for ANY match
        condition: "contains_any_in_list"
        # REQ - List of category patterns
        value: ["Premium", "VIP", "Platinum"]
      # Filter 3: High-priority tier levels only
      # (For DATE comparisons use a pandas_expression filter - see
      # the pandas_expression examples below. The ordering
      # conditions coerce the column numerically and cannot
      # compare dates.)
      - # REQ - Column name to filter on
        column: "Priority_Tier"
        # REQ - Numeric comparison condition
        condition: "greater_equal"
        # REQ - Numeric value for comparison
        value: 2
      # Filter 4: Exclude cancelled orders
      - # REQ - Column name to filter on
        column: "Status"
        # REQ - Negation condition
        condition: "not_equals"
        # REQ - Status to exclude
        value: "Cancelled"
```

### pandas expression basic

Use pandas expression for complex boolean logic

```yaml
settings:
  description: "Filter using pandas expression syntax for complex conditions"
  stages:
    - stage_name: "stg_raw_data"
      description: "Source data"
      protected: false
    - stage_name: "stg_filtered_data"
      description: "Filtered using pandas expression"
      protected: false

recipe:
  - step_description: "Import source data"
    processor_type: "import_file"
    input_file: "data/orders.xlsx"
    save_to_stage: "stg_raw_data"

  - step_description: "Filter using pandas boolean logic"
    processor_type: "filter_data"
    source_stage: "stg_raw_data"
    # Use pandas expression for complex logic that would be impossible with regular filters
    pandas_expression: '(Status == "Active") & ((Priority == "High") | (Amount > 1000))'
    save_to_stage: "stg_filtered_data"
```

### pandas expression completed vans

Real-world example: Keep recent Completed Vans entries

```yaml
settings:
  description: "Filter out old Completed Vans while keeping recent ones"
  variables:
    week_start_date: "2024-08-04"
  stages:
    - stage_name: "stg_all_orders"
      description: "All order data"
      protected: false
    - stage_name: "stg_filtered_orders"
      description: "Filtered to exclude old Completed Vans"
      protected: false

recipe:
  - step_description: "Import all orders"
    processor_type: "import_file"
    input_file: "data/orders.xlsx"
    save_to_stage: "stg_all_orders"

  - step_description: "Keep recent Completed Vans and all other entries"
    processor_type: "filter_data"
    source_stage: "stg_all_orders"
    # Logic: NOT (Completed Vans AND old date) = Keep everything except old Completed Vans
    pandas_expression: '~(Workflow.str.contains("Completed Vans") & (`Paid Date` < "{week_start_date}"))'
    save_to_stage: "stg_filtered_orders"
```

### pandas expression string operations

Use pandas string methods in expressions

```yaml
settings:
  description: "Complex text filtering using pandas string operations"
  stages:
    - stage_name: "stg_products"
      description: "Product data"
      protected: false
    - stage_name: "stg_filtered_products"
      description: "Filtered products"
      protected: false

recipe:
  - step_description: "Filter products with complex text conditions"
    processor_type: "filter_data"
    source_stage: "stg_products"
    # Multiple string conditions with different logic
    pandas_expression: '`Product Name`.str.contains("Premium|Deluxe", case=False) & ~`Product Name`.str.startswith("Test")'
    save_to_stage: "stg_filtered_products"
```

### pandas expression vs regular filters

When to use pandas expressions vs regular filters

```yaml
settings:
  description: "Comparison of pandas expressions vs regular filter conditions"
  stages:
    - stage_name: "stg_orders"
      description: "Order data"
      protected: false
    - stage_name: "stg_simple_filtered"
      description: "Simple filter results"
      protected: false
    - stage_name: "stg_complex_filtered"
      description: "Complex filter results"
      protected: false

recipe:
  - step_description: "Simple filtering (use regular filters)"
    processor_type: "filter_data"
    source_stage: "stg_orders"
    # For simple AND conditions, regular filters are cleaner
    filters:
      - column: "Status"
        condition: "equals"
        value: "Active"
      - column: "Amount"
        condition: "greater_than"
        value: 1000
    save_to_stage: "stg_simple_filtered"

  - step_description: "Complex filtering (use pandas expression)"
    processor_type: "filter_data"
    source_stage: "stg_orders"
    # For OR logic or complex conditions, use pandas expressions
    pandas_expression: '(Status == "Active" & Amount > 1000) | (Status == "Pending" & Priority == "High")'
    save_to_stage: "stg_complex_filtered"
```

### typed variable thresholds

Numeric thresholds from recipe variables via typed references

```yaml
# Recipe variables substitute as STRINGS unless typed at the
# reference site. An untyped "{min_sales}" delivers the TEXT
# "120", and ordering comparisons fail loud rather than guess.
# Typed references - {int:name}, {float:name}, {list_int:name} -
# substitute the actual typed value, preventing the error when
# the value is not meant to be a string. This also works when
# the variable arrives as a string from --variable on the CLI
# or from an interactive prompt: conversion happens at the
# reference, loudly if it cannot.

settings:
  description: "Filter sales rows above a configurable threshold"
  variables:
    # Declared as a number; quoted "120" would also work because
    # the typed reference converts - but declare the real type.
    min_sales: 120
    allowed_tiers: [2, 3]
  stages:
    - stage_name: "stg_sales_raw"
      description: "Imported sales rows"
      protected: false
    - stage_name: "stg_sales_qualified"
      description: "Rows at or above the threshold"
      protected: false

recipe:
  - step_description: "Keep rows meeting the configured threshold"
    processor_type: "filter_data"
    source_stage: "stg_sales_raw"
    save_to_stage: "stg_sales_qualified"
    filters:
      - # REQ - Column name to filter on
        column: "Sales"
        # REQ - Numeric comparison condition
        condition: "greater_equal"
        # REQ - Typed reference: substitutes the int 120, not "120"
        value: "{int:min_sales}"
      - # REQ - Column name to filter on
        column: "Tier"
        # REQ - Membership condition
        condition: "in_list"
        # REQ - Typed list reference: every member converted to
        # int, loudly on failure ({list_any:...} passes mixed
        # members through as declared)
        value: "{list_int:allowed_tiers}"
```

## Parameter notes

- `processor_type` (required): Must be 'filter_data' for this processor type
- `step_description` (default `Unnamed filter_data step`): Human-readable description of the filtering operation
- `source_stage` (required): Stage name to read data from (must be declared in settings.stages)
- `save_to_stage` (required): Stage name to save filtered results (must be declared in settings.stages)
- `filters` (required): List of filter conditions to apply (AND logic between conditions)

