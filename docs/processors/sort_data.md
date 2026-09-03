# `sort_data`

**Family:** `transform`

Sort DataFrame rows by one or multiple columns

## Keys

Generated from the declared schema; keys not listed are refused at recipe load.

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `source_stage`: stage_in; REQUIRED - Stage to read
- `save_to_stage`: stage_out; REQUIRED - Stage to write
- `confirm_stage_replacement`: bool; default false - Required true to overwrite an existing stage
- `columns`: list of str; REQUIRED - Column names
- `sort_type`: str; REQUIRED; one of ascending, descending, custom
- `custom_orders`: open_mapping - column -> ordered value list, for sort_type custom
- `case_sensitive`: bool; default false - Excel default: case-insensitive
- `na_position`: str; default "last"; one of first, last

## Examples

Every step below validates against the schema (tests/test_examples_validate_against_schemas.py).

### basic

Simple single column sort using clear sort_type specification

```yaml
settings:
  description: "Sort customer data by name alphabetically"
  stages:
    - stage_name: "stg_customer_data"
      description: "Raw customer data"
      protected: false
    - stage_name: "stg_sorted_customers"
      description: "Customers sorted alphabetically"
      protected: false

recipe:
  - # Step 1: Import customer data
    step_description: "Import customer database"
    processor_type: "import_file"
    input_file: "data/customers.xlsx"
    save_to_stage: "stg_customer_data"

  - # Step 2: Sort by customer name
    # OPT - Human-readable step description
    # Default value: "Unnamed sort_data step"
    step_description: "Sort customers alphabetically by name"
    # REQ - Must be "sort_data" for this processor type
    processor_type: "sort_data"
    # REQ - Stage to read data from (must be declared in settings.stages)
    source_stage: "stg_customer_data"
    # REQ - Column(s) to sort by (accepts single string or list)
    columns: ["Customer_Name"]
    # REQ - Type of sorting to apply
    # Valid values: "ascending", "descending", "custom"
    sort_type: "ascending"
    # REQ - Stage to save sorted data
    save_to_stage: "stg_sorted_customers"
```

### multi column

Multi-column sort with mixed directions using new API

```yaml
settings:
  description: "Sort sales data by region ascending and amount descending"
  stages:
    - stage_name: "stg_sales_data"
      description: "Raw sales transaction data"
      protected: false
    - stage_name: "stg_region_sorted"
      description: "Sales sorted by region ascending"
      protected: false
    - stage_name: "stg_final_sorted"
      description: "Sales sorted by region and amount"
      protected: false

recipe:
  - # Step 1: Import sales data
    step_description: "Import sales transactions"
    processor_type: "import_file"
    input_file: "data/sales_transactions.xlsx"
    save_to_stage: "stg_sales_data"

  - # Step 2: Sort by region (A-Z)
    step_description: "Sort sales by region ascending"
    processor_type: "sort_data"
    source_stage: "stg_sales_data"
    columns: ["Sales_Region"]
    sort_type: "ascending"
    save_to_stage: "stg_region_sorted"

  - # Step 3: Sort by amount (highest first) within regions
    step_description: "Sort by amount descending within regions"
    processor_type: "sort_data"
    source_stage: "stg_region_sorted"
    columns: ["Sales_Region", "Sale_Amount"]
    sort_type: "descending"
    save_to_stage: "stg_final_sorted"
```

### custom order

Sort using custom business logic ordering for categorical data

```yaml
settings:
  description: "Sort orders by priority and status using business logic"
  stages:
    - stage_name: "stg_order_data"
      description: "Order processing queue"
      protected: false
    - stage_name: "stg_prioritized_orders"
      description: "Orders sorted by business priority"
      protected: false

recipe:
  - # Step 1: Import order queue
    step_description: "Import current order queue"
    processor_type: "import_file"
    input_file: "data/order_queue.xlsx"
    save_to_stage: "stg_order_data"

  - # Step 2: Sort by custom priority order
    step_description: "Prioritize orders by customer tier and order status"
    processor_type: "sort_data"
    source_stage: "stg_order_data"
    # REQ - Sort by multiple columns with custom orders
    columns: ["Customer_Tier", "Order_Status"]
    # REQ - Use custom ordering
    sort_type: "custom"
    # REQ - Define custom sort order for specified columns
    # Required when sort_type is "custom"
    custom_orders:
      # Define business priority for customer tiers
      Customer_Tier: ["Platinum", "Gold", "Silver", "Bronze"]
      # Define processing priority for statuses
      Order_Status: ["Urgent", "Rush", "Standard", "Deferred"]
    save_to_stage: "stg_prioritized_orders"
```

### shipment report

Van Report style regional sorting with custom Alaska regional ordering

```yaml
settings:
  description: "Sort Van Report data by region and origin for matrix display"
  stages:
    - stage_name: "stg_van_data"
      description: "Van shipment data"
      protected: false
    - stage_name: "stg_sorted_van_data"
      description: "Van data sorted for report matrix"
      protected: false

recipe:
  - # Step 1: Import Van Report data
    step_description: "Import Van shipment data"
    processor_type: "import_file"
    input_file: "data/shipment_export.xlsx"
    save_to_stage: "stg_van_data"

  - # Step 2: Sort by Alaska regions in geographic order
    step_description: "Sort by regional groups and product origin"
    processor_type: "sort_data"
    source_stage: "stg_van_data"
    columns: ["Regional_Group", "Product_Origin", "Carrier"]
    sort_type: "custom"
    # OPT - Custom Alaska regional ordering
    custom_orders:
      Regional_Group: [
        "Other Region",
        "North Region", 
        "Prince William Sound",
        "Southeast Alaska",
        "Other Alaska"
      ]
      Carrier: ["Matson", "CMA", "MSC", "ONE", "Other"]
    # OPT - Position of null/missing values
    # Default value: "last"
    # Valid values: "first", "last"
    na_position: "last"
    save_to_stage: "stg_sorted_van_data"
```

### case insensitive

Case-insensitive text sorting for mixed-case data

```yaml
settings:
  description: "Sort product catalog ignoring case variations"
  stages:
    - stage_name: "stg_product_catalog"
      description: "Product catalog with mixed case names"
      protected: false
    - stage_name: "stg_sorted_catalog"
      description: "Products sorted alphabetically ignoring case"
      protected: false

recipe:
  - # Step 1: Import product catalog
    step_description: "Import product catalog data"
    processor_type: "import_file"
    input_file: "data/product_catalog.xlsx"
    save_to_stage: "stg_product_catalog"

  - # Step 2: Sort products alphabetically ignoring case
    step_description: "Sort products A-Z treating 'widget' and 'Widget' as equal"
    processor_type: "sort_data"
    source_stage: "stg_product_catalog"
    columns: ["Product_Name", "Category"]
    sort_type: "ascending"
    # OPT - Ignore case differences when sorting text columns
    # Default value: false
    case_sensitive: false
    save_to_stage: "stg_sorted_catalog"
```

### complex business sort

Complex multi-step sort for business reporting

```yaml
settings:
  description: "Create executive dashboard with complex sorting logic"
  stages:
    - stage_name: "stg_performance_data"
      description: "Employee performance metrics"
      protected: false
    - stage_name: "stg_dept_sorted"
      description: "Sorted by department hierarchy"
      protected: false
    - stage_name: "stg_final_dashboard"
      description: "Final executive dashboard order"
      protected: false

recipe:
  - # Step 1: Import performance data
    step_description: "Import quarterly performance metrics"
    processor_type: "import_file"
    input_file: "data/q4_performance.xlsx"
    save_to_stage: "stg_performance_data"

  - # Step 2: First sort by department hierarchy
    step_description: "Sort by executive org structure"
    processor_type: "sort_data"
    source_stage: "stg_performance_data"
    columns: ["Division", "Department"]
    sort_type: "custom"
    custom_orders:
      Division: ["Executive", "Sales", "Operations", "Support", "Admin"]
      Department: [
        "C-Suite", "VP Sales", "Regional Sales", "Inside Sales",
        "Manufacturing", "Logistics", "Quality",
        "Customer Success", "Technical Support",
        "HR", "Finance", "IT"
      ]
    save_to_stage: "stg_dept_sorted"

  - # Step 3: Final sort with performance metrics descending
    step_description: "Sort by performance within organizational hierarchy"
    processor_type: "sort_data"
    source_stage: "stg_dept_sorted"
    columns: ["Revenue_Impact"]
    sort_type: "descending"
    # OPT - Position of null/missing values
    na_position: "last"
    save_to_stage: "stg_final_dashboard"
```

### date time sort

Sort by date and time fields with null handling

```yaml
settings:
  description: "Sort event log by timestamp for timeline analysis"
  stages:
    - stage_name: "stg_event_log"
      description: "System event log data"
      protected: false
    - stage_name: "stg_timeline_sorted"
      description: "Events in chronological order"
      protected: false

recipe:
  - # Step 1: Import event log
    step_description: "Import system event log"
    processor_type: "import_file"
    input_file: "logs/system_events.csv"
    save_to_stage: "stg_event_log"

  - # Step 2: Sort by timestamp (newest first) with nulls
    step_description: "Create timeline with newest events first"
    processor_type: "sort_data"
    source_stage: "stg_event_log"
    columns: ["Event_Date", "Event_Time"]
    sort_type: "descending"  # Newest dates/times first
    # OPT - Put records with missing timestamps at the beginning for investigation
    na_position: "first"
    save_to_stage: "stg_timeline_sorted"
```

## Parameter notes

- `processor_type` (required): Must be 'sort_data' for this processor type
- `step_description` (default `Unnamed sort_data step`): Human-readable description of what this sorting operation does
- `source_stage` (required): Stage to read data from (must be declared in settings.stages)
- `save_to_stage` (required): Stage to save sorted data (must be declared in settings.stages)
- `columns` (required): Column(s) to sort by. For multi-column sorts, order matters for hierarchy
- `sort_type` (required): Type of sorting to apply. Determines the sort algorithm and direction
- `custom_orders` (default `{}`): Define custom sort sequences for categorical columns. Only used with sort_type 'custom'
- `na_position` (default `last`): Where to place null/missing values in sort order
- `case_sensitive` (default `True`): Ignore case differences when sorting text columns

