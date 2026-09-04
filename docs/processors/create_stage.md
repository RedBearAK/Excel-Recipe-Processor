# `create_stage`

**Family:** `import`

Create stages from inline data with support for lists, tables, and dictionaries

## Keys

Generated from the declared schema; keys not listed are refused at recipe load.

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `save_to_stage`: stage_out; REQUIRED - Stage to write
- `confirm_stage_replacement`: bool; default false - Required true to overwrite an existing stage
- `data`: mapping; REQUIRED
  - `format`: str; REQUIRED; one of list, table, dictionary
  - `values`: any
  - `columns`: any
  - `rows`: any
  - `data`: any
  - `column`: str
  - `key_column`: str
  - `value_column`: str
- `description`: str; default ""
- `overwrite`: bool; default false

## Examples

Every step below validates against the schema (tests/test_examples_validate_against_schemas.py).

### basic list

Create a simple list stage for filtering approved customers

```yaml
# Create a list of approved customer IDs for filtering

settings:
  description: "Filter orders for approved customers only"
  stages:
    - stage_name: "stg_approved_customers"
      description: "List of approved customer IDs"
      protected: true
    - stage_name: "stg_all_orders"
      description: "All order data"
      protected: false
    - stage_name: "stg_approved_orders"
      description: "Orders from approved customers only"
      protected: false

recipe:
  # Step 1: Create approved customer list
  - # OPT - Human-readable step description
    # Default value: "Unnamed create_stage step"
    step_description: "Create approved customer list"
    # REQ - Must be "create_stage" for this processor type
    processor_type: "create_stage"
    # REQ - Name for the new stage (must be declared in settings.stages)
    save_to_stage: "stg_approved_customers"
    # OPT - Description of what this stage contains
    # Default value: "Stage created by create_stage processor"
    description: "Customer IDs approved for promotional pricing"
    # REQ - Data definition for the stage
    data:
      # REQ - Format type: "list", "table", or "dictionary"
      format: "list"
      # REQ - Column name for the list (format: list only)
      column: "Customer_ID"
      # REQ - List of values (format: list only)
      values:
        - "CUST001"
        - "CUST002"
        - "CUST003"
        - "CUST004"
        - "CUST005"
```

### table mapping

Create a lookup table for region mapping

```yaml
# Create a table mapping states to regions for aggregation

settings:
  description: "Map customer locations to sales regions"
  stages:
    - stage_name: "stg_region_mapping"
      description: "State to region lookup table"
      protected: true
    - stage_name: "stg_customer_data"
      description: "Customer data with state information"
      protected: false

recipe:
  # Step 1: Create region mapping table
  - # OPT - Human-readable step description
    # Default value: "Unnamed create_stage step"
    step_description: "Create state-to-region mapping table"
    # REQ - Must be "create_stage" for this processor type
    processor_type: "create_stage"
    # REQ - Name for the new stage
    save_to_stage: "stg_region_mapping"
    # OPT - Description of what this stage contains
    # Default value: "Stage created by create_stage processor"
    description: "Maps US states to sales regions"
    # REQ - Data definition for the stage
    data:
      # REQ - Format type: "list", "table", or "dictionary"
      format: "table"
      # REQ - Column names for the table (format: table only)
      columns: ["State", "Region", "Territory_Manager"]
      # REQ - List of rows, each with values matching columns (format: table only)
      rows:
        - ["CA", "West", "Sarah Johnson"]
        - ["OR", "West", "Sarah Johnson"]
        - ["WA", "West", "Sarah Johnson"]
        - ["TX", "South", "Mike Davis"]
        - ["FL", "South", "Mike Davis"]
        - ["NY", "East", "Linda Chen"]
        - ["MA", "East", "Linda Chen"]
```

### dictionary lookup

Create a dictionary for customer tier lookups

```yaml
# Create a dictionary mapping customer IDs to tier levels

settings:
  description: "Apply customer tier-based pricing"
  stages:
    - stage_name: "stg_customer_tiers"
      description: "Customer tier assignments"
      protected: true
    - stage_name: "stg_order_data"
      description: "Order data to apply tier pricing"
      protected: false

recipe:
  # Step 1: Create customer tier dictionary
  - # OPT - Human-readable step description
    # Default value: "Unnamed create_stage step"
    step_description: "Create customer tier lookup"
    # REQ - Must be "create_stage" for this processor type
    processor_type: "create_stage"
    # REQ - Name for the new stage
    save_to_stage: "stg_customer_tiers"
    # OPT - Description of what this stage contains
    # Default value: "Stage created by create_stage processor"
    description: "Maps customer IDs to service tier levels"
    # REQ - Data definition for the stage
    data:
      # REQ - Format type: "list", "table", or "dictionary"
      format: "dictionary"
      # REQ - Column name for keys (format: dictionary only)
      key_column: "Customer_ID"
      # REQ - Column name for values (format: dictionary only)
      value_column: "Service_Tier"
      # REQ - Dictionary entries as key-value pairs (format: dictionary only)
      data:
        "CUST001": "Premium"
        "CUST002": "Standard"
        "CUST003": "Premium"
        "CUST004": "Basic"
        "CUST005": "Premium"
        "CUST006": "Standard"
```

### advanced filter criteria

Create complex filter criteria lists for multi-step filtering

```yaml
# Create multiple filter lists for sophisticated data filtering

settings:
  description: "Multi-criteria filtering for product analysis"
  variables:
    min_revenue: "10000"
  stages:
    - stage_name: "stg_priority_products"
      description: "High-priority product codes"
      protected: true
    - stage_name: "stg_excluded_regions"
      description: "Regions to exclude from analysis"
      protected: true
    - stage_name: "stg_raw_sales"
      description: "Raw sales data"
      protected: false
    - stage_name: "stg_filtered_sales"
      description: "Sales after filtering"
      protected: false

recipe:
  # Step 1: Create priority product list
  - # OPT - Human-readable step description
    # Default value: "Unnamed create_stage step"
    step_description: "Define priority products for analysis"
    # REQ - Must be "create_stage" for this processor type
    processor_type: "create_stage"
    # REQ - Name for the new stage
    save_to_stage: "stg_priority_products"
    # OPT - Description of what this stage contains
    # Default value: "Stage created by create_stage processor"
    description: "Product codes flagged as high priority for Q4"
    # OPT - Whether to overwrite if stage already exists
    # Default value: false
    overwrite: false
    # REQ - Data definition for the stage
    data:
      # REQ - Format type
      format: "list"
      # REQ - Column name
      column: "Product_Code"
      # REQ - List of values
      values:
        - "PROD-A100"
        - "PROD-A200"
        - "PROD-B150"
        - "PROD-C300"
        - "PROD-D250"
  
  # Step 2: Create excluded regions list
  - # OPT - Human-readable step description
    # Default value: "Unnamed create_stage step"
    step_description: "Define regions to exclude"
    # REQ - Must be "create_stage" for this processor type
    processor_type: "create_stage"
    # REQ - Name for the new stage
    save_to_stage: "stg_excluded_regions"
    # OPT - Description of what this stage contains
    # Default value: "Stage created by create_stage processor"
    description: "Regions excluded due to incomplete data"
    # REQ - Data definition for the stage
    data:
      # REQ - Format type
      format: "list"
      # REQ - Column name
      column: "Region"
      # REQ - List of values
      values:
        - "Test Market"
        - "Internal"
        - "Discontinued"
  
  # Step 3: Use the created stages in filtering
  - # OPT - Human-readable step description
    # Default value: "Unnamed filter_data step"
    step_description: "Filter for priority products in valid regions"
    # REQ - Must be "filter_data" for this processor type
    processor_type: "filter_data"
    # REQ - Stage to read data from
    source_stage: "stg_raw_sales"
    # REQ - Stage to save filtered results
    save_to_stage: "stg_filtered_sales"
    # REQ - List of filter conditions
    filters:
      - column: "Product_Code"
        condition: "in_stage"
        stage_name: "stg_priority_products"
        stage_column: "Product_Code"
      - column: "Region"
        condition: "not_in_stage"
        stage_name: "stg_excluded_regions"
        stage_column: "Region"
      - column: "Revenue"
        condition: "greater_than"
        value: "{min_revenue}"
```

### configuration driven

Use inline stages for configuration-driven processing

```yaml
# Create configuration stages that control processing behavior

settings:
  description: "Configuration-driven data validation and cleanup"
  stages:
    - stage_name: "stg_validation_rules"
      description: "Column validation configuration"
      protected: true
    - stage_name: "stg_cleanup_mappings"
      description: "Value cleanup mappings"
      protected: true
    - stage_name: "stg_input_data"
      description: "Data to process"
      protected: false

recipe:
  # Step 1: Define validation rules
  - # OPT - Human-readable step description
    # Default value: "Unnamed create_stage step"
    step_description: "Configure validation rules"
    # REQ - Must be "create_stage" for this processor type
    processor_type: "create_stage"
    # REQ - Name for the new stage
    save_to_stage: "stg_validation_rules"
    # OPT - Description of what this stage contains
    # Default value: "Stage created by create_stage processor"
    description: "Defines required columns and their validation rules"
    # REQ - Data definition for the stage
    data:
      # REQ - Format type
      format: "table"
      # REQ - Column names
      columns: ["Column_Name", "Required", "Min_Length", "Pattern"]
      # REQ - List of rows
      rows:
        - ["Customer_ID", "Yes", "5", "^CUST[0-9]+$"]
        - ["Email", "Yes", "5", "^[^@]+@[^@]+$"]
        - ["Phone", "No", "10", "^[0-9-]+$"]
        - ["Status", "Yes", "1", "^(Active|Inactive|Pending)$"]
  
  # Step 2: Define cleanup mappings
  - # OPT - Human-readable step description
    # Default value: "Unnamed create_stage step"
    step_description: "Configure cleanup mappings"
    # REQ - Must be "create_stage" for this processor type
    processor_type: "create_stage"
    # REQ - Name for the new stage
    save_to_stage: "stg_cleanup_mappings"
    # OPT - Description of what this stage contains
    # Default value: "Stage created by create_stage processor"
    description: "Maps common data entry errors to correct values"
    # REQ - Data definition for the stage
    data:
      # REQ - Format type
      format: "dictionary"
      # REQ - Column name for keys
      key_column: "Incorrect_Value"
      # REQ - Column name for values
      value_column: "Correct_Value"
      # REQ - Dictionary entries
      data:
        "Activ": "Active"
        "Inactiv": "Inactive"
        "Pend": "Pending"
        "Y": "Yes"
        "N": "No"
        "": "Unknown"
```

## Parameter notes

- `stage_name` (required): Name for the new stage (must be declared in settings.stages)
- `description` (default `Stage created by create_stage processor`): Human-readable description of what the stage contains
- `overwrite` (default `False`): Whether to overwrite existing stage with same name
- `data` (required): Data definition containing format and content specification

