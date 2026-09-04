# `group_data`

**Family:** `transform`

Group values into categories from various source types and workflows

## Keys

Generated from the declared schema; keys not listed are refused at recipe load.

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `source_stage`: stage_in; REQUIRED - Stage to read
- `save_to_stage`: stage_out; REQUIRED - Stage to write
- `confirm_stage_replacement`: bool; default false - Required true to overwrite an existing stage
- `source_column`: str; REQUIRED
- `target_column`: str - Default: <source_column>_Group
- `groups`: open_mapping - group name -> list of values
- `groups_source`: mapping
  - `type`: str; REQUIRED; one of stage, lookup, file
  - `stage_name`: stage_in
  - `lookup_stage`: stage_in
  - `lookup_key`: str
  - `filename`: str
  - `sheet`: any
  - `encoding`: str
  - `separator`: str
  - `format_type`: str; one of xlsx, csv, tsv
  - `format`: str; default "wide"; one of wide, long - Shape of the definitions table
  - `group_column`: str
  - `group_name_column`: str
  - `values_column`: str
  - `filter_condition`: any
- `groups_file`: str
- `unmatched_action`: str; default "keep_original"; one of keep_original, set_default, error
- `unmatched_value`: any; default "Other"
- `case_sensitive`: bool; default false
- `replace_source`: bool; default false
- at least one of: `groups`, `groups_source`, `groups_file`

## Examples

Every step below validates against the schema (tests/test_examples_validate_against_schemas.py).

### basic

Simple inline grouping of cities into regions

```yaml
settings:
  description: "Group customer cities into sales regions"
  stages:
    - stage_name: "stg_customer_data"
      description: "Raw customer data with city information"
      protected: false
    - stage_name: "stg_regional_customers"
      description: "Customers grouped by sales region"
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

  - # Step 2: Group cities into regions
    # OPT - Human-readable step description
    # Default value: "Unnamed group_data step"
    step_description: "Group customer cities into sales regions"
    # REQ - Must be "group_data" for this processor type
    processor_type: "group_data"
    # REQ - Stage to read data from (must be declared in settings.stages)
    source_stage: "stg_customer_data"
    # REQ - Column containing values to group
    source_column: "City"
    # REQ - New column name for grouped values
    target_column: "Sales_Region"
    # REQ - Group definitions (category: [list of values])
    groups:
      "West Coast": ["Seattle", "Portland", "San Francisco", "Los Angeles"]
      "East Coast": ["New York", "Boston", "Philadelphia", "Washington DC"]
      "Central": ["Chicago", "Detroit", "Minneapolis", "Kansas City"]
      "South": ["Atlanta", "Miami", "Dallas", "Houston"]
    # OPT - How to handle values not in any group
    # Default value: "keep_original"
    # Valid values: "keep_original", "set_default", "error"
    unmatched_action: "set_default"
    # OPT - Default value for unmatched items (when unmatched_action is "set_default")
    # Default value: "Ungrouped"
    unmatched_value: "Other Region"
    # REQ - Stage to save grouped data
    save_to_stage: "stg_regional_customers"
```

### stage based grouping

Load group definitions from another stage for dynamic grouping

```yaml
settings:
  description: "Use stage-based group definitions for flexible product categorization"
  stages:
    - stage_name: "stg_product_data"
      description: "Raw product data"
      protected: false
    - stage_name: "stg_category_definitions"
      description: "Product category group definitions"
      protected: false
    - stage_name: "stg_categorized_products"
      description: "Products with category assignments"
      protected: false

recipe:
  - # Step 1: Import product data
    step_description: "Import product catalog"
    processor_type: "import_file"
    input_file: "data/product_catalog.xlsx"
    save_to_stage: "stg_product_data"

  - # Step 2: Import category definitions
    step_description: "Import category mapping definitions"
    processor_type: "import_file"
    input_file: "config/product_categories.xlsx"
    save_to_stage: "stg_category_definitions"

  - # Step 3: Group products using stage-based definitions
    step_description: "Categorize products using imported definitions"
    processor_type: "group_data"
    source_stage: "stg_product_data"
    # REQ - Column to group
    source_column: "Product_Name"
    # REQ - New column for groups
    target_column: "Product_Category"
    # REQ - Advanced source configuration for group definitions
    groups_source:
      # REQ - Source type for group definitions
      # Valid values: "stage", "file", "lookup"
      type: "stage"
      # REQ - Stage containing group definitions (when type is "stage")
      stage_name: "stg_category_definitions"
      # OPT - Format of group definitions in source
      # Default value: "wide"
      # Valid values: "wide", "long"
      # Wide: Category | Product1 | Product2 | Product3
      # Long: Category | Product_Name (one row per product)
      format: "long"
      # OPT - Column containing group names (for long format)
      # Default value: first column
      group_column: "Category"
      # OPT - Column containing values to match (for long format)
      # Default value: second column
      values_column: "Product_Name"
    # OPT - Handle unmatched products
    unmatched_action: "set_default"
    unmatched_value: "Miscellaneous"
    save_to_stage: "stg_categorized_products"
```

### file based grouping

Load group definitions from external files with variable substitution

```yaml
settings:
  description: "Dynamic grouping using external definition files"
  variables:
    region_type: "sales"
    year: "2024"
  stages:
    - stage_name: "stg_customer_data"
      description: "Customer data for regional analysis"
      protected: false
    - stage_name: "stg_regionally_grouped"
      description: "Customers grouped by dynamic regional definitions"
      protected: false

recipe:
  - # Step 1: Import customer data
    step_description: "Import customer database"
    processor_type: "import_file"
    input_file: "data/customers_{year}.xlsx"
    save_to_stage: "stg_customer_data"

  - # Step 2: Group using external file definitions
    step_description: "Group customers using external regional definitions"
    processor_type: "group_data"
    source_stage: "stg_customer_data"
    source_column: "State"
    target_column: "Sales_Territory"
    # REQ - File-based group source configuration
    groups_source:
      # REQ - Use external file for group definitions
      type: "file"
      # REQ - Path to file containing group definitions (supports variables)
      # Variables: {region_type}, {year} defined in settings
      filename: "config/{region_type}_regions_{year}.xlsx"
      # OPT - File format (wide or long)
      format: "wide"
      # OPT - Sheet name for Excel files
      # Default value: first sheet
      sheet: "Regional_Mapping"
      # OPT - Which row contains group names (for wide format)
      # OPT - Starting column for values (for wide format)
    # OPT - Case sensitivity for matching
    # Default value: false
    case_sensitive: false
    unmatched_action: "keep_original"
    save_to_stage: "stg_regionally_grouped"
```

### hierarchical grouping

Multi-level hierarchical grouping for complex categorization

```yaml
settings:
  description: "Create hierarchical grouping structure for sales analysis"
  stages:
    - stage_name: "stg_sales_data"
      description: "Raw sales transaction data"
      protected: false
    - stage_name: "stg_city_grouped"
      description: "Sales data grouped by city"
      protected: false
    - stage_name: "stg_region_grouped"
      description: "Sales data with city and region grouping"
      protected: false
    - stage_name: "stg_territory_grouped"
      description: "Complete hierarchical grouping structure"
      protected: false

recipe:
  - # Step 1: Import sales data
    step_description: "Import sales transaction data"
    processor_type: "import_file"
    input_file: "data/sales_transactions.xlsx"
    save_to_stage: "stg_sales_data"

  - # Step 2: First level - group by city (cleanup/standardization)
    step_description: "Standardize city names and group variations"
    processor_type: "group_data"
    source_stage: "stg_sales_data"
    source_column: "Customer_City"
    target_column: "Standardized_City"
    groups:
      "New York": ["NYC", "New York City", "Manhattan", "Brooklyn"]
      "Los Angeles": ["LA", "Los Angeles", "Hollywood", "Beverly Hills"]
      "Chicago": ["Chicago", "Chi-town", "Windy City"]
      "San Francisco": ["SF", "San Francisco", "San Fran"]
    # OPT - Replace original column with grouped values
    # Default value: false
    replace_source: false
    unmatched_action: "keep_original"
    save_to_stage: "stg_city_grouped"

  - # Step 3: Second level - group cities into regions
    step_description: "Group standardized cities into sales regions"
    processor_type: "group_data"
    source_stage: "stg_city_grouped"
    source_column: "Standardized_City"
    target_column: "Sales_Region"
    groups:
      "Northeast": ["New York", "Boston", "Philadelphia", "Washington DC"]
      "West": ["Los Angeles", "San Francisco", "Seattle", "Portland"]
      "Midwest": ["Chicago", "Detroit", "Minneapolis", "Cleveland"]
      "South": ["Atlanta", "Miami", "Dallas", "Houston"]
    unmatched_action: "set_default"
    unmatched_value: "Other"
    save_to_stage: "stg_region_grouped"

  - # Step 4: Third level - group regions into territories
    step_description: "Group regions into sales territories"
    processor_type: "group_data"
    source_stage: "stg_region_grouped"
    source_column: "Sales_Region"
    target_column: "Sales_Territory"
    groups:
      "Eastern Territory": ["Northeast", "South"]
      "Western Territory": ["West", "Midwest"]
    unmatched_action: "keep_original"
    save_to_stage: "stg_territory_grouped"
```

### advanced workflow

Complex workflow with lookup-based grouping and validation

```yaml
settings:
  description: "Advanced grouping workflow with cross-reference validation"
  variables:
    validation_date: "2024-03-15"
    department: "sales"
  stages:
    - stage_name: "stg_transaction_data"
      description: "Raw transaction data"
      protected: false
    - stage_name: "stg_validation_data"
      description: "Validation lookup data"
      protected: false
    - stage_name: "stg_grouped_transactions"
      description: "Transactions with validated grouping"
      protected: false
    - stage_name: "stg_final_categorized"
      description: "Final categorized transaction data"
      protected: false

recipe:
  - # Step 1: Import transaction data
    step_description: "Import sales transaction data"
    processor_type: "import_file"
    input_file: "data/{department}_transactions.xlsx"
    save_to_stage: "stg_transaction_data"

  - # Step 2: Import validation lookup data
    step_description: "Import customer validation data"
    processor_type: "import_file"
    input_file: "validation/customer_lookup_{validation_date}.xlsx"
    save_to_stage: "stg_validation_data"

  - # Step 3: Group customers with validation lookup
    step_description: "Group customers with cross-reference validation"
    processor_type: "group_data"
    source_stage: "stg_transaction_data"
    source_column: "Customer_ID"
    target_column: "Customer_Tier"
    # REQ - Lookup-based grouping configuration
    groups_source:
      # REQ - Use lookup/cross-reference grouping
      type: "lookup"
      # REQ - Stage containing lookup data
      lookup_stage: "stg_validation_data"
      # REQ - Column in lookup data to match against
      lookup_key: "Customer_ID"
      # OPT - Join type for lookup
      # Default value: "left"
    # OPT - Handle customers not found in lookup
    unmatched_action: "error"  # Strict validation - fail if customer not found
    save_to_stage: "stg_grouped_transactions"

  - # Step 4: Secondary grouping for analysis categories
    step_description: "Create analysis categories from customer tiers"
    processor_type: "group_data"
    source_stage: "stg_grouped_transactions"
    source_column: "Customer_Tier"
    target_column: "Analysis_Category"
    groups:
      "High Value": ["Premium", "VIP", "Enterprise"]
      "Standard": ["Gold", "Silver", "Standard"]
      "Basic": ["Bronze", "Basic", "Trial"]
    # OPT - Replace the tier column with analysis category
    replace_source: true
    unmatched_action: "set_default"
    unmatched_value: "Unclassified"
    save_to_stage: "stg_final_categorized"
```

## Parameter notes

- `processor_type` (required): Must be 'group_data' for this processor type
- `step_description` (default `Unnamed group_data step`): Human-readable description of what this grouping operation does
- `source_stage` (required): Stage to read data from (must be declared in settings.stages)
- `save_to_stage` (required): Stage to save grouped data (must be declared in settings.stages)
- `source_column` (required): Column containing values to group into categories
- `target_column` (required): Name of new column to contain grouped values
- `groups` (default `{}`): Inline group definitions (traditional approach). Maps group names to lists of values
- `groups_source` (default `None`): Advanced source configuration for group definitions from external sources
- `replace_source` (default `False`): Replace source column with grouped values instead of creating new column
- `unmatched_action` (default `keep_original`): How to handle values that don't match any group
- `unmatched_value` (default `Ungrouped`): Default value for unmatched items (when unmatched_action is 'set_default')
- `case_sensitive` (default `False`): Whether value matching is case sensitive

