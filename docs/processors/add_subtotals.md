# `add_subtotals`

**Family:** `transform`

Add subtotal rows to grouped data with various aggregation functions

## Keys

Generated from the declared schema; keys not listed are refused at recipe load.

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `source_stage`: stage_in; REQUIRED - Stage to read
- `save_to_stage`: stage_out; REQUIRED - Stage to write
- `confirm_stage_replacement`: bool; default false - Required true to overwrite an existing stage
- `group_by`: list of str; REQUIRED - Column names
- `subtotal_columns`: list of str; REQUIRED - Column names
- `subtotal_functions`: list of str; default ["sum"]
- `subtotal_label`: str; default "Subtotal"
- `position`: str; default "after_group"; one of after_group, before_group
- `preserve_totals`: bool; default true

## Examples

Every step below validates against the schema (tests/test_examples_validate_against_schemas.py).

### basic

Simple regional sales subtotals

```yaml
settings:
  description: "Create sales report with regional subtotals"
  stages:
    - stage_name: "stg_sales_data"
      description: "Raw sales data from import"
      protected: false
    - stage_name: "stg_sales_with_subtotals"
      description: "Sales data with regional subtotals"
      protected: false

recipe:
  - # Step 1: Import sales data
    step_description: "Import monthly sales data"
    # REQ - Must be "import_file" for file import
    processor_type: "import_file"
    # REQ - Path to input Excel file
    input_file: "data/monthly_sales.xlsx"
    # REQ - Stage to save imported data (import_file only needs save_to_stage)
    save_to_stage: "stg_sales_data"

  - # Step 2: Add regional subtotals
    # OPT - Human-readable step description
    # Default value: "Unnamed add_subtotals step"
    step_description: "Add regional sales subtotals"
    # REQ - Must be "add_subtotals" for this processor type
    processor_type: "add_subtotals"
    # REQ - Stage to read data from (must be declared in settings.stages)
    source_stage: "stg_sales_data"
    # REQ - Columns to group by for subtotal calculation
    # Groups data by these columns before adding subtotal rows
    group_by: ["Region"]
    # REQ - Columns to calculate subtotals for
    # Must be numeric columns for mathematical operations
    subtotal_columns: ["Sales_Amount", "Quantity_Sold"]
    # OPT - Aggregation functions to use for each subtotal column
    # Default value: ["sum"] for all columns
    # Must match length of subtotal_columns or use single value for all
    subtotal_functions: ["sum", "sum"]
    # OPT - Label text for subtotal rows
    # Default value: "Subtotal"
    subtotal_label: "Regional Total"
    # REQ - Stage to save data with subtotals
    save_to_stage: "stg_sales_with_subtotals"
```

### hierarchical

Multi-level hierarchical subtotals for complex reporting

```yaml
settings:
  description: "Create hierarchical financial report with division and department subtotals"
  stages:
    - stage_name: "stg_financial_data" 
      description: "Imported financial data"
      protected: false
    - stage_name: "stg_sorted_data"
      description: "Data sorted for proper grouping"
      protected: false
    - stage_name: "stg_division_subtotals"
      description: "Data with division-level subtotals"
      protected: false
    - stage_name: "stg_department_subtotals"
      description: "Data with department-level subtotals"
      protected: false

recipe:
  - # Step 1: Import financial data
    step_description: "Import quarterly financial report"
    processor_type: "import_file"
    input_file: "reports/Q4_financial_data.xlsx"
    save_to_stage: "stg_financial_data"

  - # Step 2: Sort data for proper grouping
    step_description: "Sort by division and department for hierarchical subtotals"
    processor_type: "sort_data"
    source_stage: "stg_financial_data"
    # REQ - Columns to sort by (should match grouping hierarchy)
    columns: ["Division", "Department", "Cost_Center"]
    # OPT - Sort order for each column
    # Default value: [true] for all columns
    sort_type: "ascending"
    save_to_stage: "stg_sorted_data"

  - # Step 3: Add division-level subtotals
    step_description: "Add division-level financial subtotals"
    processor_type: "add_subtotals"
    source_stage: "stg_sorted_data"
    # REQ - Group by division for high-level subtotals
    group_by: ["Division"]
    # REQ - Financial columns to subtotal
    subtotal_columns: ["Revenue", "Expenses", "Net_Income"]
    # OPT - Use sum for all financial aggregations
    subtotal_functions: ["sum", "sum", "sum"]
    # OPT - Clear label for division subtotals
    subtotal_label: "Division Total"
    save_to_stage: "stg_division_subtotals"

  - # Step 4: Add department-level subtotals  
    step_description: "Add department-level subtotals within divisions"
    processor_type: "add_subtotals"
    source_stage: "stg_division_subtotals"
    # REQ - Multi-level grouping for hierarchical subtotals
    # Creates subtotals for each Division/Department combination
    group_by: ["Division", "Department"]
    subtotal_columns: ["Revenue", "Expenses", "Net_Income"]
    subtotal_functions: ["sum", "sum", "sum"]
    subtotal_label: "Department Total"
    save_to_stage: "stg_department_subtotals"
```

### multiple functions

Use different aggregation functions for comprehensive analysis

```yaml
settings:
  description: "Customer analysis with multiple aggregation functions"
  stages:
    - stage_name: "stg_customer_data"
      description: "Raw customer transaction data"
      protected: false
    - stage_name: "stg_customer_analysis"
      description: "Customer data with comprehensive subtotals"
      protected: false

recipe:
  - # Step 1: Import customer data
    step_description: "Import customer transaction data"
    processor_type: "import_file"
    input_file: "data/customer_transactions.xlsx"
    save_to_stage: "stg_customer_data"

  - # Step 2: Add customer tier analysis with multiple functions
    step_description: "Analyze customer performance by tier"
    processor_type: "add_subtotals"
    source_stage: "stg_customer_data"
    # REQ - Group by customer tier
    group_by: ["Customer_Tier"]
    # REQ - Multiple columns for comprehensive analysis
    subtotal_columns: ["Order_Value", "Order_Count", "Days_Since_Order", "Customer_Satisfaction"]
    # OPT - Different aggregation functions for different insights
    # sum for totals, count for frequency, mean for averages, max for extremes
    subtotal_functions: ["sum", "count", "mean", "max"]
    # OPT - Descriptive label for business context
    subtotal_label: "Tier Summary"
    save_to_stage: "stg_customer_analysis"
```

### pivot enhancement

Enhance existing pivot table results with subtotals

```yaml
settings:
  description: "Create pivot table and enhance with custom subtotals"
  stages:
    - stage_name: "stg_product_data"
      description: "Raw product sales data"
      protected: false
    - stage_name: "stg_pivot_results"
      description: "Pivot table cross-tabulation"
      protected: false
    - stage_name: "stg_enhanced_pivot"
      description: "Pivot table with custom subtotals"
      protected: false

recipe:
  - # Step 1: Import product data
    step_description: "Import product sales data"
    processor_type: "import_file"
    input_file: "data/product_sales.xlsx"
    save_to_stage: "stg_product_data"

  - # Step 2: Create pivot table
    step_description: "Create sales pivot by region and product"
    processor_type: "pivot_table"
    source_stage: "stg_product_data"
    # REQ - Pivot table configuration
    # Rows: Region, Product_Category
    index: ["Region", "Product_Category"]
    # Columns: Quarter
    columns: ["Quarter"]
    # Values: Sales_Amount
    values: ["Sales_Amount"]
    # REQ - Aggregation function for pivot
    aggfunc: "sum"
    # OPT - Include grand totals
    # Default value: false
    margins: true
    save_to_stage: "stg_pivot_results"

  - # Step 3: Add custom subtotals to pivot results
    step_description: "Add regional subtotals to pivot table"
    processor_type: "add_subtotals"
    source_stage: "stg_pivot_results"
    # REQ - Group by region (first level of original pivot)
    group_by: ["Region"]
    # REQ - Subtotal the quarterly columns created by pivot
    # Note: Column names will be Q1, Q2, Q3, Q4 from pivot operation
    subtotal_columns: ["Q1", "Q2", "Q3", "Q4"]
    subtotal_functions: ["sum", "sum", "sum", "sum"]
    subtotal_label: "Regional Total"
    # OPT - Preserve existing grand totals from pivot
    # Default value: true
    preserve_totals: true
    save_to_stage: "stg_enhanced_pivot"
```

### advanced workflow

Complex multi-step workflow with filtering and subtotal positioning

```yaml
settings:
  description: "Advanced workflow with data filtering, sorting, and positioned subtotals"
  variables:
    year: "2024"
    min_sales: "1000"
  stages:
    - stage_name: "stg_raw_data"
      description: "Complete imported dataset"
      protected: false
    - stage_name: "stg_filtered_data"
      description: "Data filtered for analysis criteria"
      protected: false
    - stage_name: "stg_prepared_data"
      description: "Data sorted and prepared for subtotals"
      protected: false
    - stage_name: "stg_subtotaled_data"
      description: "Final data with positioned subtotals"
      protected: false

recipe:
  - # Step 1: Import complete dataset
    step_description: "Import annual sales data"
    processor_type: "import_file"
    # REQ - File path with variable substitution
    # Variables: {year} defined in settings
    input_file: "data/sales_{year}.xlsx"
    save_to_stage: "stg_raw_data"

  - # Step 2: Filter for significant sales only
    step_description: "Filter for sales above minimum threshold"
    processor_type: "filter_data"
    source_stage: "stg_raw_data"
    # REQ - Filter criteria
    filters:
      - column: "Sales_Amount"
        condition: "greater_than"
        value: 1000
      - column: "Status"
        condition: "equals"
        value: "Closed"
    save_to_stage: "stg_filtered_data"

  - # Step 3: Sort for optimal subtotal grouping
    step_description: "Sort data for proper subtotal grouping"
    processor_type: "sort_data"
    source_stage: "stg_filtered_data"
    columns: ["Territory", "Sales_Rep", "Deal_Size"]
    sort_type: "ascending"
    save_to_stage: "stg_prepared_data"

  - # Step 4: Add subtotals with custom positioning
    step_description: "Add territory subtotals with advanced configuration"
    processor_type: "add_subtotals"
    source_stage: "stg_prepared_data"
    # REQ - Multi-level grouping for detailed analysis
    group_by: ["Territory", "Sales_Rep"]
    # REQ - Multiple analysis columns
    subtotal_columns: ["Sales_Amount", "Commission", "Deal_Count", "Avg_Deal_Size"]
    # OPT - Mixed aggregation functions for comprehensive insights
    subtotal_functions: ["sum", "sum", "count", "mean"]
    # OPT - Professional labeling
    subtotal_label: "Territory Summary"
    # OPT - Position subtotals after each group
    # Default value: "after_group"
    # Valid values: "before_group", "after_group"
    position: "after_group"
    # OPT - Preserve any existing totals
    preserve_totals: true
    save_to_stage: "stg_subtotaled_data"
```

## Parameter notes

- `processor_type` (required): Must be 'add_subtotals' for this processor type
- `step_description` (default `Unnamed add_subtotals step`): Human-readable description of what this step does
- `source_stage` (required): Stage to read data from (must be declared in settings.stages)
- `save_to_stage` (required): Stage to save data with subtotals (must be declared in settings.stages)
- `group_by` (required): Columns to group by for subtotal calculation. Data is grouped by these columns before adding subtotal rows
- `subtotal_columns` (required): Columns to calculate subtotals for. Must be numeric columns for mathematical operations
- `subtotal_functions` (default `['sum']`): Aggregation functions to use for each subtotal column. Must match length of subtotal_columns or use single value for all
- `subtotal_label` (default `Subtotal`): Label text that appears in subtotal rows to identify them clearly
- `position` (default `after_group`): Where to position subtotal rows relative to their data groups
- `preserve_totals` (default `True`): Whether to preserve existing grand total rows (useful when enhancing pivot table results)

