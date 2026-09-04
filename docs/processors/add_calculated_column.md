# `add_calculated_column`

**Family:** `transform`

Add new columns with calculated values based on existing data

## Notes

- **spill columns**: Extra columns a calculation fills beside new_column, in order; only for expression and first_match; the result width must match
- **first match**: calculation: {pandas_rules: [{when, then: [one slot per column]}], pandas_default: [one slot per column]}; first true 'when' wins the row; "" is the typed blank

## Keys

Generated from the declared schema; keys not listed are refused at recipe load.

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `source_stage`: stage_in; REQUIRED - Stage to read
- `save_to_stage`: stage_out; REQUIRED - Stage to write
- `confirm_stage_replacement`: bool; default false - Required true to overwrite an existing stage
- `new_column`: str; REQUIRED - The calculated column
- `spill_columns`: list of str - Further columns the same calculation fills, in order
- `calculation_type`: str; default "expression"; one of expression, first_match, concat, conditional, math, date, text, constant, row_number
- `overwrite`: bool; default false
- when `calculation_type` = `expression`:
  - `calculation`: mapping; REQUIRED
    - `pandas_formula`: str - pandas text with {col:Name} references
    - `formula_components`: list of any - Structured column / operator / value parts
    - at least one of: `pandas_formula`, `formula_components`
- when `calculation_type` = `first_match`:
  - `calculation`: mapping; REQUIRED
    - `pandas_rules`: list_of_mappings; REQUIRED
      - `when`: str; REQUIRED - pandas predicate, one boolean per row
      - `then`: list of any; REQUIRED - One slot per declared column: expression, quoted literal, number, or ""
    - `pandas_default`: list of any; REQUIRED - Slots when no rule matches; same shape as a then
- when `calculation_type` = `concat`:
  - `calculation`: mapping; REQUIRED
    - `columns`: list of str; REQUIRED - Column names
    - `separator`: str; default ""
- when `calculation_type` = `conditional`:
  - `calculation`: mapping; REQUIRED
    - `condition_column`: str; REQUIRED
    - `condition`: str; REQUIRED; one of equals, greater_than, less_than, contains, is_null, not_null
    - `condition_value`: any
    - `value_if_true`: any; REQUIRED
    - `value_if_false`: any; REQUIRED
- when `calculation_type` = `math`:
  - `calculation`: mapping; REQUIRED
    - `operation`: str; REQUIRED; one of add, subtract, multiply, divide, sum, mean, min, max
    - `column1`: str
    - `column2`: str
    - `columns`: list of str - Column names
- when `calculation_type` = `date`:
  - `calculation`: mapping; REQUIRED
    - `operation`: str; REQUIRED; one of days_between
    - `start_date_column`: str; REQUIRED
    - `end_date_column`: str; REQUIRED
- when `calculation_type` = `text`:
  - `calculation`: mapping; REQUIRED
    - `operation`: str; REQUIRED; one of length, upper, lower, extract_numbers, substring
    - `column`: str; REQUIRED
    - `start`: int; default 0
    - `length`: int
- when `calculation_type` = `constant`:
  - `calculation`: mapping; REQUIRED
    - `value`: any; REQUIRED
- when `calculation_type` = `row_number`:
  - `calculation`: mapping; REQUIRED
    - `start`: int; default 1

## Examples

Every step below validates against the schema (tests/test_examples_validate_against_schemas.py).

### basic formula components

Simple mathematical calculation using robust formula_components syntax

```yaml
# Create a total value column by multiplying price and quantity with explicit components

settings:
  description: "Calculate order totals from price and quantity using formula_components"
  stages:
    - stage_name: "stg_order_data"
      description: "Raw order data"
      protected: false
    - stage_name: "stg_calculated_orders"
      description: "Orders with calculated totals"
      protected: false

recipe:
  # Previous steps populate order_data stage
  - step_description: "Calculate total order value using formula_components"
    # REQ - Must be "add_calculated_column" for this processor type
    processor_type: "add_calculated_column"
    # REQ - Stage to read data from
    source_stage: "stg_order_data"
    # REQ - Stage to save results with new column
    save_to_stage: "stg_calculated_orders"
    # REQ - Name of the new column to create
    new_column: "Total_Value"
    # OPT - Type of calculation to perform
    # Default value: "expression"
    calculation_type: "expression"
    # REQ - Calculation configuration dictionary
    calculation:
      # REQ - Robust list-based formula (recommended for all column names)
      # Each element is either a column name, operator, or value
      # No quoting needed - handles spaces and special characters perfectly
      formula_components: ["Price", "*", "Quantity"]
```

### complex formula components

Complex calculations with grouping and multiple operations

```yaml
# Calculate profit margin percentage with proper grouping

settings:
  description: "Calculate complex profit metrics using formula_components"
  stages:
    - stage_name: "stg_financial_data"
      description: "Revenue and cost data with spaces in column names"
      protected: false
    - stage_name: "stg_profit_analysis"
      description: "Financial data with calculated profit margins"
      protected: false

recipe:
  - step_description: "Calculate profit margin percentage"
    processor_type: "add_calculated_column"
    source_stage: "stg_financial_data"
    save_to_stage: "stg_profit_analysis"
    new_column: "Profit_Margin_Percent"
    calculation_type: "expression"
    calculation:
      # Complex formula: ((Revenue - Cost) / Revenue) * 100
      # Nested lists create grouping (parentheses)
      formula_components: [
        [["Revenue", "-", "Total Cost"], "/", "Revenue"], "*", "100"
      ]
```

### conditional formula components

Conditional logic with formula_components for business rules

```yaml
# Apply different pricing based on order quantity with complex conditions

settings:
  description: "Dynamic pricing based on quantity tiers"
  stages:
    - stage_name: "stg_order_data"
      description: "Order data with quantities"
      protected: false
    - stage_name: "stg_priced_orders"
      description: "Orders with tier-based pricing"
      protected: false

recipe:
  - step_description: "Apply quantity-based pricing tiers"
    processor_type: "add_calculated_column"
    source_stage: "stg_order_data"
    save_to_stage: "stg_priced_orders"
    new_column: "Final_Price"
    calculation_type: "expression"
    calculation:
      # Conditional pricing: if quantity >= 100 then bulk rate, else standard rate
      formula_components: [
        {
          "condition": {
            "column": "Order Quantity",
            "operator": ">=", 
            "value": 100
          },
          "if_true": ["Unit Price", "*", "Order Quantity", "*", "0.85"],
          "if_false": ["Unit Price", "*", "Order Quantity"]
        }
      ]
```

### mixed conditional

Complex business logic with multiple conditions and calculations

```yaml
# Calculate shipping cost with customer tier discounts and weight factors

settings:
  description: "Complex shipping calculation with multiple business rules"
  stages:
    - stage_name: "stg_shipping_data"
      description: "Orders with customer and weight data"
      protected: false
    - stage_name: "stg_final_shipping"
      description: "Orders with calculated shipping costs"
      protected: false

recipe:
  - step_description: "Calculate shipping with tier discounts and weight factors"
    processor_type: "add_calculated_column"
    source_stage: "stg_shipping_data"
    save_to_stage: "stg_final_shipping"
    new_column: "Shipping_Cost"
    calculation_type: "expression"
    calculation:
      # Base shipping + weight surcharge - customer discount
      formula_components: [
        "15.00", "+",
        {
          "condition": {
            "column": "Package Weight (lbs)",
            "operator": ">",
            "value": 50
          },
          "if_true": [["Package Weight (lbs)", "-", "50"], "*", "0.25"],
          "if_false": "0"
        },
        "-",
        {
          "condition": {
            "column": "Customer Tier",
            "operator": "==",
            "value": "Premium"
          },
          "if_true": "5.00",
          "if_false": "0"
        }
      ]
```

### string conditions

String-based conditions with contains and membership operations

```yaml
# Categorize products based on name patterns and categories

settings:
  description: "Product categorization using string matching"
  stages:
    - stage_name: "stg_product_data"
      description: "Product catalog data"
      protected: false
    - stage_name: "stg_categorized_products"
      description: "Products with calculated categories"
      protected: false

recipe:
  - step_description: "Assign product categories based on name and type"
    processor_type: "add_calculated_column"
    source_stage: "stg_product_data"
    save_to_stage: "stg_categorized_products"
    new_column: "Product_Category"
    calculation_type: "expression"
    calculation:
      formula_components: [
        {
          "condition": {
            "column": "Product Name",
            "operator": "contains",
            "value": "Premium"
          },
          "if_true": "'High-End'",
          "if_false": {
            "condition": {
              "column": "Product Type",
              "operator": "in",
              "value": ["Electronics", "Computers", "Mobile"]
            },
            "if_true": "'Technology'",
            "if_false": "'Standard'"
          }
        }
      ]
```

### legacy formula

Legacy formula syntax (still supported but limited)

```yaml
# Legacy string-based formula (has limitations with column names containing spaces)

settings:
  description: "Legacy formula approach for simple cases"
  stages:
    - stage_name: "stg_simple_data"
      description: "Data with simple column names"
      protected: false
    - stage_name: "stg_calculated_data"
      description: "Data with calculations"
      protected: false

recipe:
  - step_description: "Calculate using legacy formula syntax"
    processor_type: "add_calculated_column"
    source_stage: "stg_simple_data"
    save_to_stage: "stg_calculated_data"
    new_column: "Total"
    calculation_type: "expression"
    calculation:
      # Legacy formula - works only with simple column names (no spaces/special chars)
      # For robust column name handling, use formula_components instead
      pandas_formula: "{col:Price} * {col:Quantity}"
```

### spill columns

One calculation filling two columns - a horizontal spill, declared and shape-checked

```yaml
# An expression may return more than one column, the way an Excel
# formula spills sideways. new_column is the calculated column; the
# rest are named in spill_columns, in order. The result's width must
# match the declaration and every column must be one value per row -
# an undeclared spill, a missing spill, or a vertical spill is an error.

settings:
  description: "Due date and the reason it was chosen, from one calculation"
  stages:
    - stage_name: "stg_orders"
      description: "Orders with a ship date and payment terms"
      protected: false
    - stage_name: "stg_orders_dated"
      description: "Orders with a due date and its basis"
      protected: false

recipe:
  - step_description: "Compute the due date and its basis together"
    processor_type: "add_calculated_column"
    source_stage: "stg_orders"
    save_to_stage: "stg_orders_dated"
    # REQ - The calculated column
    new_column: "Due Date"
    # OPT - Further columns the same calculation fills, left to right.
    # Only the evaluated calculation types (expression, first_match) may spill.
    spill_columns: ["Due Basis"]
    calculation_type: "expression"
    calculation:
      # A DataFrame (or a tuple of Series) with one column per declared name
      pandas_formula: "pd.DataFrame({'d': {col:Ship Date} + pd.to_timedelta({col:Terms Days}, unit='D'), 'b': 'Ship + terms'})"
```

### first match

Ordered rule table - first true predicate wins the row and fills every declared column

```yaml
# The IFS / CASE of the processor, with paired outputs. Rules are
# tried in order; the first whose 'when' is true supplies EVERY
# declared column from its own 'then' row, so a value and the label
# explaining it can never drift apart. Every rule and the default
# carry exactly one slot per declared column: an expression, a
# quoted literal or a number (broadcast), or "" for the column's
# typed blank - a blank is always visible, never an omitted term.
#
# Keys name their dialect: pandas_rules and pandas_default hold
# pandas text with {col:Name} references, so a quoted string inside
# a slot is a literal exactly as it would be inside pandas_formula.
# Shape and column references are checked for every rule BEFORE any
# evaluation, so a mistake is reported by rule number. The log
# reports how many rows each rule won, including rules that never
# matched.

settings:
  description: "Projected due date with an audit tag, by first matching rule"
  stages:
    - stage_name: "stg_orders"
      description: "Orders with kind, dates, terms and price"
      protected: false
    - stage_name: "stg_orders_projected"
      description: "Orders with a projected due date and the branch that produced it"
      protected: false

recipe:
  - step_description: "Project the due date by first matching rule"
    processor_type: "add_calculated_column"
    source_stage: "stg_orders"
    save_to_stage: "stg_orders_projected"
    new_column: "Proj Due Date"
    spill_columns: ["Diagnostics"]
    calculation_type: "first_match"
    calculation:
      # REQ - Ordered rules; each 'then' has one slot per declared column,
      # in the order new_column, then spill_columns
      pandas_rules:
        - when: "{col:Paid Date}.notna() | ~({col:Price}.fillna(0) > 0)"
          then: ["", "'Paid or No Price'"]
        - when: "({col:Kind} == 'Fresh') & {col:Ship Date}.notna()"
          then: ["{col:Ship Date} + pd.to_timedelta({col:Terms Days} + 7, unit='D')", "'Fr-Ship'"]
        - when: "{col:Kind} == 'Fresh'"
          then: ["", "'Fr-NoShip'"]
        - when: "({col:Kind} == 'Frozen') & {col:Invoice Date}.notna()"
          then: ["{col:Invoice Date} + pd.to_timedelta({col:Terms Days}, unit='D')", "'Fz-Invoice'"]
        - when: "({col:Kind} == 'Frozen') & {col:Ship Date}.notna()"
          then: ["{col:Ship Date} + pd.to_timedelta({col:Terms Days} + 30, unit='D')", "'Fz-ShipFallback'"]
      # REQ - What a row gets when no rule matches, same shape as a 'then'
      pandas_default: ["", "'Unknown-Kind'"]
```

### row number

Number rows 1..N as a sort anchor for order-dependent sheets

```yaml
# A display sheet whose meaning depends on row order (blanked repeated
# labels, running groups) gets a column that lets anyone sort BACK to
# baseline after re-sorting in Excel. Add it after the final sort and
# before any order-dependent display step.

settings:
  description: "Sort anchor for a display sheet"
  stages:
    - stage_name: "stg_summary"
      description: "Summary in its shipping order"
      protected: false
    - stage_name: "stg_summary_numbered"
      description: "Summary with the Sort Order anchor"
      protected: false

recipe:
  - step_description: "Number the baseline row order"
    processor_type: "add_calculated_column"
    source_stage: "stg_summary"
    new_column: "Sort Order"
    calculation_type: "row_number"
    calculation:
      # REQ - Number of the first row (the calculation dict cannot be
      # empty, so state the start explicitly)
      start: 1
    save_to_stage: "stg_summary_numbered"
```

## Parameter notes

- `formula_components`: Robust list-based formula specification that handles any column names
- `spill_columns`: Further columns the same calculation fills beside new_column, in order. Only expression and first_match may spill. The result's width must equal 1 + len(spill_columns) and every column must be one value per row; otherwise the step halts.

- `first_match`: First true 'when' wins the row and every declared column takes the slot from that rule's 'then'. A slot is pandas text: an expression, a quoted literal or number, or "" for the typed blank. Rule shape and column references are validated before evaluation; per-rule hit counts are logged.

- `pandas_formula`: Legacy string-based formula (limited column name support)

