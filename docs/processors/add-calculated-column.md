# Add Calculated Column Processor

Create new columns with calculated values based on existing data.

## Overview

The `add_calculated_column` processor creates new columns using mathematical expressions, conditional logic, string operations, and date calculations. It's like having Excel formulas directly in your recipe.

## Basic Usage

```yaml
- processor_type: "add_calculated_column"
  new_column: "Total_Value"
  calculation:
    type: "expression"
    expression: "Price * Quantity"
```

This creates a `Total_Value` column by multiplying `Price` and `Quantity`.

## Calculation Types

### Expression (Default)
General pandas-style calculations using column names:

```yaml
- processor_type: "add_calculated_column"
  new_column: "Total_Value"
  calculation:
    type: "expression"
    expression: "Price * Quantity * (1 + Tax_Rate)"
```

### Mathematical Operations
Specific math operations between columns:

```yaml
- processor_type: "add_calculated_column"
  new_column: "Profit"
  calculation:
    type: "math"
    operation: "subtract"
    column1: "Revenue"
    column2: "Cost"
```

### String Concatenation
Combine text columns:

```yaml
- processor_type: "add_calculated_column"
  new_column: "Full_Name"
  calculation:
    type: "concat"
    columns: ["First_Name", "Last_Name"]
    separator: " "
```

### Conditional Logic
If-then-else operations:

```yaml
- processor_type: "add_calculated_column"
  new_column: "Category"
  calculation:
    type: "conditional"
    condition_column: "Order_Value"
    condition: "greater_than"
    condition_value: 1000
    value_if_true: "High Value"
    value_if_false: "Standard"
```

### Date Calculations
Date arithmetic:

```yaml
- processor_type: "add_calculated_column"
  new_column: "Processing_Days"
  calculation:
    type: "date"
    operation: "days_between"
    start_date_column: "Order_Date"
    end_date_column: "Ship_Date"
```

### Text Operations
String manipulations:

```yaml
- processor_type: "add_calculated_column"
  new_column: "Product_Code_Length"
  calculation:
    type: "text"
    operation: "length"
    column: "Product_Code"
```

## Configuration Options

### Required Fields
```yaml
- processor_type: "add_calculated_column"
  new_column: "Column_Name"       # Required: name of new column
  calculation: {...}              # Required: calculation definition
```

### Complete Configuration
```yaml
- processor_type: "add_calculated_column"
  new_column: "Calculated_Field"        # Name of new column
  calculation_type: "expression"        # Type of calculation (optional)
  calculation: {...}                    # Calculation definition
  overwrite: false                      # Overwrite existing column (default: false)
```

## Real-World Examples

### Business Calculations
```yaml
# Calculate commission
- step_description: "Calculate sales commission"
  processor_type: "add_calculated_column"
  new_column: "Commission"
  calculation:
    type: "expression"
    expression: "Sales_Amount * Commission_Rate"

# Add markup to cost
- step_description: "Calculate selling price with markup"
  processor_type: "add_calculated_column"
  new_column: "Selling_Price"
  calculation:
    type: "math"
    operation: "multiply"
    column1: "Cost"
    column2: "Markup_Factor"

# Calculate profit margin percentage
- step_description: "Calculate profit margin"
  processor_type: "add_calculated_column"
  new_column: "Profit_Margin_Pct"
  calculation:
    type: "expression"
    expression: "((Revenue - Cost) / Revenue) * 100"
```

### Customer Categorization
```yaml
- step_description: "Categorize customers by order value"
  processor_type: "add_calculated_column"
  new_column: "Customer_Tier"
  calculation:
    type: "conditional"
    condition_column: "Total_Orders_Value"
    condition: "greater_than"
    condition_value: 5000
    value_if_true: "Premium"
    value_if_false: "Standard"

# Multiple tier logic (requires multiple steps)
- step_description: "Refine customer tiers"
  processor_type: "add_calculated_column"
  new_column: "Detailed_Tier"
  calculation:
    type: "conditional"
    condition_column: "Total_Orders_Value"
    condition: "greater_than"
    condition_value: 25000
    value_if_true: "Platinum"
    value_if_false: "Premium"  # Previously calculated tier
```

### Van Report Calculations
```yaml
# Calculate days since shipment
- step_description: "Calculate days in transit"
  processor_type: "add_calculated_column"
  new_column: "Days_In_Transit"
  calculation:
    type: "date"
    operation: "days_between"
    start_date_column: "Ship_Date"
    end_date_column: "Current_Date"

# Create container description
- step_description: "Create container description"
  processor_type: "add_calculated_column"
  new_column: "Container_Description"
  calculation:
    type: "concat"
    columns: ["Van_Number", "Carrier", "Destination"]
    separator: " - "

# Flag high-value shipments
- step_description: "Flag high-value containers"
  processor_type: "add_calculated_column"
  new_column: "High_Value_Flag"
  calculation:
    type: "conditional"
    condition_column: "Container_Value"
    condition: "greater_than"
    condition_value: 100000
    value_if_true: "High Value"
    value_if_false: "Standard"
```

## Mathematical Operations

### Binary Operations
```yaml
# Addition
calculation:
  type: "math"
  operation: "add"
  column1: "Base_Price"
  column2: "Tax_Amount"

# Subtraction
calculation:
  type: "math"
  operation: "subtract"
  column1: "Gross_Amount"
  column2: "Discount"

# Multiplication
calculation:
  type: "math"
  operation: "multiply"
  column1: "Quantity"
  column2: "Unit_Price"

# Division
calculation:
  type: "math"
  operation: "divide"
  column1: "Total_Sales"
  column2: "Number_of_Orders"
```

### Aggregation Operations
```yaml
# Sum multiple columns
calculation:
  type: "math"
  operation: "sum"
  columns: ["Q1_Sales", "Q2_Sales", "Q3_Sales", "Q4_Sales"]

# Average of columns
calculation:
  type: "math"
  operation: "mean"
  columns: ["Score1", "Score2", "Score3"]

# Maximum value across columns
calculation:
  type: "math"
  operation: "max"
  columns: ["Bid1", "Bid2", "Bid3"]

# Minimum value across columns
calculation:
  type: "math"
  operation: "min"
  columns: ["Cost_Option1", "Cost_Option2", "Cost_Option3"]
```

## Complex Expressions

### Pandas-Style Formulas
```yaml
# Complex business logic
- processor_type: "add_calculated_column"
  new_column: "Adjusted_Price"
  calculation:
    type: "expression"
    expression: "Base_Price * (1 + Tax_Rate) * (1 - Discount_Rate) + Shipping_Fee"

# Conditional expressions
- processor_type: "add_calculated_column"
  new_column: "Shipping_Cost"
  calculation:
    type: "expression"
    expression: "Order_Weight * 2.5 if Order_Weight < 50 else Order_Weight * 1.8"

# String operations in expressions
- processor_type: "add_calculated_column"
  new_column: "Product_Category_Length"
  calculation:
    type: "expression"
    expression: "Product_Category.str.len()"
```

## Conditional Logic

### Basic Conditions
```yaml
# Equals
calculation:
  type: "conditional"
  condition_column: "Status"
  condition: "equals"
  condition_value: "Active"
  value_if_true: "Current"
  value_if_false: "Inactive"

# Greater than
calculation:
  type: "conditional"
  condition_column: "Score"
  condition: "greater_than"
  condition_value: 80
  value_if_true: "Pass"
  value_if_false: "Fail"

# Contains text
calculation:
  type: "conditional"
  condition_column: "Product_Name"
  condition: "contains"
  condition_value: "Premium"
  value_if_true: "High End"
  value_if_false: "Standard"

# Check for null values
calculation:
  type: "conditional"
  condition_column: "Manager"
  condition: "is_null"
  value_if_true: "Unassigned"
  value_if_false: "Assigned"
```

### Multi-Level Conditions
```yaml
# First level: High/Medium/Low based on amount
- step_description: "Initial categorization"
  processor_type: "add_calculated_column"
  new_column: "Initial_Category"
  calculation:
    type: "conditional"
    condition_column: "Order_Amount"
    condition: "greater_than"
    condition_value: 1000
    value_if_true: "High"
    value_if_false: "Medium/Low"

# Second level: Refine Medium vs Low
- step_description: "Refine categorization"
  processor_type: "add_calculated_column"
  new_column: "Final_Category"
  calculation:
    type: "conditional"
    condition_column: "Order_Amount"
    condition: "greater_than"
    condition_value: 500
    value_if_true: "Medium"
    value_if_false: "Low"
  # Note: This overwrites, so you'd need logic to preserve "High" values
```

## Text Operations

### String Manipulations
```yaml
# Get text length
calculation:
  type: "text"
  operation: "length"
  column: "Customer_Name"

# Convert to uppercase
calculation:
  type: "text"
  operation: "upper"
  column: "Product_Code"

# Convert to lowercase  
calculation:
  type: "text"
  operation: "lower"
  column: "Email"

# Extract numbers from text
calculation:
  type: "text"
  operation: "extract_numbers"
  column: "Invoice_Number"

# Get substring
calculation:
  type: "text"
  operation: "substring"
  column: "Product_Code"
  start: 0
  length: 3  # First 3 characters
```

### Advanced String Examples
```yaml
# Extract product category from SKU
- step_description: "Extract category from SKU"
  processor_type: "add_calculated_column"
  new_column: "SKU_Category"
  calculation:
    type: "text"
    operation: "substring"
    column: "Product_SKU"
    start: 0
    length: 2

# Create email domain indicator
- step_description: "Flag company email domains"
  processor_type: "add_calculated_column"
  new_column: "Company_Email"
  calculation:
    type: "conditional"
    condition_column: "Email"
    condition: "contains"
    condition_value: "@company.com"
    value_if_true: "Internal"
    value_if_false: "External"
```

## Integration with Other Processors

### Complete Workflow
```yaml
# 1. Group locations into regions
- processor_type: "group_data"
  source_column: "City"
  target_column: "Region"
  groups:
    "West": ["Seattle", "Portland"]
    "East": ["Boston", "New York"]

# 2. Add regional details via lookup
- processor_type: "lookup_data"
  lookup_source: "regional_data.csv"
  lookup_key: "Region_Code"
  source_key: "Region"
  lookup_columns: ["Tax_Rate", "Shipping_Rate"]

# 3. Calculate totals with regional factors
- step_description: "Calculate region-adjusted totals"
  processor_type: "add_calculated_column"
  new_column: "Total_With_Tax"
  calculation:
    type: "expression"
    expression: "Order_Amount * (1 + Tax_Rate)"

- step_description: "Add shipping costs"
  processor_type: "add_calculated_column"
  new_column: "Grand_Total"
  calculation:
    type: "math"
    operation: "add"
    column1: "Total_With_Tax"
    column2: "Shipping_Rate"
```

### Business Intelligence Metrics
```yaml
# After grouping and lookups, create KPIs
- step_description: "Calculate order velocity"
  processor_type: "add_calculated_column"
  new_column: "Orders_Per_Day"
  calculation:
    type: "math"
    operation: "divide"
    column1: "Total_Orders"
    column2: "Days_Active"

# Customer lifetime value
- step_description: "Calculate customer LTV"
  processor_type: "add_calculated_column"
  new_column: "Customer_LTV"
  calculation:
    type: "expression"
    expression: "Average_Order_Value * Orders_Per_Month * 12"

# Performance indicators
- step_description: "Flag underperforming accounts"
  processor_type: "add_calculated_column"
  new_column: "Performance_Flag"
  calculation:
    type: "conditional"
    condition_column: "Customer_LTV"
    condition: "less_than"
    condition_value: 1000
    value_if_true: "Review Required"
    value_if_false: "Performing Well"
```

## Advanced Features

### Column Overwriting
```yaml
# Replace existing column
- processor_type: "add_calculated_column"
  new_column: "Price"  # Existing column
  calculation:
    type: "expression"
    expression: "Price * 1.1"  # 10% increase
  overwrite: true
```

### Complex Date Calculations
```yaml
# Business days calculation (simplified)
- step_description: "Calculate business days"
  processor_type: "add_calculated_column"
  new_column: "Business_Days"
  calculation:
    type: "date"
    operation: "days_between"
    start_date_column: "Order_Date"
    end_date_column: "Delivery_Date"
  
# Then adjust for weekends (requires additional logic)
- step_description: "Adjust for weekends"
  processor_type: "add_calculated_column"
  new_column: "Approximate_Business_Days"
  calculation:
    type: "expression"
    expression: "Business_Days * 0.71"  # Rough adjustment
```

## Error Handling & Troubleshooting

### Common Issues

#### Column Not Found Error
**Problem**: `Column 'Product_Price' not found`

**Solution**: Check exact column names
```yaml
- processor_type: "debug_breakpoint"
  message: "Check available columns"

# Then use exact names in calculation
```

#### Division by Zero
**Problem**: Math errors in division operations

**Solution**: Add conditional checks
```yaml
- processor_type: "add_calculated_column"
  new_column: "Safe_Division"
  calculation:
    type: "conditional"
    condition_column: "Denominator"
    condition: "equals"
    condition_value: 0
    value_if_true: 0
    value_if_false: "Numerator / Denominator"  # Use expression for division
```

#### Data Type Errors
**Problem**: Math operations on text columns

**Solution**: Clean data first
```yaml
- processor_type: "clean_data"
  rules:
    - column: "Price"
      action: "fix_numeric"

- processor_type: "add_calculated_column"
  new_column: "Total"
  calculation:
    type: "math"
    operation: "multiply"
    column1: "Price"
    column2: "Quantity"
```

## Best Practices

### Readable Column Names
```yaml
# ✅ Descriptive names
new_column: "Days_Since_Last_Order"
new_column: "Customer_Lifetime_Value"
new_column: "High_Value_Customer_Flag"

# ❌ Cryptic names
new_column: "calc1"
new_column: "temp_col"
```

### Step-by-Step Calculations
```yaml
# ✅ Break complex calculations into steps
- step_description: "Calculate base total"
  processor_type: "add_calculated_column"
  new_column: "Base_Total"
  calculation:
    type: "math"
    operation: "multiply"
    column1: "Price"
    column2: "Quantity"

- step_description: "Add tax"
  processor_type: "add_calculated_column"
  new_column: "Total_With_Tax"
  calculation:
    type: "expression"
    expression: "Base_Total * (1 + Tax_Rate)"

# ❌ One complex expression
calculation:
  expression: "Price * Quantity * (1 + Tax_Rate) * (1 - Discount_Rate) + Shipping"
```

### Meaningful Step Descriptions
```yaml
# ✅ Clear descriptions
step_description: "Calculate customer lifetime value using average order value"

# ❌ Generic descriptions
step_description: "Add calculated column"
```

## Performance Tips

- **Simple operations**: Use specific calculation types rather than complex expressions when possible
- **Data types**: Ensure numeric columns are properly typed before math operations
- **Step order**: Calculate base values first, then build on them
- **Debugging**: Use debug breakpoints to verify intermediate calculations

## See Also

- [Group Data](group-data.md) - Categorize data before calculations
- [Lookup Data](lookup-data.md) - Enrich data for calculations  
- [Clean Data](clean-data.md) - Fix data types before math operations
- [Filter Data](filter-data.md) - Filter calculated results
