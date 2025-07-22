# Sort Data Processor

Order DataFrame rows by one or multiple columns with flexible sorting options.

## Overview

The `sort_data` processor organizes your data in meaningful ways for analysis and presentation. It supports standard sorting, custom ordering, and advanced options like frequency-based and case-insensitive sorting.

## Basic Usage

```yaml
- processor_type: "sort_data"
  columns: ["Order_Date"]
  ascending: false  # Most recent first
```

This sorts by a single column in descending order (newest first).

## Sort Types

### Single Column Sort
```yaml
- processor_type: "sort_data"
  columns: ["Customer_Name"]
  ascending: true
```

### Multi-Column Sort
```yaml
- processor_type: "sort_data"
  columns: ["Region", "Customer_Name", "Order_Date"]
  ascending: [true, true, false]
  # Region A-Z, then Customer A-Z, then Date newest first
```

### Custom Sort Orders
```yaml
- processor_type: "sort_data"
  columns: ["Priority"]
  custom_orders:
    Priority: ["Critical", "High", "Medium", "Low"]
```

## Configuration Options

### Required Fields
```yaml
- processor_type: "sort_data"
  columns: ["Column_Name"]     # Required: column(s) to sort by
```

### Complete Configuration
```yaml
- processor_type: "sort_data"
  columns: ["Col1", "Col2"]              # Columns to sort by
  ascending: [true, false]               # Sort directions per column
  na_position: "last"                    # Where to put null values: "first"/"last"
  custom_orders:                         # Custom ordering for specific columns
    Col1: ["Value1", "Value2", "Value3"]
  ignore_case: false                     # Case-insensitive text sorting
```

## Real-World Examples

### Van Report Final Ordering
```yaml
- step_description: "Sort by region priority and origin alphabetically"
  processor_type: "sort_data"
  columns: ["Regional_Group", "Origin_Location"]
  custom_orders:
    Regional_Group: ["Bristol Bay Region", "Kodiak Region", "Prince William Sound", "Southeast Alaska"]
  ascending: [true, true]
```

### Business Report Ordering
```yaml
- step_description: "Sort customers by value and recency"
  processor_type: "sort_data"
  columns: ["Customer_Tier", "Last_Order_Date", "Total_Value"]
  custom_orders:
    Customer_Tier: ["Platinum", "Gold", "Silver", "Bronze"]
  ascending: [true, false, false]
  # Tier in priority order, then newest orders, then highest value
```

### Sales Performance Dashboard
```yaml
- step_description: "Sort sales team by performance"
  processor_type: "sort_data"
  columns: ["Region", "Performance_Rating", "Total_Sales"]
  custom_orders:
    Performance_Rating: ["Exceeds", "Meets", "Below", "Unsatisfactory"]
  ascending: [true, true, false]
  # Region A-Z, then by performance level, then by sales amount
```

### Product Catalog Organization
```yaml
- step_description: "Sort products for catalog display"
  processor_type: "sort_data"
  columns: ["Category", "Price", "Product_Name"]
  ascending: [true, true, true]
  ignore_case: true
  # Category alphabetical, then price low to high, then name A-Z (ignore case)
```

## Advanced Sorting Options

### Custom Priority Orders
```yaml
# Business priority ordering
- step_description: "Sort by business priority"
  processor_type: "sort_data"
  columns: ["Status", "Priority", "Due_Date"]
  custom_orders:
    Status: ["Active", "Pending", "On Hold", "Cancelled"]
    Priority: ["Urgent", "High", "Medium", "Low"]
  ascending: [true, true, true]
```

### Geographic Ordering
```yaml
# Logical geographic flow
- step_description: "Sort by geographic regions"
  processor_type: "sort_data"
  columns: ["Time_Zone", "State", "City"]
  custom_orders:
    Time_Zone: ["Pacific", "Mountain", "Central", "Eastern"]
  ascending: [true, true, true]
```

### Process Flow Ordering  
```yaml
# Workflow stage ordering
- step_description: "Sort by process stage"
  processor_type: "sort_data"
  columns: ["Stage", "Updated_Date"]
  custom_orders:
    Stage: ["Submitted", "Review", "Approved", "In Progress", "Completed", "Delivered"]
  ascending: [true, false]  # Stage in order, newest updates first
```

## Multi-Column Sorting

### Hierarchical Business Logic
```yaml
- step_description: "Sort by complete business hierarchy"
  processor_type: "sort_data"
  columns: ["Division", "Department", "Manager", "Employee_Name"]
  ascending: [true, true, true, true]
  ignore_case: true
```

### Performance Analysis Order
```yaml
- step_description: "Sort for performance review"
  processor_type: "sort_data" 
  columns: ["Performance_Score", "Years_Experience", "Salary"]
  ascending: [false, false, true]
  # Highest performers first, most experienced, then by salary
```

### Financial Reporting Order
```yaml
- step_description: "Sort for financial analysis"
  processor_type: "sort_data"
  columns: ["Fiscal_Quarter", "Department", "Budget_Category", "Amount"]
  custom_orders:
    Fiscal_Quarter: ["Q1", "Q2", "Q3", "Q4"]
  ascending: [true, true, true, false]
  # Quarter order, dept A-Z, category A-Z, highest amounts first
```

## Case Sensitivity and Text Handling

### Case-Insensitive Sorting
```yaml
- step_description: "Sort customer names ignoring case"
  processor_type: "sort_data"
  columns: ["Customer_Name"]
  ignore_case: true
  # "apple Corp", "Apple Inc", "APPLE Systems" sort together
```

### Mixed Case Data
```yaml
- step_description: "Handle inconsistent case data"
  processor_type: "sort_data"
  columns: ["Product_Code", "Category"]
  ignore_case: true
  ascending: [true, true]
```

## Null Value Handling

### Nulls Last (Default)
```yaml
- processor_type: "sort_data"
  columns: ["Ship_Date"]
  na_position: "last"
  # Records with ship dates first, unshipped items at end
```

### Nulls First
```yaml
- processor_type: "sort_data"
  columns: ["Manager_Assigned"] 
  na_position: "first"
  # Unassigned items first for attention
```

## Integration with Other Processors

### Complete Van Report Workflow
```yaml
# 1. Group locations
- processor_type: "group_data"
  source_column: "Product Origin"
  target_column: "Regional_Group"
  groups:
    "Bristol Bay Region": ["Dillingham", "Naknek", "False Pass"]
    "Kodiak Region": ["Kodiak", "Kodiak West"]
    "Prince William Sound": ["Cordova", "Seward"]
    "Southeast Alaska": ["Craig", "Ketchikan", "Sitka"]

# 2. Add calculated metrics
- processor_type: "add_calculated_column"
  new_column: "Days_In_Transit"
  calculation:
    type: "date"
    operation: "days_between"
    start_date_column: "Ship_Date"
    end_date_column: "Current_Date"

# 3. Create summary pivot
- processor_type: "pivot_table"
  index: ["Regional_Group", "Product Origin"]
  columns: ["Carrier"]
  values: ["Van_Number"]
  aggfunc: "nunique"

# 4. Clean column names
- processor_type: "rename_columns"
  rename_type: "mapping"
  mapping:
    "Regional_Group": "Alaska Region"
    "Product Origin": "Origin Port"

# 5. Final logical ordering
- step_description: "Sort for final van report presentation"
  processor_type: "sort_data"
  columns: ["Alaska Region", "Origin Port"]
  custom_orders:
    "Alaska Region": ["Bristol Bay Region", "Kodiak Region", "Prince William Sound", "Southeast Alaska"]
  ascending: [true, true]
```

### Business Intelligence Dashboard
```yaml
# After all calculations and groupings
- step_description: "Final dashboard ordering"
  processor_type: "sort_data"
  columns: ["Customer_Tier", "Total_Value", "Customer_Name"] 
  custom_orders:
    Customer_Tier: ["Platinum", "Gold", "Silver", "Bronze"]
  ascending: [true, false, true]
  # Tier priority, highest value first, name A-Z
```

### Financial Report Presentation
```yaml
# After pivot tables and calculations
- step_description: "Sort for executive presentation"
  processor_type: "sort_data"
  columns: ["Account_Type", "Total_Revenue", "Account_Name"]
  custom_orders:
    Account_Type: ["Strategic", "Enterprise", "Commercial", "SMB"]
  ascending: [true, false, true]
```

## Advanced Use Cases

### Frequency-Based Ordering
Sort by how often values appear (most common first):

```yaml
# This would require custom logic, but you can approximate:
- step_description: "Sort products by popularity"
  processor_type: "sort_data"
  columns: ["Order_Count", "Product_Name"]
  ascending: [false, true]
  # Assumes you've calculated order counts
```

### Date-Based Logical Ordering
```yaml
- step_description: "Sort by logical date progression"
  processor_type: "sort_data"
  columns: ["Order_Date", "Ship_Date", "Delivery_Date"]
  ascending: [false, false, false]
  na_position: "last"
  # Most recent orders first, with unshipped/undelivered last
```

### Performance Ranking
```yaml
- step_description: "Rank sales performance"
  processor_type: "sort_data"
  columns: ["Sales_Quarter", "Total_Sales", "Sales_Rep"]
  custom_orders:
    Sales_Quarter: ["Q4 2024", "Q3 2024", "Q2 2024", "Q1 2024"]
  ascending: [true, false, true]
  # Recent quarter first, top performers first, rep name A-Z
```

## Complex Sorting Scenarios

### Multi-Level Priority System
```yaml
- step_description: "Sort support tickets by complete priority system"
  processor_type: "sort_data"
  columns: ["Severity", "Priority", "Customer_Tier", "Created_Date"]
  custom_orders:
    Severity: ["Critical", "High", "Medium", "Low"]
    Priority: ["P1", "P2", "P3", "P4"]
    Customer_Tier: ["Enterprise", "Professional", "Standard"]
  ascending: [true, true, true, true]
```

### Seasonal Business Logic
```yaml
- step_description: "Sort by seasonal importance"
  processor_type: "sort_data"
  columns: ["Season", "Product_Category", "Sales_Volume"]
  custom_orders:
    Season: ["Spring", "Summer", "Fall", "Winter"]  # Or business-specific order
  ascending: [true, true, false]
```

### Project Timeline Organization
```yaml
- step_description: "Sort project tasks by dependency order"
  processor_type: "sort_data"
  columns: ["Phase", "Priority", "Due_Date", "Task_Name"]
  custom_orders:
    Phase: ["Planning", "Design", "Development", "Testing", "Deployment"]
    Priority: ["Blocker", "Critical", "High", "Medium", "Low"]
  ascending: [true, true, true, true]
```

## Error Handling & Troubleshooting

### Common Issues

#### Column Not Found Error
**Problem**: `Sort column 'Region' not found`

**Solution**: Check exact column names
```yaml
- processor_type: "debug_breakpoint"
  message: "Check available columns before sorting"

- processor_type: "sort_data"
  columns: ["Exact_Column_Name"]  # Use exact name from debug
```

#### Mixed Data Types
**Problem**: Cannot sort column with mixed types

**Solution**: Clean data first
```yaml
- processor_type: "clean_data"
  rules:
    - column: "Mixed_Column"
      action: "fix_numeric"  # or appropriate cleaning

- processor_type: "sort_data"
  columns: ["Mixed_Column"]
```

#### Custom Order Incomplete
**Problem**: Values not in custom_order appear at end unpredictably

**Solution**: Include all expected values in custom order
```yaml
custom_orders:
  Status: ["Active", "Pending", "Inactive", "Cancelled", "Unknown"]
  # Include "Unknown" for unexpected values
```

## Best Practices

### Logical Sort Order
```yaml
# ✅ Business-logical ordering
custom_orders:
  Priority: ["Critical", "High", "Medium", "Low"]
  Status: ["New", "In Progress", "Review", "Complete"]

# ❌ Random ordering
custom_orders:
  Priority: ["Medium", "Critical", "Low", "High"]
```

### Consistent Direction Logic
```yaml
# ✅ Consistent logic: Important things first
columns: ["Priority", "Due_Date", "Created_Date"]
custom_orders:
  Priority: ["Critical", "High", "Medium", "Low"]
ascending: [true, true, false]  # Priority order, due date ascending, created descending

# ✅ All dates newest first for consistency
ascending: [true, false, false]
```

### Meaningful Final Presentation
```yaml
# ✅ Sort for end-user readability
- step_description: "Final sort for business presentation"
  processor_type: "sort_data"
  columns: ["Business_Unit", "Performance_Score", "Employee_Name"]
  ascending: [true, false, true]
  # Unit A-Z, best performers first, name A-Z within each group
```

### Handle Edge Cases
```yaml
# ✅ Plan for null values
- processor_type: "sort_data"
  columns: ["Manager", "Department"]
  na_position: "first"  # Show unassigned items first for action
```

## Performance Tips

- **Sort late**: Apply sorting as one of the final steps after filtering and calculations
- **Use custom orders**: More efficient than complex conditional logic
- **Limit sort columns**: Each additional sort column increases processing time
- **Consider data size**: Very large datasets may need chunked processing

## Sort Order Examples

### Business Priority Orders
```yaml
# Customer tiers
Customer_Tier: ["Platinum", "Gold", "Silver", "Bronze"]

# Urgency levels  
Urgency: ["Critical", "Urgent", "High", "Medium", "Low"]

# Project phases
Phase: ["Planning", "Design", "Development", "Testing", "Deployment", "Maintenance"]

# Geographic regions (West to East)
Region: ["Pacific", "Mountain", "Central", "Eastern"]

# Quarters (chronological)
Quarter: ["Q1", "Q2", "Q3", "Q4"]
```

### Van Report Specific
```yaml
# Alaska regions (geographic/operational logic)
Regional_Group: ["Bristol Bay Region", "Kodiak Region", "Prince William Sound", "Southeast Alaska"]

# Carrier preference (hypothetical business logic)  
Carrier: ["Matson", "CMA", "MSC", "ONE"]

# Container status
Status: ["Loading", "In Transit", "Arrived", "Delivered"]
```

## See Also

- [Rename Columns](rename-columns.md) - Often done before final sorting
- [Add Calculated Column](add-calculated-column.md) - Create sort keys
- [Filter Data](filter-data.md) - Reduce data before sorting for performance
- [Pivot Table](pivot-table.md) - Often needs sorting afterward
