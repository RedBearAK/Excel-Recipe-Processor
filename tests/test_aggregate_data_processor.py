"""
Test the AggregateDataProcessor functionality.
"""

import pandas as pd

from excel_recipe_processor.processors.aggregate_data_processor import AggregateDataProcessor
from excel_recipe_processor.processors.base_processor import StepProcessorError


def create_sales_test_data():
    """Create sample sales data for testing aggregations."""
    return pd.DataFrame({
        'Region': [
            'North', 'South', 'North', 'East', 'West', 'South', 
            'North', 'East', 'West', 'South', 'East', 'West'
        ],
        'Department': [
            'Electronics', 'Tools', 'Electronics', 'Hardware', 'Tools', 'Electronics',
            'Tools', 'Hardware', 'Electronics', 'Tools', 'Electronics', 'Hardware'
        ],
        'Sales_Amount': [1500, 800, 2200, 950, 1200, 1800, 600, 1100, 1750, 900, 1300, 850],
        'Order_Count': [15, 8, 22, 9, 12, 18, 6, 11, 17, 9, 13, 8],
        'Customer_Count': [12, 7, 18, 8, 10, 15, 5, 9, 14, 8, 11, 7],
        'Product_Category': [
            'A', 'B', 'A', 'C', 'B', 'A', 'B', 'C', 'A', 'B', 'A', 'C'
        ]
    })


def test_single_column_aggregation():
    """Test basic aggregation by single column."""
    
    print("Testing single column aggregation...")
    
    test_df = create_sales_test_data()
    print(f"✓ Created test data: {len(test_df)} rows")
    
    # Test sum of sales by region
    step_config = {
        'processor_type': 'aggregate_data',
        'step_description': 'Sum sales by region',
        'group_by': 'Region',
        'aggregations': [
            {
                'column': 'Sales_Amount',
                'function': 'sum',
                'new_column_name': 'Total_Sales'
            }
        ]
    }
    
    processor = AggregateDataProcessor(step_config)
    result = processor.execute(test_df)
    
    print(f"✓ Single aggregation: {len(test_df)} rows → {len(result)} groups")
    print(f"✓ Columns: {list(result.columns)}")
    
    # Check that we have expected columns
    if 'Region' in result.columns and 'Total_Sales' in result.columns:
        print("✓ Expected columns found")
        
        # Show results
        print("Sales by region:")
        for i in range(len(result)):
            region = result.iloc[i]['Region']
            total = result.iloc[i]['Total_Sales']
            print(f"  {region}: ${total:,.0f}")
        
        # Verify totals make sense
        original_total = test_df['Sales_Amount'].sum()
        aggregated_total = result['Total_Sales'].sum()
        
        if abs(original_total - aggregated_total) < 0.01:
            print("✓ Totals match - aggregation is correct")
            return True
        else:
            print(f"✗ Total mismatch: {original_total} vs {aggregated_total}")
    
    print("✗ Single column aggregation failed")
    return False


def test_multi_column_aggregation():
    """Test aggregation by multiple columns."""
    
    print("\nTesting multi-column aggregation...")
    
    test_df = create_sales_test_data()
    
    # Test aggregation by region and department
    step_config = {
        'processor_type': 'aggregate_data',
        'step_description': 'Aggregate by region and department',
        'group_by': ['Region', 'Department'],
        'aggregations': [
            {
                'column': 'Sales_Amount',
                'function': 'sum',
                'new_column_name': 'Total_Sales'
            },
            {
                'column': 'Order_Count',
                'function': 'mean',
                'new_column_name': 'Avg_Orders'
            }
        ]
    }
    
    processor = AggregateDataProcessor(step_config)
    result = processor.execute(test_df)
    
    print(f"✓ Multi-column aggregation: {len(test_df)} rows → {len(result)} groups")
    
    # Check expected columns
    expected_cols = ['Region', 'Department', 'Total_Sales', 'Avg_Orders']
    has_expected = all(col in result.columns for col in expected_cols)
    
    if has_expected:
        print("✓ Multi-column aggregation created correctly")
        
        # Show sample results
        print("Sample results:")
        for i in range(min(5, len(result))):
            region = result.iloc[i]['Region']
            dept = result.iloc[i]['Department']
            sales = result.iloc[i]['Total_Sales']
            orders = result.iloc[i]['Avg_Orders']
            print(f"  {region}-{dept}: ${sales:,.0f}, {orders:.1f} avg orders")
        
        return True
    else:
        print(f"✗ Missing expected columns: {list(result.columns)}")
        return False


def test_multiple_functions_same_column():
    """Test multiple aggregation functions on the same column."""
    
    print("\nTesting multiple functions on same column...")
    
    test_df = create_sales_test_data()
    
    # Test multiple stats on sales amount
    step_config = {
        'processor_type': 'aggregate_data',
        'step_description': 'Sales statistics by region',
        'group_by': 'Region',
        'aggregations': [
            {
                'column': 'Sales_Amount',
                'function': 'sum',
                'new_column_name': 'Total_Sales'
            },
            {
                'column': 'Sales_Amount',
                'function': 'mean',
                'new_column_name': 'Avg_Sales'
            },
            {
                'column': 'Sales_Amount',
                'function': 'min',
                'new_column_name': 'Min_Sales'
            },
            {
                'column': 'Sales_Amount',
                'function': 'max',
                'new_column_name': 'Max_Sales'
            },
            {
                'column': 'Sales_Amount',
                'function': 'count',
                'new_column_name': 'Sales_Count'
            }
        ]
    }
    
    processor = AggregateDataProcessor(step_config)
    result = processor.execute(test_df)
    
    print(f"✓ Multiple functions: {len(result)} groups")
    
    # Check all expected columns exist
    expected_cols = ['Region', 'Total_Sales', 'Avg_Sales', 'Min_Sales', 'Max_Sales', 'Sales_Count']
    has_all = all(col in result.columns for col in expected_cols)
    
    if has_all:
        print("✓ All aggregation functions applied correctly")
        
        # Show detailed stats for one region
        first_row = result.iloc[0]
        region = first_row['Region']
        print(f"Statistics for {region}:")
        print(f"  Total: ${first_row['Total_Sales']:,.0f}")
        print(f"  Average: ${first_row['Avg_Sales']:,.0f}")
        print(f"  Range: ${first_row['Min_Sales']:,.0f} - ${first_row['Max_Sales']:,.0f}")
        print(f"  Count: {first_row['Sales_Count']}")
        
        return True
    else:
        print(f"✗ Missing columns: {list(result.columns)}")
        return False


def test_different_data_types():
    """Test aggregation with different column data types."""
    
    print("\nTesting different data types...")
    
    test_df = create_sales_test_data()
    
    # Test aggregations on numeric and categorical data
    step_config = {
        'processor_type': 'aggregate_data',
        'step_description': 'Mixed data type aggregations',
        'group_by': 'Region',
        'aggregations': [
            {
                'column': 'Sales_Amount',
                'function': 'sum',
                'new_column_name': 'Total_Sales'
            },
            {
                'column': 'Department',
                'function': 'count',
                'new_column_name': 'Dept_Records'
            },
            {
                'column': 'Department',
                'function': 'nunique',
                'new_column_name': 'Unique_Depts'
            }
        ]
    }
    
    processor = AggregateDataProcessor(step_config)
    result = processor.execute(test_df)
    
    print(f"✓ Mixed data types: {len(result)} groups")
    
    # Check results
    if all(col in result.columns for col in ['Total_Sales', 'Dept_Records', 'Unique_Depts']):
        print("✓ Different data types handled correctly")
        
        # Show results
        for i in range(len(result)):
            region = result.iloc[i]['Region']
            sales = result.iloc[i]['Total_Sales']
            records = result.iloc[i]['Dept_Records']
            unique_depts = result.iloc[i]['Unique_Depts']
            print(f"  {region}: ${sales:,.0f}, {records} records, {unique_depts} departments")
        
        return True
    else:
        print("✗ Mixed data type aggregation failed")
        return False


def test_summary_aggregation_helper():
    """Test the create_summary_aggregation helper method."""
    
    print("\nTesting summary aggregation helper...")
    
    test_df = create_sales_test_data()
    
    processor = AggregateDataProcessor({
        'processor_type': 'aggregate_data',
        'step_description': 'Test helper',
        'group_by': 'Region',
        'aggregations': []
    })
    
    # Use helper to create standard summary
    result = processor.create_summary_aggregation(
        test_df, 
        'Region', 
        ['Sales_Amount', 'Order_Count']
    )
    
    print(f"✓ Summary helper: {len(result)} groups")
    print(f"✓ Columns: {list(result.columns)}")
    
    # Check for expected summary columns
    expected_patterns = ['total', 'average', 'count']
    has_summaries = any(
        any(pattern in col.lower() for pattern in expected_patterns)
        for col in result.columns
    )
    
    if has_summaries:
        print("✓ Summary aggregation helper worked correctly")
        return True
    else:
        print("✗ Summary aggregation helper failed")
        return False


def test_crosstab_aggregation_helper():
    """Test the create_crosstab_aggregation helper method."""
    
    print("\nTesting crosstab aggregation helper...")
    
    test_df = create_sales_test_data()
    
    processor = AggregateDataProcessor({
        'processor_type': 'aggregate_data',
        'step_description': 'Test crosstab',
        'group_by': 'Region',
        'aggregations': []
    })
    
    # Create crosstab: Region vs Department
    result = processor.create_crosstab_aggregation(
        test_df,
        row_field='Region',
        col_field='Department', 
        value_field='Sales_Amount',
        aggfunc='sum'
    )
    
    print(f"✓ Crosstab helper: {len(result)} rows, {len(result.columns)} columns")
    print(f"✓ Columns: {list(result.columns)}")
    
    # Check that we have regions and departments as columns
    has_region_col = 'Region' in result.columns
    has_dept_columns = any('Electronics' in str(col) or 'Tools' in str(col) or 'Hardware' in str(col) 
                          for col in result.columns)
    
    if has_region_col and has_dept_columns:
        print("✓ Crosstab aggregation helper worked correctly")
        return True
    else:
        print("✗ Crosstab aggregation helper failed")
        return False


def test_aggregation_analysis():
    """Test the get_aggregation_analysis method."""
    
    print("\nTesting aggregation analysis...")
    
    test_df = create_sales_test_data()
    
    processor = AggregateDataProcessor({
        'processor_type': 'aggregate_data',
        'step_description': 'Test analysis',
        'group_by': 'Region',
        'aggregations': []
    })
    
    # Analyze aggregation potential
    analysis = processor.get_aggregation_analysis(test_df, 'Region')
    
    print(f"✓ Analysis results:")
    print(f"  Total rows: {analysis['total_rows']}")
    print(f"  Unique groups: {analysis['unique_groups']}")
    print(f"  Numeric columns: {analysis['numeric_columns']}")
    print(f"  Categorical columns: {analysis['categorical_columns']}")
    print(f"  Suggested aggregations: {len(analysis['suggested_aggregations'])}")
    
    # Check analysis quality
    has_basics = (
        analysis['total_rows'] == len(test_df) and
        analysis['unique_groups'] > 0 and
        len(analysis['numeric_columns']) > 0 and
        len(analysis['suggested_aggregations']) > 0
    )
    
    if has_basics:
        print("✓ Aggregation analysis worked correctly")
        return True
    else:
        print("✗ Aggregation analysis failed")
        return False


def test_configuration_options():
    """Test various configuration options."""
    
    print("\nTesting configuration options...")
    
    test_df = create_sales_test_data()
    
    # Test without keeping group columns
    step_config1 = {
        'processor_type': 'aggregate_data',
        'step_description': 'Test without group columns',
        'group_by': 'Region',
        'aggregations': [
            {
                'column': 'Sales_Amount',
                'function': 'sum',
                'new_column_name': 'Total_Sales'
            }
        ],
        'keep_group_columns': False
    }
    
    processor1 = AggregateDataProcessor(step_config1)
    result1 = processor1.execute(test_df)
    
    print(f"✓ Without group columns: {list(result1.columns)}")
    
    # Test without sorting
    step_config2 = {
        'processor_type': 'aggregate_data',
        'step_description': 'Test without sorting',
        'group_by': 'Region',
        'aggregations': [
            {
                'column': 'Sales_Amount',
                'function': 'sum',
                'new_column_name': 'Total_Sales'
            }
        ],
        'sort_by_groups': False
    }
    
    processor2 = AggregateDataProcessor(step_config2)
    result2 = processor2.execute(test_df)
    
    print(f"✓ Without sorting: {len(result2)} groups")
    
    if len(result1) > 0 and len(result2) > 0:
        print("✓ Configuration options worked correctly")
        return True
    else:
        print("✗ Configuration options failed")
        return False


def test_capabilities_method():
    """Test the get_capabilities method."""
    
    print("\nTesting capabilities method...")
    
    processor = AggregateDataProcessor({
        'processor_type': 'aggregate_data',
        'group_by': 'test',
        'aggregations': []
    })
    
    capabilities = processor.get_capabilities()
    
    print(f"✓ Capabilities: {capabilities.keys()}")
    
    # Check expected capability fields
    expected_keys = ['description', 'aggregation_functions', 'grouping_features', 'helper_methods']
    has_expected = all(key in capabilities for key in expected_keys)
    
    supported_functions = capabilities.get('aggregation_functions', [])
    print(f"✓ Supported functions: {len(supported_functions)} available")
    
    if has_expected and len(supported_functions) > 0:
        print("✓ Capabilities method worked correctly")
        return True
    else:
        print("✗ Capabilities method failed")
        return False


def test_error_handling():
    """Test error handling for various failure cases."""
    
    print("\nTesting error handling...")
    
    test_df = create_sales_test_data()
    
    # Test missing required fields
    try:
        bad_config = {
            'processor_type': 'aggregate_data',
            'step_description': 'Missing fields'
            # Missing 'group_by' and 'aggregations'
        }
        processor = AggregateDataProcessor(bad_config)
        processor.execute(test_df)
        print("✗ Should have failed with missing fields")
    except StepProcessorError as e:
        print(f"✓ Caught expected error: {e}")
    
    # Test invalid group_by column
    try:
        bad_config = {
            'processor_type': 'aggregate_data',
            'step_description': 'Invalid group column',
            'group_by': 'NonExistentColumn',
            'aggregations': [
                {
                    'column': 'Sales_Amount',
                    'function': 'sum'
                }
            ]
        }
        processor = AggregateDataProcessor(bad_config)
        processor.execute(test_df)
        print("✗ Should have failed with invalid group column")
    except StepProcessorError as e:
        print(f"✓ Caught expected error: {e}")
    
    # Test invalid aggregation column
    try:
        bad_config = {
            'processor_type': 'aggregate_data',
            'step_description': 'Invalid agg column',
            'group_by': 'Region',
            'aggregations': [
                {
                    'column': 'NonExistentColumn',
                    'function': 'sum'
                }
            ]
        }
        processor = AggregateDataProcessor(bad_config)
        processor.execute(test_df)
        print("✗ Should have failed with invalid aggregation column")
    except StepProcessorError as e:
        print(f"✓ Caught expected error: {e}")
    
    # Test invalid aggregation function
    try:
        bad_config = {
            'processor_type': 'aggregate_data',
            'step_description': 'Invalid function',
            'group_by': 'Region',
            'aggregations': [
                {
                    'column': 'Sales_Amount',
                    'function': 'invalid_function'
                }
            ]
        }
        processor = AggregateDataProcessor(bad_config)
        processor.execute(test_df)
        print("✗ Should have failed with invalid function")
    except StepProcessorError as e:
        print(f"✓ Caught expected error: {e}")


def test_real_world_scenario():
    """Test a realistic aggregation scenario."""
    
    print("\nTesting real-world scenario...")
    
    # Simulate business sales data
    test_df = pd.DataFrame({
        'Sales_Rep': ['Alice', 'Bob', 'Charlie', 'Alice', 'Bob', 'Diana', 'Charlie', 'Alice'],
        'Territory': ['North', 'South', 'East', 'North', 'West', 'South', 'East', 'North'], 
        'Product_Line': ['A', 'B', 'A', 'C', 'B', 'A', 'C', 'B'],
        'Quarter': ['Q1', 'Q1', 'Q1', 'Q2', 'Q1', 'Q2', 'Q2', 'Q2'],
        'Sales': [15000, 12000, 18000, 14000, 11000, 16000, 19000, 13000],
        'Units': [150, 120, 180, 140, 110, 160, 190, 130],
        'Deals': [5, 4, 6, 4, 3, 5, 6, 4]
    })
    
    print(f"✓ Created business scenario data: {len(test_df)} records")
    
    # Create quarterly territory performance summary
    step_config = {
        'processor_type': 'aggregate_data',
        'step_description': 'Quarterly territory performance',
        'group_by': ['Territory', 'Quarter'],
        'aggregations': [
            {
                'column': 'Sales',
                'function': 'sum',
                'new_column_name': 'Total_Revenue'
            },
            {
                'column': 'Units',
                'function': 'sum', 
                'new_column_name': 'Total_Units'
            },
            {
                'column': 'Deals',
                'function': 'sum',
                'new_column_name': 'Total_Deals'
            },
            {
                'column': 'Sales',
                'function': 'mean',
                'new_column_name': 'Avg_Deal_Size'
            },
            {
                'column': 'Sales_Rep',
                'function': 'nunique',
                'new_column_name': 'Active_Reps'
            }
        ]
    }
    
    processor = AggregateDataProcessor(step_config)
    result = processor.execute(test_df)
    
    print(f"✓ Business summary: {len(result)} territory-quarter combinations")
    
    # Show business results
    print("Quarterly Territory Performance:")
    for i in range(len(result)):
        territory = result.iloc[i]['Territory']
        quarter = result.iloc[i]['Quarter']
        revenue = result.iloc[i]['Total_Revenue']
        units = result.iloc[i]['Total_Units']
        deals = result.iloc[i]['Total_Deals']
        avg_deal = result.iloc[i]['Avg_Deal_Size']
        reps = result.iloc[i]['Active_Reps']
        
        print(f"  {territory} {quarter}: ${revenue:,} ({units} units, {deals} deals, ${avg_deal:,.0f} avg, {reps} reps)")
    
    # Verify business logic
    total_revenue = result['Total_Revenue'].sum()
    original_revenue = test_df['Sales'].sum()
    
    if abs(total_revenue - original_revenue) < 0.01:
        print("✓ Real-world scenario worked correctly")
        return True
    else:
        print("✗ Real-world scenario failed")
        return False


if __name__ == '__main__':
    success = True
    
    success &= test_single_column_aggregation()
    success &= test_multi_column_aggregation()
    success &= test_multiple_functions_same_column()
    success &= test_different_data_types()
    success &= test_summary_aggregation_helper()
    success &= test_crosstab_aggregation_helper()
    success &= test_aggregation_analysis()
    success &= test_configuration_options()
    success &= test_capabilities_method()
    success &= test_real_world_scenario()
    test_error_handling()
    
    if success:
        print("\n✓ All aggregate data processor tests passed!")
    else:
        print("\n✗ Some aggregate data processor tests failed!")
    
    # Show supported functions
    processor = AggregateDataProcessor({
        'processor_type': 'aggregate_data',
        'group_by': 'test', 
        'aggregations': []
    })
    print(f"\nSupported aggregation functions: {processor.get_supported_functions()}")
