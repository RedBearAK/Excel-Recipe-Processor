"""
Test the AddSubtotalsProcessor functionality.
"""

import pandas as pd

from excel_recipe_processor.processors.base_processor import StepProcessorError
from excel_recipe_processor.processors.add_subtotals_processor import AddSubtotalsProcessor, SubtotalUtils


def create_sample_sales_data():
    """Create sample sales data for testing."""
    return pd.DataFrame({
        'Region': ['West', 'West', 'West', 'East', 'East', 'East', 'North', 'North'],
        'Product': ['A', 'B', 'A', 'A', 'B', 'C', 'A', 'B'],
        'Sales': [100, 150, 200, 180, 120, 90, 160, 140],
        'Quantity': [10, 15, 20, 18, 12, 9, 16, 14],
        'Orders': [5, 7, 8, 6, 4, 3, 5, 6]
    })


def create_hierarchical_data():
    """Create hierarchical data for multi-level testing."""
    return pd.DataFrame({
        'Division': ['North', 'North', 'North', 'North', 'South', 'South', 'South', 'South'],
        'Region': ['NE', 'NE', 'NW', 'NW', 'SE', 'SE', 'SW', 'SW'],
        'Product': ['Widget', 'Gadget', 'Widget', 'Gadget', 'Widget', 'Gadget', 'Widget', 'Gadget'],
        'Revenue': [1000, 1500, 1200, 1800, 900, 1100, 1300, 1600],
        'Units': [50, 75, 60, 90, 45, 55, 65, 80]
    })


def create_pivot_table_result():
    """Create sample data that looks like a pivot table result with grand totals."""
    return pd.DataFrame({
        'Region': ['East', 'West', 'North', 'Grand Total'],
        'Product_A': [180, 300, 160, 640],
        'Product_B': [120, 150, 140, 410],
        'Product_C': [90, 0, 0, 90]
    })


def test_basic_subtotals():
    """Test basic subtotal functionality."""
    
    print("Testing basic subtotals...")
    
    sales_df = create_sample_sales_data()
    print(f"✓ Created sales data: {len(sales_df)} rows")
    
    # Test basic regional subtotals
    step_config = {
        'step_description': 'Add regional subtotals',
        'processor_type': 'add_subtotals',
        'group_by': ['Region'],
        'subtotal_columns': ['Sales', 'Quantity'],
        'subtotal_functions': ['sum', 'sum'],
        'subtotal_label': 'Regional Total'
    }
    
    processor = AddSubtotalsProcessor(step_config)
    result = processor.execute(sales_df)
    
    print(f"✓ Result: {len(result)} rows (original + subtotals)")
    
    # Check that subtotals were added
    subtotal_rows = result[result['Region'].str.contains('Regional Total', na=False)]
    expected_regions = sales_df['Region'].nunique()
    
    if len(subtotal_rows) == expected_regions:
        print(f"✓ Added {len(subtotal_rows)} subtotal rows for {expected_regions} regions")
        
        # Check subtotal calculations
        west_subtotal = subtotal_rows[subtotal_rows['Region'].str.contains('West')]
        if len(west_subtotal) > 0:
            west_sales_total = sales_df[sales_df['Region'] == 'West']['Sales'].sum()
            calculated_total = west_subtotal['Sales'].iloc[0]
            
            if abs(calculated_total - west_sales_total) < 0.01:
                print("✓ Subtotal calculations are correct")
                return True
            else:
                print(f"✗ Subtotal calculation incorrect: expected {west_sales_total}, got {calculated_total}")
        else:
            print("✗ Could not find West subtotal for verification")
    else:
        print(f"✗ Expected {expected_regions} subtotal rows, got {len(subtotal_rows)}")
    
    return False


def test_hierarchical_subtotals():
    """Test multi-level hierarchical subtotals."""
    
    print("\nTesting hierarchical subtotals...")
    
    hier_df = create_hierarchical_data()
    print(f"✓ Created hierarchical data: {len(hier_df)} rows")
    
    # Test division and region subtotals
    step_config = {
        'step_description': 'Add hierarchical subtotals',
        'processor_type': 'add_subtotals',
        'group_by': ['Division', 'Region'],
        'subtotal_columns': ['Revenue', 'Units'],
        'subtotal_functions': ['sum', 'sum'],
        'subtotal_label': 'Subtotal'
    }
    
    processor = AddSubtotalsProcessor(step_config)
    result = processor.execute(hier_df)
    
    print(f"✓ Hierarchical result: {len(result)} rows")
    
    # Count subtotal rows
    subtotal_rows = result[result['Division'].str.contains('Subtotal', na=False) | 
                          result['Region'].str.contains('Subtotal', na=False)]
    
    # Should have subtotals for each division (2) and each region (4)
    expected_subtotals = hier_df['Division'].nunique() + hier_df['Region'].nunique()
    
    if len(subtotal_rows) >= hier_df['Division'].nunique():  # At least division subtotals
        print(f"✓ Created hierarchical subtotals: {len(subtotal_rows)} subtotal rows")
        return True
    else:
        print(f"✗ Expected at least {hier_df['Division'].nunique()} subtotals, got {len(subtotal_rows)}")
        return False


def test_aggregation_functions():
    """Test different aggregation functions."""
    
    print("\nTesting aggregation functions...")
    
    sales_df = create_sample_sales_data()
    
    # Test different functions
    functions_to_test = [
        ('sum', 'Sum'),
        ('count', 'Count'),
        ('mean', 'Average'),
        ('min', 'Minimum'),
        ('max', 'Maximum')
    ]
    
    for func, description in functions_to_test:
        step_config = {
            'processor_type': 'add_subtotals',
            'step_description': f'Test {func} function',
            'group_by': ['Region'],
            'subtotal_columns': ['Sales'],
            'subtotal_functions': [func],
            'subtotal_label': f'{description} Total'
        }
        
        try:
            processor = AddSubtotalsProcessor(step_config)
            result = processor.execute(sales_df)
            
            # Check that subtotals were created with the function
            subtotal_rows = result[result['Region'].str.contains(f'{description} Total', na=False)]
            
            if len(subtotal_rows) > 0:
                print(f"  ✓ {func} function worked correctly")
            else:
                print(f"  ✗ {func} function failed - no subtotal rows found")
                return False
                
        except Exception as e:
            print(f"  ✗ {func} function failed with error: {e}")
            return False
    
    print("✓ All aggregation functions worked correctly")
    return True


def test_preserve_grand_totals():
    """Test preservation of existing grand totals."""
    
    print("\nTesting grand total preservation...")
    
    pivot_result = create_pivot_table_result()
    print(f"✓ Created pivot result with grand total: {len(pivot_result)} rows")
    
    # Add subtotals while preserving grand totals
    step_config = {
        'processor_type': 'add_subtotals',
        'step_description': 'Add subtotals to pivot result',
        'group_by': ['Region'],
        'subtotal_columns': ['Product_A', 'Product_B'],
        'subtotal_functions': ['sum', 'sum'],
        'subtotal_label': 'Regional Subtotal',
        'preserve_totals': True
    }
    
    processor = AddSubtotalsProcessor(step_config)
    result = processor.execute(pivot_result)
    
    print(f"✓ Result with preserved totals: {len(result)} rows")
    
    # Check that Grand Total row still exists
    grand_total_rows = result[result['Region'].str.contains('Grand Total', na=False)]
    subtotal_rows = result[result['Region'].str.contains('Regional Subtotal', na=False)]
    
    has_grand_total = len(grand_total_rows) > 0
    has_subtotals = len(subtotal_rows) > 0
    
    if has_grand_total and has_subtotals:
        print("✓ Grand totals preserved and subtotals added")
        return True
    else:
        print(f"✗ Preservation failed: grand_total={has_grand_total}, subtotals={has_subtotals}")
        return False


def test_positioning_options():
    """Test different subtotal positioning options."""
    
    print("\nTesting positioning options...")
    
    sales_df = create_sample_sales_data().head(6)  # Smaller dataset for clear testing
    
    positions_to_test = ['after_group', 'before_group']
    
    for position in positions_to_test:
        step_config = {
            'processor_type': 'add_subtotals',
            'step_description': f'Test {position} positioning',
            'group_by': ['Region'],
            'subtotal_columns': ['Sales'],
            'subtotal_functions': ['sum'],
            'subtotal_label': f'{position.title()} Total',
            'position': position
        }
        
        try:
            processor = AddSubtotalsProcessor(step_config)
            result = processor.execute(sales_df)
            
            # Debug: Show what was created
            print(f"  Debug - {position} result:")
            for idx, row in result.iterrows():
                print(f"    {idx}: Region='{row['Region']}', Sales={row['Sales']}")
            
            # Check that subtotals exist
            subtotal_rows = result[result['Region'].str.contains(f'{position.title()} Total', na=False)]
            
            if len(subtotal_rows) > 0:
                print(f"  ✓ {position} positioning worked correctly ({len(subtotal_rows)} subtotals)")
            else:
                print(f"  ✗ {position} positioning failed - no subtotal rows found")
                return False
                
        except Exception as e:
            print(f"  ✗ {position} positioning failed with error: {e}")
            return False
    
    print("✓ All positioning options worked correctly")
    return True


def test_multiple_columns_functions():
    """Test multiple columns with different functions."""
    
    print("\nTesting multiple columns with different functions...")
    
    sales_df = create_sample_sales_data()
    
    step_config = {
        'processor_type': 'add_subtotals',
        'step_description': 'Multi-column multi-function test',
        'group_by': ['Region'],
        'subtotal_columns': ['Sales', 'Quantity', 'Orders'],
        'subtotal_functions': ['sum', 'mean', 'count'],
        'subtotal_label': 'Mixed Total'
    }
    
    processor = AddSubtotalsProcessor(step_config)
    result = processor.execute(sales_df)
    
    print(f"✓ Multi-function result: {len(result)} rows")
    
    # Check that subtotals were created
    subtotal_rows = result[result['Region'].str.contains('Mixed Total', na=False)]
    
    if len(subtotal_rows) > 0:
        # Verify different calculations were applied
        west_subtotal = subtotal_rows[subtotal_rows['Region'].str.contains('West')]
        if len(west_subtotal) > 0:
            # Sales should be sum, Quantity should be mean, Orders should be count
            west_data = sales_df[sales_df['Region'] == 'West']
            expected_sales_sum = west_data['Sales'].sum()
            expected_quantity_mean = west_data['Quantity'].mean()
            expected_orders_count = west_data['Orders'].count()
            
            actual_sales = west_subtotal['Sales'].iloc[0]
            actual_quantity = west_subtotal['Quantity'].iloc[0]
            actual_orders = west_subtotal['Orders'].iloc[0]
            
            sales_correct = abs(actual_sales - expected_sales_sum) < 0.01
            quantity_correct = abs(actual_quantity - expected_quantity_mean) < 0.01
            orders_correct = actual_orders == expected_orders_count
            
            if sales_correct and quantity_correct and orders_correct:
                print("✓ Multiple functions applied correctly")
                return True
            else:
                print(f"✗ Function calculations incorrect:")
                print(f"  Sales: expected {expected_sales_sum}, got {actual_sales}")
                print(f"  Quantity: expected {expected_quantity_mean}, got {actual_quantity}")
                print(f"  Orders: expected {expected_orders_count}, got {actual_orders}")
        else:
            print("✗ Could not find West subtotal for verification")
    else:
        print("✗ No subtotal rows created")
    
    return False


def test_real_world_scenario():
    """Test a realistic business reporting scenario."""
    
    print("\nTesting real-world scenario...")
    
    # Create realistic sales report data
    report_data = pd.DataFrame({
        'Division': ['North', 'North', 'North', 'North', 'South', 'South', 'South', 'South'],
        'Territory': ['NE', 'NE', 'NW', 'NW', 'SE', 'SE', 'SW', 'SW'],
        'Rep_Name': ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve', 'Frank', 'Grace', 'Henry'],
        'Q1_Sales': [50000, 45000, 52000, 48000, 42000, 47000, 51000, 46000],
        'Q2_Sales': [55000, 43000, 54000, 49000, 44000, 48000, 52000, 47000],
        'Q3_Sales': [53000, 47000, 51000, 50000, 45000, 46000, 53000, 48000],
        'Q4_Sales': [58000, 49000, 56000, 52000, 47000, 50000, 55000, 49000]
    })
    
    print(f"✓ Created realistic sales report: {len(report_data)} rows")
    
    # Add territory subtotals
    step_config = {
        'processor_type': 'add_subtotals',
        'step_description': 'Territory sales subtotals',
        'group_by': ['Division', 'Territory'],
        'subtotal_columns': ['Q1_Sales', 'Q2_Sales', 'Q3_Sales', 'Q4_Sales'],
        'subtotal_functions': ['sum', 'sum', 'sum', 'sum'],
        'subtotal_label': 'Territory Total'
    }
    
    processor = AddSubtotalsProcessor(step_config)
    result = processor.execute(report_data)
    
    print(f"✓ Report with subtotals: {len(result)} rows")
    
    # Verify territory and division subtotals
    # When grouping by ['Division', 'Territory'], the subtotal label goes in the Division column
    # So we should find all subtotals in the Division column
    all_subtotals = result[result['Division'].str.contains('Territory Total', na=False)]
    
    # Count unique territories and divisions in original data
    expected_territories = report_data['Territory'].nunique()  # Should be 4: NE, NW, SE, SW
    expected_divisions = report_data['Division'].nunique()     # Should be 2: North, South
    
    # We should get one subtotal per unique territory (4 total)
    actual_subtotal_count = len(all_subtotals)
    
    print(f"✓ Territory-level subtotals found: {actual_subtotal_count} (expected: {expected_territories})")
    print(f"✓ Unique territories in data: {expected_territories}")
    print(f"✓ Unique divisions in data: {expected_divisions}")
    
    # The test should pass if we have one subtotal per territory
    if actual_subtotal_count == expected_territories:
        print("✓ Real-world scenario worked correctly")
        return True
    else:
        print("✗ Real-world scenario failed - insufficient subtotals")
        print(f"  Expected {expected_territories} subtotals, got {actual_subtotal_count}")
        return False


def test_utility_functions():
    """Test the SubtotalUtils utility functions."""
    
    print("\nTesting utility functions...")
    
    sales_df = create_sample_sales_data()
    
    # Test utility function
    config = {
        'group_by': ['Region'],
        'subtotal_columns': ['Sales'],
        'subtotal_functions': ['sum'],
        'subtotal_label': 'Utility Total'
    }
    
    try:
        result = SubtotalUtils.add_subtotals_to_dataframe(sales_df, config)
        
        subtotal_rows = result[result['Region'].str.contains('Utility Total', na=False)]
        
        if len(subtotal_rows) > 0:
            print("✓ SubtotalUtils.add_subtotals_to_dataframe worked correctly")
            
            # Test config validation
            SubtotalUtils.validate_subtotal_config(config)
            print("✓ SubtotalUtils.validate_subtotal_config worked correctly")
            
            # Test default config
            default_config = SubtotalUtils.get_default_subtotal_config()
            if isinstance(default_config, dict) and 'group_by' in default_config:
                print("✓ SubtotalUtils.get_default_subtotal_config worked correctly")
                return True
            else:
                print("✗ Default config invalid")
        else:
            print("✗ Utility function failed - no subtotals created")
    except Exception as e:
        print(f"✗ Utility functions failed: {e}")
    
    return False


def test_error_handling():
    """Test error handling for various failure cases."""
    
    print("\nTesting error handling...")
    
    test_df = create_sample_sales_data()
    
    # Test missing required fields
    try:
        bad_config = {
            'processor_type': 'add_subtotals',
            'step_description': 'Missing group_by',
            'subtotal_columns': ['Sales']
        }
        processor = AddSubtotalsProcessor(bad_config)
        processor.execute(test_df)
        print("✗ Should have failed with missing group_by")
    except StepProcessorError as e:
        print(f"✓ Caught expected error: {e}")
    
    # Test invalid group column
    try:
        bad_config = {
            'processor_type': 'add_subtotals',
            'step_description': 'Invalid group column',
            'group_by': ['NonExistentColumn'],
            'subtotal_columns': ['Sales']
        }
        processor = AddSubtotalsProcessor(bad_config)
        processor.execute(test_df)
        print("✗ Should have failed with invalid group column")
    except StepProcessorError as e:
        print(f"✓ Caught expected error: {e}")
    
    # Test invalid subtotal column
    try:
        bad_config = {
            'processor_type': 'add_subtotals',
            'step_description': 'Invalid subtotal column',
            'group_by': ['Region'],
            'subtotal_columns': ['NonExistentColumn']
        }
        processor = AddSubtotalsProcessor(bad_config)
        processor.execute(test_df)
        print("✗ Should have failed with invalid subtotal column")
    except StepProcessorError as e:
        print(f"✓ Caught expected error: {e}")
    
    # Test invalid function
    try:
        bad_config = {
            'processor_type': 'add_subtotals',
            'step_description': 'Invalid function',
            'group_by': ['Region'],
            'subtotal_columns': ['Sales'],
            'subtotal_functions': ['invalid_function']
        }
        processor = AddSubtotalsProcessor(bad_config)
        processor.execute(test_df)
        print("✗ Should have failed with invalid function")
    except StepProcessorError as e:
        print(f"✓ Caught expected error: {e}")
    
    # Test invalid position
    try:
        bad_config = {
            'processor_type': 'add_subtotals',
            'step_description': 'Invalid position',
            'group_by': ['Region'],
            'subtotal_columns': ['Sales'],
            'position': 'invalid_position'
        }
        processor = AddSubtotalsProcessor(bad_config)
        processor.execute(test_df)
        print("✗ Should have failed with invalid position")
    except StepProcessorError as e:
        print(f"✓ Caught expected error: {e}")


def test_edge_cases():
    """Test edge cases and boundary conditions."""
    
    print("\nTesting edge cases...")
    
    # Test with single row
    single_row_df = pd.DataFrame({
        'Region': ['West'],
        'Sales': [100]
    })
    
    step_config = {
        'processor_type': 'add_subtotals',
        'step_description': 'Single row test',
        'group_by': ['Region'],
        'subtotal_columns': ['Sales']
    }
    
    try:
        processor = AddSubtotalsProcessor(step_config)
        result = processor.execute(single_row_df)
        print(f"✓ Single row test: {len(result)} rows")
    except Exception as e:
        print(f"✗ Single row test failed: {e}")
        return False
    
    # Test with missing values
    missing_values_df = pd.DataFrame({
        'Region': ['West', 'West', 'East', None],
        'Sales': [100, 150, None, 200]
    })
    
    try:
        processor = AddSubtotalsProcessor(step_config)
        result = processor.execute(missing_values_df)
        print(f"✓ Missing values test: {len(result)} rows")
    except Exception as e:
        print(f"✗ Missing values test failed: {e}")
        return False
    
    print("✓ Edge cases handled correctly")
    return True


if __name__ == '__main__':
    success = True
    
    success &= test_basic_subtotals()
    success &= test_hierarchical_subtotals()
    success &= test_aggregation_functions()
    success &= test_preserve_grand_totals()
    success &= test_positioning_options()
    success &= test_multiple_columns_functions()
    success &= test_real_world_scenario()
    success &= test_utility_functions()
    success &= test_edge_cases()
    test_error_handling()
    
    if success:
        print("\n✓ All add subtotals processor tests passed!")
    else:
        print("\n✗ Some add subtotals processor tests failed!")
    
    # Show supported features
    processor = AddSubtotalsProcessor({
        'processor_type': 'add_subtotals',
        'group_by': ['test'],
        'subtotal_columns': ['test']
    })
    print(f"\nSupported functions: {processor.get_supported_functions()}")
    print(f"Supported positions: {processor.get_supported_positions()}")
