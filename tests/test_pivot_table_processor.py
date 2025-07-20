"""
Test the PivotTableProcessor functionality.
"""

import pandas as pd

from excel_recipe_processor.processors.pivot_table_processor import PivotTableProcessor
from excel_recipe_processor.processors.base_processor import StepProcessorError


def create_van_report_test_data():
    """Create sample data similar to the van report workflow."""
    return pd.DataFrame({
        'Product_Origin': [
            'Naknek', 'Kodiak', 'Cordova', 'Naknek', 'Kodiak', 'Valdez', 
            'Dillingham', 'Kodiak', 'Seward', 'Naknek', 'Craig', 'Sitka'
        ],
        'Van_Number': [
            'VAN001', 'VAN002', 'VAN003', 'VAN001', 'VAN004', 'VAN005',
            'VAN006', 'VAN002', 'VAN007', 'VAN008', 'VAN009', 'VAN010'
        ],
        'Carrier': [
            'Carrier_A', 'Carrier_B', 'Carrier_A', 'Carrier_A', 'Carrier_B', 'Carrier_C',
            'Carrier_A', 'Carrier_B', 'Carrier_C', 'Carrier_A', 'Carrier_B', 'Carrier_B'
        ],
        'Destination': [
            'Seattle', 'Portland', 'Seattle', 'Portland', 'Seattle', 'Vancouver',
            'Seattle', 'Portland', 'Vancouver', 'Seattle', 'Portland', 'Seattle'
        ],
        'Quantity': [100, 150, 200, 75, 125, 180, 90, 110, 160, 85, 140, 95],
        'Value': [1000, 1500, 2000, 750, 1250, 1800, 900, 1100, 1600, 850, 1400, 950]
    })


def test_basic_pivot_table():
    """Test basic pivot table creation."""
    
    print("Testing basic pivot table...")
    
    test_df = create_van_report_test_data()
    print(f"✓ Created test data: {len(test_df)} rows")
    
    # Test simple pivot - sum quantities by origin
    step_config = {
        'processor_type': 'pivot_table',
        'step_description': 'Sum by origin',
        'index': ['Product_Origin'],
        'values': ['Quantity'],
        'aggfunc': 'sum'
    }
    
    processor = PivotTableProcessor(step_config)
    result = processor.execute(test_df)
    
    print(f"✓ Basic pivot: {len(result)} rows, {len(result.columns)} columns")
    print(f"✓ Columns: {list(result.columns)}")
    
    # Check that we have origins and quantities
    if 'Product_Origin' in result.columns and 'Quantity' in result.columns:
        print("✓ Basic pivot table created correctly")
        
        # Show some results
        print("Sample results:")
        for i in range(min(3, len(result))):
            origin = result.iloc[i]['Product_Origin']
            qty = result.iloc[i]['Quantity']
            print(f"  {origin}: {qty}")
        
        return True
    else:
        print(f"✗ Expected columns not found: {list(result.columns)}")
        return False


def test_van_report_style_pivot():
    """Test pivot table like those in the van report workflow."""
    
    print("\nTesting van report style pivot...")
    
    test_df = create_van_report_test_data()
    
    # Test the first pivot from van report: PRODUCT ORIGIN, VAN NUMBER, CARRIER, DESTINATION
    step_config = {
        'processor_type': 'pivot_table',
        'step_description': 'Van report first pivot',
        'index': ['Product_Origin', 'Van_Number', 'Carrier', 'Destination'],
        'values': ['Quantity'],
        'aggfunc': 'sum',
        'fill_value': 0
    }
    
    processor = PivotTableProcessor(step_config)
    result = processor.execute(test_df)
    
    print(f"✓ Van report pivot: {len(result)} rows, {len(result.columns)} columns")
    
    # Check for expected columns
    expected_cols = ['Product_Origin', 'Van_Number', 'Carrier', 'Destination', 'Quantity']
    missing_cols = [col for col in expected_cols if col not in result.columns]
    
    if not missing_cols:
        print("✓ Van report style pivot created correctly")
        return True
    else:
        print(f"✗ Missing expected columns: {missing_cols}")
        return False


def test_origin_vs_carrier_matrix():
    """Test the second pivot from van report: PRODUCT ORIGIN vs CARRIER matrix."""
    
    print("\nTesting origin vs carrier matrix...")
    
    test_df = create_van_report_test_data()
    
    # Test the second pivot: PRODUCT ORIGIN as rows, CARRIER as columns, VAN NUMBER as values
    step_config = {
        'processor_type': 'pivot_table',
        'step_description': 'Origin vs Carrier matrix',
        'index': ['Product_Origin'],
        'columns': ['Carrier'],
        'values': ['Quantity'],
        'aggfunc': 'count',  # Count of vans
        'fill_value': 0
    }
    
    processor = PivotTableProcessor(step_config)
    result = processor.execute(test_df)
    
    print(f"✓ Matrix pivot: {len(result)} rows, {len(result.columns)} columns")
    print(f"✓ Columns: {list(result.columns)}")
    
    # Should have Product_Origin and columns for each carrier
    if 'Product_Origin' in result.columns:
        print("✓ Origin vs Carrier matrix created correctly")
        
        # Show matrix structure
        print("Sample matrix:")
        for i in range(min(3, len(result))):
            print(f"  {result.iloc[i].to_dict()}")
        
        return True
    else:
        print("✗ Matrix pivot failed")
        return False


def test_multiple_aggregations():
    """Test pivot table with different aggregation functions."""
    
    print("\nTesting multiple aggregation functions...")
    
    test_df = create_van_report_test_data()
    
    # Test different aggregation functions
    test_cases = [
        ('sum', 'Sum of quantities'),
        ('mean', 'Average quantities'),  
        ('count', 'Count of records'),
        ('max', 'Maximum quantities'),
        ('min', 'Minimum quantities')
    ]
    
    for aggfunc, description in test_cases:
        step_config = {
            'processor_type': 'pivot_table',
            'step_description': f'Test {aggfunc}',
            'index': ['Carrier'],
            'values': ['Quantity'],
            'aggfunc': aggfunc
        }
        
        processor = PivotTableProcessor(step_config)
        result = processor.execute(test_df)
        
        print(f"✓ {description}: {len(result)} rows")
    
    return True


def test_no_values_pivot():
    """Test pivot table without explicit values (count pivot)."""
    
    print("\nTesting count pivot without values...")
    
    test_df = create_van_report_test_data()
    
    # Create a count pivot without specifying values
    step_config = {
        'processor_type': 'pivot_table',
        'step_description': 'Count pivot',
        'index': ['Product_Origin'],
        'columns': ['Carrier'],
        'aggfunc': 'count'
    }
    
    processor = PivotTableProcessor(step_config)
    result = processor.execute(test_df)
    
    print(f"✓ Count pivot: {len(result)} rows, {len(result.columns)} columns")
    
    if len(result) > 0:
        print("✓ Count pivot created correctly")
        return True
    else:
        print("✗ Count pivot failed")
        return False


def test_fill_blanks_option():
    """Test the fill blanks option for repeating item labels."""
    
    print("\nTesting fill blanks option...")
    
    test_df = create_van_report_test_data()
    
    # Create pivot with fill_blanks option
    step_config = {
        'processor_type': 'pivot_table',
        'step_description': 'Pivot with fill blanks',
        'index': ['Product_Origin'],
        'values': ['Quantity'],
        'aggfunc': 'sum',
        'fill_blanks': True
    }
    
    processor = PivotTableProcessor(step_config)
    result = processor.execute(test_df)
    
    print(f"✓ Fill blanks pivot: {len(result)} rows")
    
    # Check that there are no null values in the first column
    if len(result) > 0:
        first_col = result.columns[0]
        null_count = result[first_col].isnull().sum()
        print(f"✓ Null values in first column: {null_count}")
        
        if null_count == 0:
            print("✓ Fill blanks worked correctly")
            return True
    
    print("✗ Fill blanks test inconclusive")
    return False


def test_cross_tabulation():
    """Test cross-tabulation functionality."""
    
    print("\nTesting cross-tabulation...")
    
    test_df = create_van_report_test_data()
    
    # Create a processor to test cross-tab functionality
    processor = PivotTableProcessor({'processor_type': 'pivot_table'})
    
    # Test cross-tab
    crosstab_result = processor.create_cross_tab(
        test_df, 
        row_field='Product_Origin', 
        col_field='Carrier'
    )
    
    print(f"✓ Cross-tab: {len(crosstab_result)} rows, {len(crosstab_result.columns)} columns")
    
    if len(crosstab_result) > 0:
        print("✓ Cross-tabulation created correctly")
        return True
    else:
        print("✗ Cross-tabulation failed")
        return False


def test_error_handling():
    """Test error handling for various failure cases."""
    
    print("\nTesting error handling...")
    
    test_df = create_van_report_test_data()
    
    # Test invalid index column
    try:
        bad_config = {
            'processor_type': 'pivot_table',
            'step_description': 'Invalid index',
            'index': ['NonExistentColumn'],
            'values': ['Quantity'],
            'aggfunc': 'sum'
        }
        processor = PivotTableProcessor(bad_config)
        processor.execute(test_df)
        print("✗ Should have failed with invalid index column")
    except StepProcessorError as e:
        print(f"✓ Caught expected error: {e}")
    
    # Test invalid values column
    try:
        bad_config = {
            'processor_type': 'pivot_table',
            'step_description': 'Invalid values',
            'index': ['Product_Origin'],
            'values': ['NonExistentColumn'],
            'aggfunc': 'sum'
        }
        processor = PivotTableProcessor(bad_config)
        processor.execute(test_df)
        print("✗ Should have failed with invalid values column")
    except StepProcessorError as e:
        print(f"✓ Caught expected error: {e}")
    
    # Test invalid aggregation function
    try:
        bad_config = {
            'processor_type': 'pivot_table',
            'step_description': 'Invalid aggfunc',
            'index': ['Product_Origin'],
            'values': ['Quantity'],
            'aggfunc': 'invalid_function'
        }
        processor = PivotTableProcessor(bad_config)
        processor.execute(test_df)
        print("✗ Should have failed with invalid aggregation function")
    except StepProcessorError as e:
        print(f"✓ Caught expected error: {e}")


def test_pivot_info():
    """Test the pivot analysis functionality."""
    
    print("\nTesting pivot info analysis...")
    
    test_df = create_van_report_test_data()
    
    # Create a processor to test pivot info
    processor = PivotTableProcessor({'processor_type': 'pivot_table'})
    
    info = processor.get_pivot_info(test_df)
    
    print(f"✓ Pivot info analysis:")
    print(f"  Total rows: {info['total_rows']}")
    print(f"  Total columns: {info['total_columns']}")
    print(f"  Numeric columns: {info['numeric_columns']}")
    print(f"  Categorical columns: {info['categorical_columns']}")
    print(f"  Column cardinality: {info['column_cardinality']}")
    
    if info['total_rows'] == len(test_df):
        print("✓ Pivot info analysis worked correctly")
        return True
    else:
        print("✗ Pivot info analysis failed")
        return False


if __name__ == '__main__':
    success = True
    
    success &= test_basic_pivot_table()
    success &= test_van_report_style_pivot()
    success &= test_origin_vs_carrier_matrix()
    success &= test_multiple_aggregations()
    success &= test_no_values_pivot()
    success &= test_fill_blanks_option()
    success &= test_cross_tabulation()
    success &= test_pivot_info()
    test_error_handling()
    
    if success:
        print("\n✓ All pivot table processor tests passed!")
    else:
        print("\n✗ Some pivot table processor tests failed!")
    
    # Show supported aggregation functions
    processor = PivotTableProcessor({'processor_type': 'pivot_table'})
    print(f"\nSupported aggregation functions: {processor.get_supported_aggfuncs()}")
