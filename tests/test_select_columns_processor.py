"""
Test the SelectColumnsProcessor functionality.

tests/test_select_columns_processor.py
"""

import pandas as pd

from excel_recipe_processor.core.base_processor import StepProcessorError
from excel_recipe_processor.processors.select_columns_processor import SelectColumnsProcessor


def create_test_data():
    """Create sample DataFrame for testing."""
    return pd.DataFrame({
        'Customer_ID': ['C001', 'C002', 'C003', 'C004', 'C005'],
        'Product_Name': ['Widget A', 'Gadget B', 'Tool C', 'Device D', 'Item E'],
        'Price': [10.50, 25.00, 15.75, 30.00, 8.25],
        'Quantity': [100, 50, 75, 25, 200],
        'Category': ['Electronics', 'Tools', 'Hardware', 'Electronics', 'Tools'],
        'Status': ['Active', 'Active', 'Cancelled', 'Active', 'Active'],
        'Internal_Notes': ['Note 1', 'Note 2', 'Note 3', 'Note 4', 'Note 5'],
        'Debug_Info': ['Debug 1', 'Debug 2', 'Debug 3', 'Debug 4', 'Debug 5']
    })


def test_basic_column_selection():
    """Test basic column selection functionality."""
    
    print("Testing basic column selection...")
    
    test_df = create_test_data()
    print(f"✓ Created test data: {len(test_df)} rows, {len(test_df.columns)} columns")
    
    # Test selecting specific columns
    step_config = {
        'processor_type': 'select_columns',
        'step_description': 'Select essential columns',
        'columns_to_keep': ['Customer_ID', 'Product_Name', 'Price']
    }
    
    processor = SelectColumnsProcessor(step_config)
    result = processor.execute(test_df)
    
    print(f"✓ Selected columns: {list(result.columns)}")
    
    # Check that we got exactly the columns we wanted
    expected_columns = ['Customer_ID', 'Product_Name', 'Price']
    if list(result.columns) == expected_columns:
        print("✓ Basic column selection worked correctly")
        
        # Check that data is preserved
        if len(result) == len(test_df):
            print("✓ All rows preserved during selection")
            return True
        else:
            print(f"✗ Row count changed: {len(test_df)} → {len(result)}")
    else:
        print(f"✗ Expected {expected_columns}, got {list(result.columns)}")
        
    return False


def test_column_reordering():
    """Test column reordering functionality."""
    
    print("\nTesting column reordering...")
    
    test_df = create_test_data()
    
    # Test reordering - Price first, then ID, then Name
    step_config = {
        'processor_type': 'select_columns',
        'step_description': 'Reorder columns',
        'columns_to_keep': ['Price', 'Customer_ID', 'Product_Name', 'Status']
    }
    
    processor = SelectColumnsProcessor(step_config)
    result = processor.execute(test_df)
    
    print(f"✓ Reordered columns: {list(result.columns)}")
    
    # Check that columns are in the correct order
    expected_order = ['Price', 'Customer_ID', 'Product_Name', 'Status']
    if list(result.columns) == expected_order:
        print("✓ Column reordering worked correctly")
        
        # Verify data integrity with new order
        first_row_price = result.iloc[0]['Price']
        first_row_id = result.iloc[0]['Customer_ID']
        
        if first_row_price == 10.50 and first_row_id == 'C001':
            print("✓ Data integrity maintained during reordering")
            return True
        else:
            print("✗ Data integrity compromised")
    else:
        print(f"✗ Expected order {expected_order}, got {list(result.columns)}")
        
    return False


def test_column_dropping():
    """Test column exclusion functionality."""
    
    print("\nTesting column dropping...")
    
    test_df = create_test_data()
    original_columns = set(test_df.columns)
    
    # Test dropping internal columns
    step_config = {
        'processor_type': 'select_columns',
        'step_description': 'Remove internal columns',
        'columns_to_drop': ['Internal_Notes', 'Debug_Info']
    }
    
    processor = SelectColumnsProcessor(step_config)
    result = processor.execute(test_df)
    
    print(f"✓ Remaining columns: {list(result.columns)}")
    
    # Check that unwanted columns are gone
    unwanted_columns = ['Internal_Notes', 'Debug_Info']
    has_unwanted = any(col in result.columns for col in unwanted_columns)
    
    if not has_unwanted:
        print("✓ Unwanted columns successfully removed")
        
        # Check that other columns remain
        expected_remaining = original_columns - set(unwanted_columns)
        actual_remaining = set(result.columns)
        
        if actual_remaining == expected_remaining:
            print("✓ All other columns preserved")
            return True
        else:
            print(f"✗ Column mismatch: expected {expected_remaining}, got {actual_remaining}")
    else:
        print("✗ Unwanted columns still present")
        
    return False


def test_column_duplication():
    """Test column duplication functionality."""
    
    print("\nTesting column duplication...")
    
    test_df = create_test_data()
    
    # Test duplicating Price column for comparison
    step_config = {
        'processor_type': 'select_columns',
        'step_description': 'Duplicate price for comparison',
        'columns_to_keep': ['Product_Name', 'Price', 'Price', 'Status'],
        'allow_duplicates': True
    }
    
    processor = SelectColumnsProcessor(step_config)
    result = processor.execute(test_df)
    
    print(f"✓ Result columns: {list(result.columns)}")
    print(f"✓ Result shape: {result.shape}")
    
    # Check that we have the expected columns including duplicates
    expected_columns = ['Product_Name', 'Price', 'Price', 'Status']
    if list(result.columns) == expected_columns:
        print("✓ Column duplication worked correctly")
        
        # Verify that both Price columns contain the same data
        price_col_1 = result.iloc[:, 1]  # First Price column
        price_col_2 = result.iloc[:, 2]  # Second Price column
        
        if price_col_1.equals(price_col_2):
            print("✓ Duplicated columns contain identical data")
            return True
        else:
            print("✗ Duplicated columns have different data")
    else:
        print(f"✗ Expected {expected_columns}, got {list(result.columns)}")
        
    return False


def test_strict_mode_handling():
    """Test strict mode vs flexible handling of missing columns."""
    
    print("\nTesting strict mode handling...")
    
    test_df = create_test_data()
    
    # Test strict mode (should fail on missing column)
    strict_config = {
        'processor_type': 'select_columns',
        'step_description': 'Strict mode test',
        'columns_to_keep': ['Customer_ID', 'NonExistentColumn', 'Price'],
        'strict_mode': True
    }
    
    try:
        processor = SelectColumnsProcessor(strict_config)
        processor.execute(test_df)
        print("✗ Strict mode should have failed with missing column")
        return False
    except StepProcessorError as e:
        print(f"✓ Strict mode correctly failed: {e}")
    
    # Test flexible mode (should skip missing column)
    flexible_config = {
        'processor_type': 'select_columns',
        'step_description': 'Flexible mode test',
        'columns_to_keep': ['Customer_ID', 'NonExistentColumn', 'Price'],
        'strict_mode': False
    }
    
    try:
        processor = SelectColumnsProcessor(flexible_config)
        result = processor.execute(test_df)
        
        # Should have Customer_ID and Price, but not NonExistentColumn
        expected_columns = ['Customer_ID', 'Price']
        if list(result.columns) == expected_columns:
            print("✓ Flexible mode correctly skipped missing column")
            return True
        else:
            print(f"✗ Expected {expected_columns}, got {list(result.columns)}")
    except Exception as e:
        print(f"✗ Flexible mode failed unexpectedly: {e}")
        return False
    
    return False


def test_duplicate_prevention():
    """Test prevention of duplicate columns when not allowed."""
    
    print("\nTesting duplicate prevention...")
    
    test_df = create_test_data()
    
    # Test with allow_duplicates=False
    step_config = {
        'processor_type': 'select_columns',
        'step_description': 'No duplicates allowed',
        'columns_to_keep': ['Customer_ID', 'Price', 'Price'],  # Duplicate Price
        'allow_duplicates': False
    }
    
    try:
        processor = SelectColumnsProcessor(step_config)
        processor.execute(test_df)
        print("✗ Should have failed with duplicate prevention")
        return False
    except StepProcessorError as e:
        print(f"✓ Duplicate prevention worked: {e}")
        return True


def test_error_handling():
    """Test various error conditions."""
    
    print("\nTesting error handling...")
    
    test_df = create_test_data()
    
    # Test with both inclusion and exclusion specified
    try:
        bad_config = {
            'processor_type': 'select_columns',
            'columns_to_keep': ['Customer_ID'],
            'columns_to_drop': ['Debug_Info']  # Can't have both
        }
        processor = SelectColumnsProcessor(bad_config)
        processor.execute(test_df)
        print("✗ Should have failed with both inclusion and exclusion")
    except StepProcessorError as e:
        print(f"✓ Caught expected error: {e}")
    
    # Test with neither inclusion nor exclusion
    try:
        bad_config = {
            'processor_type': 'select_columns',
            'step_description': 'Missing selection criteria'
            # No columns_to_keep or columns_to_drop
        }
        processor = SelectColumnsProcessor(bad_config)
        processor.execute(test_df)
        print("✗ Should have failed with no selection criteria")
    except StepProcessorError as e:
        print(f"✓ Caught expected error: {e}")
    
    # Test empty columns_to_keep list
    try:
        bad_config = {
            'processor_type': 'select_columns',
            'columns_to_keep': []  # Empty list
        }
        processor = SelectColumnsProcessor(bad_config)
        processor.execute(test_df)
        print("✗ Should have failed with empty columns list")
    except StepProcessorError as e:
        print(f"✓ Caught expected error: {e}")
    
    print("✓ Error handling tests completed")


def test_helper_methods():
    """Test helper methods for column analysis."""
    
    print("\nTesting helper methods...")
    
    test_df = create_test_data()
    
    processor = SelectColumnsProcessor({
        'processor_type': 'select_columns',
        'columns_to_keep': ['Customer_ID']
    })
    
    # Test column info
    column_info = processor.get_column_info(test_df)
    
    if column_info['total_columns'] == len(test_df.columns):
        print("✓ get_column_info works correctly")
    else:
        print("✗ get_column_info failed")
        return False
    
    # Test column suggestions
    suggestions = processor.suggest_common_columns(test_df)
    
    if 'id_columns' in suggestions and 'Customer_ID' in suggestions['id_columns']:
        print("✓ suggest_common_columns works correctly")
        return True
    else:
        print("✗ suggest_common_columns failed")
        return False


def test_capabilities():
    """Test processor capabilities reporting."""
    
    print("\nTesting capabilities...")
    
    processor = SelectColumnsProcessor({
        'processor_type': 'select_columns',
        'columns_to_keep': ['test']
    })
    
    capabilities = processor.get_capabilities()
    
    if 'column_operations' in capabilities:
        print("✓ Capabilities include column operations")
    else:
        print("✗ Missing column operations in capabilities")
        return False
    
    supported_modes = processor.get_supported_selection_modes()
    if 'inclusion' in supported_modes and 'exclusion' in supported_modes:
        print("✓ Supported selection modes reported correctly")
        return True
    else:
        print(f"✗ Missing expected selection modes: {supported_modes}")
        return False


def main():
    """Run all tests with success tracking."""
    
    print("Testing SelectColumnsProcessor...")
    success = True
    
    success &= test_basic_column_selection()
    success &= test_column_reordering()
    success &= test_column_dropping()
    success &= test_column_duplication()
    success &= test_strict_mode_handling()
    success &= test_duplicate_prevention()
    success &= test_helper_methods()
    success &= test_capabilities()
    
    # Error handling doesn't return success/failure
    test_error_handling()
    
    if success:
        print("\n🎉 All SelectColumnsProcessor tests passed!")
        return 1
    else:
        print("\n❌ Some SelectColumnsProcessor tests failed!")
        return 0


if __name__ == '__main__':
    print("Testing SelectColumnsProcessor...")
    success = True
    
    success &= test_basic_column_selection()
    success &= test_column_reordering()
    success &= test_column_dropping()
    success &= test_column_duplication()
    success &= test_strict_mode_handling()
    success &= test_duplicate_prevention()
    success &= test_helper_methods()
    success &= test_capabilities()
    
    # Error handling doesn't return success/failure
    test_error_handling()
    
    if success:
        print("\n🎉 All SelectColumnsProcessor tests passed!")
    else:
        print("\n❌ Some SelectColumnsProcessor tests failed!")
    
    print("\nTo run with pytest: pytest test_select_columns_processor.py -v")


# End of file #
