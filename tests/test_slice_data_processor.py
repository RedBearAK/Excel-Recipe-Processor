"""
Test the SliceDataProcessor functionality.
"""

import pandas as pd

from excel_recipe_processor.core.stage_manager import StageManager
from excel_recipe_processor.processors.slice_data_processor import SliceDataProcessor


def create_test_data():
    """Create sample data that mimics a real Excel file with metadata."""
    return pd.DataFrame([
        ['Company Monthly Report - March 2024', '', '', ''],
        ['Generated on: 2024-03-15', '', '', ''],
        ['', '', '', ''],  # Blank row
        ['Product ID', 'Product Name', 'Quantity', 'Price'],  # Headers
        ['A001', 'Widget A', '100', '10.50'],
        ['B002', 'Gadget B', '50', '25.00'],
        ['C003', 'Tool C', '75', '15.75'],
        ['A001', 'Widget A', '25', '10.50'],
        ['', '', '', ''],  # Another blank row
        ['Total Records: 4', '', '', ''],
        ['Report End', '', '', '']
    ])


def test_basic_row_slicing():
    """Test basic row range slicing with 1-based indexing."""
    print("Testing basic row slicing...")
    
    test_df = create_test_data()
    
    # Extract metadata rows (1-2 in user terms, 0-1 in pandas)
    step_config = {
        'processor_type': 'slice_data',
        'step_description': 'Extract metadata section',
        'slice_type': 'row_range',
        'start_row': 1,  # First row in Excel
        'end_row': 2     # Second row in Excel
    }
    
    processor = SliceDataProcessor(step_config)
    result = processor.execute(test_df)
    
    # Should get 2 rows of metadata
    if len(result) == 2:
        print("✓ Row slicing returned correct number of rows")
    else:
        print(f"✗ Expected 2 rows, got {len(result)}")
        return False
    
    # Check that we got the right content
    if 'Company Monthly Report' in str(result.iloc[0, 0]):
        print("✓ Row slicing extracted correct content")
        return True
    else:
        print(f"✗ Wrong content: {result.iloc[0, 0]}")
        return False


def test_data_section_with_headers():
    """Test extracting data section and promoting headers."""
    print("\nTesting data section extraction with header promotion...")
    
    test_df = create_test_data()
    
    # Extract data rows starting from row 4, promote first row of slice to headers
    step_config = {
        'processor_type': 'slice_data',
        'step_description': 'Extract data with headers',
        'slice_type': 'row_range',
        'start_row': 4,    # Where headers start (1-based)
        'end_row': 8,      # End of data (1-based)
        'slice_result_contains_headers': True  # First row of slice becomes headers
    }
    
    processor = SliceDataProcessor(step_config)
    result = processor.execute(test_df)
    
    # Should get 4 rows of data (after removing header row)
    if len(result) == 4:
        print("✓ Data extraction returned correct number of rows")
    else:
        print(f"✗ Expected 4 rows, got {len(result)}")
        return False
    
    # Check that headers were promoted correctly
    if 'Product ID' in result.columns:
        print("✓ Headers promoted correctly")
    else:
        print(f"✗ Headers not promoted correctly: {list(result.columns)}")
        return False
    
    # Check data content
    if result.iloc[0]['Product ID'] == 'A001':
        print("✓ Data content preserved correctly")
        return True
    else:
        print(f"✗ Data content incorrect: {result.iloc[0]['Product ID']}")
        return False


def test_column_slicing():
    """Test column range slicing."""
    print("\nTesting column slicing...")
    
    test_df = create_test_data()
    
    # Extract first 2 columns
    step_config = {
        'processor_type': 'slice_data',
        'step_description': 'Extract first two columns',
        'slice_type': 'column_range',
        'start_col': 1,  # First column (1-based)
        'end_col': 2     # Second column (1-based)
    }
    
    processor = SliceDataProcessor(step_config)
    result = processor.execute(test_df)
    
    # Should get all rows but only 2 columns
    if len(result.columns) == 2:
        print("✓ Column slicing returned correct number of columns")
    else:
        print(f"✗ Expected 2 columns, got {len(result.columns)}")
        return False
    
    if len(result) == len(test_df):
        print("✓ Column slicing preserved all rows")
        return True
    else:
        print(f"✗ Row count changed: {len(test_df)} → {len(result)}")
        return False


def test_slice_without_headers():
    """Test slicing without header promotion (default behavior)."""
    print("\nTesting slice without header promotion...")
    
    test_df = create_test_data()
    
    # Extract data rows without promoting headers
    step_config = {
        'processor_type': 'slice_data',
        'step_description': 'Extract without headers',
        'slice_type': 'row_range',
        'start_row': 4,
        'end_row': 6
        # slice_result_contains_headers defaults to False
    }
    
    processor = SliceDataProcessor(step_config)
    result = processor.execute(test_df)
    
    # Should get 3 rows with generic column names
    if len(result) == 3:
        print("✓ Slice without headers returned correct number of rows")
    else:
        print(f"✗ Expected 3 rows, got {len(result)}")
        return False
    
    # Check that headers were NOT promoted (should have generic column names)
    if list(result.columns) == [0, 1, 2, 3]:
        print("✓ Generic column names preserved (headers not promoted)")
        return True
    else:
        print(f"✗ Unexpected column names: {list(result.columns)}")
        return False


def test_excel_column_references():
    """Test Excel-style column references (A, B, etc.)."""
    print("\nTesting Excel column references...")
    
    test_df = create_test_data()
    
    # Extract columns A-B (Excel style)
    step_config = {
        'processor_type': 'slice_data',
        'step_description': 'Extract columns A-B',
        'slice_type': 'column_range',
        'start_col': 'A',
        'end_col': 'B'
    }
    
    processor = SliceDataProcessor(step_config)
    result = processor.execute(test_df)
    
    if len(result.columns) == 2:
        print("✓ Excel column references work correctly")
        return True
    else:
        print(f"✗ Expected 2 columns from A-B, got {len(result.columns)}")
        return False


def test_stage_integration():
    """Test loading data from a stage instead of input data."""
    print("\nTesting stage integration...")
    
    # Setup stage manager and save test data
    StageManager.initialize_stages()
    
    try:
        test_df = create_test_data()
        StageManager.save_stage('Test Data', test_df, description='Test data for slicing')
        
        # Slice from stage instead of input data
        step_config = {
            'processor_type': 'slice_data',
            'step_description': 'Extract from stage',
            'slice_type': 'row_range',
            'start_row': 1,
            'end_row': 3,
            'source_stage': 'Test Data'
        }
        
        processor = SliceDataProcessor(step_config)
        # Pass None as input data since we're using source_stage
        result = processor.execute(None)
        
        if len(result) == 3:
            print("✓ Stage integration works correctly")
            return True
        else:
            print(f"✗ Expected 3 rows from stage, got {len(result)}")
            return False
            
    finally:
        StageManager.cleanup_stages()


def test_error_handling():
    """Test error handling for invalid configurations."""
    print("\nTesting error handling...")
    
    test_df = create_test_data()
    
    # Test invalid start_row
    try:
        step_config = {
            'processor_type': 'slice_data',
            'slice_type': 'row_range',
            'start_row': 0,  # Invalid: should be >= 1
            'end_row': 2
        }
        processor = SliceDataProcessor(step_config)
        processor.execute(test_df)
        print("✗ Should have failed with invalid start_row")
        return False
    except Exception as e:
        print(f"✓ Caught expected error for invalid start_row: {type(e).__name__}")
    
    # Test start_row beyond data
    try:
        step_config = {
            'processor_type': 'slice_data',
            'slice_type': 'row_range',
            'start_row': 100,  # Beyond data length
            'end_row': 102
        }
        processor = SliceDataProcessor(step_config)
        processor.execute(test_df)
        print("✗ Should have failed with start_row beyond data")
        return False
    except Exception as e:
        print(f"✓ Caught expected error for start_row beyond data: {type(e).__name__}")
    
    print("✓ Error handling works correctly")
    return True


def test_capabilities():
    """Test processor capabilities reporting."""
    print("\nTesting capabilities...")
    
    step_config = {
        'processor_type': 'slice_data',
        'slice_type': 'row_range',
        'start_row': 1,
        'end_row': 1
    }
    
    processor = SliceDataProcessor(step_config)
    capabilities = processor.get_capabilities()
    
    if 'slice_operations' in capabilities:
        print("✓ Capabilities include slice operations")
    else:
        print("✗ Missing slice operations in capabilities")
        return False
    
    supported_types = processor.get_supported_slice_types()
    if 'row_range' in supported_types and 'column_range' in supported_types:
        print("✓ Supported slice types reported correctly")
        return True
    else:
        print(f"✗ Missing expected slice types: {supported_types}")
        return False


if __name__ == '__main__':
    print("Testing SliceDataProcessor...")
    success = True
    
    success &= test_basic_row_slicing()
    success &= test_data_section_with_headers()
    success &= test_slice_without_headers()
    success &= test_column_slicing()
    success &= test_excel_column_references()
    success &= test_stage_integration()
    success &= test_error_handling()
    success &= test_capabilities()
    
    if success:
        print("\n🎉 All SliceDataProcessor tests passed!")
    else:
        print("\n❌ Some SliceDataProcessor tests failed!")
    
    print("\nTo run with pytest: pytest test_slice_data_processor.py -v")
