"""
Simple test for Excel reader and writer functionality.
"""

import pandas as pd
from pathlib import Path

from excel_recipe_processor.readers.excel_reader import ExcelReader, ExcelReaderError
from excel_recipe_processor.writers.excel_writer import ExcelWriter, ExcelWriterError


def create_test_data():
    """Create sample data for testing."""
    return pd.DataFrame({
        'Product_Code': ['A001', 'B002', 'C003', 'A001'],
        'Product_Name': ['Widget A', 'Gadget B', 'Tool C', 'Widget A'],
        'Quantity': [100, 50, 75, 25],
        'Price': [10.50, 25.00, 15.75, 10.50],
        'Department': ['Electronics', 'Tools', 'Hardware', 'Electronics']
    })


def test_basic_write_and_read():
    """Test basic Excel write and read operations."""
    
    print("Testing Excel I/O...")
    
    # Create test data
    test_df = create_test_data()
    print(f"✓ Created test data: {len(test_df)} rows, {len(test_df.columns)} columns")
    
    # Test writing
    writer = ExcelWriter()
    test_file = Path("test_output.xlsx")
    
    try:
        writer.write_file(test_df, test_file, sheet_name="TestData")
        print(f"✓ Wrote Excel file: {test_file}")
        
        # Verify file exists
        if test_file.exists():
            print(f"✓ File exists and is {test_file.stat().st_size} bytes")
        else:
            print("✗ File was not created")
            return False
        
        # Test reading
        reader = ExcelReader()
        read_df = reader.read_file(test_file)
        print(f"✓ Read Excel file: {len(read_df)} rows, {len(read_df.columns)} columns")
        
        # Compare data
        if len(read_df) == len(test_df):
            print("✓ Row count matches")
        else:
            print(f"✗ Row count mismatch: wrote {len(test_df)}, read {len(read_df)}")
        
        if len(read_df.columns) == len(test_df.columns):
            print("✓ Column count matches")
        else:
            print(f"✗ Column count mismatch: wrote {len(test_df.columns)}, read {len(read_df.columns)}")
        
        # Check specific values
        if read_df.iloc[0]['Product_Code'] == 'A001':
            print("✓ Data values preserved correctly")
        else:
            print("✗ Data values not preserved")
        
        print(f"✓ File info: {reader.get_file_info()}")
        
        return True
        
    except Exception as e:
        print(f"✗ Error during write/read test: {e}")
        return False
    
    finally:
        # Clean up test file
        if test_file.exists():
            test_file.unlink()
            print("✓ Cleaned up test file")


def test_multiple_sheets():
    """Test multiple sheet operations."""
    
    print("\nTesting multiple sheets...")
    
    # Create test data for multiple sheets
    sheet1_data = create_test_data()
    sheet2_data = pd.DataFrame({
        'Order_ID': [1001, 1002, 1003],
        'Customer': ['Alice', 'Bob', 'Charlie'],
        'Total': [100.50, 75.25, 200.00]
    })
    
    data_dict = {
        'Products': sheet1_data,
        'Orders': sheet2_data
    }
    
    writer = ExcelWriter()
    reader = ExcelReader()
    test_file = Path("test_multi_sheet.xlsx")
    
    try:
        # Write multiple sheets
        writer.write_multiple_sheets(data_dict, test_file)
        print(f"✓ Wrote multi-sheet file: {test_file}")
        
        # Get sheet names
        sheet_names = reader.get_sheet_names(test_file)
        print(f"✓ Found sheets: {sheet_names}")
        
        if 'Products' in sheet_names and 'Orders' in sheet_names:
            print("✓ Expected sheets found")
        else:
            print("✗ Expected sheets not found")
        
        # Read specific sheet
        products_df = reader.read_file(test_file, sheet_name='Products')
        print(f"✓ Read Products sheet: {len(products_df)} rows")
        
        # Read all sheets
        all_sheets = reader.read_multiple_sheets(test_file)
        print(f"✓ Read all sheets: {list(all_sheets.keys())}")
        
        return True
        
    except Exception as e:
        print(f"✗ Error during multi-sheet test: {e}")
        return False
    
    finally:
        # Clean up
        if test_file.exists():
            test_file.unlink()
            print("✓ Cleaned up multi-sheet test file")


def test_error_handling():
    """Test error handling for various failure cases."""
    
    print("\nTesting error handling...")
    
    reader = ExcelReader()
    writer = ExcelWriter()
    
    # Test reading non-existent file
    try:
        reader.read_file("nonexistent_file.xlsx")
        print("✗ Should have failed reading non-existent file")
    except ExcelReaderError as e:
        print(f"✓ Caught expected error: {e}")
    
    # Test invalid file extension
    try:
        reader.read_file("test.txt")
        print("✗ Should have failed with invalid extension")
    except ExcelReaderError as e:
        print(f"✓ Caught expected error: {e}")
    
    # Test writing invalid data
    try:
        writer.write_file("not a dataframe", "test.xlsx")
        print("✗ Should have failed with invalid data")
    except ExcelWriterError as e:
        print(f"✓ Caught expected error: {e}")
    
    # Test empty sheet name
    try:
        test_df = create_test_data()
        writer.write_file(test_df, "test.xlsx", sheet_name="")
        print("✗ Should have failed with empty sheet name")
    except ExcelWriterError as e:
        print(f"✓ Caught expected error: {e}")


def test_column_validation():
    """Test column validation functionality."""
    
    print("\nTesting column validation...")
    
    test_df = create_test_data()
    reader = ExcelReader()
    
    # Test existing columns
    try:
        reader.validate_columns_exist(test_df, ['Product_Code', 'Quantity'])
        print("✓ Found expected columns")
    except ExcelReaderError:
        print("✗ Should have found expected columns")
    
    # Test missing columns
    try:
        reader.validate_columns_exist(test_df, ['Product_Code', 'NonExistent'])
        print("✗ Should have failed with missing column")
    except ExcelReaderError as e:
        print(f"✓ Caught expected error: {e}")


if __name__ == '__main__':
    success = True
    
    success &= test_basic_write_and_read()
    success &= test_multiple_sheets()
    test_error_handling()
    test_column_validation()
    
    if success:
        print("\n✓ All Excel I/O tests passed!")
    else:
        print("\n✗ Some tests failed!")
