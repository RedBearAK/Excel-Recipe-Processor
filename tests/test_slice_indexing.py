"""
Focused indexing tests for SliceDataProcessor.

tests/test_slice_indexing.py

Specifically tests 1-based to 0-based indexing conversions to catch off-by-one errors
and ensure user-facing indexing matches Excel/screen expectations.
"""

import pandas as pd

from excel_recipe_processor.core.base_processor import StepProcessorError
from excel_recipe_processor.processors.slice_data_processor import SliceDataProcessor


def create_indexed_test_data():
    """Create test data with known positions for indexing verification."""
    return pd.DataFrame({
        'Col_A': ['A1', 'A2', 'A3', 'A4', 'A5', 'A6'],  # Column A (position 1)
        'Col_B': ['B1', 'B2', 'B3', 'B4', 'B5', 'B6'],  # Column B (position 2) 
        'Col_C': ['C1', 'C2', 'C3', 'C4', 'C5', 'C6'],  # Column C (position 3)
        'Col_D': ['D1', 'D2', 'D3', 'D4', 'D5', 'D6'],  # Column D (position 4)
        'Col_E': ['E1', 'E2', 'E3', 'E4', 'E5', 'E6']   # Column E (position 5)
    })


def test_row_indexing_first_row():
    """Test that row 1 (user) = row 0 (pandas)."""
    
    print("Testing row indexing - first row...")
    
    test_df = create_indexed_test_data()
    
    # Extract just the first row (row 1 in user terms)
    step_config = {
        'processor_type': 'slice_data',
        'slice_type': 'row_range',
        'start_row': 1,
        'end_row': 1
    }
    
    processor = SliceDataProcessor(step_config)
    result = processor.execute(test_df)
    
    print(f"✓ Row 1 extraction: {len(result)} rows")
    
    # Should get exactly 1 row with values from pandas row 0
    if len(result) == 1:
        first_value = result.iloc[0]['Col_A']
        if first_value == 'A1':  # This is from pandas row 0
            print("✓ Row 1 correctly maps to pandas row 0")
            return True
        else:
            print(f"✗ Row 1 gave wrong data: expected 'A1', got '{first_value}'")
    else:
        print(f"✗ Expected 1 row, got {len(result)}")
    
    return False


def test_row_indexing_middle_row():
    """Test that row 3 (user) = row 2 (pandas)."""
    
    print("\nTesting row indexing - middle row...")
    
    test_df = create_indexed_test_data()
    
    # Extract row 3 only
    step_config = {
        'processor_type': 'slice_data',
        'slice_type': 'row_range',
        'start_row': 3,
        'end_row': 3
    }
    
    processor = SliceDataProcessor(step_config)
    result = processor.execute(test_df)
    
    print(f"✓ Row 3 extraction: {len(result)} rows")
    
    # Should get row with values from pandas row 2
    if len(result) == 1:
        third_row_value = result.iloc[0]['Col_A']
        if third_row_value == 'A3':  # This is from pandas row 2
            print("✓ Row 3 correctly maps to pandas row 2")
            return True
        else:
            print(f"✗ Row 3 gave wrong data: expected 'A3', got '{third_row_value}'")
    else:
        print(f"✗ Expected 1 row, got {len(result)}")
    
    return False


def test_row_indexing_range():
    """Test row range indexing (inclusive end)."""
    
    print("\nTesting row range indexing...")
    
    test_df = create_indexed_test_data()
    
    # Extract rows 2-4 (user terms)
    step_config = {
        'processor_type': 'slice_data',
        'slice_type': 'row_range', 
        'start_row': 2,
        'end_row': 4
    }
    
    processor = SliceDataProcessor(step_config)
    result = processor.execute(test_df)
    
    print(f"✓ Rows 2-4 extraction: {len(result)} rows")
    
    # Should get 3 rows (rows 2, 3, 4 in user terms = pandas rows 1, 2, 3)
    if len(result) == 3:
        values = [result.iloc[i]['Col_A'] for i in range(3)]
        expected = ['A2', 'A3', 'A4']  # From pandas rows 1, 2, 3
        
        if values == expected:
            print(f"✓ Row range 2-4 correctly maps to pandas rows 1-3: {values}")
            return True
        else:
            print(f"✗ Row range gave wrong data: expected {expected}, got {values}")
    else:
        print(f"✗ Expected 3 rows, got {len(result)}")
    
    return False


def test_column_indexing_numeric_first():
    """Test that column 1 (user) = column 0 (pandas)."""
    
    print("\nTesting column indexing - first column by number...")
    
    test_df = create_indexed_test_data()
    
    # Extract just the first column (column 1 in user terms)
    step_config = {
        'processor_type': 'slice_data',
        'slice_type': 'column_range',
        'start_col': 1,
        'end_col': 1
    }
    
    processor = SliceDataProcessor(step_config)
    result = processor.execute(test_df)
    
    print(f"✓ Column 1 extraction: {len(result.columns)} columns")
    print(f"✓ Column name: {list(result.columns)}")
    
    # Should get exactly 1 column - the first one (Col_A)
    if len(result.columns) == 1:
        col_name = result.columns[0]
        if col_name == 'Col_A':
            print("✓ Column 1 correctly maps to pandas column 0 (Col_A)")
            
            # Verify data
            if result.iloc[0][col_name] == 'A1':
                print("✓ Column data is correct")
                return True
            else:
                print("✗ Column data is wrong")
        else:
            print(f"✗ Column 1 gave wrong column: expected 'Col_A', got '{col_name}'")
    else:
        print(f"✗ Expected 1 column, got {len(result.columns)}")
    
    return False


def test_column_indexing_numeric_middle():
    """Test that column 3 (user) = column 2 (pandas)."""
    
    print("\nTesting column indexing - middle column by number...")
    
    test_df = create_indexed_test_data()
    
    # Extract column 3 only
    step_config = {
        'processor_type': 'slice_data',
        'slice_type': 'column_range',
        'start_col': 3,
        'end_col': 3
    }
    
    processor = SliceDataProcessor(step_config)
    result = processor.execute(test_df)
    
    print(f"✓ Column 3 extraction: {len(result.columns)} columns")
    print(f"✓ Column name: {list(result.columns)}")
    
    # Should get Col_C (pandas column 2)
    if len(result.columns) == 1:
        col_name = result.columns[0]
        if col_name == 'Col_C':
            print("✓ Column 3 correctly maps to pandas column 2 (Col_C)")
            return True
        else:
            print(f"✗ Column 3 gave wrong column: expected 'Col_C', got '{col_name}'")
    else:
        print(f"✗ Expected 1 column, got {len(result.columns)}")
    
    return False


def test_column_indexing_excel_references():
    """Test Excel-style column references (A, B, C)."""
    
    print("\nTesting Excel column references...")
    
    test_df = create_indexed_test_data()
    
    # Test A = first column
    step_config_a = {
        'processor_type': 'slice_data',
        'slice_type': 'column_range',
        'start_col': 'A',
        'end_col': 'A'
    }
    
    processor = SliceDataProcessor(step_config_a)
    result_a = processor.execute(test_df)
    
    print(f"✓ Column A extraction: {list(result_a.columns)}")
    
    if len(result_a.columns) == 1 and result_a.columns[0] == 'Col_A':
        print("✓ Column A correctly maps to first column (Col_A)")
    else:
        print(f"✗ Column A failed: got {list(result_a.columns)}")
        return False
    
    # Test C = third column
    step_config_c = {
        'processor_type': 'slice_data',
        'slice_type': 'column_range',
        'start_col': 'C',
        'end_col': 'C'
    }
    
    processor = SliceDataProcessor(step_config_c)
    result_c = processor.execute(test_df)
    
    print(f"✓ Column C extraction: {list(result_c.columns)}")
    
    if len(result_c.columns) == 1 and result_c.columns[0] == 'Col_C':
        print("✓ Column C correctly maps to third column (Col_C)")
        return True
    else:
        print(f"✗ Column C failed: got {list(result_c.columns)}")
        return False


def test_column_range_indexing():
    """Test column range indexing."""
    
    print("\nTesting column range indexing...")
    
    test_df = create_indexed_test_data()
    
    # Extract columns 2-4 (user terms)
    step_config = {
        'processor_type': 'slice_data',
        'slice_type': 'column_range',
        'start_col': 2,
        'end_col': 4
    }
    
    processor = SliceDataProcessor(step_config)
    result = processor.execute(test_df)
    
    print(f"✓ Columns 2-4 extraction: {list(result.columns)}")
    
    # Should get Col_B, Col_C, Col_D (pandas columns 1, 2, 3)
    expected_columns = ['Col_B', 'Col_C', 'Col_D']
    if list(result.columns) == expected_columns:
        print("✓ Column range 2-4 correctly maps to pandas columns 1-3")
        return True
    else:
        print(f"✗ Expected {expected_columns}, got {list(result.columns)}")
        return False


def test_boundary_conditions():
    """Test boundary conditions and edge cases."""
    
    print("\nTesting boundary conditions...")
    
    test_df = create_indexed_test_data()
    success = True
    
    # Test last row
    step_config = {
        'processor_type': 'slice_data',
        'slice_type': 'row_range',
        'start_row': 6,
        'end_row': 6
    }
    
    processor = SliceDataProcessor(step_config)
    result = processor.execute(test_df)
    
    if len(result) == 1 and result.iloc[0]['Col_A'] == 'A6':
        print("✓ Last row (6) correctly maps to pandas row 5")
    else:
        print(f"✗ Last row failed: got {result.iloc[0]['Col_A'] if len(result) > 0 else 'no data'}")
        success = False
    
    # Test last column
    step_config = {
        'processor_type': 'slice_data',
        'slice_type': 'column_range',
        'start_col': 5,
        'end_col': 5
    }
    
    processor = SliceDataProcessor(step_config)
    result = processor.execute(test_df)
    
    if len(result.columns) == 1 and result.columns[0] == 'Col_E':
        print("✓ Last column (5) correctly maps to pandas column 4")
    else:
        print(f"✗ Last column failed: got {list(result.columns)}")
        success = False
    
    return success


def test_indexing_error_conditions():
    """Test error conditions for out-of-range indexing."""
    
    print("\nTesting indexing error conditions...")
    
    test_df = create_indexed_test_data()
    success = True
    
    # Test row 0 (should fail - 1-based indexing)
    try:
        step_config = {
            'processor_type': 'slice_data',
            'slice_type': 'row_range',
            'start_row': 0
        }
        processor = SliceDataProcessor(step_config)
        processor.execute(test_df)
        print("✗ Row 0 should have failed but didn't")
        success = False
    except StepProcessorError as e:
        if "positive integer" in str(e) or "1-based" in str(e):
            print("✓ Row 0 correctly rejected with appropriate error")
        else:
            print(f"✗ Row 0 rejected but with wrong error: {e}")
            success = False
    
    # Test out-of-range row
    try:
        step_config = {
            'processor_type': 'slice_data',
            'slice_type': 'row_range',
            'start_row': 99
        }
        processor = SliceDataProcessor(step_config)
        processor.execute(test_df)
        print("✗ Row 99 should have failed but didn't")
        success = False
    except StepProcessorError as e:
        if "exceeds" in str(e).lower():
            print("✓ Row 99 correctly rejected as out of range")
        else:
            print(f"✗ Row 99 rejected but with wrong error: {e}")
            success = False
    
    # Test out-of-range column number
    try:
        step_config = {
            'processor_type': 'slice_data',
            'slice_type': 'column_range',
            'start_col': 99
        }
        processor = SliceDataProcessor(step_config)
        processor.execute(test_df)
        print("✗ Column 99 should have failed but didn't")
        success = False
    except StepProcessorError as e:
        if "exceeds" in str(e).lower():
            print("✓ Column 99 correctly rejected as out of range")
        else:
            print(f"✗ Column 99 rejected but with wrong error: {e}")
            success = False
    
    return success


def test_excel_column_conversion():
    """Test Excel column letter to index conversion."""
    
    print("\nTesting Excel column conversion logic...")
    
    test_df = create_indexed_test_data()
    
    # Create a processor to access the conversion method
    processor = SliceDataProcessor({
        'processor_type': 'slice_data',
        'slice_type': 'column_range',
        'start_col': 'A'
    })
    
    # Test the conversion method directly if accessible
    if hasattr(processor, '_excel_col_to_index'):
        conversions = {
            'A': 0, 'B': 1, 'C': 2, 'D': 3, 'E': 4,
            'Z': 25, 'AA': 26, 'AB': 27, 'AZ': 51
        }
        
        success = True
        for excel_ref, expected_idx in conversions.items():
            try:
                actual_idx = processor._excel_col_to_index(excel_ref)
                if actual_idx == expected_idx:
                    print(f"✓ {excel_ref} → {actual_idx} (correct)")
                else:
                    print(f"✗ {excel_ref} → {actual_idx} (expected {expected_idx})")
                    success = False
            except Exception as e:
                print(f"✗ {excel_ref} conversion failed: {e}")
                success = False
        
        return success
    else:
        print("✗ Cannot access _excel_col_to_index method for direct testing")
        return False


def test_blank_rows_header_scenario():
    """Test the exact scenario: 2 blank rows + headers in row 3."""
    
    print("\nTesting blank rows + headers scenario...")
    
    # Create data that matches the user's real-world scenario
    test_df = pd.DataFrame({
        0: ['', '', 'Product_ID', 'P001', 'P002'],      # Blank, Blank, Header, Data, Data
        1: ['', '', 'Product_Name', 'Widget', 'Gadget'], # Blank, Blank, Header, Data, Data  
        2: ['', '', 'Price', '10.50', '25.00'],          # Blank, Blank, Header, Data, Data
    })
    
    print("Test data structure:")
    print(f"Row 1 (pandas 0): {list(test_df.iloc[0])}")  # Blanks
    print(f"Row 2 (pandas 1): {list(test_df.iloc[1])}")  # Blanks
    print(f"Row 3 (pandas 2): {list(test_df.iloc[2])}")  # Headers
    print(f"Row 4 (pandas 3): {list(test_df.iloc[3])}")  # Data
    print(f"Row 5 (pandas 4): {list(test_df.iloc[4])}")  # Data
    
    # Test scenario 1: start_row: 3 (what the user originally tried)
    print("\n🔍 Testing start_row: 3 (user's original attempt)...")
    step_config_3 = {
        'processor_type': 'slice_data',
        'slice_type': 'row_range',
        'start_row': 3,  # Should get row 2 (headers) + rows 3,4 (data)
        'slice_result_contains_headers': True
    }
    
    processor = SliceDataProcessor(step_config_3)
    result_3 = processor.execute(test_df)
    
    print(f"✓ start_row: 3 result: {len(result_3)} rows, {len(result_3.columns)} columns")
    print(f"✓ Column names: {list(result_3.columns)}")
    if len(result_3) > 0:
        print(f"✓ First data row: {list(result_3.iloc[0])}")
    else:
        print("✓ No data rows!")
    
    # Test scenario 2: start_row: 2 (what worked for the user)  
    print("\n🔍 Testing start_row: 2 (user's working solution)...")
    step_config_2 = {
        'processor_type': 'slice_data',
        'slice_type': 'row_range', 
        'start_row': 2,  # Should get row 1 (blank) + row 2 (headers) + rows 3,4 (data)
        'slice_result_contains_headers': True
    }
    
    processor = SliceDataProcessor(step_config_2)
    result_2 = processor.execute(test_df)
    
    print(f"✓ start_row: 2 result: {len(result_2)} rows, {len(result_2.columns)} columns")
    print(f"✓ Column names: {list(result_2.columns)}")
    if len(result_2) > 0:
        print(f"✓ First data row: {list(result_2.iloc[0])}")
    
    # Analysis
    print("\n🔍 Analysis:")
    
    # Check if start_row: 3 has proper headers
    has_proper_headers_3 = any('Product' in str(col) for col in result_3.columns)
    has_proper_headers_2 = any('Product' in str(col) for col in result_2.columns)
    
    print(f"start_row: 3 has proper headers: {has_proper_headers_3}")
    print(f"start_row: 2 has proper headers: {has_proper_headers_2}")
    
    # This should help identify the issue
    if not has_proper_headers_3 and has_proper_headers_2:
        print("🚨 ISSUE IDENTIFIED: start_row: 3 loses headers, start_row: 2 works!")
        print("This matches the user's experience.")
        return False  # Issue confirmed
    else:
        print("✓ Both scenarios work as expected (issue not reproduced)")
        return True


def main():
    """Run all indexing tests with success tracking."""
    
    print("🔍 Testing SliceDataProcessor Indexing Logic...")
    print("=" * 60)
    success = True
    
    # Row indexing tests
    print("\n📊 ROW INDEXING TESTS")
    success &= test_row_indexing_first_row()
    success &= test_row_indexing_middle_row()
    success &= test_row_indexing_range()
    
    # Column indexing tests
    print("\n📊 COLUMN INDEXING TESTS")
    success &= test_column_indexing_numeric_first()
    success &= test_column_indexing_numeric_middle()
    success &= test_column_indexing_excel_references()
    success &= test_column_range_indexing()
    
    # Edge cases and error conditions
    print("\n📊 BOUNDARY AND ERROR TESTS")
    success &= test_boundary_conditions()
    success &= test_indexing_error_conditions()
    success &= test_excel_column_conversion()
    
    # Real-world scenario test
    print("\n📊 REAL-WORLD SCENARIO TEST")
    success &= test_blank_rows_header_scenario()
    
    print("\n" + "=" * 60)
    if success:
        print("🎉 All SliceDataProcessor indexing tests passed!")
        print("✅ 1-based to 0-based conversions are working correctly")
        return 1
    else:
        print("❌ Some SliceDataProcessor indexing tests failed!")
        print("⚠️  There may be issues with real-world scenarios")
        return 0


if __name__ == '__main__':
    main()


# End of file #
