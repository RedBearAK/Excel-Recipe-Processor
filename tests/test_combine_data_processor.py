"""
Test the CombineDataProcessor functionality.
"""

import pandas as pd

from excel_recipe_processor.core.stage_manager import StageManager
from excel_recipe_processor.processors.combine_data_processor import CombineDataProcessor


def create_metadata_data():
    """Create sample metadata DataFrame with same column structure as data."""
    return pd.DataFrame({
        'Product ID': ['Company Report - March 2024', 'Generated on: 2024-03-15', 'Department: Sales'],
        'Product Name': ['', '', ''],
        'Price': ['', '', '']
    })


def create_data_section():
    """Create sample data DataFrame with proper headers."""
    return pd.DataFrame({
        'Product ID': ['A001', 'B002', 'C003'],
        'Product Name': ['Widget A', 'Gadget B', 'Tool C'],
        'Price': [10.50, 25.00, 15.75]
    })


def create_summary_data():
    """Create sample summary DataFrame."""
    return pd.DataFrame({
        'Product ID': ['TOTAL', 'AVERAGE'],
        'Product Name': ['All Products', 'Per Product'],
        'Price': [51.25, 17.08]
    })


def create_different_columns_data():
    """Create DataFrame with different columns for mismatch testing."""
    return pd.DataFrame({
        'Customer ID': ['C001', 'C002'],
        'Customer Name': ['Alice Corp', 'Bob Industries'],
        'Region': ['West', 'East']
    })


def test_basic_vertical_stacking():
    """Test basic vertical stacking of DataFrames."""
    print("Testing basic vertical stacking...")
    
    StageManager.initialize_stages()
    
    try:
        # Create test data and save to stages
        data_section = create_data_section()
        summary_data = create_summary_data()
        
        StageManager.save_stage('Data Section', data_section, description='Main data')
        StageManager.save_stage('Summary Section', summary_data, description='Summary data')
        
        step_config = {
            'processor_type': 'combine_data',
            'step_description': 'Combine data and summary',
            'combine_type': 'vertical_stack',
            'data_sources': [
                {'insert_from_stage': 'Data Section'},
                {'insert_from_stage': 'Summary Section'}
            ]
        }
        
        processor = CombineDataProcessor(step_config)
        result = processor.execute(None)  # No current data needed
        
        # Should have 5 rows total (3 + 2)
        if len(result) == 5:
            print("✓ Vertical stacking returned correct number of rows")
        else:
            print(f"✗ Expected 5 rows, got {len(result)}")
            return False
        
        # Check that all data is present
        if ('A001' in result['Product ID'].values and 
            'TOTAL' in result['Product ID'].values):
            print("✓ Vertical stacking preserved all data")
            return True
        else:
            print("✗ Vertical stacking lost data")
            return False
            
    finally:
        StageManager.cleanup_stages()


def test_vertical_stacking_with_blank_rows():
    """Test vertical stacking with blank rows between sections."""
    print("\nTesting vertical stacking with blank rows...")
    
    StageManager.initialize_stages()
    
    try:
        # Create test data
        data_section = create_data_section()
        summary_data = create_summary_data()
        
        StageManager.save_stage('Data Section', data_section, description='Main data')
        StageManager.save_stage('Summary Section', summary_data, description='Summary data')
        
        step_config = {
            'processor_type': 'combine_data',
            'step_description': 'Combine with blank rows',
            'combine_type': 'vertical_stack',
            'data_sources': [
                {'insert_from_stage': 'Data Section'},
                {'insert_blank_rows': 2},
                {'insert_from_stage': 'Summary Section'}
            ]
        }
        
        processor = CombineDataProcessor(step_config)
        result = processor.execute(None)
        
        # Should have 7 rows total (3 + 2 blank + 2)
        if len(result) == 7:
            print("✓ Blank rows inserted correctly")
        else:
            print(f"✗ Expected 7 rows with blank rows, got {len(result)}")
            return False
        
        # Check for blank rows (should have empty strings)
        blank_row_1 = result.iloc[3]  # First blank row
        blank_row_2 = result.iloc[4]  # Second blank row
        
        if (all(blank_row_1 == '') and all(blank_row_2 == '')):
            print("✓ Blank rows contain empty values")
            return True
        else:
            print("✗ Blank rows don't contain expected empty values")
            return False
            
    finally:
        StageManager.cleanup_stages()


def test_horizontal_concatenation():
    """Test horizontal concatenation of DataFrames."""
    print("\nTesting horizontal concatenation...")
    
    StageManager.initialize_stages()
    
    try:
        # Create test data with different structures
        products = pd.DataFrame({
            'Product ID': ['A001', 'B002', 'C003'],
            'Product Name': ['Widget A', 'Gadget B', 'Tool C']
        })
        
        prices = pd.DataFrame({
            'Price': [10.50, 25.00, 15.75],
            'Currency': ['USD', 'USD', 'USD']
        })
        
        StageManager.save_stage('Products', products, description='Product info')
        StageManager.save_stage('Prices', prices, description='Price info')
        
        step_config = {
            'processor_type': 'combine_data',
            'step_description': 'Combine horizontally',
            'combine_type': 'horizontal_concat',
            'data_sources': [
                {'insert_from_stage': 'Products'},
                {'insert_from_stage': 'Prices'}
            ]
        }
        
        processor = CombineDataProcessor(step_config)
        result = processor.execute(None)
        
        # Should have 3 rows and 4 columns
        if len(result) == 3 and len(result.columns) == 4:
            print("✓ Horizontal concatenation returned correct dimensions")
        else:
            print(f"✗ Expected 3×4, got {len(result)}×{len(result.columns)}")
            return False
        
        # Check that all columns are present
        expected_columns = ['Product ID', 'Product Name', 'Price', 'Currency']
        if all(col in result.columns for col in expected_columns):
            print("✓ Horizontal concatenation preserved all columns")
            return True
        else:
            print(f"✗ Missing columns. Got: {list(result.columns)}")
            return False
            
    finally:
        StageManager.cleanup_stages()


def test_combining_with_current_data():
    """Test combining stages with current pipeline data."""
    print("\nTesting combination with current data...")
    
    StageManager.initialize_stages()
    
    try:
        # Create stage data
        metadata = create_metadata_data()
        StageManager.save_stage('Metadata', metadata, description='File metadata')
        
        # Current data (simulating pipeline data)
        current_data = create_data_section()
        
        step_config = {
            'processor_type': 'combine_data',
            'step_description': 'Combine metadata with current data',
            'combine_type': 'vertical_stack',
            'data_sources': [
                {'insert_from_stage': 'Metadata'},
                {'insert_blank_rows': 1},
                {'insert_from_stage': 'current_dataframe'}
            ]
        }
        
        processor = CombineDataProcessor(step_config)
        result = processor.execute(current_data)
        
        # Should have 7 rows (3 metadata + 1 blank + 3 data)
        if len(result) == 7:
            print("✓ Combination with current data worked correctly")
        else:
            print(f"✗ Expected 7 rows, got {len(result)}")
            return False
        
        # Check that both metadata and data are present
        # Expected structure: 3 metadata + 1 blank + 3 data = 7 rows
        # Row 0-2: Metadata, Row 3: Blank, Row 4-6: Data
        metadata_check = 'Company Report' in str(result.iloc[0, 0])
        data_check = 'A001' in str(result.iloc[4, 0])  # First data row after blank
        
        if metadata_check and data_check:
            print("✓ Both metadata and current data preserved")
            return True
        else:
            print("✗ Data not preserved correctly")
            print(f"Row 0 (metadata): {result.iloc[0, 0]}")
            print(f"Row 4 (first data): {result.iloc[4, 0]}")
            print(f"Full result:\n{result}")
            return False
            
    finally:
        StageManager.cleanup_stages()


def test_column_mismatch_handling():
    """Test handling of column mismatches."""
    print("\nTesting column mismatch handling...")
    
    StageManager.initialize_stages()
    
    try:
        # Create data with different columns
        data1 = create_data_section()  # Product data
        data2 = create_different_columns_data()  # Customer data
        
        print(f"Product data columns: {list(data1.columns)}")
        print(f"Customer data columns: {list(data2.columns)}")
        
        StageManager.save_stage('Products', data1, description='Product data')
        StageManager.save_stage('Customers', data2, description='Customer data')
        
        # Test 1: Should fail without ignore_column_mismatch
        step_config = {
            'processor_type': 'combine_data',
            'step_description': 'Test column mismatch',
            'combine_type': 'vertical_stack',
            'data_sources': [
                {'insert_from_stage': 'Products'},
                {'insert_from_stage': 'Customers'}
            ]
            # ignore_column_mismatch defaults to False
        }
        
        processor = CombineDataProcessor(step_config)
        
        try:
            result = processor.execute(None)
            print("✗ Should have failed with column mismatch")
            print(f"Result shape: {result.shape}")
            print(f"Result columns: {list(result.columns)}")
            return False
        except Exception as e:
            if "Column mismatch" in str(e):
                print(f"✓ Correctly caught column mismatch error: {type(e).__name__}")
            else:
                print(f"✗ Wrong error type: {e}")
                return False
        
        # Test 2: Should succeed with ignore_column_mismatch
        step_config = {
            'processor_type': 'combine_data',
            'step_description': 'Test column mismatch with ignore',
            'combine_type': 'vertical_stack',
            'data_sources': [
                {'insert_from_stage': 'Products'},
                {'insert_from_stage': 'Customers'}
            ],
            'ignore_column_mismatch': True
        }
        
        processor = CombineDataProcessor(step_config)
        result = processor.execute(None)
        
        if len(result) == 5:  # 3 + 2 rows
            print("✓ Column mismatch ignored successfully")
            return True
        else:
            print(f"✗ Expected 5 rows with mismatch ignored, got {len(result)}")
            return False
            
    finally:
        StageManager.cleanup_stages()


def test_error_handling():
    """Test error handling for invalid configurations."""
    print("\nTesting error handling...")
    
    StageManager.initialize_stages()
    
    try:
        # Test missing stage
        step_config = {
            'processor_type': 'combine_data',
            'combine_type': 'vertical_stack',
            'data_sources': [
                {'insert_from_stage': 'NonExistent Stage'}
            ]
        }
        
        processor = CombineDataProcessor(step_config)
        
        try:
            processor.execute(None)
            print("✗ Should have failed with missing stage")
            return False
        except Exception as e:
            print(f"✓ Caught expected error for missing stage: {type(e).__name__}")
        
        # Test invalid combine_type
        step_config = {
            'processor_type': 'combine_data',
            'combine_type': 'invalid_type',
            'data_sources': [
                {'insert_from_stage': 'current_dataframe'}
            ]
        }
        
        processor = CombineDataProcessor(step_config)
        
        try:
            processor.execute(create_data_section())
            print("✗ Should have failed with invalid combine_type")
            return False
        except Exception as e:
            print(f"✓ Caught expected error for invalid combine_type: {type(e).__name__}")
        
        # Test empty data_sources
        step_config = {
            'processor_type': 'combine_data',
            'combine_type': 'vertical_stack',
            'data_sources': []
        }
        
        processor = CombineDataProcessor(step_config)
        
        try:
            processor.execute(None)
            print("✗ Should have failed with empty data_sources")
            return False
        except Exception as e:
            print(f"✓ Caught expected error for empty data_sources: {type(e).__name__}")
        
        print("✓ Error handling works correctly")
        return True
        
    finally:
        StageManager.cleanup_stages()


def test_horizontal_with_blank_columns():
    """Test horizontal concatenation with blank columns."""
    print("\nTesting horizontal concatenation with blank columns...")
    
    StageManager.initialize_stages()
    
    try:
        # Create test data
        section1 = pd.DataFrame({
            'A': [1, 2, 3],
            'B': [4, 5, 6]
        })
        
        section2 = pd.DataFrame({
            'C': [7, 8, 9],
            'D': [10, 11, 12]
        })
        
        StageManager.save_stage('Section1', section1, description='First section')
        StageManager.save_stage('Section2', section2, description='Second section')
        
        step_config = {
            'processor_type': 'combine_data',
            'step_description': 'Horizontal with blank columns',
            'combine_type': 'horizontal_concat',
            'data_sources': [
                {'insert_from_stage': 'Section1'},
                {'insert_blank_cols': 1},
                {'insert_from_stage': 'Section2'}
            ]
        }
        
        processor = CombineDataProcessor(step_config)
        result = processor.execute(None)
        
        # Should have 3 rows and 5 columns (2 + 1 blank + 2)
        if len(result) == 3 and len(result.columns) == 5:
            print("✓ Blank columns inserted correctly")
        else:
            print(f"✗ Expected 3×5, got {len(result)}×{len(result.columns)}")
            return False
        
        # Check for blank column (should be named Blank_1)
        if 'Blank_1' in result.columns:
            print("✓ Blank column properly named")
            return True
        else:
            print(f"✗ Blank column not found. Columns: {list(result.columns)}")
            return False
            
    finally:
        StageManager.cleanup_stages()


def test_capabilities():
    """Test processor capabilities reporting."""
    print("\nTesting capabilities...")
    
    step_config = {
        'processor_type': 'combine_data',
        'combine_type': 'vertical_stack',
        'data_sources': [{'insert_from_stage': 'current_dataframe'}]
    }
    
    processor = CombineDataProcessor(step_config)
    capabilities = processor.get_capabilities()
    
    if 'combination_operations' in capabilities:
        print("✓ Capabilities include combination operations")
    else:
        print("✗ Missing combination operations in capabilities")
        return False
    
    supported_types = processor.get_supported_combine_types()
    if 'vertical_stack' in supported_types and 'horizontal_concat' in supported_types:
        print("✓ Supported combine types reported correctly")
        return True
    else:
        print(f"✗ Missing expected combine types: {supported_types}")
        return False


if __name__ == '__main__':
    print("Testing CombineDataProcessor...")
    success = True
    
    success &= test_basic_vertical_stacking()
    success &= test_vertical_stacking_with_blank_rows()
    success &= test_horizontal_concatenation()
    success &= test_combining_with_current_data()
    success &= test_column_mismatch_handling()
    success &= test_horizontal_with_blank_columns()
    success &= test_error_handling()
    success &= test_capabilities()
    
    if success:
        print("\n🎉 All CombineDataProcessor tests passed!")
    else:
        print("\n❌ Some CombineDataProcessor tests failed!")
    
    print("\nTo run with pytest: pytest test_combine_data_processor.py -v")
