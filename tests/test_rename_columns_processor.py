"""
Test the RenameColumnsProcessor functionality.
"""

import pandas as pd

from excel_recipe_processor.processors.rename_columns_processor import RenameColumnsProcessor
from excel_recipe_processor.processors.base_processor import StepProcessorError


def create_messy_columns_data():
    """Create sample data with messy column names."""
    return pd.DataFrame({
        'Product Code': [1, 2, 3, 4, 5],
        'Product Name!': ['A', 'B', 'C', 'D', 'E'],
        'PRICE USD': [10.5, 15.0, 20.0, 12.5, 8.75],
        'qty_in_stock': [100, 50, 75, 200, 125],
        ' Department ': ['Electronics', 'Tools', 'Hardware', 'Electronics', 'Tools'],
        'Order-Date': ['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05']
    })


def test_mapping_rename():
    """Test direct column name mapping."""
    
    print("Testing mapping rename...")
    
    test_df = create_messy_columns_data()
    print(f"✓ Created test data with columns: {list(test_df.columns)}")
    
    # Test direct mapping rename
    step_config = {
        'step_description': 'Clean up column names',
        'processor_type': 'rename_columns',
        'rename_type': 'mapping',
        'mapping': {
            'Product Code': 'product_code',
            'Product Name!': 'product_name',
            'PRICE USD': 'price_usd',
            'qty_in_stock': 'quantity'
        }
    }
    
    processor = RenameColumnsProcessor(step_config)
    result = processor.execute(test_df)
    
    print(f"✓ Renamed columns: {list(result.columns)}")
    
    # Check that specific columns were renamed
    expected_renames = ['product_code', 'product_name', 'price_usd', 'quantity']
    renamed_correctly = all(col in result.columns for col in expected_renames)
    
    if renamed_correctly:
        print("✓ Mapping rename worked correctly")
        return True
    else:
        print("✗ Some columns were not renamed correctly")
        return False


def test_pattern_rename():
    """Test pattern-based renaming with regex."""
    
    print("\nTesting pattern rename...")
    
    test_df = pd.DataFrame({
        'col_2024_jan': [1, 2, 3],
        'col_2024_feb': [4, 5, 6], 
        'col_2024_mar': [7, 8, 9],
        'other_column': [10, 11, 12]
    })
    
    print(f"✓ Created data with columns: {list(test_df.columns)}")
    
    # Test pattern-based rename - remove 'col_' prefix
    step_config = {
        'step_description': 'Remove col_ prefix',
        'processor_type': 'rename_columns',
        'rename_type': 'pattern',
        'pattern': r'^col_',
        'replacement': ''
    }
    
    processor = RenameColumnsProcessor(step_config)
    result = processor.execute(test_df)
    
    print(f"✓ Pattern renamed columns: {list(result.columns)}")
    
    # Check that col_ prefix was removed
    expected_columns = ['2024_jan', '2024_feb', '2024_mar', 'other_column']
    columns_match = list(result.columns) == expected_columns
    
    if columns_match:
        print("✓ Pattern rename worked correctly")
        return True
    else:
        print(f"✗ Expected {expected_columns}, got {list(result.columns)}")
        return False


def test_transform_rename():
    """Test transformation-based renaming."""
    
    print("\nTesting transform rename...")
    
    test_df = create_messy_columns_data()
    print(f"✓ Created data with columns: {list(test_df.columns)}")
    
    # Test multiple transformations
    step_config = {
        'step_description': 'Transform column names',
        'processor_type': 'rename_columns',
        'rename_type': 'transform',
        'case_conversion': 'snake_case',
        'strip_characters': ' !',
        'replace_spaces': '_'
    }
    
    processor = RenameColumnsProcessor(step_config)
    result = processor.execute(test_df)
    
    print(f"✓ Transformed columns: {list(result.columns)}")
    
    # Check that transformations were applied
    # Should be snake_case, no spaces, no special chars
    expected_pattern = True
    for col in result.columns:
        if ' ' in col or '!' in col or col != col.lower():
            expected_pattern = False
            break
    
    if expected_pattern:
        print("✓ Transform rename worked correctly")
        return True
    else:
        print("✗ Transform rename failed")
        return False


def test_case_conversions():
    """Test different case conversion options."""
    
    print("\nTesting case conversions...")
    
    test_df = pd.DataFrame({
        'product name': [1, 2],
        'PRICE USD': [3, 4],
        'qty-in-stock': [5, 6]
    })
    
    # Test each case conversion
    case_tests = [
        ('upper', ['PRODUCT NAME', 'PRICE USD', 'QTY-IN-STOCK']),
        ('lower', ['product name', 'price usd', 'qty-in-stock']),
        ('title', ['Product Name', 'Price Usd', 'Qty-In-Stock']),
        ('snake_case', ['product_name', 'price_usd', 'qty_in_stock']),
        ('camel_case', ['productName', 'priceUsd', 'qtyInStock'])
    ]
    
    for case_type, expected in case_tests:
        step_config = {
            'step_description': f'Test {case_type}',
            'processor_type': 'rename_columns',
            'rename_type': 'transform',
            'case_conversion': case_type
        }
        
        processor = RenameColumnsProcessor(step_config)
        result = processor.execute(test_df.copy())
        
        actual = list(result.columns)
        print(f"✓ {case_type}: {actual}")
        
        if actual == expected:
            print(f"  ✓ {case_type} conversion worked correctly")
        else:
            print(f"  ✗ {case_type} failed: expected {expected}")
            return False
    
    return True


def test_prefix_suffix():
    """Test adding prefixes and suffixes."""
    
    print("\nTesting prefix and suffix...")
    
    test_df = pd.DataFrame({
        'step_description': [1, 2],
        'price': [3, 4],
        'quantity': [5, 6]
    })
    
    # Test adding prefix and suffix
    step_config = {
        'step_description': 'Add prefix and suffix',
        'processor_type': 'rename_columns',
        'rename_type': 'transform',
        'add_prefix': 'col_',
        'add_suffix': '_data'
    }
    
    processor = RenameColumnsProcessor(step_config)
    result = processor.execute(test_df)
    
    print(f"✓ Prefix/suffix result: {list(result.columns)}")
    
    expected = ['col_name_data', 'col_price_data', 'col_quantity_data']
    actual = list(result.columns)
    
    if actual == expected:
        print("✓ Prefix/suffix worked correctly")
        return True
    else:
        print(f"✗ Expected {expected}, got {actual}")
        return False


def test_standardize_helper():
    """Test the standardize column names helper method."""
    
    print("\nTesting standardize helper...")
    
    test_df = pd.DataFrame({
        'Product Name!': [1, 2],
        'PRICE (USD)': [3, 4],
        ' Department ': [5, 6],
        'Order-Date': [7, 8]
    })
    
    print(f"✓ Messy columns: {list(test_df.columns)}")
    
    processor = RenameColumnsProcessor({'processor_type': 'rename_columns', 'rename_type': 'mapping', 'mapping': {}})
    result = processor.standardize_column_names(test_df)
    
    print(f"✓ Standardized columns: {list(result.columns)}")
    
    # Check that standardization applied snake_case and cleaned up names
    expected_clean = True
    for col in result.columns:
        # Should be lowercase, no spaces, no special chars except underscores
        if not col.islower() or ' ' in col or any(c in col for c in '!()'):
            expected_clean = False
            break
    
    if expected_clean:
        print("✓ Standardize helper worked correctly")
        return True
    else:
        print("✗ Standardize helper failed")
        return False


def test_column_analysis():
    """Test column name analysis functionality."""
    
    print("\nTesting column analysis...")
    
    test_df = create_messy_columns_data()
    
    processor = RenameColumnsProcessor({'processor_type': 'rename_columns', 'rename_type': 'mapping', 'mapping': {}})
    analysis = processor.get_column_analysis(test_df)
    
    print(f"✓ Analysis results:")
    print(f"  Total columns: {analysis['total_columns']}")
    print(f"  Issues found: {len(analysis['naming_issues'])}")
    print(f"  Recommendations: {len(analysis['recommendations'])}")
    
    # Check that analysis detected common issues
    issues_found = len(analysis['naming_issues']) > 0
    recommendations_made = len(analysis['recommendations']) > 0
    
    if issues_found and recommendations_made:
        print("✓ Column analysis worked correctly")
        return True
    else:
        print("✗ Column analysis failed to detect issues")
        return False


def test_duplicate_new_names():
    """Test error handling for duplicate new column names."""
    
    print("\nTesting duplicate name detection...")
    
    test_df = pd.DataFrame({
        'col1': [1, 2],
        'col2': [3, 4],
        'col3': [5, 6]
    })
    
    # Try to rename multiple columns to the same name
    step_config = {
        'processor_type': 'rename_columns',
        'step_description': 'Duplicate names test',
        'rename_type': 'mapping',
        'mapping': {
            'col1': 'new_name',
            'col2': 'new_name',  # Duplicate!
            'col3': 'different_name'
        }
    }
    
    try:
        processor = RenameColumnsProcessor(step_config)
        processor.execute(test_df)
        print("✗ Should have failed with duplicate new names")
        return False
    except StepProcessorError as e:
        print(f"✓ Caught expected error: {e}")
        return True


def test_error_handling():
    """Test error handling for various failure cases."""
    
    print("\nTesting error handling...")
    
    test_df = create_messy_columns_data()
    
    # Test missing column in mapping
    try:
        bad_config = {
            'processor_type': 'rename_columns',
            'step_description': 'Missing column',
            'rename_type': 'mapping',
            'mapping': {
                'NonExistentColumn': 'new_name'
            }
        }
        processor = RenameColumnsProcessor(bad_config)
        processor.execute(test_df)
        print("✗ Should have failed with missing column")
    except StepProcessorError as e:
        print(f"✓ Caught expected error: {e}")
    
    # Test empty mapping
    try:
        bad_config = {
            'processor_type': 'rename_columns',
            'step_description': 'Empty mapping',
            'rename_type': 'mapping',
            'mapping': {}
        }
        processor = RenameColumnsProcessor(bad_config)
        processor.execute(test_df)
        print("✗ Should have failed with empty mapping")
    except StepProcessorError as e:
        print(f"✓ Caught expected error: {e}")
    
    # Test invalid pattern
    try:
        bad_config = {
            'processor_type': 'rename_columns',
            'step_description': 'Invalid pattern',
            'rename_type': 'pattern',
            'pattern': '[invalid regex',
            'replacement': 'test'
        }
        processor = RenameColumnsProcessor(bad_config)
        processor.execute(test_df)
        print("✗ Should have failed with invalid regex")
    except StepProcessorError as e:
        print(f"✓ Caught expected error: {e}")


def test_real_world_scenario():
    """Test a realistic column renaming scenario."""
    
    print("\nTesting real-world scenario...")
    
    # Simulate messy export data
    test_df = pd.DataFrame({
        'Prod Code': ['A001', 'B002'],
        'Product Name (English)': ['Widget', 'Gadget'],
        'Price $USD': [10.5, 15.0],
        'QTY ON HAND': [100, 50],
        'Last Modified Date': ['2024-01-01', '2024-01-02'],
        'Category/Type': ['Electronics', 'Tools']
    })
    
    print(f"✓ Messy export columns: {list(test_df.columns)}")
    
    # Clean up using transforms
    step_config = {
        'processor_type': 'rename_columns',
        'step_description': 'Clean export data',
        'rename_type': 'transform',
        'case_conversion': 'snake_case',
        'strip_characters': ' ()',
        'replace_spaces': '_'
    }
    
    processor = RenameColumnsProcessor(step_config)
    result = processor.execute(test_df)
    
    print(f"✓ Cleaned columns: {list(result.columns)}")
    
    # Check that columns are now clean and consistent
    all_clean = True
    for col in result.columns:
        if not col.islower() or ' ' in col or '(' in col or ')' in col:
            all_clean = False
            break
    
    if all_clean:
        print("✓ Real-world scenario worked correctly")
        return True
    else:
        print("✗ Real-world scenario failed")
        return False


if __name__ == '__main__':
    success = True
    
    success &= test_mapping_rename()
    success &= test_pattern_rename()
    success &= test_transform_rename()
    success &= test_case_conversions()
    success &= test_prefix_suffix()
    success &= test_standardize_helper()
    success &= test_column_analysis()
    success &= test_duplicate_new_names()
    success &= test_real_world_scenario()
    test_error_handling()
    
    if success:
        print("\n✓ All rename columns processor tests passed!")
    else:
        print("\n✗ Some rename columns processor tests failed!")
    
    # Show supported features
    processor = RenameColumnsProcessor({'processor_type': 'rename_columns', 'processor_type': 'mapping', 'mapping': {}})
    print(f"\nSupported rename types: {processor.get_supported_rename_types()}")
    print(f"Supported case conversions: {processor.get_supported_case_conversions()}")
