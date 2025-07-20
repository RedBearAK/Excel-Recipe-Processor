"""
Test the CleanDataProcessor functionality.
"""

import pandas as pd

from excel_recipe_processor.processors.clean_data_processor import CleanDataProcessor
from excel_recipe_processor.processors.base_processor import StepProcessorError


def create_messy_test_data():
    """Create sample data with various cleaning needs."""
    return pd.DataFrame({
        'Product_Name': ['  CANNED Beans  ', 'fresh fish', 'CANNED CORN!', 'dried fruit', None],
        'Component': ['FLESH', 'flesh', 'FLESH', 'FLESH', 'FLESH'],
        'Price': ['$10.50', '25.00', '$15.75', 'invalid', '12.00'],
        'Quantity': [100, 50, 75, 25, None],
        'Date_Text': ['2024-01-15', '01/15/2024', '2024-15-01', 'invalid', '2024-02-20'],
        'Status': ['active', 'ACTIVE', 'Active', 'cancelled', 'pending']
    })


def test_replace_operations():
    """Test find and replace operations like the van report workflow."""
    
    print("Testing replace operations...")
    
    test_df = create_messy_test_data()
    print(f"✓ Created messy test data: {len(test_df)} rows")
    
    # Test basic replace - like replacing FLESH with CANS in van report
    step_config = {
        'type': 'clean_data',
        'name': 'Replace FLESH with CANS',
        'rules': [
            {
                'column': 'Component',
                'action': 'replace',
                'old_value': 'FLESH',
                'new_value': 'CANS'
            }
        ]
    }
    
    processor = CleanDataProcessor(step_config)
    result = processor.execute(test_df)
    
    # Check that FLESH was replaced with CANS
    flesh_count = (result['Component'] == 'FLESH').sum()
    cans_count = (result['Component'] == 'CANS').sum()
    
    print(f"✓ Replace FLESH→CANS: {flesh_count} FLESH remaining, {cans_count} CANS found")
    
    if flesh_count == 1 and cans_count == 4:  # One 'flesh' lowercase should remain
        print("✓ Case-sensitive replace worked correctly")
    else:
        print(f"✗ Unexpected replace result: {flesh_count} FLESH, {cans_count} CANS")
    
    # Test case-insensitive replace
    step_config2 = {
        'type': 'clean_data',
        'name': 'Case-insensitive replace',
        'rules': [
            {
                'column': 'Component',
                'action': 'replace',
                'old_value': 'flesh',
                'new_value': 'MEAT',
                'case_sensitive': False
            }
        ]
    }
    
    processor2 = CleanDataProcessor(step_config2)
    result2 = processor2.execute(test_df)
    
    meat_count = (result2['Component'] == 'MEAT').sum()
    print(f"✓ Case-insensitive replace: {meat_count} MEAT found")
    
    return True


def test_text_transformations():
    """Test text case and whitespace operations."""
    
    print("\nTesting text transformations...")
    
    test_df = create_messy_test_data()
    
    # Test multiple text cleaning operations
    step_config = {
        'type': 'clean_data',
        'name': 'Text cleaning',
        'rules': [
            {
                'column': 'Product_Name',
                'action': 'strip_whitespace'
            },
            {
                'column': 'Product_Name',
                'action': 'uppercase'
            },
            {
                'column': 'Product_Name',
                'action': 'remove_special_chars'
            },
            {
                'column': 'Status',
                'action': 'lowercase'
            }
        ]
    }
    
    processor = CleanDataProcessor(step_config)
    result = processor.execute(test_df)
    
    # Check results
    first_product = result.iloc[0]['Product_Name']
    first_status = result.iloc[0]['Status']
    
    print(f"✓ Cleaned product name: '{first_product}'")
    print(f"✓ Lowercased status: '{first_status}'")
    
    if first_product == 'CANNED BEANS' and first_status == 'active':
        print("✓ Text transformations worked correctly")
        return True
    else:
        print("✗ Text transformations failed")
        return False


def test_numeric_cleaning():
    """Test numeric data cleaning."""
    
    print("\nTesting numeric cleaning...")
    
    test_df = create_messy_test_data()
    
    # Clean price column - remove $ signs and convert to numeric
    step_config = {
        'type': 'clean_data',
        'name': 'Clean prices',
        'rules': [
            {
                'column': 'Price',
                'action': 'fix_numeric',
                'fill_na': 0.0
            }
        ]
    }
    
    processor = CleanDataProcessor(step_config)
    result = processor.execute(test_df)
    
    # Check that prices are now numeric
    price_column = result['Price']
    print(f"✓ Price column dtype: {price_column.dtype}")
    print(f"✓ Sample prices: {price_column.head().tolist()}")
    
    if pd.api.types.is_numeric_dtype(price_column):
        print("✓ Numeric cleaning worked correctly")
        return True
    else:
        print("✗ Numeric cleaning failed")
        return False


def test_fill_empty_values():
    """Test filling empty/null values."""
    
    print("\nTesting fill empty values...")
    
    test_df = create_messy_test_data()
    
    # Fill empty values
    step_config = {
        'type': 'clean_data',
        'name': 'Fill empty values',
        'rules': [
            {
                'column': 'Product_Name',
                'action': 'fill_empty',
                'fill_value': 'Unknown Product',
                'method': 'value'
            },
            {
                'column': 'Quantity',
                'action': 'fill_empty',
                'fill_value': 0,
                'method': 'value'
            }
        ]
    }
    
    processor = CleanDataProcessor(step_config)
    result = processor.execute(test_df)
    
    # Check that null values were filled
    null_products = result['Product_Name'].isnull().sum()
    null_quantities = result['Quantity'].isnull().sum()
    
    print(f"✓ Null products remaining: {null_products}")
    print(f"✓ Null quantities remaining: {null_quantities}")
    
    if null_products == 0 and null_quantities == 0:
        print("✓ Fill empty values worked correctly")
        return True
    else:
        print("✗ Fill empty values failed")
        return False


def test_regex_operations():
    """Test regex-based cleaning."""
    
    print("\nTesting regex operations...")
    
    test_df = create_messy_test_data()
    
    # Use regex to clean up product names
    step_config = {
        'type': 'clean_data',
        'name': 'Regex cleaning',
        'rules': [
            {
                'column': 'Product_Name',
                'action': 'regex_replace',
                'pattern': r'[!@#$%^&*()]',
                'replacement': ''
            }
        ]
    }
    
    processor = CleanDataProcessor(step_config)
    result = processor.execute(test_df)
    
    # Check that special characters were removed
    third_product = result.iloc[2]['Product_Name']
    print(f"✓ Cleaned product (was 'CANNED CORN!'): '{third_product}'")
    
    if '!' not in str(third_product):
        print("✓ Regex operations worked correctly")
        return True
    else:
        print("✗ Regex operations failed")
        return False


def test_standardize_values():
    """Test value standardization using mappings."""
    
    print("\nTesting value standardization...")
    
    test_df = create_messy_test_data()
    
    # Standardize status values
    step_config = {
        'type': 'clean_data',
        'name': 'Standardize status',
        'rules': [
            {
                'column': 'Status',
                'action': 'standardize_values',
                'mapping': {
                    'active': 'Active',
                    'ACTIVE': 'Active',
                    'cancelled': 'Cancelled',
                    'pending': 'Pending'
                }
            }
        ]
    }
    
    processor = CleanDataProcessor(step_config)
    result = processor.execute(test_df)
    
    # Check standardization
    unique_statuses = result['Status'].unique()
    print(f"✓ Standardized statuses: {sorted(unique_statuses)}")
    
    expected_statuses = {'Active', 'Cancelled', 'Pending'}
    actual_statuses = set(unique_statuses)
    
    if actual_statuses == expected_statuses:
        print("✓ Value standardization worked correctly")
        return True
    else:
        print(f"✗ Expected {expected_statuses}, got {actual_statuses}")
        return False


def test_multiple_rules():
    """Test applying multiple cleaning rules in sequence."""
    
    print("\nTesting multiple cleaning rules...")
    
    test_df = create_messy_test_data()
    
    # Complex cleaning like in van report workflow
    step_config = {
        'type': 'clean_data',
        'name': 'Complex cleaning sequence',
        'rules': [
            # First strip whitespace
            {
                'column': 'Product_Name',
                'action': 'strip_whitespace'
            },
            # Then convert to uppercase
            {
                'column': 'Product_Name',
                'action': 'uppercase'
            },
            # Replace FLESH with CANS in component
            {
                'column': 'Component',
                'action': 'replace',
                'old_value': 'FLESH',
                'new_value': 'CANS'
            },
            # Clean up prices
            {
                'column': 'Price',
                'action': 'fix_numeric'
            }
        ]
    }
    
    processor = CleanDataProcessor(step_config)
    result = processor.execute(test_df)
    
    # Verify multiple transformations
    first_product = result.iloc[0]['Product_Name']
    first_component = result.iloc[0]['Component']
    
    print(f"✓ Final product name: '{first_product}'")
    print(f"✓ Final component: '{first_component}'")
    
    if first_product == 'CANNED BEANS' and first_component == 'CANS':
        print("✓ Multiple rules applied correctly")
        return True
    else:
        print("✗ Multiple rules failed")
        return False


def test_error_handling():
    """Test error handling for various failure cases."""
    
    print("\nTesting error handling...")
    
    test_df = create_messy_test_data()
    
    # Test missing required fields
    try:
        bad_config = {
            'type': 'clean_data',
            'name': 'Missing rules'
            # No 'rules' field
        }
        processor = CleanDataProcessor(bad_config)
        processor.execute(test_df)
        print("✗ Should have failed with missing rules")
    except StepProcessorError as e:
        print(f"✓ Caught expected error: {e}")
    
    # Test invalid column
    try:
        bad_config = {
            'type': 'clean_data',
            'name': 'Invalid column',
            'rules': [
                {
                    'column': 'NonExistentColumn',
                    'action': 'uppercase'
                }
            ]
        }
        processor = CleanDataProcessor(bad_config)
        processor.execute(test_df)
        print("✗ Should have failed with invalid column")
    except StepProcessorError as e:
        print(f"✓ Caught expected error: {e}")
    
    # Test invalid action
    try:
        bad_config = {
            'type': 'clean_data',
            'name': 'Invalid action',
            'rules': [
                {
                    'column': 'Status',
                    'action': 'invalid_action'
                }
            ]
        }
        processor = CleanDataProcessor(bad_config)
        processor.execute(test_df)
        print("✗ Should have failed with invalid action")
    except StepProcessorError as e:
        print(f"✓ Caught expected error: {e}")


if __name__ == '__main__':
    success = True
    
    success &= test_replace_operations()
    success &= test_text_transformations()
    success &= test_numeric_cleaning()
    success &= test_fill_empty_values()
    success &= test_regex_operations()
    success &= test_standardize_values()
    success &= test_multiple_rules()
    test_error_handling()
    
    if success:
        print("\n✓ All clean data processor tests passed!")
    else:
        print("\n✗ Some clean data processor tests failed!")
    
    # Show supported actions
    processor = CleanDataProcessor({'type': 'clean_data', 'rules': []})
    print(f"\nSupported actions: {processor.get_supported_actions()}")
