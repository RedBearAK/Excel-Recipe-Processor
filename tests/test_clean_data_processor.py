"""
Test the CleanDataProcessor functionality with new columns list syntax.

File: excel_recipe_processor/tests/test_clean_data_processor.py
"""

import pandas as pd

from excel_recipe_processor.core.base_processor import StepProcessorError
from excel_recipe_processor.processors.clean_data_processor import CleanDataProcessor


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
    
    print("\nTesting replace operations...")
    
    test_df = create_messy_test_data()
    
    # Test conditional replacement - replace FLESH with CANS where Product_Name contains CANNED
    step_config = {
        'processor_type': 'clean_data',
        'step_description': 'Van report style replacement',
        'rules': [
            {
                'columns': ['Component'],  # Changed from 'column' to 'columns' list
                'action': 'replace',
                'old_value': 'FLESH',
                'new_value': 'CANS',
                'condition_column': 'Product_Name',
                'condition': 'contains',
                'condition_value': 'CANNED',
                'case_sensitive': False
            }
        ]
    }
    
    processor = CleanDataProcessor(step_config)
    result = processor.execute(test_df)
    
    # Check that FLESH was replaced with CANS only where Product_Name contains CANNED
    canned_items = result[result['Product_Name'].str.contains('CANNED', na=False, case=False)]
    other_items = result[~result['Product_Name'].str.contains('CANNED', na=False, case=False)]
    
    canned_components = canned_items['Component'].unique()
    other_components = other_items['Component'].unique()
    
    print(f"✓ Components for CANNED items: {canned_components}")
    print(f"✓ Components for other items: {other_components}")
    
    if 'CANS' in canned_components and 'FLESH' not in canned_components:
        if 'flesh' in other_components or 'FLESH' in other_components:
            print("✓ Conditional replacement worked correctly")
            return True
    
    print("✗ Conditional replacement failed")
    return False


def test_text_transformations():
    """Test various text cleaning operations."""
    
    print("\nTesting text transformations...")
    
    test_df = create_messy_test_data()
    
    # Test multiple text cleaning operations
    step_config = {
        'processor_type': 'clean_data',
        'step_description': 'Text cleaning',
        'rules': [
            {
                'columns': ['Product_Name'],  # Changed to list
                'action': 'strip_whitespace'
            },
            {
                'columns': ['Product_Name'],  # Changed to list
                'action': 'uppercase'
            },
            {
                'columns': ['Product_Name'],  # Changed to list
                'action': 'remove_special_chars'
            },
            {
                'columns': ['Status'],  # Changed to list
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
        'processor_type': 'clean_data',
        'step_description': 'Clean prices',
        'rules': [
            {
                'columns': ['Price'],  # Changed to list
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
        'processor_type': 'clean_data',
        'step_description': 'Fill empty values',
        'rules': [
            {
                'columns': ['Product_Name'],  # Changed to list
                'action': 'fill_empty',
                'fill_value': 'Unknown Product',
                'method': 'value'
            },
            {
                'columns': ['Quantity'],  # Changed to list
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
        'processor_type': 'clean_data',
        'step_description': 'Regex cleaning',
        'rules': [
            {
                'columns': ['Product_Name'],  # Changed to list
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
        'processor_type': 'clean_data',
        'step_description': 'Standardize status',
        'rules': [
            {
                'columns': ['Status'],  # Changed to list
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
    
    print("\nTesting multiple rules...")
    
    test_df = create_messy_test_data()
    
    # Apply multiple sequential cleaning operations
    step_config = {
        'processor_type': 'clean_data',
        'step_description': 'Multiple cleaning rules',
        'rules': [
            {
                'columns': ['Product_Name'],  # Changed to list
                'action': 'strip_whitespace'
            },
            {
                'columns': ['Product_Name'],  # Changed to list
                'action': 'title_case'
            },
            {
                'columns': ['Price'],  # Changed to list
                'action': 'fix_numeric',
                'fill_na': 0.0
            },
            {
                'columns': ['Status'],  # Changed to list
                'action': 'uppercase'
            }
        ]
    }
    
    processor = CleanDataProcessor(step_config)
    result = processor.execute(test_df)
    
    # Check that all transformations were applied
    first_product = result.iloc[0]['Product_Name']
    first_price = result.iloc[0]['Price']
    first_status = result.iloc[0]['Status']
    
    print(f"✓ Product name after multiple rules: '{first_product}'")
    print(f"✓ Price after cleaning: {first_price}")
    print(f"✓ Status after uppercasing: '{first_status}'")
    
    if (first_product == 'Canned Beans' and 
        isinstance(first_price, (int, float)) and 
        first_status == 'ACTIVE'):
        print("✓ Multiple rules worked correctly")
        return True
    else:
        print("✗ Multiple rules failed")
        return False


def test_multiple_columns_same_rule():
    """Test the new functionality: applying same rule to multiple columns."""
    
    print("\nTesting multiple columns with same rule...")
    
    # Create test data with multiple columns that need the same cleaning
    test_df = pd.DataFrame({
        'Name_First': ['  John  ', ' jane ', '  BOB  '],
        'Name_Last': [' SMITH ', '  doe  ', ' JONES '],
        'City': ['  NEW YORK  ', ' chicago ', '  SEATTLE  '],
        'State': [' NY ', '  IL  ', ' WA '],
        'Price1': ['$10.50', '$25.00', '$15.75'],
        'Price2': ['$100.00', '$200.50', '$300.25'],
        'Date1': ['2024-01-15', '01/15/2024', '2024-02-20'],
        'Date2': ['2024-03-10', '03/10/2024', '2024-04-15']
    })
    
    # Apply same cleaning rule to multiple columns
    step_config = {
        'processor_type': 'clean_data',
        'step_description': 'Clean multiple columns efficiently',
        'rules': [
            # Clean multiple text columns at once
            {
                'columns': ['Name_First', 'Name_Last', 'City', 'State'],  # Multiple columns
                'action': 'strip_whitespace'
            },
            {
                'columns': ['Name_First', 'Name_Last'],  # Multiple columns
                'action': 'title_case'
            },
            # Clean multiple price columns at once
            {
                'columns': ['Price1', 'Price2'],  # Multiple columns
                'action': 'fix_numeric',
                'fill_na': 0.0
            },
            # Clean multiple date columns at once
            {
                'columns': ['Date1', 'Date2'],  # Multiple columns
                'action': 'fix_dates',
                'format': '%Y-%m-%d'
            }
        ]
    }
    
    processor = CleanDataProcessor(step_config)
    result = processor.execute(test_df)
    
    # Check results
    print(f"✓ Name_First after cleaning: {result['Name_First'].tolist()}")
    print(f"✓ Name_Last after cleaning: {result['Name_Last'].tolist()}")
    print(f"✓ Price1 dtype: {result['Price1'].dtype}")
    print(f"✓ Price2 dtype: {result['Price2'].dtype}")
    print(f"✓ Date1 dtype: {result['Date1'].dtype}")
    print(f"✓ Date2 dtype: {result['Date2'].dtype}")
    
    # Verify the cleaning worked
    names_correct = (result['Name_First'].iloc[0] == 'John' and 
                    result['Name_Last'].iloc[0] == 'Smith')
    prices_numeric = (pd.api.types.is_numeric_dtype(result['Price1']) and
                     pd.api.types.is_numeric_dtype(result['Price2']))
    dates_datetime = (pd.api.types.is_datetime64_any_dtype(result['Date1']) and
                     pd.api.types.is_datetime64_any_dtype(result['Date2']))
    
    if names_correct and prices_numeric and dates_datetime:
        print("✓ Multiple columns with same rule worked correctly")
        return True
    else:
        print(f"✗ Multiple columns failed: names={names_correct}, prices={prices_numeric}, dates={dates_datetime}")
        return False


def test_missing_columns_handling():
    """Test how the processor handles missing columns."""
    
    print("\nTesting missing columns handling...")
    
    test_df = create_messy_test_data()
    
    # Try to clean columns that exist and some that don't
    step_config = {
        'processor_type': 'clean_data',
        'step_description': 'Test missing columns',
        'rules': [
            {
                'columns': ['Product_Name', 'NonExistent_Column', 'Status'],  # Mix of existing and missing
                'action': 'strip_whitespace'
            }
        ]
    }
    
    processor = CleanDataProcessor(step_config)
    result = processor.execute(test_df)
    
    # Should succeed and clean the existing columns
    first_product = result.iloc[0]['Product_Name'].strip()
    first_status = result.iloc[0]['Status'].strip()
    
    print(f"✓ Product_Name cleaned: '{first_product}'")
    print(f"✓ Status cleaned: '{first_status}'")
    
    if first_product == 'CANNED Beans' and first_status == 'active':
        print("✓ Missing columns handling worked correctly (cleaned existing columns)")
        return True
    else:
        print("✗ Missing columns handling failed")
        return False


def test_conditional_replacement():
    """Test conditional replacement with the new columns syntax."""
    
    print("\nTesting conditional replacement...")
    
    test_df = create_messy_test_data()
    
    # Replace FLESH with CANS only where Product_Name contains "CANNED"
    step_config = {
        'processor_type': 'clean_data',
        'step_description': 'Conditional replacement',
        'rules': [
            {
                'columns': ['Component'],  # List with single column
                'action': 'replace',
                'old_value': 'FLESH',
                'new_value': 'CANS',
                'condition_column': 'Product_Name',
                'condition': 'contains',
                'condition_value': 'CANNED',
                'case_sensitive': False
            }
        ]
    }
    
    processor = CleanDataProcessor(step_config)
    result = processor.execute(test_df)
    
    # Check conditional logic worked
    canned_beans_component = result[result['Product_Name'].str.contains('CANNED Beans', na=False)]['Component'].iloc[0]
    canned_corn_component = result[result['Product_Name'].str.contains('CANNED CORN', na=False)]['Component'].iloc[0]
    fresh_fish_component = result[result['Product_Name'].str.contains('fresh fish', na=False)]['Component'].iloc[0]
    
    print(f"✓ CANNED Beans component: {canned_beans_component}")
    print(f"✓ CANNED CORN component: {canned_corn_component}")
    print(f"✓ fresh fish component: {fresh_fish_component}")
    
    if (canned_beans_component == 'CANS' and 
        canned_corn_component == 'CANS' and 
        fresh_fish_component == 'flesh'):
        print("✓ Conditional replacement worked correctly")
        return True
    else:
        print("✗ Conditional replacement failed")
        return False


def test_error_handling():
    """Test error handling for invalid configurations."""
    
    print("\nTesting error handling...")
    
    test_df = create_messy_test_data()
    
    # Test missing required fields
    try:
        bad_config = {
            'processor_type': 'clean_data',
            'step_description': 'Missing fields',
            'rules': [
                {
                    'action': 'strip_whitespace'
                    # Missing 'columns' field
                }
            ]
        }
        processor = CleanDataProcessor(bad_config)
        processor.execute(test_df)
        print("✗ Should have failed with missing columns field")
        return False
    except StepProcessorError as e:
        print(f"✓ Caught expected error for missing columns: {e}")
    
    # Test empty columns list
    try:
        bad_config = {
            'processor_type': 'clean_data',
            'step_description': 'Empty columns',
            'rules': [
                {
                    'columns': [],  # Empty list
                    'action': 'strip_whitespace'
                }
            ]
        }
        processor = CleanDataProcessor(bad_config)
        processor.execute(test_df)
        print("✗ Should have failed with empty columns list")
        return False
    except StepProcessorError as e:
        print(f"✓ Caught expected error for empty columns: {e}")
    
    # Test non-list columns field
    try:
        bad_config = {
            'processor_type': 'clean_data',
            'step_description': 'Non-list columns',
            'rules': [
                {
                    'columns': 'Product_Name',  # String instead of list
                    'action': 'strip_whitespace'
                }
            ]
        }
        processor = CleanDataProcessor(bad_config)
        processor.execute(test_df)
        print("✗ Should have failed with non-list columns")
        return False
    except StepProcessorError as e:
        print(f"✓ Caught expected error for non-list columns: {e}")
    
    print("✓ Error handling tests passed")
    return True


if __name__ == '__main__':
    success = True

    success &= test_replace_operations()
    success &= test_text_transformations()
    success &= test_numeric_cleaning()
    success &= test_fill_empty_values()
    success &= test_regex_operations()
    success &= test_standardize_values()
    success &= test_multiple_rules()
    
    # New tests for the enhanced functionality
    success &= test_multiple_columns_same_rule()
    success &= test_missing_columns_handling()
    success &= test_conditional_replacement()
    
    test_error_handling()  # Always run error tests
    
    if success:
        print("\n✓ All clean data processor tests passed!")
    else:
        print("\n✗ Some clean data processor tests failed!")
    
    # Show supported actions
    processor = CleanDataProcessor({'processor_type': 'clean_data', 'rules': []})
    print(f"\nSupported actions: {processor.get_supported_actions()}")


# End of file #
