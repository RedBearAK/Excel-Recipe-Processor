"""
Test the FilterDataProcessor functionality.
"""

import pandas as pd

from excel_recipe_processor.processors.filter_data_processor import FilterDataProcessor
from excel_recipe_processor.processors.base_processor import StepProcessorError


def create_test_data():
    """Create sample data for testing."""
    return pd.DataFrame({
        'Product_Name': ['CANNED BEANS', 'FRESH FISH', 'CANNED CORN', 'DRIED FRUIT', 'CANNED SOUP'],
        'Component': ['FLESH', 'FLESH', 'FLESH', 'FLESH', 'FLESH'],
        'Quantity': [100, 50, 75, 25, 200],
        'Price': [10.50, 25.00, 15.75, 8.25, 12.00],
        'Department': ['Grocery', 'Fresh', 'Grocery', 'Snacks', 'Grocery'],
        'Status': ['Active', 'Active', 'Cancelled', 'Active', 'Active']
    })


def test_basic_filtering():
    """Test basic filter operations."""
    
    print("Testing basic filtering...")
    
    test_df = create_test_data()
    print(f"✓ Created test data: {len(test_df)} rows")
    
    # Test 'contains' filter - like the van report workflow
    step_config = {
        'processor_type': 'filter_data',
        'step_description': 'Filter for canned products',
        'filters': [
            {
                'column': 'Product_Name',
                'condition': 'contains',
                'value': 'CANNED'
            }
        ]
    }
    
    processor = FilterDataProcessor(step_config)
    result = processor.execute(test_df)
    
    print(f"✓ Filter for 'CANNED': {len(test_df)} → {len(result)} rows")
    
    # Should have 3 canned products
    if len(result) == 3:
        print("✓ Correct number of canned products found")
    else:
        print(f"✗ Expected 3 canned products, got {len(result)}")
    
    # Test 'equals' filter
    step_config2 = {
        'processor_type': 'filter_data',
        'step_description': 'Filter for grocery department',
        'filters': [
            {
                'column': 'Department',
                'condition': 'equals',
                'value': 'Grocery'
            }
        ]
    }
    
    processor2 = FilterDataProcessor(step_config2)
    result2 = processor2.execute(test_df)
    
    print(f"✓ Filter for Department='Grocery': {len(test_df)} → {len(result2)} rows")
    
    # Test 'not_equals' filter
    step_config3 = {
        'processor_type': 'filter_data',
        'step_description': 'Filter out cancelled items',
        'filters': [
            {
                'column': 'Status',
                'condition': 'not_equals',
                'value': 'Cancelled'
            }
        ]
    }
    
    processor3 = FilterDataProcessor(step_config3)
    result3 = processor3.execute(test_df)
    
    print(f"✓ Filter out Status='Cancelled': {len(test_df)} → {len(result3)} rows")
    
    return True


def test_multiple_filters():
    """Test applying multiple filters in sequence."""
    
    print("\nTesting multiple filters...")
    
    test_df = create_test_data()
    
    # Apply multiple filters like in the van report workflow
    step_config = {
        'processor_type': 'filter_data',
        'step_description': 'Multiple filter test',
        'filters': [
            {
                'column': 'Product_Name',
                'condition': 'contains',
                'value': 'CANNED'
            },
            {
                'column': 'Status',
                'condition': 'not_equals',
                'value': 'Cancelled'
            },
            {
                'column': 'Quantity',
                'condition': 'greater_than',
                'value': 80
            }
        ]
    }
    
    processor = FilterDataProcessor(step_config)
    result = processor.execute(test_df)
    
    print(f"✓ Multiple filters: {len(test_df)} → {len(result)} rows")
    
    # Should be CANNED products, not cancelled, with quantity > 80
    # That should be: CANNED BEANS (100) and CANNED SOUP (200)
    # But CANNED CORN is cancelled, so only 2 should remain
    if len(result) == 2:
        print("✓ Multiple filters applied correctly")
        return True
    else:
        print(f"✗ Expected 2 rows after multiple filters, got {len(result)}")
        print("Remaining products:", result['Product_Name'].tolist())
        return False


def test_numeric_conditions():
    """Test numeric comparison conditions."""
    
    print("\nTesting numeric conditions...")
    
    test_df = create_test_data()
    
    # Test greater_than
    step_config = {
        'processor_type': 'filter_data',
        'step_description': 'Price greater than 15',
        'filters': [
            {
                'column': 'Price',
                'condition': 'greater_than',
                'value': 15.0
            }
        ]
    }
    
    processor = FilterDataProcessor(step_config)
    result = processor.execute(test_df)
    
    print(f"✓ Price > 15.0: {len(test_df)} → {len(result)} rows")
    
    # Test less_equal
    step_config2 = {
        'processor_type': 'filter_data',
        'step_description': 'Quantity less or equal 75',
        'filters': [
            {
                'column': 'Quantity',
                'condition': 'less_equal',
                'value': 75
            }
        ]
    }
    
    processor2 = FilterDataProcessor(step_config2)
    result2 = processor2.execute(test_df)
    
    print(f"✓ Quantity <= 75: {len(test_df)} → {len(result2)} rows")
    
    return True


def test_list_conditions():
    """Test in_list and not_in_list conditions."""
    
    print("\nTesting list conditions...")
    
    test_df = create_test_data()
    
    # Test in_list
    step_config = {
        'processor_type': 'filter_data',
        'step_description': 'Department in list',
        'filters': [
            {
                'column': 'Department',
                'condition': 'in_list',
                'value': ['Grocery', 'Snacks']
            }
        ]
    }
    
    processor = FilterDataProcessor(step_config)
    result = processor.execute(test_df)
    
    print(f"✓ Department in ['Grocery', 'Snacks']: {len(test_df)} → {len(result)} rows")
    
    return True


def test_error_handling():
    """Test error handling for various failure cases."""
    
    print("\nTesting error handling...")
    
    test_df = create_test_data()
    
    # Test missing required fields
    try:
        bad_config = {
            'processor_type': 'filter_data',
            'step_description': 'Missing filters'
            # No 'filters' field
        }
        processor = FilterDataProcessor(bad_config)
        processor.execute(test_df)
        print("✗ Should have failed with missing filters")
    except StepProcessorError as e:
        print(f"✓ Caught expected error: {e}")
    
    # Test invalid column
    try:
        bad_config = {
            'processor_type': 'filter_data',
            'step_description': 'Invalid column',
            'filters': [
                {
                    'column': 'NonExistentColumn',
                    'condition': 'equals',
                    'value': 'test'
                }
            ]
        }
        processor = FilterDataProcessor(bad_config)
        processor.execute(test_df)
        print("✗ Should have failed with invalid column")
    except StepProcessorError as e:
        print(f"✓ Caught expected error: {e}")
    
    # Test invalid condition
    try:
        bad_config = {
            'processor_type': 'filter_data',
            'step_description': 'Invalid condition',
            'filters': [
                {
                    'column': 'Status',
                    'condition': 'invalid_condition',
                    'value': 'test'
                }
            ]
        }
        processor = FilterDataProcessor(bad_config)
        processor.execute(test_df)
        print("✗ Should have failed with invalid condition")
    except StepProcessorError as e:
        print(f"✓ Caught expected error: {e}")
    
    # Test missing value for condition that requires it
    try:
        bad_config = {
            'processor_type': 'filter_data',
            'step_description': 'Missing value',
            'filters': [
                {
                    'column': 'Status',
                    'condition': 'equals'
                    # No 'value' field
                }
            ]
        }
        processor = FilterDataProcessor(bad_config)
        processor.execute(test_df)
        print("✗ Should have failed with missing value")
    except StepProcessorError as e:
        print(f"✓ Caught expected error: {e}")


def test_empty_conditions():
    """Test edge cases like no filters, empty data."""
    
    print("\nTesting edge cases...")
    
    test_df = create_test_data()
    
    # Test no filters
    step_config = {
        'processor_type': 'filter_data',
        'step_description': 'No filters',
        'filters': []
    }
    
    processor = FilterDataProcessor(step_config)
    result = processor.execute(test_df)
    
    if len(result) == len(test_df):
        print("✓ No filters applied correctly (returns original data)")
    else:
        print("✗ No filters should return original data unchanged")
    
    # Test not_empty condition
    test_df_with_empties = test_df.copy()
    test_df_with_empties.loc[2, 'Department'] = ''  # Make one empty
    test_df_with_empties.loc[3, 'Department'] = None  # Make one None
    
    step_config2 = {
        'processor_type': 'filter_data',
        'step_description': 'Remove empty departments',
        'filters': [
            {
                'column': 'Department',
                'condition': 'not_empty'
            }
        ]
    }
    
    processor2 = FilterDataProcessor(step_config2)
    result2 = processor2.execute(test_df_with_empties)
    
    print(f"✓ Remove empty departments: {len(test_df_with_empties)} → {len(result2)} rows")


if __name__ == '__main__':
    success = True
    
    success &= test_basic_filtering()
    success &= test_multiple_filters()
    success &= test_numeric_conditions()
    success &= test_list_conditions()
    test_error_handling()
    test_empty_conditions()
    
    if success:
        print("\n✓ All filter processor tests passed!")
    else:
        print("\n✗ Some filter processor tests failed!")
    
    # Show supported conditions
    processor = FilterDataProcessor({'processor_type': 'filter_data', 'filters': []})
    print(f"\nSupported conditions: {processor.get_supported_conditions()}")
