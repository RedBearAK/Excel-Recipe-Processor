"""
Test the FillDataProcessor functionality.
"""

import pandas as pd

from excel_recipe_processor.processors.fill_data_processor import FillDataProcessor
from excel_recipe_processor.processors.base_processor import StepProcessorError


def create_test_data():
    """Create sample data with missing values."""
    return pd.DataFrame({
        'name': ['Alice', None, 'Charlie', 'David', None],
        'age': [25, None, 35, None, 45],
        'city': ['New York', 'Boston', None, 'Seattle', None],
        'salary': [50000, 60000, None, 70000, 80000],
        'department': ['Sales', 'Sales', 'Engineering', None, 'Engineering'],
        'score': [85.5, None, 92.0, 88.0, None]
    })


def create_conditional_test_data():
    """Create test data for conditional filling."""
    return pd.DataFrame({
        'employee_id': [1, 2, 3, 4, 5],
        'department': ['Sales', 'Engineering', 'Sales', None, 'Marketing'],
        'salary': [None, 75000, None, 65000, None],
        'bonus': [5000, None, 3000, None, 4000]
    })



def test_get_minimal_config():
    """Test get_minimal_config returns required fields."""
    print("Testing get_minimal_config...")
    
    config = FillDataProcessor.get_minimal_config()
    
    required_fields = ['columns', 'fill_method', 'fill_value']
    has_all_fields = all(field in config for field in required_fields)
    
    if has_all_fields:
        print("✓ get_minimal_config returned all required fields")
        return True
    else:
        print(f"✗ Missing fields in minimal config: {config}")
        return False


def test_constant_fill_single_column():
    """Test filling with constant value in single column."""
    print("Testing constant fill single column...")
    
    test_data = create_test_data()
    print(f"✓ Created test data with {len(test_data)} rows")
    print("Name column before fill:", test_data['name'].tolist())
    
    step_config = {
        'step_description': 'Fill name with Unknown',
        'processor_type': 'fill_data',
        'columns': ['name'],
        'fill_method': 'constant',
        'fill_value': 'Unknown'
    }
    
    processor = FillDataProcessor(step_config)
    result = processor.execute(test_data)
    
    print("Name column after fill:", result['name'].tolist())
    
    # Check that nulls were filled
    name_filled_correctly = (
        result.loc[1, 'name'] == 'Unknown' and
        result.loc[4, 'name'] == 'Unknown' and
        result.loc[0, 'name'] == 'Alice' and  # Original value preserved
        result.loc[2, 'name'] == 'Charlie'    # Original value preserved
    )
    
    null_count_before = test_data['name'].isnull().sum()
    null_count_after = result['name'].isnull().sum()
    print(f"✓ Nulls before: {null_count_before}, after: {null_count_after}")
    
    if name_filled_correctly:
        print("✓ Constant fill single column worked correctly")
        return True
    else:
        print("✗ Constant fill single column failed")
        return False


def test_constant_fill_multiple_columns():
    """Test filling with constant value in multiple columns."""
    print("\nTesting constant fill multiple columns...")
    
    test_data = create_test_data()
    print(f"✓ Created test data with {len(test_data)} rows")
    print("Before fill:")
    print(f"  Name nulls: {test_data['name'].isnull().sum()}")
    print(f"  City nulls: {test_data['city'].isnull().sum()}")
    
    step_config = {
        'step_description': 'Fill name and city with Unknown',
        'processor_type': 'fill_data',
        'columns': ['name', 'city'],
        'fill_method': 'constant',
        'fill_value': 'Unknown'
    }
    
    processor = FillDataProcessor(step_config)
    result = processor.execute(test_data)
    
    print("After fill:")
    print(f"  Name nulls: {result['name'].isnull().sum()}")
    print(f"  City nulls: {result['city'].isnull().sum()}")
    print("Name column:", result['name'].tolist())
    print("City column:", result['city'].tolist())
    
    # Check both columns were filled
    multi_fill_correct = (
        result.loc[1, 'name'] == 'Unknown' and
        result.loc[4, 'name'] == 'Unknown' and
        result.loc[2, 'city'] == 'Unknown' and
        result.loc[4, 'city'] == 'Unknown'
    )
    
    if multi_fill_correct:
        print("✓ Constant fill multiple columns worked correctly")
        return True
    else:
        print("✗ Constant fill multiple columns failed")
        return False



def test_forward_fill():
    """Test forward fill operation."""
    print("\nTesting forward fill...")
    
    test_data = create_test_data()
    print("Before forward fill:", test_data['name'].tolist())
    
    step_config = {
        'step_description': 'Forward fill names',
        'processor_type': 'fill_data',
        'columns': ['name'],
        'fill_method': 'forward_fill'
    }
    
    processor = FillDataProcessor(step_config)
    result = processor.execute(test_data)
    
    print("After forward fill:", result['name'].tolist())
    
    # Forward fill should work like this:
    # ['Alice', None, 'Charlie', 'David', None]
    #     ↓       ↓        ↓        ↓       ↓
    # ['Alice', 'Alice', 'Charlie', 'David', 'David']
    # 
    # Index 1 gets 'Alice' (from index 0)
    # Index 4 gets 'David' (from index 3), NOT 'Charlie'
    
    forward_fill_correct = (
        result.loc[1, 'name'] == 'Alice' and
        result.loc[4, 'name'] == 'David'  # Fixed: should be 'David', not 'Charlie'
    )
    
    if forward_fill_correct:
        print("✓ Forward fill worked correctly")
        return True
    else:
        print("✗ Forward fill failed")
        print(f"  Expected: index 1='Alice', index 4='David'")
        print(f"  Actual: index 1='{result.loc[1, 'name']}', index 4='{result.loc[4, 'name']}'")
        return False


def test_backward_fill():
    """Test backward fill operation."""
    print("\nTesting backward fill...")
    
    test_data = create_test_data()
    print("Before backward fill:", test_data['name'].tolist())
    
    step_config = {
        'step_description': 'Backward fill names',
        'processor_type': 'fill_data',
        'columns': ['name'],
        'fill_method': 'backward_fill'
    }
    
    processor = FillDataProcessor(step_config)
    result = processor.execute(test_data)
    
    print("After backward fill:", result['name'].tolist())
    
    # Backward fill should work like this:
    # ['Alice', None, 'Charlie', 'David', None]
    #     ↓       ↓        ↓        ↓       ↓
    # ['Alice', 'Charlie', 'Charlie', 'David', None]
    # 
    # Index 1 gets 'Charlie' (from index 2)
    # Index 4 stays None (nothing after it to fill backward)
    
    backward_fill_correct = (
        result.loc[1, 'name'] == 'Charlie' and
        pd.isna(result.loc[4, 'name'])  # Nothing to carry backward
    )
    
    if backward_fill_correct:
        print("✓ Backward fill worked correctly")
        return True
    else:
        print("✗ Backward fill failed")
        print(f"  Expected: index 1='Charlie', index 4=None")
        print(f"  Actual: index 1='{result.loc[1, 'name']}', index 4='{result.loc[4, 'name']}'")
        return False


def test_mean_fill():
    """Test filling with mean value for numeric column."""
    print("\nTesting mean fill...")
    
    test_data = create_test_data()
    
    step_config = {
        'step_description': 'Fill age with mean',
        'processor_type': 'fill_data',
        'columns': ['age'],
        'fill_method': 'mean'
    }
    
    processor = FillDataProcessor(step_config)
    result = processor.execute(test_data)
    
    # Calculate expected mean (25 + 35 + 45) / 3 = 35
    expected_mean = 35.0
    
    mean_fill_correct = (
        result.loc[1, 'age'] == expected_mean and
        result.loc[3, 'age'] == expected_mean
    )
    
    if mean_fill_correct:
        print("✓ Mean fill worked correctly")
        return True
    else:
        print("✗ Mean fill failed")
        return False


def test_mode_fill():
    """Test filling with mode value."""
    print("\nTesting mode fill...")
    
    test_data = create_test_data()
    
    step_config = {
        'step_description': 'Fill department with mode',
        'processor_type': 'fill_data',
        'columns': ['department'],
        'fill_method': 'mode'
    }
    
    processor = FillDataProcessor(step_config)
    result = processor.execute(test_data)
    
    # Mode should be one of the most frequent values
    filled_value = result.loc[3, 'department']
    mode_fill_correct = filled_value in ['Sales', 'Engineering']
    
    if mode_fill_correct:
        print("✓ Mode fill worked correctly")
        return True
    else:
        print("✗ Mode fill failed")
        return False



def test_zero_fill():
    """Test filling with zero."""
    print("\nTesting zero fill...")
    
    test_data = create_test_data()
    
    step_config = {
        'step_description': 'Fill age with zero',
        'processor_type': 'fill_data',
        'columns': ['age'],
        'fill_method': 'zero'
    }
    
    processor = FillDataProcessor(step_config)
    result = processor.execute(test_data)
    
    zero_fill_correct = (
        result.loc[1, 'age'] == 0 and
        result.loc[3, 'age'] == 0
    )
    
    if zero_fill_correct:
        print("✓ Zero fill worked correctly")
        return True
    else:
        print("✗ Zero fill failed")
        return False


def test_conditional_fill():
    """Test conditional filling based on other columns."""
    print("\nTesting conditional fill...")
    
    test_data = create_conditional_test_data()
    
    step_config = {
        'step_description': 'Conditional salary fill',
        'processor_type': 'fill_data',
        'columns': ['salary'],
        'fill_method': 'constant',
        'fill_value': 0,  # Default value (required for constant method)
        'conditions': [
            {
                'condition_column': 'department',
                'condition_type': 'equals',
                'condition_value': 'Sales',
                'fill_value': 55000
            },
            {
                'condition_column': 'department',
                'condition_type': 'equals',
                'condition_value': 'Marketing',
                'fill_value': 60000
            }
        ]
    }
    
    processor = FillDataProcessor(step_config)
    result = processor.execute(test_data)
    
    conditional_fill_correct = (
        result.loc[0, 'salary'] == 55000 and  # Sales department
        result.loc[2, 'salary'] == 55000 and  # Sales department
        result.loc[4, 'salary'] == 60000      # Marketing department
    )
    
    if conditional_fill_correct:
        print("✓ Conditional fill worked correctly")
        return True
    else:
        print("✗ Conditional fill failed")
        return False


def test_error_column_not_found():
    """Test error when column doesn't exist."""
    print("\nTesting column not found error...")
    
    test_data = create_test_data()
    
    step_config = {
        'step_description': 'Fill nonexistent column',
        'processor_type': 'fill_data',
        'columns': ['nonexistent_column'],
        'fill_method': 'constant',
        'fill_value': 'test'
    }
    
    try:
        processor = FillDataProcessor(step_config)
        processor.execute(test_data)
        print("✗ Should have failed with column not found error")
        return False
    except StepProcessorError as e:
        if 'not found' in str(e):
            print("✓ Correctly caught column not found error")
            return True
        else:
            print(f"✗ Wrong error type: {e}")
            return False


def test_error_invalid_fill_method():
    """Test error with invalid fill method."""
    print("\nTesting invalid fill method error...")
    
    test_data = create_test_data()
    
    step_config = {
        'step_description': 'Invalid fill method',
        'processor_type': 'fill_data',
        'columns': ['name'],
        'fill_method': 'invalid_method'
    }
    
    try:
        processor = FillDataProcessor(step_config)
        processor.execute(test_data)
        print("✗ Should have failed with invalid fill method error")
        return False
    except StepProcessorError as e:
        if 'Unknown fill method' in str(e):
            print("✓ Correctly caught invalid fill method error")
            return True
        else:
            print(f"✗ Wrong error type: {e}")
            return False


def test_helper_methods():
    """Test helper methods."""
    print("\nTesting helper methods...")
    
    test_data = create_test_data()
    
    # Create processor for helper method tests
    step_config = {
        'step_description': 'Helper test',
        'processor_type': 'fill_data',
        'columns': ['name'],
        'fill_method': 'constant',
        'fill_value': 'test'
    }
    
    processor = FillDataProcessor(step_config)
    
    # Test fill_blanks_with_value helper
    result = processor.fill_blanks_with_value(test_data, ['name', 'city'], 'Unknown')
    helper_fill_correct = (
        result.loc[1, 'name'] == 'Unknown' and
        result.loc[2, 'city'] == 'Unknown'
    )
    
    # Test analyze_missing_data helper
    analysis = processor.analyze_missing_data(test_data)
    analysis_correct = (
        analysis['total_rows'] == 5 and
        'name' in analysis['columns_with_missing'] and
        'age' in analysis['columns_with_missing']
    )
    
    if helper_fill_correct and analysis_correct:
        print("✓ Helper methods worked correctly")
        return True
    else:
        print("✗ Helper methods failed")
        return False


if __name__ == '__main__':
    success = True
    
    success &= test_get_minimal_config()
    success &= test_constant_fill_single_column()
    success &= test_constant_fill_multiple_columns()
    success &= test_forward_fill()
    success &= test_backward_fill()
    success &= test_mean_fill()
    success &= test_mode_fill()
    success &= test_zero_fill()
    success &= test_conditional_fill()
    success &= test_helper_methods()
    
    # Error tests (don't affect success tracking)
    test_error_column_not_found()
    test_error_invalid_fill_method()
    
    if success:
        print("\n✓ All fill data processor tests passed!")
    else:
        print("\n✗ Some fill data processor tests failed!")
    
    # Show supported features
    processor = FillDataProcessor({
        'processor_type': 'fill_data',
        'columns': ['test'],
        'fill_method': 'constant',
        'fill_value': 'test'
    })
    
    # Try to show capabilities if method exists
    try:
        capabilities = processor.get_capabilities()
        print(f"\nProcessor Capabilities:")
        for key, value in capabilities.items():
            if isinstance(value, list):
                print(f"  {key}: {', '.join(map(str, value))}")
            else:
                print(f"  {key}: {value}")
    except AttributeError:
        print(f"\nSupported fill methods: ['constant', 'forward_fill', 'backward_fill', 'mean', 'mode', 'zero']")
