"""
Test the AddCalculatedColumnProcessor functionality.
"""

import pandas as pd

from excel_recipe_processor.processors.add_calculated_column_processor import AddCalculatedColumnProcessor
from excel_recipe_processor.processors.base_processor import StepProcessorError


def create_test_data():
    """Create sample data for testing calculations."""
    return pd.DataFrame({
        'Product_Code': ['A001', 'B002', 'C003', 'A001', 'D004'],
        'Product_Name': ['Widget A', 'Gadget B', 'Tool C', 'Widget A', 'Device D'],
        'Quantity': [100, 50, 75, 25, 200],
        'Price': [10.50, 25.00, 15.75, 10.50, 8.25],
        'Department': ['Electronics', 'Tools', 'Hardware', 'Electronics', 'Electronics'],
        'Order_Date': ['2024-01-15', '2024-01-20', '2024-01-18', '2024-01-22', '2024-01-25'],
        'Ship_Date': ['2024-01-20', '2024-01-25', '2024-01-23', '2024-01-27', '2024-01-30']
    })


def test_concatenation_calculation():
    """Test string concatenation calculations."""
    
    print("Testing concatenation calculations...")
    
    test_df = create_test_data()
    print(f"✓ Created test data: {len(test_df)} rows")
    
    # Test basic concatenation
    step_config = {
        'processor_type': 'add_calculated_column',
        'step_description': 'Create full product ID',
        'new_column': 'Full_Product_ID',
        'processor_type': 'concat',
        'calculation': {
            'columns': ['Product_Code', 'Product_Name'],
            'separator': ' - '
        }
    }
    
    processor = AddCalculatedColumnProcessor(step_config)
    result = processor.execute(test_df)
    
    print(f"✓ Concatenation result: {len(result)} rows, {len(result.columns)} columns")
    
    # Check that new column was created
    if 'Full_Product_ID' not in result.columns:
        print("✗ Full_Product_ID column not created")
        return False
    
    # Check concatenation results
    first_id = result.iloc[0]['Full_Product_ID']
    print(f"✓ First concatenated ID: '{first_id}'")
    
    if first_id == 'A001 - Widget A':
        print("✓ Concatenation worked correctly")
        return True
    else:
        print(f"✗ Expected 'A001 - Widget A', got '{first_id}'")
        return False


def test_mathematical_calculations():
    """Test mathematical operations."""
    
    print("\nTesting mathematical calculations...")
    
    test_df = create_test_data()
    
    # Test multiplication - calculate total value
    step_config = {
        'processor_type': 'add_calculated_column',
        'step_description': 'Calculate total value',
        'new_column': 'Total_Value',
        'processor_type': 'math',
        'calculation': {
            'operation': 'multiply',
            'column1': 'Quantity',
            'column2': 'Price'
        }
    }
    
    processor = AddCalculatedColumnProcessor(step_config)
    result = processor.execute(test_df)
    
    print(f"✓ Math calculation: {len(result)} rows")
    
    # Check calculation
    first_total = result.iloc[0]['Total_Value']
    expected_total = 100 * 10.50  # 1050.0
    
    print(f"✓ First total value: {first_total} (expected: {expected_total})")
    
    if abs(first_total - expected_total) < 0.01:
        print("✓ Mathematical calculation worked correctly")
        return True
    else:
        print(f"✗ Math calculation failed")
        return False


def test_conditional_logic():
    """Test conditional (if-then-else) calculations."""
    
    print("\nTesting conditional logic...")
    
    test_df = create_test_data()
    
    # Test conditional - categorize by quantity
    step_config = {
        'processor_type': 'add_calculated_column',
        'step_description': 'Categorize by quantity',
        'new_column': 'Quantity_Category',
        'processor_type': 'conditional',
        'calculation': {
            'condition_column': 'Quantity',
            'condition': 'greater_than',
            'condition_value': 75,
            'value_if_true': 'High',
            'value_if_false': 'Low'
        }
    }
    
    processor = AddCalculatedColumnProcessor(step_config)
    result = processor.execute(test_df)
    
    print(f"✓ Conditional logic: {len(result)} rows")
    
    # Check conditional results
    high_count = (result['Quantity_Category'] == 'High').sum()
    low_count = (result['Quantity_Category'] == 'Low').sum()
    
    print(f"✓ High quantity items: {high_count}")
    print(f"✓ Low quantity items: {low_count}")
    
    # Verify specific cases
    first_category = result.iloc[0]['Quantity_Category']  # Quantity = 100, should be 'High'
    second_category = result.iloc[1]['Quantity_Category']  # Quantity = 50, should be 'Low'
    
    if first_category == 'High' and second_category == 'Low':
        print("✓ Conditional logic worked correctly")
        return True
    else:
        print(f"✗ Conditional logic failed: {first_category}, {second_category}")
        return False


def test_date_calculations():
    """Test date-based calculations."""
    
    print("\nTesting date calculations...")
    
    test_df = create_test_data()
    
    # Test days between dates
    step_config = {
        'processor_type': 'add_calculated_column',
        'step_description': 'Calculate shipping days',
        'new_column': 'Shipping_Days',
        'processor_type': 'date',
        'calculation': {
            'operation': 'days_between',
            'start_date_column': 'Order_Date',
            'end_date_column': 'Ship_Date'
        }
    }
    
    processor = AddCalculatedColumnProcessor(step_config)
    result = processor.execute(test_df)
    
    print(f"✓ Date calculation: {len(result)} rows")
    
    # Check date calculation
    first_days = result.iloc[0]['Shipping_Days']
    print(f"✓ First shipping days: {first_days}")
    
    # Should be 5 days difference (2024-01-20 - 2024-01-15)
    if first_days == 5:
        print("✓ Date calculation worked correctly")
        return True
    else:
        print(f"✗ Expected 5 days, got {first_days}")
        return False


def test_text_operations():
    """Test text/string operations."""
    
    print("\nTesting text operations...")
    
    test_df = create_test_data()
    
    # Test text length calculation
    step_config = {
        'processor_type': 'add_calculated_column',
        'step_description': 'Product name length',
        'new_column': 'Name_Length',
        'processor_type': 'text',
        'calculation': {
            'operation': 'length',
            'column': 'Product_Name'
        }
    }
    
    processor = AddCalculatedColumnProcessor(step_config)
    result = processor.execute(test_df)
    
    print(f"✓ Text operation: {len(result)} rows")
    
    # Check text length
    first_length = result.iloc[0]['Name_Length']
    expected_length = len('Widget A')  # 8
    
    print(f"✓ First name length: {first_length} (expected: {expected_length})")
    
    if first_length == expected_length:
        print("✓ Text operation worked correctly")
        return True
    else:
        print(f"✗ Text operation failed")
        return False


def test_aggregation_operations():
    """Test multi-column aggregation operations."""
    
    print("\nTesting aggregation operations...")
    
    # Create data with multiple numeric columns
    test_df = pd.DataFrame({
        'Score1': [85, 90, 78, 92, 88],
        'Score2': [88, 85, 82, 89, 91],
        'Score3': [90, 88, 85, 94, 87],
        'Student': ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve']
    })
    
    # Test sum aggregation
    step_config = {
        'processor_type': 'add_calculated_column',
        'step_description': 'Calculate total score',
        'new_column': 'Total_Score',
        'processor_type': 'math',
        'calculation': {
            'operation': 'sum',
            'columns': ['Score1', 'Score2', 'Score3']
        }
    }
    
    processor = AddCalculatedColumnProcessor(step_config)
    result = processor.execute(test_df)
    
    print(f"✓ Aggregation: {len(result)} rows")
    
    # Check aggregation
    first_total = result.iloc[0]['Total_Score']
    expected_total = 85 + 88 + 90  # 263
    
    print(f"✓ First total score: {first_total} (expected: {expected_total})")
    
    if first_total == expected_total:
        print("✓ Aggregation operation worked correctly")
        return True
    else:
        print(f"✗ Aggregation failed")
        return False


def test_expression_calculation():
    """Test general expression calculations."""
    
    print("\nTesting expression calculations...")
    
    test_df = create_test_data()
    
    # Test complex expression
    step_config = {
        'processor_type': 'add_calculated_column',
        'step_description': 'Complex calculation',
        'new_column': 'Value_Per_Unit',
        'processor_type': 'expression',
        'calculation': {
            'formula': 'Price * 1.1'  # Add 10% markup
        }
    }
    
    processor = AddCalculatedColumnProcessor(step_config)
    result = processor.execute(test_df)
    
    print(f"✓ Expression calculation: {len(result)} rows")
    
    # Check expression result
    first_value = result.iloc[0]['Value_Per_Unit']
    expected_value = 10.50 * 1.1  # 11.55
    
    print(f"✓ First value per unit: {first_value} (expected: {expected_value})")
    
    if abs(first_value - expected_value) < 0.01:
        print("✓ Expression calculation worked correctly")
        return True
    else:
        print(f"✗ Expression calculation failed")
        return False


def test_overwrite_existing_column():
    """Test overwriting an existing column."""
    
    print("\nTesting column overwrite...")
    
    test_df = create_test_data()
    
    # Test overwriting existing column
    step_config = {
        'processor_type': 'add_calculated_column',
        'step_description': 'Overwrite price with doubled value',
        'new_column': 'Price',  # Same as existing column
        'processor_type': 'math',
        'calculation': {
            'operation': 'multiply',
            'column1': 'Price',
            'column2': 'Quantity'  # This will change meaning, but tests overwrite
        },
        'overwrite': True
    }
    
    processor = AddCalculatedColumnProcessor(step_config)
    result = processor.execute(test_df)
    
    print(f"✓ Overwrite test: {len(result)} rows")
    
    # Check that Price column was overwritten
    new_price = result.iloc[0]['Price']
    expected_new_price = 10.50 * 100  # Original Price * Quantity
    
    print(f"✓ Overwritten price: {new_price} (expected: {expected_new_price})")
    
    if abs(new_price - expected_new_price) < 0.01:
        print("✓ Column overwrite worked correctly")
        return True
    else:
        print(f"✗ Column overwrite failed")
        return False


def test_error_handling():
    """Test error handling for various failure cases."""
    
    print("\nTesting error handling...")
    
    test_df = create_test_data()
    
    # Test missing required fields
    try:
        bad_config = {
            'processor_type': 'add_calculated_column',
            'step_description': 'Missing fields'
            # Missing 'new_column' and 'calculation'
        }
        processor = AddCalculatedColumnProcessor(bad_config)
        processor.execute(test_df)
        print("✗ Should have failed with missing fields")
    except StepProcessorError as e:
        print(f"✓ Caught expected error: {e}")
    
    # Test overwrite protection
    try:
        bad_config = {
            'processor_type': 'add_calculated_column',
            'step_description': 'Overwrite without permission',
            'new_column': 'Price',  # Existing column
            'processor_type': 'math',
            'calculation': {
                'operation': 'multiply',
                'column1': 'Quantity',
                'column2': 'Price'
            }
            # Missing 'overwrite: true'
        }
        processor = AddCalculatedColumnProcessor(bad_config)
        processor.execute(test_df)
        print("✗ Should have failed trying to overwrite existing column")
    except StepProcessorError as e:
        print(f"✓ Caught expected error: {e}")
    
    # Test invalid column reference
    try:
        bad_config = {
            'processor_type': 'add_calculated_column',
            'step_description': 'Invalid column',
            'new_column': 'Invalid_Calc',
            'processor_type': 'math',
            'calculation': {
                'operation': 'multiply',
                'column1': 'NonExistentColumn',
                'column2': 'Price'
            }
        }
        processor = AddCalculatedColumnProcessor(bad_config)
        processor.execute(test_df)
        print("✗ Should have failed with invalid column")
    except StepProcessorError as e:
        print(f"✓ Caught expected error: {e}")


def test_multiple_calculations():
    """Test applying multiple calculated columns in sequence."""
    
    print("\nTesting multiple calculations...")
    
    test_df = create_test_data()
    
    # First calculation: Total value
    step_config1 = {
        'processor_type': 'add_calculated_column',
        'step_description': 'Calculate total value',
        'new_column': 'Total_Value',
        'processor_type': 'math',
        'calculation': {
            'operation': 'multiply',
            'column1': 'Quantity',
            'column2': 'Price'
        }
    }
    
    processor1 = AddCalculatedColumnProcessor(step_config1)
    result1 = processor1.execute(test_df)
    
    # Second calculation: Value category based on first calculation
    step_config2 = {
        'processor_type': 'add_calculated_column',
        'step_description': 'Categorize by value',
        'new_column': 'Value_Category',
        'processor_type': 'conditional',
        'calculation': {
            'condition_column': 'Total_Value',
            'condition': 'greater_than',
            'condition_value': 1000,
            'value_if_true': 'High Value',
            'value_if_false': 'Standard Value'
        }
    }
    
    processor2 = AddCalculatedColumnProcessor(step_config2)
    result2 = processor2.execute(result1)
    
    print(f"✓ Multiple calculations: {len(result2)} rows, {len(result2.columns)} columns")
    
    # Check both columns exist
    if 'Total_Value' in result2.columns and 'Value_Category' in result2.columns:
        print("✓ Both calculated columns created")
        
        # Check calculation chain
        first_total = result2.iloc[0]['Total_Value']
        first_category = result2.iloc[0]['Value_Category']
        
        print(f"✓ First row: Total_Value={first_total}, Category='{first_category}'")
        
        if first_total == 1050.0 and first_category == 'High Value':
            print("✓ Multiple calculations worked correctly")
            return True
    
    print("✗ Multiple calculations failed")
    return False


if __name__ == '__main__':
    success = True
    
    success &= test_concatenation_calculation()
    success &= test_mathematical_calculations()
    success &= test_conditional_logic()
    success &= test_date_calculations()
    success &= test_text_operations()
    success &= test_aggregation_operations()
    success &= test_expression_calculation()
    success &= test_overwrite_existing_column()
    success &= test_multiple_calculations()
    test_error_handling()
    
    if success:
        print("\n✓ All add calculated column processor tests passed!")
    else:
        print("\n✗ Some add calculated column processor tests failed!")
    
    # Show supported features
    processor = AddCalculatedColumnProcessor({'processor_type': 'add_calculated_column', 'new_column': 'x', 'calculation': {}})
    print(f"\nSupported calculation types: {processor.get_supported_calculation_types()}")
    print(f"Supported conditions: {processor.get_supported_conditions()}")
    print(f"Supported math operations: {processor.get_supported_math_operations()}")
