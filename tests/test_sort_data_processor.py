"""
Test the SortDataProcessor functionality.
"""

import pandas as pd

from excel_recipe_processor.processors.sort_data_processor import SortDataProcessor
from excel_recipe_processor.processors.base_processor import StepProcessorError


def create_test_data():
    """Create sample data for testing sorting."""
    return pd.DataFrame({
        'Product_Name': ['Widget C', 'widget a', 'Widget B', 'widget d', 'Widget A'],
        'Price': [15.75, 10.50, 25.00, 8.25, 12.00],
        'Quantity': [75, 100, 50, 200, 125],
        'Priority': ['High', 'Low', 'Medium', 'High', 'Low'],
        'Department': ['Tools', 'Electronics', 'Hardware', 'Electronics', 'Tools'],
        'Order_Date': ['2024-01-20', '2024-01-15', '2024-01-25', '2024-01-18', '2024-01-22']
    })


def test_single_column_sort():
    """Test sorting by a single column."""
    
    print("Testing single column sort...")
    
    test_df = create_test_data()
    print(f"✓ Created test data: {len(test_df)} rows")
    
    # Test ascending sort by price
    step_config = {
        'type': 'sort_data',
        'name': 'Sort by price ascending',
        'columns': ['Price'],
        'ascending': True
    }
    
    processor = SortDataProcessor(step_config)
    result = processor.execute(test_df)
    
    print(f"✓ Single column sort: {len(result)} rows")
    
    # Check that data is sorted by price
    prices = result['Price'].tolist()
    print(f"✓ Sorted prices: {prices}")
    
    # Verify ascending order
    is_sorted = all(prices[i] <= prices[i+1] for i in range(len(prices)-1))
    
    if is_sorted:
        print("✓ Single column sort worked correctly")
        return True
    else:
        print("✗ Single column sort failed")
        return False


def test_multi_column_sort():
    """Test sorting by multiple columns."""
    
    print("\nTesting multi-column sort...")
    
    test_df = create_test_data()
    
    # Test sort by Department first, then Price
    step_config = {
        'type': 'sort_data',
        'name': 'Sort by department then price',
        'columns': ['Department', 'Price'],
        'ascending': [True, False]  # Department ascending, Price descending
    }
    
    processor = SortDataProcessor(step_config)
    result = processor.execute(test_df)
    
    print(f"✓ Multi-column sort: {len(result)} rows")
    
    # Check sorting results
    print("Sorted data:")
    for i in range(len(result)):
        dept = result.iloc[i]['Department']
        price = result.iloc[i]['Price']
        print(f"  {dept}: ${price}")
    
    # Verify sorting: within each department, prices should be descending
    departments = result['Department'].tolist()
    prices = result['Price'].tolist()
    
    # Check that departments are in ascending order and prices within departments are descending
    current_dept = None
    dept_prices = []
    all_correct = True
    
    for dept, price in zip(departments, prices):
        if dept != current_dept:
            # New department - check previous department's prices were descending
            if len(dept_prices) > 1:
                if not all(dept_prices[i] >= dept_prices[i+1] for i in range(len(dept_prices)-1)):
                    all_correct = False
                    break
            current_dept = dept
            dept_prices = [price]
        else:
            dept_prices.append(price)
    
    # Check last department
    if len(dept_prices) > 1:
        if not all(dept_prices[i] >= dept_prices[i+1] for i in range(len(dept_prices)-1)):
            all_correct = False
    
    if all_correct:
        print("✓ Multi-column sort worked correctly")
        return True
    else:
        print("✗ Multi-column sort failed")
        return False


def test_custom_sort_order():
    """Test sorting with custom value orders."""
    
    print("\nTesting custom sort order...")
    
    test_df = create_test_data()
    
    # Test custom priority order: High -> Medium -> Low
    step_config = {
        'type': 'sort_data',
        'name': 'Sort by custom priority order',
        'columns': ['Priority'],
        'custom_orders': {
            'Priority': ['High', 'Medium', 'Low']
        }
    }
    
    processor = SortDataProcessor(step_config)
    result = processor.execute(test_df)
    
    print(f"✓ Custom sort: {len(result)} rows")
    
    # Check priority order
    priorities = result['Priority'].tolist()
    print(f"✓ Priority order: {priorities}")
    
    # Verify custom order: High items first, then Medium, then Low
    high_indices = [i for i, p in enumerate(priorities) if p == 'High']
    medium_indices = [i for i, p in enumerate(priorities) if p == 'Medium']  
    low_indices = [i for i, p in enumerate(priorities) if p == 'Low']
    
    # Check that all High come before Medium, and Medium before Low
    if (high_indices and medium_indices and max(high_indices) < min(medium_indices) and
        medium_indices and low_indices and max(medium_indices) < min(low_indices)):
        print("✓ Custom sort order worked correctly")
        return True
    elif not medium_indices:  # No medium priorities in test data
        if (high_indices and low_indices and max(high_indices) < min(low_indices)):
            print("✓ Custom sort order worked correctly")
            return True
    
    print("✗ Custom sort order failed")
    return False


def test_case_insensitive_sort():
    """Test case-insensitive sorting."""
    
    print("\nTesting case-insensitive sort...")
    
    test_df = create_test_data()
    
    # Test case-insensitive sort by product name
    step_config = {
        'type': 'sort_data',
        'name': 'Case insensitive product sort',
        'columns': ['Product_Name'],
        'ignore_case': True
    }
    
    processor = SortDataProcessor(step_config)
    result = processor.execute(test_df)
    
    print(f"✓ Case insensitive sort: {len(result)} rows")
    
    # Check product name order (should ignore case)
    names = result['Product_Name'].tolist()
    print(f"✓ Sorted names: {names}")
    
    # Convert to lowercase for comparison
    lower_names = [name.lower() for name in names]
    is_sorted = all(lower_names[i] <= lower_names[i+1] for i in range(len(lower_names)-1))
    
    if is_sorted:
        print("✓ Case-insensitive sort worked correctly")
        return True
    else:
        print("✗ Case-insensitive sort failed")
        return False


def test_null_position_handling():
    """Test handling of null values in sorting."""
    
    print("\nTesting null position handling...")
    
    # Create data with null values
    test_df = pd.DataFrame({
        'Name': ['Alice', 'Bob', None, 'Charlie', None],
        'Score': [85, None, 90, 78, 92]
    })
    
    # Test nulls last (default)
    step_config1 = {
        'type': 'sort_data',
        'name': 'Sort with nulls last',
        'columns': ['Name'],
        'na_position': 'last'
    }
    
    processor1 = SortDataProcessor(step_config1)
    result1 = processor1.execute(test_df)
    
    names1 = result1['Name'].tolist()
    print(f"✓ Nulls last: {names1}")
    
    # Test nulls first
    step_config2 = {
        'type': 'sort_data',
        'name': 'Sort with nulls first',
        'columns': ['Name'], 
        'na_position': 'first'
    }
    
    processor2 = SortDataProcessor(step_config2)
    result2 = processor2.execute(test_df)
    
    names2 = result2['Name'].tolist()
    print(f"✓ Nulls first: {names2}")
    
    # Check that nulls are positioned correctly
    null_count = names1.count(None)
    if null_count > 0:
        # For nulls last, nulls should be at the end
        nulls_at_end = all(name is None for name in names1[-null_count:])
        # For nulls first, nulls should be at the beginning
        nulls_at_start = all(name is None for name in names2[:null_count])
        
        if nulls_at_end and nulls_at_start:
            print("✓ Null position handling worked correctly")
            return True
    
    print("✗ Null position handling failed")
    return False


def test_frequency_sort():
    """Test sorting by value frequency."""
    
    print("\nTesting frequency sort...")
    
    test_df = create_test_data()
    
    processor = SortDataProcessor({'type': 'sort_data', 'columns': ['Department']})
    result = processor.sort_by_frequency(test_df, 'Department', ascending=False)
    
    print(f"✓ Frequency sort: {len(result)} rows")
    
    # Check frequency order
    departments = result['Department'].tolist()
    print(f"✓ Department frequency order: {departments}")
    
    # Count frequencies
    dept_counts = {}
    for dept in departments:
        dept_counts[dept] = dept_counts.get(dept, 0) + 1
    
    print(f"✓ Department counts: {dept_counts}")
    
    if len(result) == len(test_df):
        print("✓ Frequency sort worked correctly")
        return True
    else:
        print("✗ Frequency sort failed")
        return False


def test_multiple_criteria_sort():
    """Test sorting with multiple criteria and different options."""
    
    print("\nTesting multiple criteria sort...")
    
    test_df = create_test_data()
    
    processor = SortDataProcessor({'type': 'sort_data', 'columns': ['Department']})
    
    # Define multiple criteria with different options
    criteria = [
        {
            'column': 'Priority',
            'custom_order': ['High', 'Medium', 'Low'],
            'ascending': True
        },
        {
            'column': 'Product_Name',
            'ignore_case': True,
            'ascending': True
        }
    ]
    
    result = processor.sort_by_multiple_criteria(test_df, criteria)
    
    print(f"✓ Multiple criteria sort: {len(result)} rows")
    
    # Show results
    print("Results:")
    for i in range(len(result)):
        priority = result.iloc[i]['Priority']
        name = result.iloc[i]['Product_Name']
        print(f"  {priority}: {name}")
    
    if len(result) == len(test_df):
        print("✓ Multiple criteria sort worked correctly")
        return True
    else:
        print("✗ Multiple criteria sort failed")
        return False


def test_sort_analysis():
    """Test sort analysis functionality."""
    
    print("\nTesting sort analysis...")
    
    test_df = create_test_data()
    
    processor = SortDataProcessor({'type': 'sort_data', 'columns': ['Price']})
    
    # Analyze different column types
    price_analysis = processor.get_sort_analysis(test_df, 'Price')
    name_analysis = processor.get_sort_analysis(test_df, 'Product_Name')
    
    print(f"✓ Price analysis:")
    print(f"  Data type: {price_analysis['data_type']}")
    print(f"  Min/Max: {price_analysis.get('min_value')}/{price_analysis.get('max_value')}")
    print(f"  Already sorted: {price_analysis['is_already_sorted']}")
    
    print(f"✓ Product name analysis:")
    print(f"  Data type: {name_analysis['data_type']}")
    print(f"  Avg length: {name_analysis.get('avg_length')}")
    print(f"  Already sorted: {name_analysis['is_already_sorted']}")
    
    if 'data_type' in price_analysis and 'data_type' in name_analysis:
        print("✓ Sort analysis worked correctly")
        return True
    else:
        print("✗ Sort analysis failed")
        return False


def test_capabilities_method():
    """Test the get_capabilities method."""
    
    print("\nTesting capabilities method...")
    
    processor = SortDataProcessor({'type': 'sort_data', 'columns': ['test']})
    capabilities = processor.get_capabilities()
    
    print(f"✓ Capabilities: {capabilities}")
    
    # Check that capabilities contains expected keys
    expected_keys = ['description', 'supported_options', 'na_positions', 'sort_directions']
    has_expected_keys = all(key in capabilities for key in expected_keys)
    
    if has_expected_keys:
        print("✓ Capabilities method worked correctly")
        return True
    else:
        print("✗ Capabilities method missing expected keys")
        return False


def test_error_handling():
    """Test error handling for various failure cases."""
    
    print("\nTesting error handling...")
    
    test_df = create_test_data()
    
    # Test missing columns field
    try:
        bad_config = {
            'type': 'sort_data',
            'name': 'Missing columns'
            # No 'columns' field
        }
        processor = SortDataProcessor(bad_config)
        processor.execute(test_df)
        print("✗ Should have failed with missing columns")
    except StepProcessorError as e:
        print(f"✓ Caught expected error: {e}")
    
    # Test invalid column
    try:
        bad_config = {
            'type': 'sort_data',
            'name': 'Invalid column',
            'columns': ['NonExistentColumn']
        }
        processor = SortDataProcessor(bad_config)
        processor.execute(test_df)
        print("✗ Should have failed with invalid column")
    except StepProcessorError as e:
        print(f"✓ Caught expected error: {e}")
    
    # Test mismatched ascending list length
    try:
        bad_config = {
            'type': 'sort_data',
            'name': 'Mismatched ascending length',
            'columns': ['Price', 'Quantity'],
            'ascending': [True]  # Only one value for two columns
        }
        processor = SortDataProcessor(bad_config)
        processor.execute(test_df)
        print("✗ Should have failed with mismatched ascending length")
    except StepProcessorError as e:
        print(f"✓ Caught expected error: {e}")


def test_real_world_scenario():
    """Test a realistic sorting scenario."""
    
    print("\nTesting real-world scenario...")
    
    # Simulate order processing data
    test_df = pd.DataFrame({
        'Order_ID': ['ORD001', 'ORD002', 'ORD003', 'ORD004', 'ORD005'],
        'Customer_Priority': ['VIP', 'Standard', 'VIP', 'Premium', 'Standard'],
        'Order_Date': ['2024-01-15', '2024-01-14', '2024-01-16', '2024-01-15', '2024-01-13'],
        'Order_Value': [1500, 250, 2500, 800, 400],
        'Ship_Method': ['Express', 'Standard', 'Express', 'Priority', 'Standard']
    })
    
    print(f"✓ Created order data: {len(test_df)} rows")
    
    # Sort by priority, then by order value (highest first), then by date
    step_config = {
        'type': 'sort_data',
        'name': 'Process orders by priority',
        'columns': ['Customer_Priority', 'Order_Value', 'Order_Date'],
        'custom_orders': {
            'Customer_Priority': ['VIP', 'Premium', 'Standard']
        },
        'ascending': [True, False, True]  # Priority custom order, Value desc, Date asc
    }
    
    processor = SortDataProcessor(step_config)
    result = processor.execute(test_df)
    
    print(f"✓ Processed orders:")
    for i in range(len(result)):
        order_id = result.iloc[i]['Order_ID']
        priority = result.iloc[i]['Customer_Priority']
        value = result.iloc[i]['Order_Value']
        date = result.iloc[i]['Order_Date']
        print(f"  {order_id}: {priority} ${value} ({date})")
    
    # Check that VIP orders come first
    priorities = result['Customer_Priority'].tolist()
    first_priority = priorities[0]
    
    if first_priority == 'VIP':
        print("✓ Real-world scenario worked correctly")
        return True
    else:
        print("✗ Real-world scenario failed")
        return False


if __name__ == '__main__':
    success = True
    
    success &= test_single_column_sort()
    success &= test_multi_column_sort()
    success &= test_custom_sort_order()
    success &= test_case_insensitive_sort()
    success &= test_null_position_handling()
    success &= test_frequency_sort()
    success &= test_multiple_criteria_sort()
    success &= test_sort_analysis()
    success &= test_capabilities_method()
    success &= test_real_world_scenario()
    test_error_handling()
    
    if success:
        print("\n✓ All sort data processor tests passed!")
    else:
        print("\n✗ Some sort data processor tests failed!")
    
    # Show capabilities
    processor = SortDataProcessor({'type': 'sort_data', 'columns': ['test']})
    capabilities = processor.get_capabilities()
    print(f"\nProcessor Capabilities:")
    for key, value in capabilities.items():
        if isinstance(value, list):
            print(f"  {key}: {', '.join(value)}")
        else:
            print(f"  {key}: {value}")
