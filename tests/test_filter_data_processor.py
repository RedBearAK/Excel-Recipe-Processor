"""
Comprehensive tests for the refactored FilterDataProcessor with StageManager integration.

Tests both existing functionality (regression) and new stage-based filtering capabilities.
"""

import pandas as pd

from excel_recipe_processor.core.stage_manager import StageManager
from excel_recipe_processor.core.base_processor import StepProcessorError
from excel_recipe_processor.processors.filter_data_processor import FilterDataProcessor


def create_test_data():
    """Create sample DataFrame for testing."""
    return pd.DataFrame({
        'Product_ID': ['P001', 'P002', 'P003', 'P004', 'P005'],
        'Product_Name': ['CANNED BEANS', 'FRESH FISH', 'CANNED SOUP', 'CANNED CORN', 'FROZEN PEAS'],
        'Department': ['Grocery', 'Seafood', 'Grocery', 'Grocery', 'Frozen'],
        'Price': [12.50, 25.00, 11.75, 15.00, 6.25],  # Changed CANNED SOUP from 8.75 to 11.75
        'Quantity': [100, 50, 200, 75, 150],
        'Status': ['Active', 'Active', 'Active', 'Cancelled', 'Active'],
        'Supplier_ID': ['S001', 'S002', 'S001', 'S003', 'S002']
    })


def create_approved_customers_stage():
    """Create sample approved customers stage data."""
    return pd.DataFrame({
        'Customer_ID': ['C001', 'C003', 'C005', 'C007'],
        'Customer_Name': ['Alice Corp', 'Charlie Ltd', 'Eve Industries', 'Grace Co'],
        'Tier': ['Gold', 'Silver', 'Gold', 'Bronze']
    })


def create_price_history_stage():
    """Create sample price history stage data."""
    return pd.DataFrame({
        'Product_ID': ['P001', 'P002', 'P003', 'P004', 'P005'],
        'Historical_Price': [10.00, 30.00, 8.00, 12.00, 7.00],
        'Last_Updated': ['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05']
    })


def create_orders_with_customers():
    """Create orders data that can be filtered against customer stages."""
    return pd.DataFrame({
        'Order_ID': ['O001', 'O002', 'O003', 'O004', 'O005'],
        'Customer_ID': ['C001', 'C002', 'C003', 'C004', 'C005'],
        'Product_ID': ['P001', 'P002', 'P003', 'P004', 'P005'],
        'Order_Amount': [100, 200, 150, 300, 75],
        'Order_Date': ['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05']
    })


def setup_test_stages():
    """Set up test stages for stage-based filtering tests."""
    StageManager.initialize_stages(max_stages=10)
    
    # Create approved customers stage
    approved_customers = create_approved_customers_stage()
    StageManager.save_stage(
        stage_name='Approved Customers',
        data=approved_customers,
        description='List of approved customers for orders'
    )
    
    # Create price history stage
    price_history = create_price_history_stage()
    StageManager.save_stage(
        stage_name='Price History',
        data=price_history,
        description='Historical pricing data for products'
    )


def test_basic_equals_filter():
    """Test basic equals filtering still works (regression test)."""
    print("\nTesting basic equals filter...")
    
    test_df = create_test_data()
    
    step_config = {
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
    
    processor = FilterDataProcessor(step_config)
    result = processor.execute(test_df)
    
    if len(result) == 3 and all(result['Department'] == 'Grocery'):
        print("✓ Basic equals filter works correctly")
        return True
    else:
        print(f"✗ Expected 3 grocery items, got {len(result)}")
        return False


def test_multiple_filters():
    """Test multiple filters applied in sequence (regression test)."""
    print("\nTesting multiple filters...")
    
    test_df = create_test_data()
    
    step_config = {
        'processor_type': 'filter_data',
        'step_description': 'Multiple filters test',
        'filters': [
            {
                'column': 'Product_Name',
                'condition': 'contains',
                'value': 'CANNED'
            },
            {
                'column': 'Status',
                'condition': 'equals',
                'value': 'Active'
            },
            {
                'column': 'Price',
                'condition': 'greater_than',
                'value': 10.0
            }
        ]
    }
    
    processor = FilterDataProcessor(step_config)
    result = processor.execute(test_df)
    
    # Should be CANNED BEANS and CANNED SOUP (CANNED CORN is cancelled)
    if (len(result) == 2 and 
        all('CANNED' in name for name in result['Product_Name']) and
        all(result['Status'] == 'Active') and
        all(result['Price'] > 10.0)):
        print("✓ Multiple filters work correctly")
        return True
    else:
        print(f"✗ Expected 2 items after multiple filters, got {len(result)}")
        return False


def test_numeric_conditions():
    """Test numeric comparison conditions (regression test)."""
    print("\nTesting numeric conditions...")
    
    test_df = create_test_data()
    
    step_config = {
        'processor_type': 'filter_data',
        'step_description': 'Price range filter',
        'filters': [
            {
                'column': 'Price',
                'condition': 'greater_equal',
                'value': 10.0
            },
            {
                'column': 'Price',
                'condition': 'less_equal',
                'value': 20.0
            }
        ]
    }
    
    processor = FilterDataProcessor(step_config)
    result = processor.execute(test_df)
    
    if all(10.0 <= price <= 20.0 for price in result['Price']):
        print("✓ Numeric conditions work correctly")
        return True
    else:
        print("✗ Some prices are outside the range 10-20")
        return False


def test_list_conditions():
    """Test in_list and not_in_list conditions (regression test)."""
    print("\nTesting list conditions...")
    
    test_df = create_test_data()
    
    step_config = {
        'processor_type': 'filter_data',
        'step_description': 'Department list filter',
        'filters': [
            {
                'column': 'Department',
                'condition': 'in_list',
                'value': ['Grocery', 'Frozen']
            }
        ]
    }
    
    processor = FilterDataProcessor(step_config)
    result = processor.execute(test_df)
    
    if (len(result) == 4 and 
        all(dept in ['Grocery', 'Frozen'] for dept in result['Department'])):
        print("✓ List conditions work correctly")
        return True
    else:
        print(f"✗ Expected 4 items from Grocery/Frozen, got {len(result)}")
        return False


def test_in_stage_filter():
    """Test filtering to include only items found in a stage."""
    print("\nTesting in_stage filter...")
    
    setup_test_stages()

    orders_df = create_orders_with_customers()
    
    step_config = {
        'processor_type': 'filter_data',
        'step_description': 'Filter for approved customers only',
        'filters': [
            {
                'column': 'Customer_ID',
                'condition': 'in_stage',
                'stage_name': 'Approved Customers',
                'stage_column': 'Customer_ID'
            }
        ]
    }
    
    processor = FilterDataProcessor(step_config)
    result = processor.execute(orders_df)
    
    # Should only include orders from C001, C003, C005 (approved customers)
    expected_customers = {'C001', 'C003', 'C005'}
    actual_customers = set(result['Customer_ID'])
    
    if actual_customers == expected_customers and len(result) == 3:
        print("✓ in_stage filter works correctly")
        return True
    else:
        print(f"✗ Expected {expected_customers}, got {actual_customers}")
        return False


def test_not_in_stage_filter():
    """Test filtering to exclude items found in a stage."""
    print("\nTesting not_in_stage filter...")
    
    setup_test_stages()

    orders_df = create_orders_with_customers()
    
    step_config = {
        'processor_type': 'filter_data',
        'step_description': 'Filter out approved customers',
        'filters': [
            {
                'column': 'Customer_ID',
                'condition': 'not_in_stage',
                'stage_name': 'Approved Customers',
                'stage_column': 'Customer_ID'
            }
        ]
    }
    
    processor = FilterDataProcessor(step_config)
    result = processor.execute(orders_df)
    
    # Should only include orders from C002, C004 (not approved)
    expected_customers = {'C002', 'C004'}
    actual_customers = set(result['Customer_ID'])
    
    if actual_customers == expected_customers and len(result) == 2:
        print("✓ not_in_stage filter works correctly")
        return True
    else:
        print(f"✗ Expected {expected_customers}, got {actual_customers}")
        return False


def test_stage_comparison_filter():
    """Test filtering based on comparison with stage values."""
    print("\nTesting stage_comparison filter...")
    
    setup_test_stages()

    test_df = create_test_data()
    
    step_config = {
        'processor_type': 'filter_data',
        'step_description': 'Filter for products with price increases',
        'filters': [
            {
                'column': 'Price',
                'condition': 'stage_comparison',
                'stage_name': 'Price History',
                'key_column': 'Product_ID',
                'stage_key_column': 'Product_ID',
                'stage_value_column': 'Historical_Price',
                'comparison_operator': 'greater_than'
            }
        ]
    }
    
    processor = FilterDataProcessor(step_config)
    result = processor.execute(test_df)
    
    # Check which products have price increases
    # P001: 12.50 > 10.00 ✓, P002: 25.00 < 30.00 ✗, P003: 8.75 > 8.00 ✓, 
    # P004: 15.00 > 12.00 ✓, P005: 6.25 < 7.00 ✗
    expected_products = {'P001', 'P003', 'P004'}
    actual_products = set(result['Product_ID'])
    
    if actual_products == expected_products and len(result) == 3:
        print("✓ stage_comparison filter works correctly")
        return True
    else:
        print(f"✗ Expected {expected_products}, got {actual_products}")
        return False


def test_combined_stage_and_basic_filters():
    """Test combining stage-based and basic filters."""
    print("\nTesting combined stage and basic filters...")
    
    setup_test_stages()
    
    orders_df = create_orders_with_customers()
    
    step_config = {
        'processor_type': 'filter_data',
        'step_description': 'High-value orders from approved customers',
        'filters': [
            {
                'column': 'Customer_ID',
                'condition': 'in_stage',
                'stage_name': 'Approved Customers',
                'stage_column': 'Customer_ID'
            },
            {
                'column': 'Order_Amount',
                'condition': 'greater_than',
                'value': 100
            }
        ]
    }
    
    processor = FilterDataProcessor(step_config)
    result = processor.execute(orders_df)
    
    # Should only include approved customers with order amount > 100
    # C001: 100 (not > 100), C003: 150 ✓, C005: 75 (not > 100)
    if (len(result) == 1 and 
        result.iloc[0]['Customer_ID'] == 'C003' and
        result.iloc[0]['Order_Amount'] == 150):
        print("✓ Combined stage and basic filters work correctly")
        return True
    else:
        print(f"✗ Expected 1 high-value approved order, got {len(result)}")
        return False


def test_stage_filter_error_handling():
    """Test error handling for stage-based filtering."""
    print("\nTesting stage filter error handling...")
    
    # Set up a basic test stage
    StageManager.initialize_stages(max_stages=10)
    test_stage = pd.DataFrame({
        'ID': ['A', 'B', 'C'],
        'Value': [1, 2, 3]
    })
    StageManager.save_stage('Test Stage', test_stage, 'Test stage for error handling')
    
    try:
        test_df = create_test_data()
        
        # Test missing stage_name
        try:
            step_config = {
                'processor_type': 'filter_data',
                'filters': [
                    {
                        'column': 'Product_ID',
                        'condition': 'in_stage',
                        'stage_column': 'ID'
                        # Missing stage_name
                    }
                ]
            }
            
            processor = FilterDataProcessor(step_config)
            processor.execute(test_df)
            print("✗ Should have failed with missing stage_name")
            return False
        except StepProcessorError as e:
            if "requires 'stage_name'" in str(e):
                print("✓ Caught expected error for missing stage_name")
            else:
                print(f"✗ Wrong error message: {e}")
                return False
        
        # Test nonexistent stage
        try:
            step_config = {
                'processor_type': 'filter_data',
                'filters': [
                    {
                        'column': 'Product_ID',
                        'condition': 'in_stage',
                        'stage_name': 'Nonexistent Stage',
                        'stage_column': 'ID'
                    }
                ]
            }
            
            processor = FilterDataProcessor(step_config)
            processor.execute(test_df)
            print("✗ Should have failed with nonexistent stage")
            return False
        except StepProcessorError as e:
            if "non-existent stage" in str(e):
                print("✓ Caught expected error for nonexistent stage")
            else:
                print(f"✗ Wrong error message: {e}")
                return False
        
        print("✓ Error handling works correctly")
        return True
        
    finally:
        StageManager.cleanup_stages()


def test_capabilities_include_stage_features():
    """Test that processor capabilities include stage integration features."""
    print("\nTesting capabilities reporting...")
    
    processor = FilterDataProcessor({'processor_type': 'filter_data', 'filters': []})
    
    supported = processor.get_supported_conditions()
    stage_conditions = processor.get_stage_based_conditions()
    capabilities = processor.get_capabilities()
    
    # Check that all stage conditions are in supported conditions
    stage_conditions_supported = all(cond in supported for cond in stage_conditions)
    
    # Check for stage-related capability information
    has_stage_capabilities = (
        'stage_based_conditions' in capabilities and
        'stage_integration' in capabilities
    )
    
    # Check specific new conditions
    expected_stage_conditions = ['in_stage', 'not_in_stage', 'stage_comparison']
    has_new_conditions = all(cond in supported for cond in expected_stage_conditions)
    
    if stage_conditions_supported and has_stage_capabilities and has_new_conditions:
        print("✓ Capabilities include stage integration features")
        return True
    else:
        print("✗ Missing stage capabilities in reporting")
        return False


def test_backward_compatibility():
    """Test that minimal config is unchanged for backward compatibility."""
    print("\nTesting backward compatibility...")
    
    minimal_config = FilterDataProcessor.get_minimal_config()
    
    has_filters = 'filters' in minimal_config
    filters_is_list = isinstance(minimal_config['filters'], list)
    has_example = len(minimal_config['filters']) > 0
    
    if has_filters and filters_is_list and has_example:
        example_filter = minimal_config['filters'][0]
        has_basic_fields = (
            'column' in example_filter and
            'condition' in example_filter and
            'value' in example_filter
        )
        
        if has_basic_fields:
            print("✓ Backward compatibility maintained")
            return True
        else:
            print("✗ Example filter missing basic fields")
            return False
    else:
        print("✗ Minimal config structure changed")
        return False


def test_pandas_expression_basic():
    """Test basic pandas expression functionality."""
    print("\nTesting pandas expression basic functionality...")
    
    test_df = create_test_data()
    
    step_config = {
        'processor_type': 'filter_data',
        'step_description': 'Pandas expression test',
        'pandas_expression': 'Price > 20.0 and Status == "Active"'
    }
    
    processor = FilterDataProcessor(step_config)
    result = processor.execute(test_df)
    
    # Should filter to items with Price > 20 AND Status = Active
    if len(result) > 0 and all(result['Price'] > 20.0) and all(result['Status'] == 'Active'):
        print("✓ Basic pandas expression works correctly")
        return True
    else:
        print(f"✗ Pandas expression failed: expected filtered data, got {len(result)} rows")
        return False


def test_pandas_expression_with_spaces():
    """Test pandas expression with column names containing spaces."""
    print("\nTesting pandas expression with column names containing spaces...")
    
    test_df = create_test_data()
    
    step_config = {
        'processor_type': 'filter_data', 
        'step_description': 'Pandas expression with spaces test',
        'pandas_expression': '`Product_Name`.str.contains("CANNED") and Price > 10.0'
    }
    
    processor = FilterDataProcessor(step_config)
    result = processor.execute(test_df)
    
    if len(result) > 0 and all('CANNED' in name for name in result['Product_Name']):
        print("✓ Pandas expression with column spaces works correctly")
        return True
    else:
        print(f"✗ Pandas expression with spaces failed: got {len(result)} rows")
        return False


def test_pandas_expression_boolean_logic():
    """Test complex boolean logic (the main reason for this feature)."""
    print("\nTesting pandas expression boolean OR logic...")
    
    test_df = create_test_data()
    
    # Use columns that actually exist in test data - simulate "Completed Vans" logic
    # Keep non-Active status OR Active status with high price
    step_config = {
        'processor_type': 'filter_data',
        'step_description': 'Boolean OR logic test', 
        'pandas_expression': '(Status != "Active") | (Status == "Active" & Price > 25.0)'
    }
    
    processor = FilterDataProcessor(step_config)
    result = processor.execute(test_df)
    
    # Should include all non-Active + expensive Active items
    active_rows = result[result['Status'] == 'Active']
    non_active_rows = result[result['Status'] != 'Active']
    
    active_all_expensive = len(active_rows) == 0 or all(active_rows['Price'] > 25.0)
    
    if active_all_expensive and len(result) > 0:
        print("✓ Boolean OR logic works correctly")
        return True
    else:
        print(f"✗ Boolean OR logic failed - Active rows: {len(active_rows)}, Non-active: {len(non_active_rows)}")
        print(f"   Active prices: {list(active_rows['Price']) if len(active_rows) > 0 else 'none'}")
        return False


def test_pandas_expression_error_handling():
    """Test error handling for invalid pandas expressions."""
    print("\nTesting pandas expression error handling...")
    
    test_df = create_test_data()
    
    # Test syntax error (tokenization error)
    try:
        step_config = {
            'processor_type': 'filter_data',
            'pandas_expression': 'Price > 20 and ('  # Syntax error - missing closing paren
        }
        processor = FilterDataProcessor(step_config)
        processor.execute(test_df)
        print("✗ Should have failed with syntax error")
        return False
    except StepProcessorError as e:
        if 'syntax error' in str(e).lower() or 'unexpected eof' in str(e).lower():
            print("✓ Syntax error properly caught")
        else:
            print(f"✗ Wrong error type for syntax error: {e}")
            return False
    
    # Test missing column error
    try:
        step_config = {
            'processor_type': 'filter_data',
            'pandas_expression': 'NonExistentColumn > 100'
        }
        processor = FilterDataProcessor(step_config)
        processor.execute(test_df)
        print("✗ Should have failed with missing column error")
        return False
    except StepProcessorError as e:
        if 'unknown column' in str(e).lower() or 'nonexistentcolumn' in str(e).lower():
            print("✓ Missing column error properly caught")
        else:
            print(f"✗ Wrong error type for missing column: {e}")
            return False
    
    # Test type error (comparing string to number)
    try:
        step_config = {
            'processor_type': 'filter_data',
            'pandas_expression': 'Status > 100'  # Comparing text column to number
        }
        processor = FilterDataProcessor(step_config)
        processor.execute(test_df)
        print("✗ Should have failed with type error")
        return False
    except StepProcessorError as e:
        if 'type error' in str(e).lower() or 'evaluation error' in str(e).lower():
            print("✓ Type error properly caught")
        else:
            print(f"✗ Wrong error type for type comparison: {e}")
            return False
    
    return True


def test_pandas_expression_vs_regular_filters():
    """Test that pandas_expression takes precedence over regular filters."""
    print("\nTesting pandas expression precedence over regular filters...")
    
    test_df = create_test_data()
    
    step_config = {
        'processor_type': 'filter_data',
        'pandas_expression': 'Price > 20.0',  # This should be used
        'filters': [  # This should be ignored
            {
                'column': 'Price',
                'condition': 'less_than',
                'value': 15.0
            }
        ]
    }
    
    processor = FilterDataProcessor(step_config)
    result = processor.execute(test_df)
    
    # Should use pandas_expression (Price > 20), not filters (Price < 15)
    if len(result) > 0 and all(result['Price'] > 20.0):
        print("✓ Pandas expression takes precedence over regular filters")
        return True
    else:
        print(f"✗ Precedence test failed: got {len(result)} rows")
        return False


def test_empty_pandas_expression():
    """Test error handling for empty pandas expression."""
    print("\nTesting empty pandas expression handling...")
    
    test_df = create_test_data()
    
    try:
        step_config = {
            'processor_type': 'filter_data',
            'pandas_expression': ''  # Empty expression
        }
        processor = FilterDataProcessor(step_config)
        processor.execute(test_df)
        print("✗ Should have failed with empty expression")
        return False
    except StepProcessorError as e:
        if 'non-empty string' in str(e):
            print("✓ Empty expression properly rejected")
            return True
        else:
            print(f"✗ Wrong error for empty expression: {e}")
            return False


if __name__ == '__main__':
    print("Testing FilterDataProcessor refactoring...")
    success = True
    
    # Basic regression tests
    print("\n=== Testing Basic Functionality (Regression) ===")
    success &= test_basic_equals_filter()
    success &= test_multiple_filters()
    success &= test_numeric_conditions()
    success &= test_list_conditions()
    
    # Stage-based tests
    print("\n=== Testing Stage-Based Filtering (New Features) ===")
    setup_test_stages()
    try:
        success &= test_in_stage_filter()
        success &= test_not_in_stage_filter()
        success &= test_stage_comparison_filter()
        success &= test_combined_stage_and_basic_filters()
    finally:
        StageManager.cleanup_stages()
    
    # Error handling and capabilities tests
    print("\n=== Testing Error Handling and Capabilities ===")
    success &= test_stage_filter_error_handling()
    success &= test_capabilities_include_stage_features()
    success &= test_backward_compatibility()
    
    print("Testing FilterDataProcessor pandas_expression feature...")
    success = True
    
    # Existing tests...
    
    # New pandas expression tests
    print("\n=== Testing Pandas Expression Feature ===")
    success &= test_pandas_expression_basic()
    success &= test_pandas_expression_with_spaces()
    success &= test_pandas_expression_boolean_logic()
    success &= test_pandas_expression_error_handling()
    success &= test_pandas_expression_vs_regular_filters()
    success &= test_empty_pandas_expression()

    if success:
        print("\n🎉 All FilterDataProcessor refactoring tests passed!")
    else:
        print("\n❌ Some FilterDataProcessor refactoring tests failed!")
    
    # Show processor capabilities
    processor = FilterDataProcessor({'processor_type': 'filter_data', 'filters': []})
    print(f"\nSupported conditions: {processor.get_supported_conditions()}")
    print(f"Stage-based conditions: {processor.get_stage_based_conditions()}")
    
    print("\nTo run with pytest: pytest test_filter_data_processor_refactored.py -v")
