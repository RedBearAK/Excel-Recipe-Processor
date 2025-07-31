"""
Test the MergeDataProcessor functionality.
"""

import os
import pandas as pd
import tempfile

from excel_recipe_processor.core.stage_manager import StageManager
from excel_recipe_processor.core.base_processor import StepProcessorError
from excel_recipe_processor.processors.merge_data_processor import MergeDataProcessor


def create_sample_orders_data():
    """Create sample orders data for testing."""
    return pd.DataFrame({
        'Order_ID': [1001, 1002, 1003, 1004, 1005],
        'Customer_ID': ['C001', 'C002', 'C001', 'C003', 'C002'],
        'Product_Code': ['P001', 'P002', 'P003', 'P001', 'P004'],
        'Quantity': [10, 5, 3, 7, 2],
        'Order_Date': ['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05']
    })


def create_sample_customer_data():
    """Create sample customer data for merging."""
    return pd.DataFrame({
        'Customer_ID': ['C001', 'C002', 'C003', 'C004'],
        'Customer_Name': ['Alice Corp', 'Bob Industries', 'Charlie LLC', 'Delta Inc'],
        'Region': ['West', 'East', 'North', 'South'],
        'Customer_Type': ['Premium', 'Standard', 'Premium', 'Standard']
    })


def create_sample_product_data():
    """Create sample product data for merging."""
    return pd.DataFrame({
        'Product_Code': ['P001', 'P002', 'P003', 'P004', 'P005'],
        'Product_Name': ['Widget A', 'Widget B', 'Gadget X', 'Tool Y', 'Device Z'],
        'Category': ['Electronics', 'Electronics', 'Hardware', 'Tools', 'Electronics'],
        'Price': [10.99, 15.50, 8.75, 22.00, 45.99]
    })


def test_dictionary_merge():
    """Test merging with dictionary data source."""
    
    print("Testing dictionary merge...")
    
    orders_df = create_sample_orders_data()
    print(f"✓ Created orders data: {len(orders_df)} rows")
    
    # Test dictionary merge with customer data
    step_config = {
        'step_description': 'Add customer info from dictionary',
        'processor_type': 'merge_data',
        'merge_source': {
            'type': 'dictionary',
            'data': {
                'C001': {'Customer_Name': 'Alice Corp', 'Region': 'West'},
                'C002': {'Customer_Name': 'Bob Industries', 'Region': 'East'},
                'C003': {'Customer_Name': 'Charlie LLC', 'Region': 'North'}
            }
        },
        'left_key': 'Customer_ID',
        'right_key': 'key',
        'join_type': 'left'
    }
    
    processor = MergeDataProcessor(step_config)
    result = processor.execute(orders_df)
    
    print(f"✓ Merge result: {len(result)} rows, {len(result.columns)} columns")
    print(f"✓ Result columns: {list(result.columns)}")
    
    # Check that customer data was added
    has_customer_name = 'Customer_Name' in result.columns
    has_region = 'Region' in result.columns
    all_orders_preserved = len(result) == len(orders_df)
    
    if has_customer_name and has_region and all_orders_preserved:
        print("✓ Dictionary merge worked correctly")
        return True
    else:
        print("✗ Dictionary merge failed")
        return False


def test_different_join_types():
    """Test different join types (left, right, inner, outer)."""
    
    print("\nTesting different join types...")
    
    # Create test data with some missing matches
    # Note: Using int ID in left_df but string keys in dictionary (as would come from YAML)
    # The merge processor should handle this data type conversion automatically
    left_df = pd.DataFrame({
        'ID': [1, 2, 3, 4],
        'Value_Left': ['A', 'B', 'C', 'D']
    })
    
    right_data = {
        'type': 'dictionary',
        'data': {
            '2': {'Value_Right': 'X'},  # String keys (as they would come from YAML)
            '3': {'Value_Right': 'Y'},
            '5': {'Value_Right': 'Z'}  # Not in left
        }
    }
    
    join_tests = [
        ('left', 4, "All left rows preserved"),
        ('inner', 2, "Only matching rows"),
        ('outer', 5, "All rows from both sides")
    ]
    
    for join_type, expected_rows, description in join_tests:
        step_config = {
            'processor_type': 'merge_data',
            'step_description': f'Test {join_type} join',
            'merge_source': right_data,
            'left_key': 'ID',
            'right_key': 'key',
            'join_type': join_type
        }
        
        processor = MergeDataProcessor(step_config)
        result = processor.execute(left_df)
        
        actual_rows = len(result)
        print(f"✓ {join_type} join: {actual_rows} rows ({description})")
        
        if actual_rows == expected_rows:
            print(f"  ✓ {join_type} join worked correctly")
        else:
            print(f"  ✗ {join_type} join failed: expected {expected_rows}, got {actual_rows}")
            return False
    
    return True


def test_csv_file_merge():
    """Test merging with CSV file (simulated with temp file)."""
    
    print("\nTesting CSV file merge...")
    
    orders_df = create_sample_orders_data()
    customer_df = create_sample_customer_data()
    
    # Create temporary CSV file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as temp_file:
        customer_df.to_csv(temp_file.name, index=False)
        temp_csv_path = temp_file.name
    
    try:
        step_config = {
            'processor_type': 'merge_data',
            'step_description': 'Merge with customer CSV',
            'merge_source': {
                'type': 'csv',
                'path': temp_csv_path
            },
            'left_key': 'Customer_ID',
            'right_key': 'Customer_ID',
            'join_type': 'left'
        }
        
        processor = MergeDataProcessor(step_config)
        result = processor.execute(orders_df)
        
        print(f"✓ CSV merge result: {len(result)} rows, {len(result.columns)} columns")
        
        # Check that customer data was merged
        has_customer_name = 'Customer_Name' in result.columns
        has_region = 'Region' in result.columns
        
        if has_customer_name and has_region:
            print("✓ CSV file merge worked correctly")
            return True
        else:
            print("✗ CSV file merge failed")
            return False
            
    finally:
        # Clean up temp file
        os.unlink(temp_csv_path)


def test_excel_file_merge():
    """Test merging with Excel file (simulated with temp file)."""
    
    print("\nTesting Excel file merge...")
    
    orders_df = create_sample_orders_data()
    product_df = create_sample_product_data()
    
    # Create temporary Excel file
    with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as temp_file:
        product_df.to_excel(temp_file.name, index=False)
        temp_excel_path = temp_file.name
    
    try:
        step_config = {
            'processor_type': 'merge_data',
            'step_description': 'Merge with product Excel',
            'merge_source': {
                'type': 'excel',
                'path': temp_excel_path,
                'sheet': 1
            },
            'left_key': 'Product_Code',
            'right_key': 'Product_Code',
            'join_type': 'left'
        }
        
        processor = MergeDataProcessor(step_config)
        result = processor.execute(orders_df)
        
        print(f"✓ Excel merge result: {len(result)} rows, {len(result.columns)} columns")
        
        # Check that product data was merged
        has_product_name = 'Product_Name' in result.columns
        has_category = 'Category' in result.columns
        has_price = 'Price' in result.columns
        
        if has_product_name and has_category and has_price:
            print("✓ Excel file merge worked correctly")
            return True
        else:
            print("✗ Excel file merge failed")
            return False
            
    finally:
        # Clean up temp file
        os.unlink(temp_excel_path)


def test_column_conflict_handling():
    """Test handling of duplicate column names."""
    
    print("\nTesting column conflict handling...")
    
    # Create data with overlapping column names
    left_df = pd.DataFrame({
        'ID': ['A', 'B', 'C'],  # Use string IDs to avoid type conversion issues
        'Name': ['Left1', 'Left2', 'Left3'],
        'Value': [10, 20, 30]
    })
    
    step_config = {
        'processor_type': 'merge_data',
        'step_description': 'Test column conflicts',
        'merge_source': {
            'type': 'dictionary',
            'data': {
                'A': {'Name': 'Right1', 'Value': 100, 'Extra': 'A'},
                'B': {'Name': 'Right2', 'Value': 200, 'Extra': 'B'},
                'C': {'Name': 'Right3', 'Value': 300, 'Extra': 'C'}
            }
        },
        'left_key': 'ID',
        'right_key': 'key',
        'join_type': 'left',
        'suffixes': ('_left', '_right')
    }
    
    processor = MergeDataProcessor(step_config)
    result = processor.execute(left_df)
    
    print(f"✓ Conflict handling result columns: {list(result.columns)}")
    
    # Check that suffixes were applied to conflicting columns
    has_name_left = 'Name_left' in result.columns
    has_name_right = 'Name_right' in result.columns
    has_value_left = 'Value_left' in result.columns
    has_value_right = 'Value_right' in result.columns
    
    if has_name_left and has_name_right and has_value_left and has_value_right:
        print("✓ Column conflict handling worked correctly")
        return True
    else:
        print("✗ Column conflict handling failed")
        return False


def test_real_world_scenario():
    """Test a realistic business scenario."""
    
    print("\nTesting real-world scenario...")
    
    # Simulate order enrichment workflow
    orders_df = pd.DataFrame({
        'Order_ID': [1001, 1002, 1003, 1004, 1005],
        'Customer_ID': ['CUST001', 'CUST002', 'CUST001', 'CUST003', 'CUST002'],
        'Product_SKU': ['SKU-A001', 'SKU-B002', 'SKU-C003', 'SKU-A001', 'SKU-D004'],
        'Quantity': [5, 2, 1, 3, 4],
        'Unit_Price': [19.99, 49.99, 129.99, 19.99, 79.99]
    })
    
    print(f"✓ Orders data: {len(orders_df)} rows")
    
    # Step 1: Add customer information
    customer_config = {
        'processor_type': 'merge_data',
        'step_description': 'Add customer details',
        'merge_source': {
            'type': 'dictionary',
            'data': {
                'CUST001': {'Customer_Name': 'TechCorp Inc', 'Region': 'West Coast', 'Tier': 'Premium'},
                'CUST002': {'Customer_Name': 'DataSys Ltd', 'Region': 'East Coast', 'Tier': 'Standard'},
                'CUST003': {'Customer_Name': 'CloudCo LLC', 'Region': 'Central', 'Tier': 'Premium'}
            }
        },
        'left_key': 'Customer_ID',
        'right_key': 'key',
        'join_type': 'left'
    }
    
    customer_processor = MergeDataProcessor(customer_config)
    enriched_orders = customer_processor.execute(orders_df)
    
    print(f"✓ After customer merge: {len(enriched_orders)} rows, {len(enriched_orders.columns)} columns")
    
    # Step 2: Add product information  
    product_config = {
        'processor_type': 'merge_data',
        'step_description': 'Add product details',
        'merge_source': {
            'type': 'dictionary',
            'data': {
                'SKU-A001': {'Product_Name': 'Wireless Mouse', 'Category': 'Electronics'},
                'SKU-B002': {'Product_Name': 'Keyboard', 'Category': 'Electronics'},
                'SKU-C003': {'Product_Name': 'Monitor', 'Category': 'Displays'},
                'SKU-D004': {'Product_Name': 'Speakers', 'Category': 'Audio'}
            }
        },
        'left_key': 'Product_SKU',
        'right_key': 'key',
        'join_type': 'left'
    }
    
    product_processor = MergeDataProcessor(product_config)
    final_result = product_processor.execute(enriched_orders)
    
    print(f"✓ Final enriched data: {len(final_result)} rows, {len(final_result.columns)} columns")
    print(f"✓ Final columns: {list(final_result.columns)}")
    
    # Validate the enrichment worked
    has_customer_name = 'Customer_Name' in final_result.columns
    has_region = 'Region' in final_result.columns
    has_product_name = 'Product_Name' in final_result.columns
    has_category = 'Category' in final_result.columns
    all_rows_preserved = len(final_result) == len(orders_df)
    
    if has_customer_name and has_region and has_product_name and has_category and all_rows_preserved:
        print("✓ Real-world scenario worked correctly")
        return True
    else:
        print("✗ Real-world scenario failed")
        return False


def test_error_handling():
    """Test error handling for various failure cases."""
    
    print("\nTesting error handling...")
    
    test_df = create_sample_orders_data()
    
    # Test missing merge source
    try:
        bad_config = {
            'processor_type': 'merge_data',
            'step_description': 'Missing merge source',
            'left_key': 'Customer_ID',
            'right_key': 'key'
        }
        processor = MergeDataProcessor(bad_config)
        processor.execute(test_df)
        print("✗ Should have failed with missing merge_source")
    except StepProcessorError as e:
        print(f"✓ Caught expected error: {e}")
    
    # Test missing left key column
    try:
        bad_config = {
            'processor_type': 'merge_data',
            'step_description': 'Missing left key',
            'merge_source': {
                'type': 'dictionary',
                'data': {'test': {'val': 1}}
            },
            'left_key': 'NonExistentColumn',
            'right_key': 'key'
        }
        processor = MergeDataProcessor(bad_config)
        processor.execute(test_df)
        print("✗ Should have failed with missing left key column")
    except StepProcessorError as e:
        print(f"✓ Caught expected error: {e}")
    
    # Test invalid join type
    try:
        bad_config = {
            'processor_type': 'merge_data',
            'step_description': 'Invalid join type',
            'merge_source': {
                'type': 'dictionary',
                'data': {'C001': {'name': 'test'}}
            },
            'left_key': 'Customer_ID',
            'right_key': 'key',
            'join_type': 'invalid_join'
        }
        processor = MergeDataProcessor(bad_config)
        processor.execute(test_df)
        print("✗ Should have failed with invalid join type")
    except StepProcessorError as e:
        print(f"✓ Caught expected error: {e}")
    
    # Test missing file
    try:
        bad_config = {
            'processor_type': 'merge_data',
            'step_description': 'Missing file',
            'merge_source': {
                'type': 'csv',
                'path': '/nonexistent/file.csv'
            },
            'left_key': 'Customer_ID',
            'right_key': 'Customer_ID'
        }
        processor = MergeDataProcessor(bad_config)
        processor.execute(test_df)
        print("✗ Should have failed with missing file")
    except StepProcessorError as e:
        print(f"✓ Caught expected error: {e}")


def test_merge_statistics():
    """Test merge statistics and logging."""
    
    print("\nTesting merge statistics...")
    
    # Create data with some unmatched keys
    orders_df = pd.DataFrame({
        'Customer_ID': ['C001', 'C002', 'C003', 'C004', 'C005'],
        'Order_Value': [100, 200, 150, 300, 75]
    })
    
    step_config = {
        'processor_type': 'merge_data',
        'step_description': 'Test merge statistics',
        'merge_source': {
            'type': 'dictionary',
            'data': {
                'C001': {'Customer_Name': 'Alice'},
                'C002': {'Customer_Name': 'Bob'},
                'C003': {'Customer_Name': 'Charlie'}
                # C004 and C005 not included - will be unmatched
            }
        },
        'left_key': 'Customer_ID',
        'right_key': 'key',
        'join_type': 'left'  # Keep all left rows
    }
    
    processor = MergeDataProcessor(step_config)
    result = processor.execute(orders_df)
    
    print(f"✓ Statistics test result: {len(result)} rows")
    
    # Check that all original rows are preserved (left join)
    matched_customers = result['Customer_Name'].notna().sum()
    total_customers = len(result)
    
    print(f"✓ Customers matched: {matched_customers}/{total_customers}")
    
    if total_customers == 5 and matched_customers == 3:
        print("✓ Merge statistics test worked correctly")
        return True
    else:
        print("✗ Merge statistics test failed")
        return False


def test_stage_merge():
    """Test merging with stage data source."""
    
    print("\nTesting stage merge...")
    
    # Initialize StageManager for testing
    StageManager.initialize_stages(max_stages=5)
    
    try:
        orders_df = create_sample_orders_data()
        customer_df = create_sample_customer_data()
        
        # Save customer data as a stage first
        StageManager.save_stage(
            stage_name="Test Customer Data",
            data=customer_df,
            description="Customer data for merge testing"
        )
        
        # Test stage merge
        step_config = {
            'processor_type': 'merge_data',
            'step_description': 'Merge with customer stage',
            'merge_source': {
                'type': 'stage',
                'stage_name': 'Test Customer Data'
            },
            'left_key': 'Customer_ID',
            'right_key': 'Customer_ID',
            'join_type': 'left'
        }
        
        processor = MergeDataProcessor(step_config)
        result = processor.execute(orders_df)
        
        print(f"✓ Stage merge result: {len(result)} rows, {len(result.columns)} columns")
        
        # Check that customer data was merged
        has_customer_name = 'Customer_Name' in result.columns
        has_region = 'Region' in result.columns
        
        if has_customer_name and has_region:
            print("✓ Stage merge worked correctly")
            return True
        else:
            print("✗ Stage merge failed")
            return False
            
    finally:
        # Clean up stages
        StageManager.cleanup_stages()


def test_stage_merge_errors():
    """Test error handling for stage merge failures."""
    
    print("\nTesting stage merge error handling...")
    
    # Initialize StageManager for testing
    StageManager.initialize_stages(max_stages=5)
    
    try:
        test_df = create_sample_orders_data()
        
        # Test missing stage
        try:
            bad_config = {
                'processor_type': 'merge_data',
                'step_description': 'Missing stage',
                'merge_source': {
                    'type': 'stage',
                    'stage_name': 'NonExistent Stage'
                },
                'left_key': 'Customer_ID',
                'right_key': 'Customer_ID'
            }
            processor = MergeDataProcessor(bad_config)
            processor.execute(test_df)
            print("✗ Should have failed with missing stage")
            return False
        except StepProcessorError as e:
            print(f"✓ Caught expected error for missing stage: {e}")
        
        # Test missing stage_name field
        try:
            bad_config = {
                'processor_type': 'merge_data',
                'step_description': 'Missing stage_name',
                'merge_source': {
                    'type': 'stage'
                    # Missing stage_name
                },
                'left_key': 'Customer_ID',
                'right_key': 'Customer_ID'
            }
            processor = MergeDataProcessor(bad_config)
            processor.execute(test_df)
            print("✗ Should have failed with missing stage_name")
            return False
        except StepProcessorError as e:
            print(f"✓ Caught expected error for missing stage_name: {e}")
        
        print("✓ Stage merge error handling worked correctly")
        return True
        
    finally:
        # Clean up stages
        StageManager.cleanup_stages()


if __name__ == '__main__':
    success = True
    
    success &= test_dictionary_merge()
    success &= test_different_join_types()
    success &= test_csv_file_merge()
    success &= test_excel_file_merge()
    success &= test_column_conflict_handling()
    success &= test_real_world_scenario()
    success &= test_merge_statistics()
    success &= test_stage_merge()
    success &= test_stage_merge_errors()
    test_error_handling()
    
    if success:
        print("\n✓ All merge data processor tests passed!")
    else:
        print("\n✗ Some merge data processor tests failed!")
    
    # Show supported features
    processor = MergeDataProcessor({
        'processor_type': 'merge_data',
        'merge_source': {'type': 'dictionary', 'data': {}},
        'left_key': 'test',
        'right_key': 'test'
    })
    print(f"\nSupported join types: {processor.get_supported_join_types()}")
    print(f"Supported source types: {processor.get_supported_source_types()}")
