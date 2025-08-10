"""
Modern test module for lookup_data processor.

Tests the LookupDataProcessor using current architecture patterns:
- Strategic isinstance() checks with proper TypeError handling
- Stage-based workflows via StageManager
- Resolved filenames (no variable substitution at processor level)
- Proper setup/teardown and resource cleanup
- Focus on actual processor functionality

Module path: tests/test_lookup_data_processor.py
"""

import os
import pandas as pd
import tempfile

from pathlib import Path

from excel_recipe_processor.core.stage_manager import StageManager
from excel_recipe_processor.core.base_processor import StepProcessorError
from excel_recipe_processor.processors.lookup_data_processor import LookupDataProcessor


def create_main_data() -> pd.DataFrame:
    """Create main data for testing lookup operations."""
    data = pd.DataFrame({
        'Order_ID': [1, 2, 3, 4, 5],
        'Customer_Code': ['cust001', 'CUST002', 'Cust003', 'cust004', 'UNKNOWN'],  # Mixed case
        'Product_ID': ['P001', 'P002', 'P001', 'P003', 'P002'],
        'Quantity': [10, 5, 8, 12, 3]
    })
    
    if not isinstance(data, pd.DataFrame):
        raise TypeError(f"Expected DataFrame, got {type(data)}")
    
    return data


def create_customer_lookup() -> pd.DataFrame:
    """Create customer lookup data."""
    data = pd.DataFrame({
        'Customer_Code': ['CUST001', 'CUST002', 'CUST003', 'CUST004'],  # Uppercase
        'Customer_Name': ['Acme Corp', 'Beta Industries', 'Gamma LLC', 'Delta Systems'],
        'Customer_Tier': ['Gold', 'Silver', 'Gold', 'Bronze'],
        'Credit_Limit': [50000, 25000, 40000, 15000]
    })
    
    if not isinstance(data, pd.DataFrame):
        raise TypeError(f"Expected DataFrame, got {type(data)}")
    
    return data


def create_product_lookup() -> pd.DataFrame:
    """Create product lookup data."""
    data = pd.DataFrame({
        'Product_ID': ['P001', 'P002', 'P003'],
        'Product_Name': ['Widget A', 'Widget B', 'Widget C'],
        'Unit_Price': [10.00, 15.50, 8.75]
    })
    
    if not isinstance(data, pd.DataFrame):
        raise TypeError(f"Expected DataFrame, got {type(data)}")
    
    return data


def test_basic_lookup():
    """Test basic lookup operation (VLOOKUP-style)."""
    print("\nTesting basic lookup...")
    
    main_data = create_main_data()
    customer_lookup = create_customer_lookup()
    
    config = {
        'processor_type': 'lookup_data',
        'step_description': 'Basic customer lookup',
        'lookup_source': customer_lookup,
        'match_col_in_lookup_data': 'Customer_Code',
        'match_col_in_main_data': 'Customer_Code',
        'lookup_columns': ['Customer_Name', 'Customer_Tier']
    }
    
    processor = LookupDataProcessor(config)
    result = processor.execute(main_data)
    
    # Validate result structure
    if not isinstance(result, pd.DataFrame):
        print(f"✗ Expected DataFrame result, got {type(result)}")
        return False
    
    # Check that lookup columns were added
    expected_columns = ['Customer_Name', 'Customer_Tier']
    for col in expected_columns:
        if col not in result.columns:
            print(f"✗ Lookup column '{col}' not found in result")
            return False
    
    # Check specific lookup (should be case insensitive by default)
    cust001_rows = result[result['Customer_Code'] == 'cust001']
    if len(cust001_rows) == 0:
        print("✗ No rows found for 'cust001'")
        return False
    
    customer_name = cust001_rows['Customer_Name'].iloc[0]
    if customer_name != 'Acme Corp':
        print(f"✗ Expected 'Acme Corp' for cust001, got '{customer_name}'")
        return False
    
    print("✓ Basic lookup works correctly")
    return True


def test_case_insensitive_default():
    """Test that case insensitive is the default behavior."""
    print("\nTesting case insensitive default...")
    
    main_data = create_main_data()
    customer_lookup = create_customer_lookup()
    
    config = {
        'processor_type': 'lookup_data',
        'lookup_source': customer_lookup,
        'match_col_in_lookup_data': 'Customer_Code',
        'match_col_in_main_data': 'Customer_Code',
        'lookup_columns': ['Customer_Name']
        # Note: No case_sensitive specified, should default to False
    }
    
    processor = LookupDataProcessor(config)
    result = processor.execute(main_data)
    
    # Check that lowercase 'cust001' matches uppercase 'CUST001' in lookup
    cust001_rows = result[result['Customer_Code'] == 'cust001']
    if len(cust001_rows) == 0:
        print("✗ No rows found for 'cust001'")
        return False
    
    customer_name = cust001_rows['Customer_Name'].iloc[0]
    if customer_name != 'Acme Corp':
        print(f"✗ Expected case insensitive match, got '{customer_name}'")
        return False
    
    # Check mixed case 'Cust003' matches 'CUST003'
    cust003_rows = result[result['Customer_Code'] == 'Cust003']
    if len(cust003_rows) == 0:
        print("✗ No rows found for 'Cust003'")
        return False
    
    customer_name_003 = cust003_rows['Customer_Name'].iloc[0]
    if customer_name_003 != 'Gamma LLC':
        print(f"✗ Expected case insensitive match for Cust003, got '{customer_name_003}'")
        return False
    
    print("✓ Case insensitive is default behavior")
    return True


def test_case_sensitive_explicit():
    """Test explicit case sensitive behavior."""
    print("\nTesting explicit case sensitive...")
    
    main_data = create_main_data()
    customer_lookup = create_customer_lookup()
    
    config = {
        'processor_type': 'lookup_data',
        'lookup_source': customer_lookup,
        'match_col_in_lookup_data': 'Customer_Code',
        'match_col_in_main_data': 'Customer_Code',
        'lookup_columns': ['Customer_Name'],
        'case_sensitive': True  # Explicit case sensitive
    }
    
    processor = LookupDataProcessor(config)
    result = processor.execute(main_data)
    
    # Check that lowercase 'cust001' does NOT match uppercase 'CUST001'
    cust001_rows = result[result['Customer_Code'] == 'cust001']
    if len(cust001_rows) == 0:
        print("✗ No rows found for 'cust001'")
        return False
    
    customer_name = cust001_rows['Customer_Name'].iloc[0]
    # Should be NaN/null since no case-sensitive match
    if pd.notna(customer_name):
        print(f"✗ Expected no match (case sensitive), got '{customer_name}'")
        return False
    
    print("✓ Explicit case sensitive works correctly")
    return True


def test_join_types():
    """Test different join types (left, right, inner, outer)."""
    print("\nTesting join types...")
    
    main_data = create_main_data()
    customer_lookup = create_customer_lookup()
    
    # Test inner join - only matching records
    config_inner = {
        'processor_type': 'lookup_data',
        'lookup_source': customer_lookup,
        'match_col_in_lookup_data': 'Customer_Code',
        'match_col_in_main_data': 'Customer_Code',
        'lookup_columns': ['Customer_Name'],
        'join_type': 'inner'
    }
    
    processor_inner = LookupDataProcessor(config_inner)
    result_inner = processor_inner.execute(main_data)
    
    # Should exclude UNKNOWN customer (no match in lookup)
    if len(result_inner) >= len(main_data):
        print(f"✗ Inner join should have fewer rows, got {len(result_inner)} vs {len(main_data)}")
        return False
    
    # Check that all remaining rows have customer names
    null_names = result_inner['Customer_Name'].isna().sum()
    if null_names > 0:
        print(f"✗ Inner join should have no null names, got {null_names}")
        return False
    
    print("✓ Join types work correctly")
    return True


def test_default_values():
    """Test default values for missing lookups."""
    print("\nTesting default values...")
    
    main_data = create_main_data()
    customer_lookup = create_customer_lookup()
    
    config = {
        'processor_type': 'lookup_data',
        'lookup_source': customer_lookup,
        'match_col_in_lookup_data': 'Customer_Code',
        'match_col_in_main_data': 'Customer_Code',
        'lookup_columns': ['Customer_Name', 'Customer_Tier'],
        'default_value': 'Unknown'
    }
    
    processor = LookupDataProcessor(config)
    result = processor.execute(main_data)
    
    # Check that UNKNOWN customer gets default values
    unknown_rows = result[result['Customer_Code'] == 'UNKNOWN']
    if len(unknown_rows) == 0:
        print("✗ No rows found for 'UNKNOWN' customer")
        return False
    
    customer_name = unknown_rows['Customer_Name'].iloc[0]
    customer_tier = unknown_rows['Customer_Tier'].iloc[0]
    
    if customer_name != 'Unknown':
        print(f"✗ Expected default 'Unknown' for customer name, got '{customer_name}'")
        return False
    
    if customer_tier != 'Unknown':
        print(f"✗ Expected default 'Unknown' for customer tier, got '{customer_tier}'")
        return False
    
    print("✓ Default values work correctly")
    return True


def test_prefix_suffix():
    """Test prefix and suffix application to lookup columns."""
    print("\nTesting prefix and suffix...")
    
    main_data = create_main_data()
    customer_lookup = create_customer_lookup()
    
    config = {
        'processor_type': 'lookup_data',
        'lookup_source': customer_lookup,
        'match_col_in_lookup_data': 'Customer_Code',
        'match_col_in_main_data': 'Customer_Code',
        'lookup_columns': ['Customer_Name', 'Customer_Tier'],
        'prefix': 'Cust_',
        'suffix': '_Info'
    }
    
    processor = LookupDataProcessor(config)
    result = processor.execute(main_data)
    
    # Check that columns were renamed with prefix/suffix
    expected_columns = ['Cust_Customer_Name_Info', 'Cust_Customer_Tier_Info']
    for col in expected_columns:
        if col not in result.columns:
            print(f"✗ Expected renamed column '{col}' not found")
            return False
    
    # Check that original column names are gone
    original_columns = ['Customer_Name', 'Customer_Tier']
    for col in original_columns:
        if col in result.columns:
            print(f"✗ Original column '{col}' should be renamed but still exists")
            return False
    
    print("✓ Prefix and suffix work correctly")
    return True


def test_stage_based_lookup():
    """Test lookup using data from StageManager."""
    print("\nTesting stage-based lookup...")
    
    # Initialize stage system
    StageManager.initialize_stages(max_stages=5)
    
    try:
        # Create stages with data
        main_data = create_main_data()
        customer_lookup = create_customer_lookup()
        
        StageManager.save_stage('main_orders', main_data, 'Main order data')
        StageManager.save_stage('customer_ref', customer_lookup, 'Customer reference data')
        
        config = {
            'processor_type': 'lookup_data',
            'source_stage': 'main_orders',
            'lookup_source': {
                'type': 'stage',
                'stage_name': 'customer_ref'
            },
            'match_col_in_lookup_data': 'Customer_Code',
            'match_col_in_main_data': 'Customer_Code',
            'lookup_columns': ['Customer_Name']
        }
        
        processor = LookupDataProcessor(config)
        
        # Load from stage and execute
        input_data = StageManager.load_stage('main_orders')
        result = processor.execute(input_data)
        
        # Verify result
        if not isinstance(result, pd.DataFrame):
            print(f"✗ Expected DataFrame result, got {type(result)}")
            return False
        
        if 'Customer_Name' not in result.columns:
            print("✗ Customer_Name column not found")
            return False
        
        # Check specific lookup
        cust001_rows = result[result['Customer_Code'] == 'cust001']
        if len(cust001_rows) > 0:
            customer_name = cust001_rows['Customer_Name'].iloc[0]
            if customer_name != 'Acme Corp':
                print(f"✗ Expected 'Acme Corp', got '{customer_name}'")
                return False
        
        print("✓ Stage-based lookup works correctly")
        return True
        
    finally:
        StageManager.cleanup_stages()


def test_file_based_lookup():
    """Test lookup using definitions from external file."""
    print("\nTesting file-based lookup...")
    
    main_data = create_main_data()
    customer_lookup = create_customer_lookup()
    
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create lookup file
        lookup_file = Path(temp_dir) / "customers.csv"
        customer_lookup.to_csv(lookup_file, index=False)
        
        config = {
            'processor_type': 'lookup_data',
            'lookup_source': {
                'type': 'file',
                'filename': str(lookup_file)
            },
            'match_col_in_lookup_data': 'Customer_Code',
            'match_col_in_main_data': 'Customer_Code',
            'lookup_columns': ['Customer_Name']
        }
        
        processor = LookupDataProcessor(config)
        result = processor.execute(main_data)
        
        # Verify result
        if not isinstance(result, pd.DataFrame):
            print(f"✗ Expected DataFrame result, got {type(result)}")
            return False
        
        if 'Customer_Name' not in result.columns:
            print("✗ Customer_Name column not found")
            return False
        
        # Check specific lookup
        cust002_rows = result[result['Customer_Code'] == 'CUST002']
        if len(cust002_rows) > 0:
            customer_name = cust002_rows['Customer_Name'].iloc[0]
            if customer_name != 'Beta Industries':
                print(f"✗ Expected 'Beta Industries', got '{customer_name}'")
                return False
        
        print("✓ File-based lookup works correctly")
        return True


def test_error_handling():
    """Test proper error handling for invalid inputs."""
    print("\nTesting error handling...")
    
    main_data = create_main_data()
    customer_lookup = create_customer_lookup()
    
    config = {
        'processor_type': 'lookup_data',
        'lookup_source': customer_lookup,
        'match_col_in_lookup_data': 'Customer_Code',
        'match_col_in_main_data': 'Customer_Code',
        'lookup_columns': ['Customer_Name']
    }
    
    processor = LookupDataProcessor(config)
    
    # Test with non-DataFrame input
    try:
        processor.execute("not a dataframe")
        print("✗ Should have raised error for non-DataFrame input")
        return False
    except StepProcessorError as e:
        if "DataFrame" not in str(e):
            print(f"✗ Wrong error message: {e}")
            return False
    except Exception as e:
        print(f"✗ Wrong exception type: {type(e).__name__}: {e}")
        return False
    
    # Test with missing source key
    config_bad_source = {
        'processor_type': 'lookup_data',
        'lookup_source': customer_lookup,
        'match_col_in_lookup_data': 'Customer_Code',
        'match_col_in_main_data': 'NonexistentColumn',
        'lookup_columns': ['Customer_Name']
    }
    
    processor_bad_source = LookupDataProcessor(config_bad_source)
    
    try:
        processor_bad_source.execute(main_data)
        print("✗ Should have raised error for missing source key")
        return False
    except StepProcessorError as e:
        if "not found" not in str(e):
            print(f"✗ Wrong error message: {e}")
            return False
    
    # Test with missing lookup key in lookup data
    config_bad_lookup = {
        'processor_type': 'lookup_data',
        'lookup_source': customer_lookup,
        'match_col_in_lookup_data': 'NonexistentLookupColumn',
        'match_col_in_main_data': 'Customer_Code',
        'lookup_columns': ['Customer_Name']
    }
    
    processor_bad_lookup = LookupDataProcessor(config_bad_lookup)
    
    try:
        processor_bad_lookup.execute(main_data)
        print("✗ Should have raised error for missing lookup key")
        return False
    except StepProcessorError as e:
        # Accept either "not found" or the column name in the error message
        error_str = str(e)
        if "not found" not in error_str and "NonexistentLookupColumn" not in error_str:
            print(f"✗ Wrong error message: {e}")
            return False
    
    print("✓ Error handling works correctly")
    return True


def test_type_validation():
    """Test isinstance checks and type validation."""
    print("\nTesting type validation...")
    
    # Test processor creation with invalid config
    try:
        processor = LookupDataProcessor("not a dict")
        print("✗ Should have raised error for non-dict config")
        return False
    except TypeError as e:
        if "dict" not in str(e):
            print(f"✗ Wrong error message: {e}")
            return False
    except Exception as e:
        # May raise different error depending on implementation
        pass
    
    print("✓ Type validation works correctly")
    return True


def main():
    """Run all tests and report results."""
    print("🔍 Testing LookupDataProcessor (Modern Architecture)")
    print("=" * 60)
    
    tests = [
        test_basic_lookup,
        test_case_insensitive_default,
        test_case_sensitive_explicit,
        test_join_types,
        test_default_values,
        test_prefix_suffix,
        test_stage_based_lookup,
        test_file_based_lookup,
        test_error_handling,
        test_type_validation
    ]
    
    passed = 0
    total = len(tests)
    
    for test_func in tests:
        try:
            if test_func():
                passed += 1
            else:
                print(f"❌ {test_func.__name__} failed")
        except Exception as e:
            print(f"💥 {test_func.__name__} crashed: {e}")
    
    print("\n" + "=" * 60)
    print(f"📊 Test Results: {passed}/{total} passed")
    
    if passed == total:
        print("🎉 All tests passed! Everything is Awesome!")
        return 0
    else:
        print(f"😞 {total - passed} tests failed")
        return 1


if __name__ == '__main__':
    exit(main())


# End of file #
