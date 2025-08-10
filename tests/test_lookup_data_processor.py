"""
Tests for the clean lookup_data processor.

tests/test_lookup_data_processor.py

Focuses on testing the core functionality without the complexity
of the original implementation.
"""

import pandas as pd
import tempfile

from pathlib import Path

from excel_recipe_processor.core.stage_manager import StageManager
from excel_recipe_processor.core.base_processor import StepProcessorError
from excel_recipe_processor.processors.lookup_data_processor import LookupDataProcessor


def create_main_data():
    """Create sample main data for testing."""
    return pd.DataFrame({
        'Order_ID': [1001, 1002, 1003, 1004],
        'Customer_ID': ['CUST001', 'CUST002', 'CUST003', 'UNKNOWN'],
        'Product_SKU': ['SKU-A001', 'SKU-B002', 'SKU-C003', 'SKU-A001'],
        'Amount': [150.00, 75.50, 200.00, 300.00]
    })


def create_customer_lookup():
    """Create customer lookup data."""
    return pd.DataFrame({
        'Customer_ID': ['CUST001', 'CUST002', 'CUST003'],
        'Customer_Name': ['Acme Corp', 'Beta Industries', 'Gamma LLC'],
        'Region': ['West', 'East', 'Central'],
        'Tier': ['Premium', 'Standard', 'Premium']
    })


def create_messy_key_data():
    """Create data with type/format issues for normalization testing."""
    return pd.DataFrame({
        'Order_ID': [1, 2, 3, 4],
        'Customer_ID': [1001, 1002.0, '1003', ' 1004 '],  # Mixed types and whitespace
        'Amount': [100, 200, 300, 400]
    })


def create_clean_lookup():
    """Create lookup data with clean string keys."""
    return pd.DataFrame({
        'Customer_ID': ['1001', '1002', '1003', '1004'],
        'Name': ['Alice', 'Bob', 'Charlie', 'David'],
        'Status': ['Active', 'Active', 'Inactive', 'Active']
    })


def test_basic_lookup():
    """Test basic lookup functionality."""
    print("\nTesting basic lookup...")
    
    StageManager.initialize_stages()
    
    try:
        # Set up test data in stages
        main_data = create_main_data()
        customer_lookup = create_customer_lookup()
        
        StageManager.save_stage('orders', main_data, 'Order data')
        StageManager.save_stage('customers', customer_lookup, 'Customer lookup')
        
        # Configure lookup
        config = {
            'processor_type': 'lookup_data',
            'step_description': 'Test basic lookup',
            'lookup_stage': 'customers',
            'match_col_in_lookup_data': 'Customer_ID',
            'match_col_in_main_data': 'Customer_ID',
            'lookup_columns': ['Customer_Name', 'Region']
        }
        
        processor = LookupDataProcessor(config)
        
        # Debug: print config to see what the processor received
        print(f"DEBUG: Processor config keys: {list(processor._config.keys()) if hasattr(processor, '_config') else 'No _config attr'}")
        print(f"DEBUG: lookup_stage value: '{processor.get_config_value('lookup_stage')}'")
        
        result = processor.execute(main_data)
        
        # Verify results
        if len(result) != len(main_data):
            print(f"✗ Row count changed: {len(main_data)} → {len(result)}")
            return False
        
        if 'Customer_Name' not in result.columns:
            print("✗ Customer_Name column not added")
            return False
        
        # Check specific lookup
        acme_rows = result[result['Customer_ID'] == 'CUST001']
        if len(acme_rows) > 0 and acme_rows['Customer_Name'].iloc[0] == 'Acme Corp':
            print("✓ Basic lookup working correctly")
            return True
        else:
            print("✗ Lookup result incorrect")
            return False
            
    finally:
        StageManager.cleanup_stages()


def test_key_normalization():
    """Test automatic key normalization."""
    print("\nTesting key normalization...")
    
    StageManager.initialize_stages()
    
    try:
        # Set up messy data
        main_data = create_messy_key_data()
        lookup_data = create_clean_lookup()
        
        StageManager.save_stage('messy_orders', main_data, 'Orders with messy keys')
        StageManager.save_stage('clean_lookup', lookup_data, 'Clean lookup data')
        
        config = {
            'processor_type': 'lookup_data',
            'step_description': 'Test key normalization',
            'lookup_stage': 'clean_lookup',
            'match_col_in_lookup_data': 'Customer_ID',
            'match_col_in_main_data': 'Customer_ID',
            'lookup_columns': ['Name', 'Status'],
            'normalize_keys': True
        }
        
        processor = LookupDataProcessor(config)
        result = processor.execute(main_data)
        
        # Check that normalization worked
        successful_lookups = result['Name'].notna().sum()
        if successful_lookups >= 3:  # Should match most/all rows
            print(f"✓ Key normalization working: {successful_lookups} successful matches")
            return True
        else:
            print(f"✗ Key normalization failed: only {successful_lookups} matches")
            return False
            
    finally:
        StageManager.cleanup_stages()


def test_column_naming():
    """Test prefix and suffix functionality."""
    print("\nTesting column naming...")
    
    StageManager.initialize_stages()
    
    try:
        main_data = create_main_data()
        customer_lookup = create_customer_lookup()
        
        StageManager.save_stage('orders', main_data, 'Order data')
        StageManager.save_stage('customers', customer_lookup, 'Customer lookup')
        
        config = {
            'processor_type': 'lookup_data',
            'step_description': 'Test column naming',
            'lookup_stage': 'customers',
            'match_col_in_lookup_data': 'Customer_ID',
            'match_col_in_main_data': 'Customer_ID',
            'lookup_columns': ['Customer_Name', 'Region'],
            'prefix': 'Cust_',
            'suffix': '_Info'
        }
        
        processor = LookupDataProcessor(config)
        result = processor.execute(main_data)
        
        # Check renamed columns
        expected_cols = ['Cust_Customer_Name_Info', 'Cust_Region_Info']
        if all(col in result.columns for col in expected_cols):
            print("✓ Column naming working correctly")
            return True
        else:
            print(f"✗ Expected columns {expected_cols}, got {list(result.columns)}")
            return False
            
    finally:
        StageManager.cleanup_stages()


def test_default_values():
    """Test default values for missing matches."""
    print("\nTesting default values...")
    
    StageManager.initialize_stages()
    
    try:
        main_data = create_main_data()
        customer_lookup = create_customer_lookup()  # Missing 'UNKNOWN' customer
        
        StageManager.save_stage('orders', main_data, 'Order data')
        StageManager.save_stage('customers', customer_lookup, 'Customer lookup')
        
        config = {
            'processor_type': 'lookup_data',
            'step_description': 'Test default values',
            'lookup_stage': 'customers',                    # Fixed
            'match_col_in_lookup_data': 'Customer_ID',      # Fixed  
            'match_col_in_main_data': 'Customer_ID',        # Fixed
            'lookup_columns': ['Customer_Name', 'Region'],
            'default_values': {
                'Customer_Name': 'Unknown Customer',
                'Region': 'Unassigned'
            }
        }
        
        processor = LookupDataProcessor(config)
        result = processor.execute(main_data)
        
        # Check that UNKNOWN customer got default values
        unknown_rows = result[result['Customer_ID'] == 'UNKNOWN']
        if len(unknown_rows) > 0:
            name = unknown_rows['Customer_Name'].iloc[0]
            region = unknown_rows['Region'].iloc[0]
            if name == 'Unknown Customer' and region == 'Unassigned':
                print("✓ Default values working correctly")
                return True
        
        print("✗ Default values not applied correctly")
        return False
        
    finally:
        StageManager.cleanup_stages()


def test_join_types():
    """Test different join types."""
    print("\nTesting join types...")
    
    StageManager.initialize_stages()
    
    try:
        main_data = create_main_data()
        customer_lookup = create_customer_lookup()
        
        StageManager.save_stage('orders', main_data, 'Order data')
        StageManager.save_stage('customers', customer_lookup, 'Customer lookup')
        
        # Test inner join - should exclude UNKNOWN customer
        config = {
            'processor_type': 'lookup_data',
            'step_description': 'Test inner join',
            'lookup_stage': 'customers',
            'match_col_in_lookup_data': 'Customer_ID',
            'match_col_in_main_data': 'Customer_ID',
            'lookup_columns': ['Customer_Name'],
            'join_type': 'inner'
        }
        
        processor = LookupDataProcessor(config)
        result = processor.execute(main_data)
        
        # Should have fewer rows (UNKNOWN customer filtered out)
        if len(result) < len(main_data):
            print(f"✓ Inner join working: {len(main_data)} → {len(result)} rows")
            return True
        else:
            print(f"✗ Inner join failed: kept all {len(result)} rows")
            return False
            
    finally:
        StageManager.cleanup_stages()


def test_error_handling():
    """Test error handling for invalid configurations."""
    print("\nTesting error handling...")
    
    StageManager.initialize_stages()
    
    try:
        main_data = create_main_data()
        
        # Test missing stage
        config = {
            'processor_type': 'lookup_data',
            'lookup_stage': 'nonexistent_stage',
            'match_col_in_lookup_data': 'Customer_ID',
            'match_col_in_main_data': 'Customer_ID',
            'lookup_columns': ['Customer_Name']
        }
        
        processor = LookupDataProcessor(config)
        
        try:
            processor.execute(main_data)
            print("✗ Should have failed with missing stage")
            return False
        except StepProcessorError as e:
            if "not found" in str(e):
                print("✓ Error handling for missing stage working")
            else:
                print(f"✗ Wrong error message: {e}")
                return False
        
        # Test missing column
        StageManager.save_stage('customers', create_customer_lookup(), 'Customer lookup')
        
        config['lookup_stage'] = 'customers'
        config['match_col_in_main_data'] = 'NonexistentColumn'
        
        try:
            processor.execute(main_data)
            print("✗ Should have failed with missing column")
            return False
        except StepProcessorError as e:
            if "not found" in str(e):
                print("✓ Error handling for missing column working")
                return True
            else:
                print(f"✗ Wrong error message: {e}")
                return False
                
    finally:
        StageManager.cleanup_stages()


def main():
    """Run all tests."""
    print("🔍 Testing Clean Lookup Data Processor")
    print("=" * 50)
    
    tests = [
        test_basic_lookup,
        test_key_normalization,
        test_column_naming,
        test_default_values,
        test_join_types,
        test_error_handling
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
    
    print("\n" + "=" * 50)
    print(f"📊 Test Results: {passed}/{total} passed")
    
    if passed == total:
        print("🎉 All tests passed! Clean lookup processor is working!")
        print("\n🎯 Key improvements over original:")
        print("  ✓ Stage-only workflow (no file handling complexity)")
        print("  ✓ Smart key normalization (handles real matching problems)")
        print("  ✓ Clean pandas merge (no temporary column mess)")
        print("  ✓ Simple configuration (clear parameter names)")
        print("  ✓ Focused functionality (does one thing well)")
        print(f"  ✓ Much smaller (~8-12K vs ~40K lines)")
        return 0
    else:
        print(f"😞 {total - passed} tests failed")
        return 1


if __name__ == '__main__':
    exit(main())


# End of file #
