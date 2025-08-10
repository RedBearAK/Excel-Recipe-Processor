"""
Isolated test to debug the default values issue.
"""

import pandas as pd
from excel_recipe_processor.core.stage_manager import StageManager
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


def test_debug():
    """Debug the default values test specifically."""
    print("🔍 Debugging Default Values Test")
    print("=" * 40)
    
    StageManager.initialize_stages()
    
    try:
        print("1. Creating test data...")
        main_data = create_main_data()
        customer_lookup = create_customer_lookup()
        print(f"   Main data: {len(main_data)} rows")
        print(f"   Lookup data: {len(customer_lookup)} rows")
        
        print("2. Setting up stages...")
        StageManager.save_stage('orders', main_data, 'Order data')
        StageManager.save_stage('customers', customer_lookup, 'Customer lookup')
        print("   Stages created successfully")
        
        print("3. Creating config...")
        config = {
            'processor_type': 'lookup_data',
            'step_description': 'Test default values',
            'lookup_stage': 'customers',
            'match_col_in_lookup_data': 'Customer_ID',
            'match_col_in_main_data': 'Customer_ID',
            'lookup_columns': ['Customer_Name', 'Region'],
            'default_values': {
                'Customer_Name': 'Unknown Customer',
                'Region': 'Unassigned'
            }
        }
        print(f"   Config created: {list(config.keys())}")
        print(f"   lookup_stage = '{config['lookup_stage']}'")
        print(f"   default_values = {config['default_values']}")
        
        print("4. Creating processor...")
        processor = LookupDataProcessor(config)
        print("   Processor created successfully")
        
        print("5. Testing config retrieval...")
        lookup_stage_val = processor.get_config_value('lookup_stage')
        match_col_lookup_val = processor.get_config_value('match_col_in_lookup_data')
        match_col_main_val = processor.get_config_value('match_col_in_main_data')
        print(f"   lookup_stage: '{lookup_stage_val}' (type: {type(lookup_stage_val)})")
        print(f"   match_col_in_lookup_data: '{match_col_lookup_val}' (type: {type(match_col_lookup_val)})")
        print(f"   match_col_in_main_data: '{match_col_main_val}' (type: {type(match_col_main_val)})")
        
        if lookup_stage_val is None:
            print("   ❌ ISSUE FOUND: lookup_stage is None!")
            print(f"   Processor has _config attr: {hasattr(processor, '_config')}")
            if hasattr(processor, '_config'):
                print(f"   _config contents: {processor._config}")
            else:
                print("   No _config attribute found")
            return False
        
        print("6. Executing processor...")
        result = processor.execute(main_data)
        print(f"   ✅ Success! Result has {len(result)} rows")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        StageManager.cleanup_stages()


if __name__ == '__main__':
    test_debug()


# End of file #
