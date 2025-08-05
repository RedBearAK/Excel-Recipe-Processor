"""
Minimal debug test to isolate the case insensitive lookup issue.
"""

import pandas as pd
from excel_recipe_processor.processors.lookup_data_processor import LookupDataProcessor

def debug_case_insensitive():
    """Debug the case insensitive lookup issue."""
    
    # Test 1: Case sensitive with matches (should this create suffixes?)
    print("=== Test 1: Case Sensitive WITH Matches ===")
    main_data = pd.DataFrame({
        'ID': [1, 2],
        'Code': ['ABC', 'DEF']  # Exact matches
    })
    
    lookup_data = pd.DataFrame({
        'Code': ['ABC', 'DEF'],  # Exact matches
        'Name': ['Alpha', 'Beta']
    })
    
    config_sensitive = {
        'processor_type': 'lookup_data',
        'lookup_source': lookup_data,
        'lookup_key': 'Code',
        'source_key': 'Code',
        'lookup_columns': ['Name'],
        'case_sensitive': True
    }
    
    try:
        processor_sensitive = LookupDataProcessor(config_sensitive)
        result_sensitive = processor_sensitive.execute(main_data)
        print("✓ Case sensitive WITH matches:")
        print(result_sensitive)
        print(f"Columns: {list(result_sensitive.columns)}")
    except Exception as e:
        print(f"✗ Case sensitive failed: {e}")
    
    # Test 2: Case insensitive (this creates suffixes)
    print("\n=== Test 2: Case Insensitive ===")
    main_data2 = pd.DataFrame({
        'ID': [1, 2],
        'Code': ['abc', 'def']  # lowercase
    })
    
    lookup_data2 = pd.DataFrame({
        'Code': ['ABC', 'DEF'],  # uppercase
        'Name': ['Alpha', 'Beta']
    })
    
    config_insensitive = {
        'processor_type': 'lookup_data',
        'lookup_source': lookup_data2,
        'lookup_key': 'Code',
        'source_key': 'Code',
        'lookup_columns': ['Name'],
        'case_sensitive': False
    }
    
    try:
        processor_insensitive = LookupDataProcessor(config_insensitive)
        result_insensitive = processor_insensitive.execute(main_data2)
        print("✓ Case insensitive:")
        print(result_insensitive)
        print(f"Columns: {list(result_insensitive.columns)}")
    except Exception as e:
        print(f"✗ Case insensitive failed: {e}")

if __name__ == '__main__':
    debug_case_insensitive()
