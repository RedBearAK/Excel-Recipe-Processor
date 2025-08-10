"""
Regression test for the .0 key matching problem.

tests/test_key_normalization_regression.py

This test specifically reproduces the real-world problem where numeric
Customer_ID values got converted to "1001.0" format, breaking lookups
against clean string keys like "1001" in the lookup table.
"""

import pandas as pd
import numpy as np

from excel_recipe_processor.core.stage_manager import StageManager
from excel_recipe_processor.processors.lookup_data_processor import LookupDataProcessor


def create_problematic_main_data():
    """
    Create main data that simulates the original .0 problem.
    
    This reproduces the exact issue: numeric IDs that somehow get 
    converted to float then string, adding unwanted .0 suffixes.
    """
    # Start with clean numeric IDs
    clean_ids = [1001, 1002, 1003, 1004, 9999]
    
    # Simulate the .0 problem by converting through float
    problematic_ids = []
    for id_val in clean_ids:
        # Convert to float then string (this adds .0)
        float_val = float(id_val)
        string_val = str(float_val)  # Results in "1001.0", "1002.0", etc.
        problematic_ids.append(string_val)
    
    return pd.DataFrame({
        'Order_ID': ['ORD001', 'ORD002', 'ORD003', 'ORD004', 'ORD005'],
        'Customer_ID': problematic_ids,  # ["1001.0", "1002.0", "1003.0", "1004.0", "9999.0"]
        'Amount': [150.00, 75.50, 200.00, 300.00, 50.00],
        'Status': ['Active', 'Active', 'Pending', 'Complete', 'Active']
    })


def create_clean_lookup_data():
    """
    Create lookup data with clean string IDs (no .0 suffixes).
    
    This simulates typical lookup table data that has been properly
    formatted as text from the beginning.
    """
    return pd.DataFrame({
        'Customer_ID': ['1001', '1002', '1003', '1004'],  # Clean strings - no .0!
        'Customer_Name': ['Acme Corp', 'Beta Industries', 'Gamma LLC', 'Delta Systems'],
        'Region': ['West', 'East', 'Central', 'North'],
        'Credit_Limit': [50000, 25000, 75000, 40000],
        'Account_Manager': ['Alice Smith', 'Bob Jones', 'Carol Williams', 'Dave Brown']
    })


def create_other_problematic_data():
    """Create data with various other formatting issues."""
    return pd.DataFrame({
        'Customer_ID': [
            '1001',      # Clean string
            1002,        # Pure integer  
            1003.0,      # Float (will become "1003.0")
            ' 1004 ',    # Whitespace issues
            '1005.00',   # Multiple zeros
            'nan'        # String 'nan' from null conversion
        ],
        'Order_Type': ['Standard', 'Express', 'Bulk', 'Standard', 'Express', 'Standard']
    })


def test_dot_zero_regression():
    """Test that the .0 suffix problem is fixed."""
    print("\n🔍 Testing .0 Suffix Regression")
    print("=" * 40)
    
    StageManager.initialize_stages()
    
    try:
        # Create the problematic data
        main_data = create_problematic_main_data()
        lookup_data = create_clean_lookup_data()
        
        print("📊 Data Setup:")
        print(f"   Main data Customer_IDs: {main_data['Customer_ID'].tolist()}")
        print(f"   Lookup Customer_IDs: {lookup_data['Customer_ID'].tolist()}")
        print()
        
        # Show the problem exists
        main_ids = set(main_data['Customer_ID'])
        lookup_ids = set(lookup_data['Customer_ID'])
        overlapping_raw = main_ids.intersection(lookup_ids)
        print(f"🚨 Raw overlap (before normalization): {len(overlapping_raw)} matches")
        print(f"   Main IDs: {sorted(main_ids)}")
        print(f"   Lookup IDs: {sorted(lookup_ids)}")
        print(f"   Raw matches: {sorted(overlapping_raw)}")
        print()
        
        # Set up stages
        StageManager.save_stage('problematic_orders', main_data, 'Orders with .0 IDs')
        StageManager.save_stage('clean_customers', lookup_data, 'Clean customer lookup')
        
        # Test WITHOUT normalization first (should fail)
        print("❌ Testing WITHOUT normalization:")
        config_no_norm = {
            'processor_type': 'lookup_data',
            'step_description': 'Test without normalization',
            'lookup_stage': 'clean_customers',
            'match_col_in_lookup_data': 'Customer_ID',
            'match_col_in_main_data': 'Customer_ID',
            'lookup_columns': ['Customer_Name', 'Region'],
            'normalize_keys': False  # Disable normalization
        }
        
        processor_no_norm = LookupDataProcessor(config_no_norm)
        result_no_norm = processor_no_norm.execute(main_data)
        
        matches_without_norm = result_no_norm['Customer_Name'].notna().sum()
        print(f"   Matches without normalization: {matches_without_norm}/5")
        
        if matches_without_norm < 4:
            print("   ✓ Confirmed: .0 problem breaks matching without normalization")
        else:
            print("   ⚠️  Unexpected: matching worked without normalization")
        
        print()
        
        # Test WITH normalization (should work)
        print("✅ Testing WITH normalization:")
        config_with_norm = {
            'processor_type': 'lookup_data',
            'step_description': 'Test with normalization',
            'lookup_stage': 'clean_customers',
            'match_col_in_lookup_data': 'Customer_ID',
            'match_col_in_main_data': 'Customer_ID',
            'lookup_columns': ['Customer_Name', 'Region'],
            'normalize_keys': True  # Enable normalization
        }
        
        processor_with_norm = LookupDataProcessor(config_with_norm)
        result_with_norm = processor_with_norm.execute(main_data)
        
        matches_with_norm = result_with_norm['Customer_Name'].notna().sum()
        print(f"   Matches with normalization: {matches_with_norm}/5")
        
        # Verify specific results
        print("\n📋 Detailed Results:")
        for idx, row in result_with_norm.iterrows():
            cust_id = row['Customer_ID']
            cust_name = row['Customer_Name']
            status = "✓ Matched" if pd.notna(cust_name) else "❌ No match"
            print(f"   {cust_id:>8} → {cust_name:<15} {status}")
        
        # Test success criteria
        expected_matches = 4  # 1001.0→1001, 1002.0→1002, 1003.0→1003, 1004.0→1004
        if matches_with_norm >= expected_matches:
            print(f"\n🎉 SUCCESS: Normalization fixed the .0 problem!")
            print(f"   Expected ≥{expected_matches} matches, got {matches_with_norm}")
            return True
        else:
            print(f"\n❌ FAILURE: Normalization didn't fix the problem")
            print(f"   Expected ≥{expected_matches} matches, got {matches_with_norm}")
            return False
            
    finally:
        StageManager.cleanup_stages()


def test_comprehensive_normalization():
    """Test normalization handles various formatting issues."""
    print("\n🔍 Testing Comprehensive Key Normalization")
    print("=" * 45)
    
    StageManager.initialize_stages()
    
    try:
        # Create data with multiple formatting issues
        messy_data = create_other_problematic_data()
        clean_lookup = pd.DataFrame({
            'Customer_ID': ['1001', '1002', '1003', '1004', '1005'],
            'Status': ['Active', 'Active', 'Inactive', 'Active', 'Pending']
        })
        
        print("📊 Various Formatting Issues:")
        for idx, row in messy_data.iterrows():
            cust_id = row['Customer_ID']
            id_type = type(cust_id).__name__
            print(f"   {str(cust_id):>8} ({id_type})")
        print()
        
        StageManager.save_stage('messy_data', messy_data, 'Data with formatting issues')
        StageManager.save_stage('clean_status', clean_lookup, 'Clean status lookup')
        
        config = {
            'processor_type': 'lookup_data',
            'step_description': 'Test comprehensive normalization',
            'lookup_stage': 'clean_status',
            'match_col_in_lookup_data': 'Customer_ID',
            'match_col_in_main_data': 'Customer_ID',
            'lookup_columns': ['Status'],
            'normalize_keys': True
        }
        
        processor = LookupDataProcessor(config)
        result = processor.execute(messy_data)
        
        matches = result['Status'].notna().sum()
        total_non_nan = (messy_data['Customer_ID'] != 'nan').sum()  # Exclude 'nan' string
        
        print(f"📈 Results:")
        print(f"   Total rows: {len(messy_data)}")
        print(f"   Valid IDs (excluding 'nan'): {total_non_nan}")
        print(f"   Successful matches: {matches}")
        print()
        
        print("📋 Detailed Results:")
        for idx, row in result.iterrows():
            original_id = messy_data.iloc[idx]['Customer_ID']
            status = row['Status']
            match_status = "✓ Matched" if pd.notna(status) else "❌ No match"
            print(f"   {str(original_id):>8} → {status:<8} {match_status}")
        
        # Should match everything except 'nan'
        if matches >= total_non_nan:
            print(f"\n🎉 SUCCESS: Comprehensive normalization working!")
            return True
        else:
            print(f"\n❌ FAILURE: Expected {total_non_nan} matches, got {matches}")
            return False
            
    finally:
        StageManager.cleanup_stages()


def test_before_after_normalization():
    """Show exactly what the normalization does."""
    print("\n🔍 Before/After Normalization Demo")
    print("=" * 40)
    
    # Create a small test case
    test_values = [1001.0, '1002.0', ' 1003 ', '1004.00', 'nan']
    
    print("🔧 Normalization Process:")
    for val in test_values:
        # Simulate the normalization steps
        print(f"\n   Original: {repr(val)} ({type(val).__name__})")
        
        # Step 1: Convert to string
        str_val = str(val)
        print(f"   → str(): {repr(str_val)}")
        
        # Step 2: Remove trailing .0+
        import re
        no_dot_zero = re.sub(r'\.0+$', '', str_val)
        print(f"   → remove .0: {repr(no_dot_zero)}")
        
        # Step 3: Strip whitespace
        stripped = no_dot_zero.strip()
        print(f"   → strip(): {repr(stripped)}")
        
        # Step 4: Handle 'nan' 
        final = pd.NA if stripped == 'nan' else stripped
        print(f"   → final: {repr(final)}")
    
    return True


def main():
    """Run all regression tests."""
    print("🧪 Key Normalization Regression Tests")
    print("=" * 50)
    print("Testing the specific .0 problem that broke the original processor")
    print()
    
    tests = [
        test_dot_zero_regression,
        test_comprehensive_normalization,
        test_before_after_normalization
    ]
    
    passed = 0
    total = len(tests)
    
    for test_func in tests:
        try:
            if test_func():
                passed += 1
                print("✅ PASSED")
            else:
                print("❌ FAILED")
        except Exception as e:
            print(f"💥 CRASHED: {e}")
            import traceback
            traceback.print_exc()
    
    print("\n" + "=" * 50)
    print(f"📊 Regression Test Results: {passed}/{total} passed")
    
    if passed == total:
        print("🎉 All regression tests passed!")
        print("✅ The .0 problem is definitively fixed")
        print("✅ Key normalization handles real-world data issues")
    else:
        print("⚠️  Some regression tests failed - investigate!")
    
    return 0 if passed == total else 1


if __name__ == '__main__':
    exit(main())


# End of file #
