"""
Test the GroupDataProcessor functionality.
"""

import pandas as pd

from excel_recipe_processor.processors.group_data_processor import GroupDataProcessor
from excel_recipe_processor.processors.base_processor import StepProcessorError


def create_van_report_origins_data():
    """Create sample data with origin cities like the van report."""
    return pd.DataFrame({
        'Product_Origin': [
            'Naknek', 'Kodiak', 'Cordova', 'Dillingham', 'Kodiak West',
            'Valdez', 'Craig', 'Seward', 'False Pass', 'Sitka',
            'Ketchikan', 'Petersburg', 'Wood River', 'Naknek West', 'Unknown City'
        ],
        'Van_Number': [
            'VAN001', 'VAN002', 'VAN003', 'VAN004', 'VAN005',
            'VAN006', 'VAN007', 'VAN008', 'VAN009', 'VAN010', 
            'VAN011', 'VAN012', 'VAN013', 'VAN014', 'VAN015'
        ],
        'Quantity': [100, 150, 200, 75, 125, 180, 90, 110, 160, 85, 140, 95, 120, 130, 50]
    })


def test_van_report_regional_grouping():
    """Test the exact regional grouping from the van report workflow."""
    
    print("Testing van report regional grouping...")
    
    test_df = create_van_report_origins_data()
    print(f"✓ Created test data: {len(test_df)} rows")
    
    # Test the exact grouping from van report Step 16
    step_config = {
        'type': 'group_data',
        'name': 'Group origins by region',
        'source_column': 'Product_Origin',
        'target_column': 'Region',
        'groups': {
            'Bristol Bay': ['Dillingham', 'False Pass', 'Naknek', 'Naknek West', 'Wood River'],
            'Kodiak': ['Kodiak', 'Kodiak West'],
            'PWS': ['Cordova', 'Seward', 'Valdez'],
            'SE': ['Craig', 'Ketchikan', 'Petersburg', 'Sitka']
        },
        'unmatched_action': 'keep_original'
    }
    
    processor = GroupDataProcessor(step_config)
    result = processor.execute(test_df)
    
    print(f"✓ Regional grouping: {len(result)} rows, {len(result.columns)} columns")
    
    # Check that Region column was created
    if 'Region' not in result.columns:
        print("✗ Region column not created")
        return False
    
    # Check specific groupings
    bristol_bay_count = (result['Region'] == 'Bristol Bay').sum()
    kodiak_count = (result['Region'] == 'Kodiak').sum()
    pws_count = (result['Region'] == 'PWS').sum()
    se_count = (result['Region'] == 'SE').sum()
    
    print(f"✓ Bristol Bay: {bristol_bay_count} cities")
    print(f"✓ Kodiak: {kodiak_count} cities")
    print(f"✓ PWS: {pws_count} cities")
    print(f"✓ SE: {se_count} cities")
    
    # Check that unmatched city kept original value
    unknown_rows = result[result['Product_Origin'] == 'Unknown City']
    if len(unknown_rows) > 0:
        unknown_region = unknown_rows.iloc[0]['Region']
        print(f"✓ Unknown city region: '{unknown_region}'")
        
        if unknown_region == 'Unknown City':
            print("✓ Unmatched value handled correctly")
        else:
            print(f"✗ Expected 'Unknown City', got '{unknown_region}'")
            return False
    
    # Verify specific cities are in correct regions
    test_cases = [
        ('Naknek', 'Bristol Bay'),
        ('Kodiak', 'Kodiak'),
        ('Cordova', 'PWS'),
        ('Craig', 'SE')
    ]
    
    for city, expected_region in test_cases:
        city_rows = result[result['Product_Origin'] == city]
        if len(city_rows) > 0:
            actual_region = city_rows.iloc[0]['Region']
            if actual_region == expected_region:
                print(f"✓ {city} → {expected_region}")
            else:
                print(f"✗ {city}: expected {expected_region}, got {actual_region}")
                return False
    
    return True


def test_helper_method_regional_groups():
    """Test the convenience helper method for regional grouping."""
    
    print("\nTesting helper method for regional groups...")
    
    test_df = create_van_report_origins_data()
    
    processor = GroupDataProcessor({'type': 'group_data'})
    result = processor.create_regional_groups(test_df, 'Product_Origin')
    
    print(f"✓ Helper method result: {len(result)} rows, {len(result.columns)} columns")
    
    # Check that region column was created
    region_col = 'Product_Origin_Region'
    if region_col in result.columns:
        print(f"✓ Created column: {region_col}")
        
        # Check regional distribution
        region_counts = result[region_col].value_counts()
        print(f"✓ Regional distribution: {region_counts.to_dict()}")
        
        return True
    else:
        print(f"✗ Expected column {region_col} not found")
        return False


def test_replace_source_column():
    """Test replacing the source column with grouped values."""
    
    print("\nTesting replace source column...")
    
    test_df = create_van_report_origins_data()
    
    step_config = {
        'type': 'group_data',
        'name': 'Replace origins with regions',
        'source_column': 'Product_Origin',
        'groups': {
            'Bristol Bay': ['Dillingham', 'False Pass', 'Naknek', 'Naknek West', 'Wood River'],
            'Kodiak': ['Kodiak', 'Kodiak West'],
            'PWS': ['Cordova', 'Seward', 'Valdez'],
            'SE': ['Craig', 'Ketchikan', 'Petersburg', 'Sitka']
        },
        'replace_source': True,
        'unmatched_action': 'set_default',
        'unmatched_value': 'Other'
    }
    
    processor = GroupDataProcessor(step_config)
    result = processor.execute(test_df)
    
    print(f"✓ Replace source: {len(result)} rows, {len(result.columns)} columns")
    
    # Check that original column names are preserved but values changed
    if 'Product_Origin' in result.columns:
        unique_origins = result['Product_Origin'].unique()
        print(f"✓ Unique values after replacement: {sorted(unique_origins)}")
        
        # Should only have region names and 'Other'
        expected_values = {'Bristol Bay', 'Kodiak', 'PWS', 'SE', 'Other'}
        actual_values = set(unique_origins)
        
        if actual_values.issubset(expected_values):
            print("✓ Source column replacement worked correctly")
            return True
        else:
            print(f"✗ Unexpected values: {actual_values - expected_values}")
            return False
    else:
        print("✗ Product_Origin column not found after replacement")
        return False


def test_case_insensitive_grouping():
    """Test case-insensitive grouping."""
    
    print("\nTesting case-insensitive grouping...")
    
    # Create data with mixed case
    test_df = pd.DataFrame({
        'City': ['naknek', 'KODIAK', 'Cordova', 'dillingham', 'CRAIG'],
        'Value': [100, 200, 150, 75, 125]
    })
    
    step_config = {
        'type': 'group_data',
        'name': 'Case insensitive grouping',
        'source_column': 'City',
        'target_column': 'Region',
        'groups': {
            'Bristol Bay': ['Naknek', 'Dillingham'], 
            'Kodiak': ['Kodiak'],
            'PWS': ['Cordova'],
            'SE': ['Craig']
        },
        'case_sensitive': False,
        'unmatched_action': 'keep_original'
    }
    
    processor = GroupDataProcessor(step_config)
    result = processor.execute(test_df)
    
    print(f"✓ Case insensitive: {len(result)} rows")
    
    # Check that all cities were matched despite case differences
    matched_count = result['Region'].isin(['Bristol Bay', 'Kodiak', 'PWS', 'SE']).sum()
    print(f"✓ Cities matched: {matched_count}/{len(result)}")
    
    if matched_count == len(result):
        print("✓ Case-insensitive grouping worked correctly")
        return True
    else:
        print("✗ Some cities not matched due to case sensitivity")
        return False


def test_unmatched_actions():
    """Test different actions for unmatched values."""
    
    print("\nTesting unmatched value actions...")
    
    test_df = pd.DataFrame({
        'Category': ['A', 'B', 'C', 'Unknown', 'X'],
        'Value': [10, 20, 30, 40, 50]
    })
    
    # Test 'set_default' action
    step_config1 = {
        'type': 'group_data',
        'name': 'Set default for unmatched',
        'source_column': 'Category',
        'target_column': 'Group',
        'groups': {
            'Group1': ['A', 'B'],
            'Group2': ['C']
        },
        'unmatched_action': 'set_default',
        'unmatched_value': 'Other'
    }
    
    processor1 = GroupDataProcessor(step_config1)
    result1 = processor1.execute(test_df)
    
    other_count = (result1['Group'] == 'Other').sum()
    print(f"✓ Set default: {other_count} values set to 'Other'")
    
    # Test 'error' action
    step_config2 = {
        'type': 'group_data',
        'name': 'Error on unmatched',
        'source_column': 'Category',
        'target_column': 'Group',
        'groups': {
            'Group1': ['A', 'B'],
            'Group2': ['C']
        },
        'unmatched_action': 'error'
    }
    
    try:
        processor2 = GroupDataProcessor(step_config2)
        processor2.execute(test_df)
        print("✗ Should have failed with unmatched values")
        return False
    except StepProcessorError as e:
        print(f"✓ Error action worked: {e}")
    
    return True


def test_duplicate_value_detection():
    """Test detection of values appearing in multiple groups."""
    
    print("\nTesting duplicate value detection...")
    
    test_df = pd.DataFrame({
        'Item': ['A', 'B', 'C'],
        'Value': [1, 2, 3]
    })
    
    # Create config with duplicate value
    step_config = {
        'type': 'group_data',
        'name': 'Duplicate value test',
        'source_column': 'Item',
        'target_column': 'Group',
        'groups': {
            'Group1': ['A', 'B'],
            'Group2': ['B', 'C']  # 'B' appears in both groups
        }
    }
    
    try:
        processor = GroupDataProcessor(step_config)
        processor.execute(test_df)
        print("✗ Should have failed with duplicate value")
        return False
    except StepProcessorError as e:
        print(f"✓ Duplicate detection worked: {e}")
        return True


def test_grouping_analysis():
    """Test the grouping potential analysis functionality."""
    
    print("\nTesting grouping analysis...")
    
    test_df = create_van_report_origins_data()
    
    processor = GroupDataProcessor({'type': 'group_data'})
    analysis = processor.analyze_grouping_potential(test_df, 'Product_Origin')
    
    print(f"✓ Analysis for Product_Origin:")
    print(f"  Total rows: {analysis['total_rows']}")
    print(f"  Unique values: {analysis['unique_values']}")
    print(f"  Recommendation: {analysis['grouping_recommendation']}")
    print(f"  Most common: {list(analysis['most_common'].keys())[:3]}")
    
    if analysis['total_rows'] == len(test_df):
        print("✓ Grouping analysis worked correctly")
        return True
    else:
        print("✗ Grouping analysis failed")
        return False


def test_error_handling():
    """Test error handling for various failure cases."""
    
    print("\nTesting error handling...")
    
    test_df = create_van_report_origins_data()
    
    # Test missing source column
    try:
        bad_config = {
            'type': 'group_data',
            'name': 'Missing source column',
            'groups': {'Group1': ['A', 'B']}
        }
        processor = GroupDataProcessor(bad_config)
        processor.execute(test_df)
        print("✗ Should have failed with missing source column")
    except StepProcessorError as e:
        print(f"✓ Caught expected error: {e}")
    
    # Test invalid source column
    try:
        bad_config = {
            'type': 'group_data',
            'name': 'Invalid source column',
            'source_column': 'NonExistentColumn',
            'groups': {'Group1': ['A', 'B']}
        }
        processor = GroupDataProcessor(bad_config)
        processor.execute(test_df)
        print("✗ Should have failed with invalid source column")
    except StepProcessorError as e:
        print(f"✓ Caught expected error: {e}")
    
    # Test empty groups
    try:
        bad_config = {
            'type': 'group_data',
            'name': 'Empty groups',
            'source_column': 'Product_Origin',
            'groups': {}
        }
        processor = GroupDataProcessor(bad_config)
        processor.execute(test_df)
        print("✗ Should have failed with empty groups")
    except StepProcessorError as e:
        print(f"✓ Caught expected error: {e}")


if __name__ == '__main__':
    success = True
    
    success &= test_van_report_regional_grouping()
    success &= test_helper_method_regional_groups()
    success &= test_replace_source_column()
    success &= test_case_insensitive_grouping()
    success &= test_unmatched_actions()
    success &= test_duplicate_value_detection()
    success &= test_grouping_analysis()
    test_error_handling()
    
    if success:
        print("\n✓ All group data processor tests passed!")
    else:
        print("\n✗ Some group data processor tests failed!")
    
    # Show supported unmatched actions
    processor = GroupDataProcessor({'type': 'group_data', 'source_column': 'x', 'groups': {}})
    print(f"\nSupported unmatched actions: {processor.get_supported_unmatched_actions()}")
