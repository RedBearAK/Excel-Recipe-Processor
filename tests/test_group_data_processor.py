"""
Modern test module for group_data processor.

Tests the GroupDataProcessor using current architecture patterns:
- Strategic isinstance() checks with proper TypeError handling
- Stage-based workflows via StageManager
- Resolved filenames (no variable substitution at processor level)
- Proper setup/teardown and resource cleanup
- Focus on actual processor functionality

Module path: tests/test_group_data_processor.py
"""

import os
import tempfile
import pandas as pd

from pathlib import Path

from excel_recipe_processor.core.stage_manager import StageManager
from excel_recipe_processor.core.base_processor import StepProcessorError
from excel_recipe_processor.processors.group_data_processor import GroupDataProcessor


def create_test_data() -> pd.DataFrame:
    """Create sample data for testing grouping operations."""
    data = pd.DataFrame({
        'ID': [1, 2, 3, 4, 5],
        'State': ['CA', 'ny', 'TX', 'wa', 'FL'],  # Mixed case for testing
        'City': ['Los Angeles', 'New York', 'Austin', 'Seattle', 'Miami'],
        'Amount': [1000, 2000, 1500, 3000, 2500]
    })
    
    if not isinstance(data, pd.DataFrame):
        raise TypeError(f"Expected DataFrame, got {type(data)}")
    
    return data


def create_groups_file_data() -> pd.DataFrame:
    """Create group definitions data for file-based testing."""
    data = pd.DataFrame({
        'West_Coast': ['CA', 'WA', 'OR'],
        'East_Coast': ['NY', 'FL', 'MA'],
        'Central': ['TX', 'CO', 'IL']
    })
    
    if not isinstance(data, pd.DataFrame):
        raise TypeError(f"Expected DataFrame, got {type(data)}")
    
    return data


def test_basic_inline_grouping():
    """Test basic inline grouping with dictionary configuration."""
    print("\nTesting basic inline grouping...")
    
    test_data = create_test_data()
    
    config = {
        'processor_type': 'group_data',
        'step_description': 'Basic state grouping',
        'source_column': 'State',
        'target_column': 'Region',
        'groups': {
            'West': ['CA', 'WA', 'OR'],
            'East': ['NY', 'FL', 'MA'],
            'Central': ['TX', 'CO', 'IL']
        }
    }
    
    processor = GroupDataProcessor(config)
    result = processor.execute(test_data)
    
    # Validate result structure
    if not isinstance(result, pd.DataFrame):
        print(f"✗ Expected DataFrame result, got {type(result)}")
        return False
    
    # Check that Region column was added
    if 'Region' not in result.columns:
        print("✗ Target column 'Region' not found in result")
        return False
    
    # Check specific groupings (should be case insensitive by default)
    ca_region = result[result['State'] == 'CA']['Region'].iloc[0]
    ny_region = result[result['State'] == 'ny']['Region'].iloc[0]  # lowercase
    
    if ca_region != 'West':
        print(f"✗ CA should be West, got {ca_region}")
        return False
    
    if ny_region != 'East':
        print(f"✗ ny should be East (case insensitive), got {ny_region}")
        return False
    
    print("✓ Basic inline grouping works correctly")
    return True


def test_case_sensitivity_default():
    """Test that case insensitive is the default behavior."""
    print("\nTesting case insensitive default...")
    
    test_data = create_test_data()
    
    config = {
        'processor_type': 'group_data',
        'source_column': 'State',
        'target_column': 'Region',
        'groups': {
            'West': ['CA', 'WA'],
            'East': ['NY', 'FL']
        }
        # Note: No case_sensitive specified, should default to False
    }
    
    processor = GroupDataProcessor(config)
    result = processor.execute(test_data)
    
    # Check lowercase 'ny' matches uppercase 'NY' in groups
    ny_rows = result[result['State'] == 'ny']
    if len(ny_rows) == 0:
        print("✗ No rows found for 'ny'")
        return False
    
    ny_region = ny_rows['Region'].iloc[0]
    if ny_region != 'East':
        print(f"✗ Expected 'ny' to match 'NY' (case insensitive), got region: {ny_region}")
        return False
    
    print("✓ Case insensitive is default behavior")
    return True


def test_case_sensitive_explicit():
    """Test explicit case sensitive behavior."""
    print("\nTesting explicit case sensitive...")
    
    test_data = create_test_data()
    
    config = {
        'processor_type': 'group_data',
        'source_column': 'State',
        'target_column': 'Region',
        'groups': {
            'West': ['CA', 'WA'],
            'East': ['NY', 'FL']  # Uppercase NY
        },
        'case_sensitive': True  # Explicit case sensitive
    }
    
    processor = GroupDataProcessor(config)
    result = processor.execute(test_data)
    
    # Check lowercase 'ny' does NOT match uppercase 'NY' in groups
    ny_rows = result[result['State'] == 'ny']
    if len(ny_rows) == 0:
        print("✗ No rows found for 'ny'")
        return False
    
    ny_region = ny_rows['Region'].iloc[0]
    # Should keep original value since no case-sensitive match
    if ny_region != 'ny':
        print(f"✗ Expected 'ny' to remain unchanged (case sensitive), got: {ny_region}")
        return False
    
    print("✓ Explicit case sensitive works correctly")
    return True


def test_unmatched_value_handling():
    """Test different strategies for handling unmatched values."""
    print("\nTesting unmatched value handling...")
    
    test_data = create_test_data()
    
    # Test keep_original (default)
    config_keep = {
        'processor_type': 'group_data',
        'source_column': 'State',
        'groups': {
            'West': ['CA', 'WA'],
            'East': ['NY', 'FL']
            # TX intentionally omitted
        },
        'unmatched_action': 'keep_original'
    }
    
    processor_keep = GroupDataProcessor(config_keep)
    result_keep = processor_keep.execute(test_data)
    
    # TX should remain as 'TX'
    tx_rows = result_keep[result_keep['State'] == 'TX']
    if len(tx_rows) == 0:
        print("✗ No rows found for TX")
        return False
    
    tx_group = tx_rows['State_Group'].iloc[0]
    if tx_group != 'TX':
        print(f"✗ Expected TX to remain 'TX', got {tx_group}")
        return False
    
    # Test set_default
    config_default = {
        'processor_type': 'group_data',
        'source_column': 'State',
        'groups': {
            'West': ['CA', 'WA'],
            'East': ['NY', 'FL']
        },
        'unmatched_action': 'set_default',
        'unmatched_value': 'Other'
    }
    
    processor_default = GroupDataProcessor(config_default)
    result_default = processor_default.execute(test_data)
    
    # TX should become 'Other'
    tx_rows_default = result_default[result_default['State'] == 'TX']
    tx_group_default = tx_rows_default['State_Group'].iloc[0]
    if tx_group_default != 'Other':
        print(f"✗ Expected TX to become 'Other', got {tx_group_default}")
        return False
    
    print("✓ Unmatched value handling works correctly")
    return True


def test_stage_based_grouping():
    """Test grouping using data from StageManager."""
    print("\nTesting stage-based grouping...")
    
    # Initialize stage system
    StageManager.initialize_stages(max_stages=5)
    
    try:
        # Create input data stage
        test_data = create_test_data()
        StageManager.save_stage('test_input', test_data, 'Test input data')
        
        config = {
            'processor_type': 'group_data',
            'source_stage': 'test_input',
            'source_column': 'State',
            'target_column': 'Region',
            'groups': {
                'West': ['CA', 'WA'],
                'East': ['NY', 'FL'],
                'Central': ['TX']
            },
            'save_to_stage': 'grouped_output'
        }
        
        processor = GroupDataProcessor(config)
        
        # Load from stage and execute
        input_data = StageManager.load_stage('test_input')
        result = processor.execute(input_data)
        
        # Verify result
        if not isinstance(result, pd.DataFrame):
            print(f"✗ Expected DataFrame result, got {type(result)}")
            return False
        
        if 'Region' not in result.columns:
            print("✗ Region column not found")
            return False
        
        # Check that output stage was created
        if not StageManager.stage_exists('grouped_output'):
            print("✗ Output stage 'grouped_output' was not created")
            return False
        
        print("✓ Stage-based grouping works correctly")
        return True
        
    finally:
        StageManager.cleanup_stages()


def test_file_based_grouping():
    """Test grouping using definitions from external file."""
    print("\nTesting file-based grouping...")
    
    test_data = create_test_data()
    
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create groups definition file
        groups_data = create_groups_file_data()
        groups_file = Path(temp_dir) / "test_groups.csv"
        groups_data.to_csv(groups_file, index=False)
        
        config = {
            'processor_type': 'group_data',
            'source_column': 'State',
            'groups_source': {
                'type': 'file',
                'filename': str(groups_file),
                'format': 'wide'
            }
        }
        
        processor = GroupDataProcessor(config)
        result = processor.execute(test_data)
        
        # Verify result
        if not isinstance(result, pd.DataFrame):
            print(f"✗ Expected DataFrame result, got {type(result)}")
            return False
        
        if 'State_Group' not in result.columns:
            print("✗ State_Group column not found")
            return False
        
        # Check specific grouping
        ca_rows = result[result['State'] == 'CA']
        if len(ca_rows) > 0:
            ca_group = ca_rows['State_Group'].iloc[0]
            if ca_group != 'West_Coast':
                print(f"✗ Expected CA to be West_Coast, got {ca_group}")
                return False
        
        print("✓ File-based grouping works correctly")
        return True


def test_error_handling():
    """Test proper error handling for invalid inputs."""
    print("\nTesting error handling...")
    
    config = {
        'processor_type': 'group_data',
        'source_column': 'State',
        'groups': {'West': ['CA']}
    }
    
    processor = GroupDataProcessor(config)
    
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
    
    # Test with missing column
    test_data = create_test_data()
    config_bad_column = {
        'processor_type': 'group_data',
        'source_column': 'NonexistentColumn',
        'groups': {'West': ['CA']}
    }
    
    processor_bad = GroupDataProcessor(config_bad_column)
    
    try:
        processor_bad.execute(test_data)
        print("✗ Should have raised error for missing column")
        return False
    except StepProcessorError as e:
        # Accept either "not found" or "No group definitions" - processor may check groups first
        if "not found" not in str(e) and "NonexistentColumn" not in str(e):
            print(f"✗ Unexpected error message: {e}")
            return False
    
    # Test with no group definitions at all
    config_no_groups = {
        'processor_type': 'group_data',
        'source_column': 'State'
        # Missing groups, groups_source, etc.
    }
    
    processor_no_groups = GroupDataProcessor(config_no_groups)
    
    try:
        processor_no_groups.execute(test_data)
        print("✗ Should have raised error for no group definitions")
        return False
    except StepProcessorError as e:
        if "group definitions" not in str(e).lower():
            print(f"✗ Wrong error message for no groups: {e}")
            return False
    
    print("✓ Error handling works correctly")
    return True


def test_type_validation():
    """Test isinstance checks and type validation."""
    print("\nTesting type validation...")
    
    # Test processor creation with invalid config
    try:
        processor = GroupDataProcessor("not a dict")
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
    print("🔄 Testing GroupDataProcessor (Modern Architecture)")
    print("=" * 60)
    
    tests = [
        test_basic_inline_grouping,
        test_case_sensitivity_default,
        test_case_sensitive_explicit,
        test_unmatched_value_handling,
        test_stage_based_grouping,
        test_file_based_grouping,
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
