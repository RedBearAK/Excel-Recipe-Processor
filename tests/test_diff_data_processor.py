#!/usr/bin/env python3
"""
Test module for DiffDataProcessor.

tests/test_diff_data_processor.py

Tests the diff data comparison functionality including change detection,
metadata generation, and filtered stage creation.
"""

import os
import sys
import json
import pandas as pd

# Add project root to Python path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from excel_recipe_processor.core.stage_manager import StageManager
from excel_recipe_processor.core.base_processor import StepProcessorError
from excel_recipe_processor.processors.diff_data_processor import DiffDataProcessor


def setup_test_stages():
    """Set up test stages for diff data processor tests."""
    StageManager.cleanup_stages()
    StageManager.initialize_stages(max_stages=50)


def create_baseline_test_data():
    """Create baseline test data for comparison."""
    return pd.DataFrame({
        'customer_id': ['C001', 'C002', 'C003', 'C004', 'C005'],
        'customer_name': ['Alice Corp', 'Bob Industries', 'Charlie Ltd', 'Delta Systems', 'Echo Enterprises'],
        'status': ['Active', 'Active', 'Pending', 'Active', 'Inactive'],
        'total_orders': [15, 23, 8, 42, 3],
        'last_order_amount': [1500.00, 3200.50, 750.25, 8900.00, 125.00]
    })


def create_current_test_data():
    """Create current test data with various types of changes."""
    return pd.DataFrame({
        'customer_id': ['C001', 'C002', 'C003', 'C006', 'C007'],  # C004, C005 deleted; C006, C007 new
        'customer_name': ['Alice Corp', 'Bob Industries Ltd', 'Charlie Ltd', 'Foxtrot Inc', 'Golf Solutions'],  # C002 name changed
        'status': ['Active', 'Active', 'Active', 'Active', 'Pending'],  # C003 status changed
        'total_orders': [15, 23, 12, 5, 1],  # C003 orders changed
        'last_order_amount': [1500.00, 3200.50, 950.75, 2100.00, 450.00]  # C003 amount changed
    })


def create_composite_key_baseline():
    """Create baseline data with composite keys."""
    return pd.DataFrame({
        'region': ['North', 'North', 'South', 'South', 'East'],
        'product': ['A001', 'A002', 'A001', 'A003', 'A001'],
        'sales': [1000, 1500, 800, 2200, 1200],
        'quarter': ['Q1', 'Q1', 'Q1', 'Q1', 'Q1']
    })


def create_composite_key_current():
    """Create current data with composite keys and changes."""
    return pd.DataFrame({
        'region': ['North', 'North', 'South', 'West', 'East'],  # South/A003 deleted, West/A002 new
        'product': ['A001', 'A002', 'A001', 'A002', 'A001'],
        'sales': [1000, 1800, 850, 900, 1200],  # North/A002 and South/A001 changed
        'quarter': ['Q2', 'Q2', 'Q2', 'Q2', 'Q2']  # Quarter changed for all
    })


def test_basic_diff_analysis():
    """Test basic diff analysis functionality."""
    print("\nTesting basic diff analysis...")
    
    setup_test_stages()
    
    # Create test data
    baseline_data = create_baseline_test_data()
    current_data = create_current_test_data()
    
    # Save to stages
    StageManager.save_stage('baseline_customers', baseline_data, 'Baseline customer data')
    StageManager.save_stage('current_customers', current_data, 'Current customer data')
    
    # Configure diff processor
    step_config = {
        'processor_type': 'diff_data',
        'step_description': 'Test basic diff analysis',
        'source_stage': 'current_customers',
        'reference_stage': 'baseline_customers',
        'key_columns': 'customer_id',
        'save_to_stage': 'diff_results',
        'exclude_columns': []
    }
    
    processor = DiffDataProcessor(step_config)
    result = processor.execute(current_data)
    
    # Validate results
    if len(result) != 7:  # 5 original + 2 new = 7 total
        print(f"✗ Expected 7 rows, got {len(result)}")
        return False
    
    # Check metadata columns exist
    required_columns = ['Row_Status', 'Changed_Fields', 'Change_Count', 'Change_Details']
    for col in required_columns:
        if col not in result.columns:
            print(f"✗ Missing metadata column: {col}")
            return False
    
    # Check row status distribution
    status_counts = result['Row_Status'].value_counts()
    expected_counts = {'NEW': 2, 'CHANGED': 2, 'UNCHANGED': 1, 'DELETED': 2}
    
    for status, expected_count in expected_counts.items():
        if status not in status_counts or status_counts[status] != expected_count:
            print(f"✗ Expected {expected_count} {status} rows, got {status_counts.get(status, 0)}")
            return False
    
    print("✓ Basic diff analysis passed")
    return True


def test_composite_key_handling():
    """Test diff analysis with composite keys."""
    print("\nTesting composite key handling...")
    
    setup_test_stages()
    
    # Create test data with composite keys
    baseline_data = create_composite_key_baseline()
    current_data = create_composite_key_current()
    
    # Save to stages
    StageManager.save_stage('baseline_sales', baseline_data, 'Baseline sales data')
    StageManager.save_stage('current_sales', current_data, 'Current sales data')
    
    # Configure diff processor with composite keys
    step_config = {
        'processor_type': 'diff_data',
        'step_description': 'Test composite key diff',
        'source_stage': 'current_sales',
        'reference_stage': 'baseline_sales',
        'key_columns': ['region', 'product'],
        'save_to_stage': 'composite_diff_results',
        'exclude_columns': ['region', 'product']
    }
    
    processor = DiffDataProcessor(step_config)
    result = processor.execute(current_data)
    
    # Validate composite key handling
    if len(result) != 6:  # 5 original + 1 new = 6 total
        print(f"✗ Expected 6 rows, got {len(result)}")
        return False
    
    # Check for changes in composite key scenario
    changed_rows = result[result['Row_Status'] == 'CHANGED']
    if len(changed_rows) != 4:  # All existing rows have quarter change: North/A001, North/A002, South/A001, East/A001
        print(f"✗ Expected 4 changed rows, got {len(changed_rows)}")
        return False
    
    print("✓ Composite key handling passed")
    return True


def test_exclude_columns():
    """Test excluding columns from change detection."""
    print("\nTesting exclude columns functionality...")
    
    setup_test_stages()
    
    # Create test data
    baseline_data = create_baseline_test_data()
    current_data = create_current_test_data()
    
    # Save to stages
    StageManager.save_stage('baseline_exclude_test', baseline_data, 'Baseline for exclude test')
    StageManager.save_stage('current_exclude_test', current_data, 'Current for exclude test')
    
    # Configure diff processor excluding last_order_amount
    step_config = {
        'processor_type': 'diff_data',
        'step_description': 'Test exclude columns',
        'source_stage': 'current_exclude_test',
        'reference_stage': 'baseline_exclude_test',
        'key_columns': 'customer_id',
        'save_to_stage': 'exclude_diff_results',
        'exclude_columns': ['customer_id', 'last_order_amount']
    }
    
    processor = DiffDataProcessor(step_config)
    result = processor.execute(current_data)
    
    # Check that C003 still shows as changed (due to status and total_orders)
    c003_row = result[result['customer_id'] == 'C003']
    if len(c003_row) != 1 or c003_row.iloc[0]['Row_Status'] != 'CHANGED':
        print("✗ C003 should still be marked as changed")
        return False
    
    # Check that last_order_amount changes are not in change details
    c003_details = c003_row.iloc[0]['Change_Details']
    if 'last_order_amount' in c003_details.lower():
        print("✗ Excluded column should not appear in change details")
        return False
    
    print("✓ Exclude columns functionality passed")
    return True


def test_filtered_stages_creation():
    """Test creation of filtered stages for each change type."""
    print("\nTesting filtered stages creation...")
    
    setup_test_stages()
    
    # Create test data
    baseline_data = create_baseline_test_data()
    current_data = create_current_test_data()
    
    # Save to stages
    StageManager.save_stage('baseline_filtered', baseline_data, 'Baseline for filtered test')
    StageManager.save_stage('current_filtered', current_data, 'Current for filtered test')
    
    # Configure diff processor with filtered stages
    step_config = {
        'processor_type': 'diff_data',
        'step_description': 'Test filtered stages',
        'source_stage': 'current_filtered',
        'reference_stage': 'baseline_filtered',
        'key_columns': 'customer_id',
        'save_to_stage': 'filtered_main_results',
        'create_filtered_stages': True,
        'filtered_stage_prefix': 'stg_test_filtered'
    }
    
    processor = DiffDataProcessor(step_config)
    result = processor.execute(current_data)
    
    # Check that filtered stages were created
    expected_stages = [
        'stg_test_filtered_new_rows_subset',
        'stg_test_filtered_changed_rows_subset',
        'stg_test_filtered_unchanged_rows_subset',
        'stg_test_filtered_deleted_rows_subset'
    ]
    
    for stage_name in expected_stages:
        try:
            stage_data = StageManager.load_stage(stage_name)
            print(f"   ✓ Created {stage_name} with {len(stage_data)} rows")
        except Exception as e:
            print(f"✗ Failed to find expected stage {stage_name}: {e}")
            return False
    
    # Validate stage contents
    new_rows = StageManager.load_stage('stg_test_filtered_new_rows_subset')
    if len(new_rows) != 2:
        print(f"✗ Expected 2 new rows, got {len(new_rows)}")
        return False
    
    print("✓ Filtered stages creation passed")
    return True


def test_json_details_option():
    """Test JSON details inclusion."""
    print("\nTesting JSON details option...")
    
    setup_test_stages()
    
    # Create test data
    baseline_data = create_baseline_test_data()
    current_data = create_current_test_data()
    
    # Save to stages
    StageManager.save_stage('baseline_json', baseline_data, 'Baseline for JSON test')
    StageManager.save_stage('current_json', current_data, 'Current for JSON test')
    
    # Configure diff processor with JSON details
    step_config = {
        'processor_type': 'diff_data',
        'step_description': 'Test JSON details',
        'source_stage': 'current_json',
        'reference_stage': 'baseline_json',
        'key_columns': 'customer_id',
        'save_to_stage': 'json_diff_results',
        'include_json_details': True
    }
    
    processor = DiffDataProcessor(step_config)
    result = processor.execute(current_data)
    
    # Check that JSON column was added
    if 'Change_Details_JSON' not in result.columns:
        print("✗ Change_Details_JSON column not found")
        return False
    
    # Check JSON content for changed row
    changed_row = result[result['Row_Status'] == 'CHANGED'].iloc[0]
    json_details = changed_row['Change_Details_JSON']
    
    if json_details:
        try:
            parsed_json = json.loads(json_details)
            if not isinstance(parsed_json, dict):
                print("✗ JSON details should be a dictionary")
                return False
            print(f"   ✓ JSON details: {list(parsed_json.keys())}")
        except json.JSONDecodeError:
            print("✗ Invalid JSON in Change_Details_JSON")
            return False
    
    print("✓ JSON details option passed")
    return True


def test_handle_deleted_rows_options():
    """Test different options for handling deleted rows."""
    print("\nTesting handle_deleted_rows options...")
    
    setup_test_stages()
    
    # Create test data
    baseline_data = create_baseline_test_data()
    current_data = create_current_test_data()
    
    # Test exclude option
    StageManager.save_stage('baseline_exclude_deleted', baseline_data, 'Baseline')
    StageManager.save_stage('current_exclude_deleted', current_data, 'Current')
    
    step_config = {
        'processor_type': 'diff_data',
        'step_description': 'Test exclude deleted',
        'source_stage': 'current_exclude_deleted',
        'reference_stage': 'baseline_exclude_deleted',
        'key_columns': 'customer_id',
        'save_to_stage': 'exclude_deleted_results',
        'handle_deleted_rows': 'exclude'
    }
    
    processor = DiffDataProcessor(step_config)
    result = processor.execute(current_data)
    
    # Should not have any DELETED rows
    deleted_count = len(result[result['Row_Status'] == 'DELETED'])
    if deleted_count != 0:
        print(f"✗ Expected 0 deleted rows with exclude option, got {deleted_count}")
        return False
    
    # Should have 5 total rows (2 new + 1 changed + 2 unchanged)
    if len(result) != 5:
        print(f"✗ Expected 5 total rows with exclude option, got {len(result)}")
        return False
    
    print("✓ Handle deleted rows options passed")
    return True


def test_configuration_validation():
    """Test configuration validation and error handling."""
    print("\nTesting configuration validation...")
    
    setup_test_stages()
    
    # Test missing required fields
    try:
        invalid_config = {
            'processor_type': 'diff_data',
            'source_stage': 'test_source',
            'save_to_stage': 'test_save'
            # Missing reference_stage and key_columns
        }
        processor = DiffDataProcessor(invalid_config)
        print("✗ Should have failed with missing required fields")
        return False
    except StepProcessorError:
        print("   ✓ Properly caught missing required fields")
    
    # Test invalid handle_deleted_rows option
    try:
        invalid_config = {
            'processor_type': 'diff_data',
            'source_stage': 'test_source',
            'reference_stage': 'test_reference',
            'key_columns': 'test_key',
            'save_to_stage': 'test_save',
            'handle_deleted_rows': 'invalid_option'
        }
        processor = DiffDataProcessor(invalid_config)
        print("✗ Should have failed with invalid handle_deleted_rows option")
        return False
    except StepProcessorError:
        print("   ✓ Properly caught invalid handle_deleted_rows option")
    
    print("✓ Configuration validation passed")
    return True


def main():
    """Run all tests and report results."""
    print("🧪 Testing DiffDataProcessor functionality...")
    
    tests = [
        test_basic_diff_analysis,
        test_composite_key_handling,
        test_exclude_columns,
        test_filtered_stages_creation,
        test_json_details_option,
        test_handle_deleted_rows_options,
        test_configuration_validation
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
            print(f"❌ {test_func.__name__} failed with error: {e}")
    
    print(f"\n📊 Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed!")
        return 0
    else:
        print("💥 Some tests failed!")
        return 1


if __name__ == "__main__":
    exit(main())


# End of file #
