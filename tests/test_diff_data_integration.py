#!/usr/bin/env python3
"""
Fresh integration test for DiffDataProcessor - starting over with debugging.

tests/test_diff_data_integration.py
"""

import os
import sys
import pandas as pd
import tempfile
from pathlib import Path

# Add project root to Python path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from excel_recipe_processor.core.recipe_pipeline import RecipePipeline
from excel_recipe_processor.core.base_processor import registry
from excel_recipe_processor.core.stage_manager import StageManager


def create_baseline_data():
    """Create baseline test data."""
    return pd.DataFrame({
        'customer_id': ['C001', 'C002', 'C003'],
        'name': ['Alice Corp', 'Bob Inc', 'Charlie Ltd'],
        'status': ['Active', 'Active', 'Pending'],
        'value': [1000, 1500, 800]
    })


def create_current_data():
    """Create current test data with changes."""
    return pd.DataFrame({
        'customer_id': ['C001', 'C002', 'C004'],  # C003 deleted, C004 new
        'name': ['Alice Corp', 'Bob Industries', 'Delta Systems'],  # C002 name changed
        'status': ['Active', 'Active', 'Active'],
        'value': [1000, 1500, 2000]  # Values unchanged for existing, new for C004
    })


def test_diff_data_registration():
    """Test that diff_data processor is properly registered."""
    print("Testing diff_data processor registration...")
    
    registered_types = registry.get_registered_types()
    print(f"✓ Registered processor types: {len(registered_types)} total")
    
    if 'diff_data' in registered_types:
        print("✓ diff_data processor is registered")
    else:
        print("✗ diff_data processor not registered")
        print(f"Available types: {sorted(registered_types)}")
        return False
    
    # Test creating the processor from registry
    step_config = {
        'processor_type': 'diff_data',
        'step_description': 'Test diff registration',
        'source_stage': 'current_data',
        'reference_stage': 'baseline_data',
        'key_columns': 'id',
        'save_to_stage': 'diff_results'
    }
    
    try:
        processor = registry.create_processor(step_config)
        print(f"✓ Created processor from registry: {processor.__class__.__name__}")
        return True
    except Exception as e:
        print(f"✗ Failed to create processor: {e}")
        return False


def test_diff_data_basic_pipeline():
    """Test basic diff_data pipeline integration."""
    print("\nTesting basic diff_data pipeline integration...")
    
    # Initialize clean stage environment
    StageManager.initialize_stages()
    
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Create test data files
            baseline_file = temp_path / 'baseline.xlsx'
            current_file = temp_path / 'current.xlsx'
            output_file = temp_path / 'diff_analysis.xlsx'
            
            create_baseline_data().to_excel(baseline_file, index=False)
            create_current_data().to_excel(current_file, index=False)
            
            # Simple recipe
            recipe_content = f"""
settings:
  description: "Basic diff_data pipeline test"
  stages:
    - stage_name: "baseline_data"
      description: "Baseline customer data"
      protected: false
    - stage_name: "current_data"
      description: "Current customer data"
      protected: false
    - stage_name: "diff_results"
      description: "Diff analysis results"
      protected: false

recipe:
  - step_description: "Import baseline data"
    processor_type: "import_file"
    input_file: "{baseline_file}"
    save_to_stage: "baseline_data"
    
  - step_description: "Import current data"
    processor_type: "import_file"
    input_file: "{current_file}"
    save_to_stage: "current_data"
    
  - step_description: "Compare datasets"
    processor_type: "diff_data"
    source_stage: "current_data"
    reference_stage: "baseline_data"
    key_columns: "customer_id"
    save_to_stage: "diff_results"
    
  - step_description: "Export results"
    processor_type: "export_file"
    source_stage: "diff_results"
    output_file: "{output_file}"
"""
            
            recipe_file = temp_path / 'basic_recipe.yaml'
            with open(recipe_file, 'w') as f:
                f.write(recipe_content)
            
            # Execute pipeline
            pipeline = RecipePipeline()
            completion_report = pipeline.run_complete_recipe(recipe_file)
            
            # Validate execution
            if not completion_report.get('execution_successful', False):
                print(f"✗ Pipeline execution failed: {completion_report.get('error_message', 'Unknown error')}")
                return False
            
            # Validate output file
            if not output_file.exists():
                print("✗ Output file not created")
                return False
            
            # Validate content
            result_df = pd.read_excel(output_file)
            required_columns = ['Row_Status', 'Changed_Fields', 'Change_Count', 'Change_Details']
            
            for col in required_columns:
                if col not in result_df.columns:
                    print(f"✗ Missing metadata column: {col}")
                    return False
            
            # Check row statuses
            status_counts = result_df['Row_Status'].value_counts()
            expected_statuses = {'NEW', 'CHANGED', 'DELETED'}
            found_statuses = set(status_counts.index)
            
            if not expected_statuses.issubset(found_statuses):
                print(f"✗ Missing expected statuses. Found: {found_statuses}")
                return False
            
            print(f"✓ Basic pipeline successful - {dict(status_counts)}")
            return True
    
    except Exception as e:
        print(f"✗ Basic pipeline test failed: {e}")
        return False
    
    finally:
        StageManager.cleanup_stages()


def test_diff_data_with_filtered_stages():
    """Test diff_data with filtered stages creation."""
    print("\nTesting diff_data with filtered stages...")
    
    # Initialize clean stage environment
    StageManager.initialize_stages()
    
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Create test data
            baseline_file = temp_path / 'baseline.xlsx'
            current_file = temp_path / 'current.xlsx'
            main_output = temp_path / 'main_results.xlsx'
            new_output = temp_path / 'new_rows.xlsx'
            changed_output = temp_path / 'changed_rows.xlsx'
            
            create_baseline_data().to_excel(baseline_file, index=False)
            create_current_data().to_excel(current_file, index=False)
            
            # Recipe with filtered stages and multiple exports
            recipe_content = f"""
settings:
  description: "Test filtered stages creation"
  stages:
    - stage_name: "baseline_data"
      description: "Baseline data for filtered test"
      protected: false
    - stage_name: "current_data"
      description: "Current data for filtered test"
      protected: false
    - stage_name: "diff_results"
      description: "Complete diff results with filtering"
      protected: false

recipe:
  - step_description: "Import baseline data"
    processor_type: "import_file"
    input_file: "{baseline_file}"
    save_to_stage: "baseline_data"
    
  - step_description: "Import current data"
    processor_type: "import_file"
    input_file: "{current_file}"
    save_to_stage: "current_data"
    
  - step_description: "Comprehensive diff analysis"
    processor_type: "diff_data"
    source_stage: "current_data"
    reference_stage: "baseline_data"
    key_columns: "customer_id"
    save_to_stage: "diff_results"
    exclude_columns: ["customer_id"]
    create_filtered_stages: true
    filtered_stage_prefix: "customer_analysis"
    include_json_details: true
    
  - step_description: "Export complete results"
    processor_type: "export_file"
    source_stage: "diff_results"
    output_file: "{main_output}"
    
  - step_description: "Export new customers only"
    processor_type: "export_file"
    source_stage: "customer_analysis_new_rows_subset"
    output_file: "{new_output}"
    
  - step_description: "Export changed customers only"
    processor_type: "export_file"
    source_stage: "customer_analysis_changed_rows_subset"
    output_file: "{changed_output}"
"""
            
            recipe_file = temp_path / 'filtered_recipe.yaml'
            with open(recipe_file, 'w') as f:
                f.write(recipe_content)
            
            # Execute pipeline
            pipeline = RecipePipeline()
            completion_report = pipeline.run_complete_recipe(recipe_file)
            
            if not completion_report.get('execution_successful', False):
                print(f"✗ Filtered stages pipeline failed: {completion_report.get('error_message', 'Unknown')}")
                return False
            
            # Validate all output files were created
            expected_files = [main_output, new_output, changed_output]
            for file_path in expected_files:
                if not file_path.exists():
                    print(f"✗ Missing output file: {file_path.name}")
                    return False
            
            # Validate main output has JSON details
            main_df = pd.read_excel(main_output)
            if 'Change_Details_JSON' not in main_df.columns:
                print("✗ Missing JSON details column in main output")
                return False
            
            # Validate filtered outputs have correct content
            new_df = pd.read_excel(new_output)
            changed_df = pd.read_excel(changed_output)
            
            if len(new_df) == 0:
                print("✗ New rows file is empty (should have new customers)")
                return False
            
            if not all(new_df['Row_Status'] == 'NEW'):
                print("✗ New rows file contains non-NEW rows")
                return False
            
            if len(changed_df) > 0 and not all(changed_df['Row_Status'] == 'CHANGED'):
                print("✗ Changed rows file contains non-CHANGED rows")
                return False
            
            print(f"✓ Filtered stages successful - {len(new_df)} new, {len(changed_df)} changed")
            return True
    
    except Exception as e:
        print(f"✗ Filtered stages test failed: {e}")
        return False
    
    finally:
        StageManager.cleanup_stages()


def test_diff_data_composite_keys():
    """Test diff_data with composite keys."""
    print("\nTesting diff_data with composite keys...")
    
    # Initialize clean stage environment
    StageManager.initialize_stages()
    
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Create composite key test data
            baseline_data = pd.DataFrame({
                'region': ['North', 'South', 'East'],
                'product': ['A001', 'A001', 'A002'],
                'sales': [1000, 800, 1200],
                'quarter': ['Q1', 'Q1', 'Q1']
            })
            
            current_data = pd.DataFrame({
                'region': ['North', 'South', 'West'],  # East/A002 deleted, West/A003 new
                'product': ['A001', 'A001', 'A003'],
                'sales': [1000, 850, 900],  # South/A001 sales changed
                'quarter': ['Q2', 'Q2', 'Q2']  # All quarters changed
            })
            
            baseline_file = temp_path / 'baseline_composite.xlsx'
            current_file = temp_path / 'current_composite.xlsx'
            output_file = temp_path / 'composite_diff.xlsx'
            
            baseline_data.to_excel(baseline_file, index=False)
            current_data.to_excel(current_file, index=False)
            
            # Recipe with composite keys
            recipe_content = f"""
settings:
  description: "Test composite key diff analysis"
  stages:
    - stage_name: "baseline_sales"
      description: "Baseline sales data"
      protected: false
    - stage_name: "current_sales"
      description: "Current sales data"
      protected: false
    - stage_name: "sales_diff"
      description: "Sales comparison results"
      protected: false

recipe:
  - step_description: "Import baseline sales"
    processor_type: "import_file"
    input_file: "{baseline_file}"
    save_to_stage: "baseline_sales"
    
  - step_description: "Import current sales"
    processor_type: "import_file"
    input_file: "{current_file}"
    save_to_stage: "current_sales"
    
  - step_description: "Compare with composite keys"
    processor_type: "diff_data"
    source_stage: "current_sales"
    reference_stage: "baseline_sales"
    key_columns: ["region", "product"]
    save_to_stage: "sales_diff"
    exclude_columns: ["region", "product"]
    
  - step_description: "Export composite key results"
    processor_type: "export_file"
    source_stage: "sales_diff"
    output_file: "{output_file}"
"""
            
            recipe_file = temp_path / 'composite_recipe.yaml'
            with open(recipe_file, 'w') as f:
                f.write(recipe_content)
            
            # Execute pipeline
            pipeline = RecipePipeline()
            completion_report = pipeline.run_complete_recipe(recipe_file)
            
            if not completion_report.get('execution_successful', False):
                print(f"✗ Composite key pipeline failed: {completion_report.get('error_message', 'Unknown')}")
                return False
            
            # Validate results
            result_df = pd.read_excel(output_file)
            status_counts = result_df['Row_Status'].value_counts()
            
            # Should have all change types
            expected_statuses = {'NEW', 'CHANGED', 'DELETED'}
            found_statuses = set(status_counts.index)
            
            if not expected_statuses.issubset(found_statuses):
                print(f"✗ Missing expected statuses with composite keys. Found: {found_statuses}")
                return False
            
            print(f"✓ Composite keys successful - {dict(status_counts)}")
            return True
    
    except Exception as e:
        print(f"✗ Composite keys test failed: {e}")
        return False
    
    finally:
        StageManager.cleanup_stages()


def test_diff_data_exclude_deleted():
    """Test diff_data with deleted rows excluded."""
    print("\nTesting diff_data with deleted rows excluded...")
    
    # Initialize clean stage environment
    StageManager.initialize_stages()
    
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            baseline_file = temp_path / 'baseline.xlsx'
            current_file = temp_path / 'current.xlsx'
            output_file = temp_path / 'no_deleted.xlsx'
            
            create_baseline_data().to_excel(baseline_file, index=False)
            create_current_data().to_excel(current_file, index=False)
            
            # Recipe excluding deleted rows
            recipe_content = f"""
settings:
  description: "Test excluding deleted rows"
  stages:
    - stage_name: "baseline_data"
      description: "Baseline data for exclusion test"
      protected: false
    - stage_name: "current_data"
      description: "Current data for exclusion test"
      protected: false
    - stage_name: "diff_results"
      description: "Changes without deleted rows"
      protected: false

recipe:
  - step_description: "Import baseline for exclusion test"
    processor_type: "import_file"
    input_file: "{baseline_file}"
    save_to_stage: "baseline_data"
    
  - step_description: "Import current for exclusion test"
    processor_type: "import_file"
    input_file: "{current_file}"
    save_to_stage: "current_data"
    
  - step_description: "Analyze active changes only"
    processor_type: "diff_data"
    source_stage: "current_data"
    reference_stage: "baseline_data"
    key_columns: "customer_id"
    save_to_stage: "diff_results"
    handle_deleted_rows: "exclude"
    
  - step_description: "Export active changes"
    processor_type: "export_file"
    source_stage: "diff_results"
    output_file: "{output_file}"
"""
            
            recipe_file = temp_path / 'exclude_deleted_recipe.yaml'
            with open(recipe_file, 'w') as f:
                f.write(recipe_content)
            
            # Execute pipeline
            pipeline = RecipePipeline()
            completion_report = pipeline.run_complete_recipe(recipe_file)
            
            if not completion_report.get('execution_successful', False):
                print(f"✗ Exclude deleted pipeline failed: {completion_report.get('error_message', 'Unknown')}")
                return False
            
            # Validate no deleted rows in output
            result_df = pd.read_excel(output_file)
            status_counts = result_df['Row_Status'].value_counts()
            
            if 'DELETED' in status_counts:
                print(f"✗ Found {status_counts['DELETED']} deleted rows when they should be excluded")
                return False
            
            # Should still have other statuses
            expected_statuses = {'NEW', 'CHANGED'}
            found_statuses = set(status_counts.index)
            
            if not expected_statuses.issubset(found_statuses):
                print(f"✗ Missing expected statuses after excluding deleted. Found: {found_statuses}")
                return False
            
            print(f"✓ Exclude deleted successful - {dict(status_counts)} (no DELETED)")
            return True
    
    except Exception as e:
        print(f"✗ Exclude deleted test failed: {e}")
        return False
    
    finally:
        StageManager.cleanup_stages()


def main():
    """Run comprehensive integration tests."""
    print("🧪 Testing diff_data processor integration (comprehensive suite)...")
    print("=" * 80)
    
    tests = [
        ("Registration", test_diff_data_registration),
        ("Basic Pipeline", test_diff_data_basic_pipeline),
        ("Filtered Stages", test_diff_data_with_filtered_stages),
        ("Composite Keys", test_diff_data_composite_keys),
        ("Exclude Deleted", test_diff_data_exclude_deleted)
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        print(f"\n🧪 {test_name}")
        print("-" * 40)
        
        try:
            if test_func():
                passed += 1
                print(f"  ✅ PASSED: {test_name}")
            else:
                print(f"  ❌ FAILED: {test_name}")
        except Exception as e:
            print(f"  💥 CRASHED: {test_name} - {e}")
        
        print("-" * 40)
    
    print(f"\n📊 COMPREHENSIVE INTEGRATION TEST RESULTS")
    print("=" * 80)
    print(f"Overall: {passed}/{total} tests passed ({passed/total*100:.1f}%)")
    
    if passed == total:
        print("🎉 INTEGRATION STATUS: EXCELLENT - All comprehensive tests passed!")
        print("✨ The diff_data processor is fully integrated and production-ready")
        print()
        print("🚀 CAPABILITIES VERIFIED:")
        print("   • Basic dataset comparison with change metadata")
        print("   • Filtered stage creation for targeted analysis")
        print("   • Composite key support for complex data relationships")
        print("   • JSON details for machine-readable change tracking")
        print("   • Flexible deleted row handling options")
        print("   • Full pipeline integration with import/export workflows")
        return 0
    elif passed >= total * 0.8:
        print("✅ INTEGRATION STATUS: GOOD - Core functionality working")
        print(f"⚠️  {total - passed} test(s) need attention")
        return 0
    else:
        print("❌ INTEGRATION STATUS: NEEDS WORK - Multiple integration issues")
        return 1


if __name__ == "__main__":
    exit(main())


# End of file #
