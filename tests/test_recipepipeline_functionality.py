#!/usr/bin/env python3
"""
Test script for RecipePipeline functionality.

Updated to use the new stage-based architecture with import_file and export_file steps.
"""

import sys
import pandas as pd
import tempfile
from pathlib import Path

# Add the parent directory to the path for imports
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from excel_recipe_processor.core.recipe_pipeline import RecipePipeline


def test_basic_recipe_execution():
    """Test basic recipe execution with import and export steps."""
    print("Testing basic recipe execution...")
    
    try:
        # Create test data
        test_data = pd.DataFrame({
            'Product': ['Widget A', 'Widget B', 'Widget C'],
            'Sales': [100, 200, 150],
            'Region': ['North', 'South', 'North']
        })
        
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Create input file
            input_file = temp_path / 'test_input.xlsx'
            test_data.to_excel(input_file, index=False)
            
            # Create recipe with new format - WITH STAGE DECLARATIONS
            recipe_content = f"""
settings:
  variables:
    output_prefix: "filtered"
  stages:
    - stage_name: "raw_data"
      description: "Raw imported test data"
      protected: false
    - stage_name: "filtered_data"
      description: "Filtered data ready for export"
      protected: false

recipe:
  - step_description: "Import test data"
    processor_type: "import_file"
    input_file: "{input_file}"
    save_to_stage: "raw_data"
    
  - step_description: "Filter for high sales"
    processor_type: "filter_data"
    source_stage: "raw_data"
    save_to_stage: "filtered_data"
    filters:
      - column: "Sales"
        condition: "greater_than"
        value: 120
        
  - step_description: "Export results"
    processor_type: "export_file"
    source_stage: "filtered_data"
    output_file: "{temp_path}/{{output_prefix}}_results.xlsx"
"""
            
            recipe_file = temp_path / 'test_recipe.yaml'
            with open(recipe_file, 'w') as f:
                f.write(recipe_content)
            
            # Execute recipe
            pipeline = RecipePipeline()
            completion_report = pipeline.run_complete_recipe(recipe_file)
            
            # Check completion report
            if not completion_report.get('execution_successful', False):
                print("✗ Recipe execution failed")
                return False
            
            if completion_report.get('steps_executed', 0) != 3:
                print(f"✗ Expected 3 steps, got {completion_report.get('steps_executed', 0)}")
                return False
            
            print(f"✓ Recipe executed successfully: {completion_report['steps_executed']} steps")
            
            # Verify output file
            output_file = temp_path / 'filtered_results.xlsx'
            if not output_file.exists():
                print("✗ Output file not created")
                return False
            
            # Check the filtered data
            result_data = pd.read_excel(output_file)
            if len(result_data) != 2:  # Widget B (200) and Widget C (150) should remain
                print(f"✗ Expected 2 rows after filtering, got {len(result_data)}")
                return False
            
            print("✓ Data filtering and export worked correctly")
            return True
            
    except Exception as e:
        print(f"✗ Test failed: {e}")
        return False


def test_variable_substitution():
    """Test variable substitution in file paths."""
    print("Testing variable substitution...")
    
    try:
        test_data = pd.DataFrame({
            'Name': ['Alice', 'Bob'],
            'Score': [95, 87]
        })
        
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            input_file = temp_path / 'scores.xlsx'
            test_data.to_excel(input_file, index=False)
            
            # Recipe with variable substitution - WITH STAGE DECLARATIONS
            recipe_content = f"""
settings:
  variables:
    report_type: "student_scores"
    version: "v1"
  stages:
    - stage_name: "student_data"
      description: "Raw student score data"
      protected: false

recipe:
  - step_description: "Import student data"
    processor_type: "import_file"
    input_file: "{input_file}"
    save_to_stage: "student_data"
    
  - step_description: "Export with variables"
    processor_type: "export_file"
    source_stage: "student_data"
    output_file: "{temp_path}/{{report_type}}_{{version}}_{{date}}.xlsx"
"""
            
            recipe_file = temp_path / 'var_test_recipe.yaml'
            with open(recipe_file, 'w') as f:
                f.write(recipe_content)
            
            pipeline = RecipePipeline()
            completion_report = pipeline.run_complete_recipe(recipe_file)
            
            if not completion_report.get('execution_successful', False):
                print("✗ Variable substitution recipe failed")
                return False
            
            # Check that a file with substituted variables was created
            # Look for files matching the pattern
            output_files = list(temp_path.glob('student_scores_v1_*.xlsx'))
            if not output_files:
                print("✗ No output file with substituted variables found")
                return False
            
            print(f"✓ Variable substitution worked: {output_files[0].name}")
            return True
            
    except Exception as e:
        print(f"✗ Variable substitution test failed: {e}")
        return False


def test_multiple_imports_exports():
    """Test recipe with multiple import and export steps."""
    print("Testing multiple imports and exports...")
    
    try:
        # Create multiple test datasets
        sales_data = pd.DataFrame({
            'Product': ['A', 'B', 'C'],
            'Sales': [100, 200, 150]
        })
        
        inventory_data = pd.DataFrame({
            'Product': ['A', 'B', 'C'],
            'Stock': [50, 25, 75]
        })
        
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            sales_file = temp_path / 'sales.xlsx'
            inventory_file = temp_path / 'inventory.xlsx'
            
            sales_data.to_excel(sales_file, index=False)
            inventory_data.to_excel(inventory_file, index=False)
            
            # Recipe with multiple steps - WITH STAGE DECLARATIONS
            recipe_content = f"""
settings:
  stages:
    - stage_name: "sales_data"
      description: "Sales data from import"
      protected: false
    - stage_name: "inventory_data"
      description: "Inventory data from import"
      protected: false

recipe:
  - step_description: "Import sales data"
    processor_type: "import_file"
    input_file: "{sales_file}"
    save_to_stage: "sales_data"
    
  - step_description: "Export sales summary"
    processor_type: "export_file"
    source_stage: "sales_data"
    output_file: "{temp_path}/sales_summary.xlsx"
    
  - step_description: "Import inventory data"
    processor_type: "import_file"
    input_file: "{inventory_file}"
    save_to_stage: "inventory_data"
    
  - step_description: "Export inventory report"
    processor_type: "export_file"
    source_stage: "inventory_data"
    output_file: "{temp_path}/inventory_report.xlsx"
"""
            
            recipe_file = temp_path / 'multi_test_recipe.yaml'
            with open(recipe_file, 'w') as f:
                f.write(recipe_content)
            
            pipeline = RecipePipeline()
            completion_report = pipeline.run_complete_recipe(recipe_file)
            
            if not completion_report.get('execution_successful', False):
                print("✗ Multiple import/export recipe failed")
                return False
            
            if completion_report.get('steps_executed', 0) != 4:
                print(f"✗ Expected 4 steps, got {completion_report.get('steps_executed', 0)}")
                return False
            
            # Check that both output files were created
            sales_output = temp_path / 'sales_summary.xlsx'
            inventory_output = temp_path / 'inventory_report.xlsx'
            
            if not sales_output.exists():
                print("✗ Sales output file not created")
                return False
                
            if not inventory_output.exists():
                print("✗ Inventory output file not created")
                return False
            
            print("✓ Multiple imports and exports worked correctly")
            return True
            
    except Exception as e:
        print(f"✗ Multiple imports/exports test failed: {e}")
        return False


def main():
    """Run all tests."""
    print("RecipePipeline Test Suite")
    print("=" * 40)
    
    tests = [
        test_basic_recipe_execution,
        test_variable_substitution,
        test_multiple_imports_exports,
    ]
    
    passed = 0
    total = len(tests)
    
    for test_func in tests:
        try:
            if test_func():
                passed += 1
            print()  # Add spacing between tests
        except Exception as e:
            print(f"✗ Test {test_func.__name__} crashed: {e}")
            print()
    
    print("=" * 40)
    print(f"Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All RecipePipeline tests passed!")
        print("✓ Stage-based architecture is working correctly")
        return True
    else:
        print("❌ Some RecipePipeline tests failed!")
        return False


if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)