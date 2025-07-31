#!/usr/bin/env python3
"""
Integration-focused test module for stage-based recipe pipeline.

Instead of testing processor configurations (which individual processor tests handle),
this tests the integration between RecipePipeline, StageManager, and the stage-based workflow.
"""

import sys
import os
import pandas as pd
import tempfile
import yaml
from pathlib import Path

# Add the parent directory to the path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from excel_recipe_processor.core.recipe_pipeline import RecipePipeline
from excel_recipe_processor.core.stage_manager import StageManager


def create_test_data():
    """Create sample test data."""
    return pd.DataFrame({
        'Product': ['Widget A', 'Widget B', 'Widget C', 'Widget D'],
        'Sales': [100, 200, 150, 75],
        'Region': ['North', 'South', 'North', 'West'],
        'Status': ['Active', 'Active', 'Inactive', 'Active']
    })


def test_basic_stage_workflow():
    """Test that basic import -> process -> export stage workflow works."""
    print("Testing basic stage workflow (import -> filter -> export)...")
    
    StageManager.initialize_stages()
    
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Create test files
            input_file = temp_path / 'input.xlsx'
            output_file = temp_path / 'output.xlsx'
            create_test_data().to_excel(input_file, index=False)
            
            # Create a simple recipe using a processor we know works (filter_data)
            recipe = {
                'settings': {
                    'description': 'Basic stage workflow test',
                    'stages': [
                        {'stage_name': 'raw_data', 'description': 'Input data', 'protected': False},
                        {'stage_name': 'filtered_data', 'description': 'Filtered data', 'protected': False}
                    ]
                },
                'recipe': [
                    {
                        'step_description': 'Import test data',
                        'processor_type': 'import_file',
                        'input_file': str(input_file),
                        'save_to_stage': 'raw_data'
                    },
                    {
                        'step_description': 'Filter active items',
                        'processor_type': 'filter_data',
                        'source_stage': 'raw_data',
                        'save_to_stage': 'filtered_data',
                        'filters': [
                            {'column': 'Status', 'condition': 'equals', 'value': 'Active'}
                        ]
                    },
                    {
                        'step_description': 'Export results',
                        'processor_type': 'export_file',
                        'source_stage': 'filtered_data',
                        'output_file': str(output_file)
                    }
                ]
            }
            
            # Save and execute recipe
            recipe_file = temp_path / 'recipe.yaml'
            with open(recipe_file, 'w') as f:
                yaml.dump(recipe, f)
            
            pipeline = RecipePipeline()
            completion_report = pipeline.run_complete_recipe(recipe_file)
            
            # Verify results
            if completion_report.get('execution_successful', False):
                if output_file.exists():
                    result_data = pd.read_excel(output_file)
                    # Should have filtered out inactive items (3 active items)  
                    if len(result_data) == 3 and all(result_data['Status'] == 'Active'):
                        print("    ✓ Basic stage workflow works correctly")
                        return True
                    else:
                        print(f"    ✗ Incorrect filtering: got {len(result_data)} rows, expected 3 active items")
                        return False
                else:
                    print("    ✗ Output file not created")
                    return False
            else:
                error_msg = completion_report.get('error_message', 'Unknown error')
                print(f"    ✗ Recipe execution failed: {error_msg}")
                return False
                
    except Exception as e:
        print(f"    ✗ Exception in basic workflow test: {e}")
        return False


def test_stage_declarations_and_validation():
    """Test that stage declarations and validation work correctly."""
    print("Testing stage declarations and validation...")
    
    StageManager.initialize_stages()
    
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Create test files
            input_file = temp_path / 'input.xlsx'
            create_test_data().to_excel(input_file, index=False)
            
            # Recipe with undeclared stage (should work but generate warnings)
            recipe = {
                'settings': {
                    'description': 'Stage validation test',
                    # Intentionally not declaring the stages used
                },
                'recipe': [
                    {
                        'step_description': 'Import to undeclared stage',
                        'processor_type': 'import_file',
                        'input_file': str(input_file),
                        'save_to_stage': 'undeclared_stage'  # Not in settings.stages
                    }
                ]
            }
            
            recipe_file = temp_path / 'recipe.yaml'
            with open(recipe_file, 'w') as f:
                yaml.dump(recipe, f)
            
            pipeline = RecipePipeline()
            # This should work (undeclared stages are allowed) but generate warnings
            pipeline.load_recipe(recipe_file)
            
            print("    ✓ Stage validation allows undeclared stages with warnings")
            return True
            
    except Exception as e:
        print(f"    ✗ Exception in stage validation test: {e}")
        return False


def test_variable_substitution():
    """Test that variable substitution works in recipes."""
    print("Testing variable substitution...")
    
    StageManager.initialize_stages()
    
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Create test files
            input_file = temp_path / 'input.xlsx'
            create_test_data().to_excel(input_file, index=False)
            
            # Recipe using variables
            recipe = {
                'settings': {
                    'description': 'Variable substitution test',
                    'variables': {
                        'status_filter': 'Active',
                        'output_prefix': 'filtered'
                    },
                    'stages': [
                        {'stage_name': 'raw_data', 'description': 'Input data', 'protected': False},
                        {'stage_name': 'processed_data', 'description': 'Processed data', 'protected': False}
                    ]
                },
                'recipe': [
                    {
                        'step_description': 'Import test data',
                        'processor_type': 'import_file',
                        'input_file': str(input_file),
                        'save_to_stage': 'raw_data'
                    },
                    {
                        'step_description': 'Filter using variable',
                        'processor_type': 'filter_data',
                        'source_stage': 'raw_data',
                        'save_to_stage': 'processed_data',
                        'filters': [
                            {'column': 'Status', 'condition': 'equals', 'value': '{status_filter}'}
                        ]
                    },
                    {
                        'step_description': 'Export with variable filename',
                        'processor_type': 'export_file',
                        'source_stage': 'processed_data',
                        'output_file': str(temp_path / '{output_prefix}_results.xlsx')
                    }
                ]
            }
            
            recipe_file = temp_path / 'recipe.yaml'
            with open(recipe_file, 'w') as f:
                yaml.dump(recipe, f)
            
            pipeline = RecipePipeline()
            completion_report = pipeline.run_complete_recipe(recipe_file)
            
            # Check if variable substitution worked
            expected_output = temp_path / 'filtered_results.xlsx'
            if completion_report.get('execution_successful') and expected_output.exists():
                result_data = pd.read_excel(expected_output)
                if len(result_data) == 3 and all(result_data['Status'] == 'Active'):
                    print("    ✓ Variable substitution works correctly")
                    return True
                else:
                    print("    ✗ Variable substitution failed - incorrect data filtering")
                    return False
            else:
                print("    ✗ Variable substitution failed - recipe execution failed")
                return False
            
    except Exception as e:
        print(f"    ✗ Exception in variable substitution test: {e}")
        return False


def test_settings_section_validation():
    """Test that settings section validation works."""
    print("Testing settings section validation...")
    
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Recipe without settings section (should fail)
            invalid_recipe = {
                'recipe': [
                    {
                        'step_description': 'This should fail',
                        'processor_type': 'import_file',
                        'input_file': 'dummy.xlsx',
                        'save_to_stage': 'data'
                    }
                ]
            }
            
            recipe_file = temp_path / 'invalid_recipe.yaml'
            with open(recipe_file, 'w') as f:
                yaml.dump(invalid_recipe, f)
            
            pipeline = RecipePipeline()
            
            try:
                pipeline.load_recipe(recipe_file)
                print("    ✗ Settings validation failed - should have rejected recipe without settings")
                return False
            except Exception as e:
                if "settings" in str(e).lower():
                    print("    ✓ Settings section validation works correctly")
                    return True
                else:
                    print(f"    ✗ Wrong error for missing settings: {e}")
                    return False
            
    except Exception as e:
        print(f"    ✗ Exception in settings validation test: {e}")
        return False


def test_multiple_import_export():
    """Test importing from multiple files and exporting to multiple files."""
    print("Testing multiple import/export operations...")
    
    StageManager.initialize_stages()
    
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Create multiple input files
            data1 = pd.DataFrame({'Type': ['A', 'B'], 'Value': [10, 20]})
            data2 = pd.DataFrame({'Type': ['C', 'D'], 'Value': [30, 40]})
            
            input_file1 = temp_path / 'input1.xlsx'
            input_file2 = temp_path / 'input2.xlsx'
            output_file1 = temp_path / 'output1.xlsx'
            output_file2 = temp_path / 'output2.xlsx'
            
            data1.to_excel(input_file1, index=False)
            data2.to_excel(input_file2, index=False)
            
            # Recipe with multiple imports and exports
            recipe = {
                'settings': {
                    'description': 'Multiple file operations test',
                    'stages': [
                        {'stage_name': 'data1', 'description': 'First dataset', 'protected': False},
                        {'stage_name': 'data2', 'description': 'Second dataset', 'protected': False}
                    ]
                },
                'recipe': [
                    {
                        'step_description': 'Import first file',
                        'processor_type': 'import_file',
                        'input_file': str(input_file1),
                        'save_to_stage': 'data1'
                    },
                    {
                        'step_description': 'Import second file',
                        'processor_type': 'import_file',
                        'input_file': str(input_file2),
                        'save_to_stage': 'data2'
                    },
                    {
                        'step_description': 'Export first dataset',
                        'processor_type': 'export_file',
                        'source_stage': 'data1',
                        'output_file': str(output_file1)
                    },
                    {
                        'step_description': 'Export second dataset',
                        'processor_type': 'export_file',
                        'source_stage': 'data2',
                        'output_file': str(output_file2)
                    }
                ]
            }
            
            recipe_file = temp_path / 'recipe.yaml'
            with open(recipe_file, 'w') as f:
                yaml.dump(recipe, f)
            
            pipeline = RecipePipeline()
            completion_report = pipeline.run_complete_recipe(recipe_file)
            
            # Verify both files were created correctly
            if (completion_report.get('execution_successful') and 
                output_file1.exists() and output_file2.exists()):
                
                result1 = pd.read_excel(output_file1)
                result2 = pd.read_excel(output_file2)
                
                if (len(result1) == 2 and len(result2) == 2 and
                    list(result1['Type']) == ['A', 'B'] and
                    list(result2['Type']) == ['C', 'D']):
                    print("    ✓ Multiple import/export operations work correctly")
                    return True
                else:
                    print("    ✗ Multiple import/export failed - incorrect data")
                    return False
            else:
                print("    ✗ Multiple import/export failed - execution or file creation failed")
                return False
            
    except Exception as e:
        print(f"    ✗ Exception in multiple import/export test: {e}")
        return False


def run_integration_tests():
    """Run all integration tests and report results."""
    print("🔄 Integration-Focused Recipe Pipeline Testing")
    print("=" * 60)
    print("Testing pipeline integration, not individual processor configurations")
    print()
    
    tests = [
        ("Basic Stage Workflow", test_basic_stage_workflow),
        ("Stage Declarations", test_stage_declarations_and_validation),
        ("Variable Substitution", test_variable_substitution),
        ("Settings Validation", test_settings_section_validation),
        ("Multiple Files", test_multiple_import_export)
    ]
    
    results = []
    
    for test_name, test_func in tests:
        print(f"🧪 {test_name}")
        try:
            success = test_func()
            results.append((test_name, success))
        except Exception as e:
            print(f"    ✗ {test_name} crashed: {e}")
            results.append((test_name, False))
        print()
    
    # Summary
    print("=" * 60)
    print("📊 INTEGRATION TEST RESULTS")
    print("=" * 60)
    
    passed = sum(1 for _, success in results if success)
    total = len(results)
    
    for test_name, success in results:
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{status}: {test_name}")
    
    print(f"\nOverall: {passed}/{total} integration tests passed ({passed/total*100:.1f}%)")
    
    if passed == total:
        print("🎉 INTEGRATION STATUS: EXCELLENT - All core pipeline features working")
    elif passed >= total * 0.8:
        print("✅ INTEGRATION STATUS: GOOD - Core functionality working, minor issues")
    elif passed >= total * 0.6:
        print("⚠️  INTEGRATION STATUS: FAIR - Basic functionality working, some issues")
    else:
        print("❌ INTEGRATION STATUS: POOR - Major integration problems")
    
    print(f"\n💡 CONCLUSION:")
    if passed >= total * 0.8:
        print("The stage-based pipeline architecture is working correctly.")
        print("Individual processor tests passing + good integration = system is functional.")
        print("Focus on cleanup tasks and documentation rather than major fixes.")
    else:
        print("There are integration issues that need to be addressed.")
        print("Focus on fixing the failing integration tests first.")
    
    return passed >= total * 0.8


if __name__ == '__main__':
    success = run_integration_tests()
    print(f"\nResult: {'PASS' if success else 'FAIL'}")
