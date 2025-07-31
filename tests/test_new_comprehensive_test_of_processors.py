#!/usr/bin/env python3
"""
Comprehensive test module for validating all processors with valid stage-based recipe configs.

This module tests the complete stage-based workflow by creating valid recipes for each
processor type and running them through the RecipePipeline.

Creation date: 2025-07-30
"""

import sys
import os
import pandas as pd
import tempfile
import yaml
from pathlib import Path

# Add the parent directory to the path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from excel_recipe_processor.core.stage_manager import StageManager
from excel_recipe_processor.core.base_processor import registry
from excel_recipe_processor.core.recipe_pipeline import RecipePipeline


def create_test_data():
    """Create sample test data for processors."""
    return pd.DataFrame({
        'Product': ['Widget A', 'Widget B', 'Widget C', 'Widget D'],
        'Sales': [100, 200, 150, 75],
        'Region': ['North', 'South', 'North', 'West'],
        'Status': ['Active', 'Active', 'Inactive', 'Active'],
        'Date': pd.to_datetime(['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04']),
        'Category': ['Electronics', 'Hardware', 'Electronics', 'Software']
    })


def create_lookup_data():
    """Create lookup data for testing."""
    return pd.DataFrame({
        'Region': ['North', 'South', 'West', 'East'],
        'Manager': ['Alice', 'Bob', 'Charlie', 'Diana'],
        'Target': [1000, 1500, 800, 1200]
    })


def test_processor_with_recipe(processor_type, recipe_config, test_name):
    """Test a processor with a complete stage-based recipe."""
    print(f"\n  Testing {processor_type} - {test_name}...")
    
    StageManager.initialize_stages()
    
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Create input data files
            input_file = temp_path / 'test_input.xlsx'
            lookup_file = temp_path / 'lookup.xlsx'
            output_file = temp_path / 'output.xlsx'
            
            create_test_data().to_excel(input_file, index=False)
            create_lookup_data().to_excel(lookup_file, index=False)
            
            # Update file paths in recipe config
            recipe_str = yaml.dump(recipe_config)
            recipe_str = recipe_str.replace('{input_file}', str(input_file))
            recipe_str = recipe_str.replace('{lookup_file}', str(lookup_file))
            recipe_str = recipe_str.replace('{output_file}', str(output_file))
            
            recipe_config = yaml.safe_load(recipe_str)
            
            # Save recipe to file
            recipe_file = temp_path / 'test_recipe.yaml'
            with open(recipe_file, 'w') as f:
                yaml.dump(recipe_config, f)
            
            # Execute recipe
            pipeline = RecipePipeline()
            completion_report = pipeline.run_complete_recipe(recipe_file)
            
            # Validate results
            if completion_report.get('execution_successful', False):
                steps_executed = completion_report.get('steps_executed', 0)
                expected_steps = len(recipe_config.get('recipe', []))
                
                if steps_executed == expected_steps:
                    print(f"    ✓ {processor_type} - {test_name} passed ({steps_executed} steps)")
                    return True
                else:
                    print(f"    ✗ {processor_type} - {test_name} - wrong step count: {steps_executed}/{expected_steps}")
                    return False
            else:
                error_msg = completion_report.get('error_message', 'Unknown error')
                print(f"    ✗ {processor_type} - {test_name} failed: {error_msg}")
                return False
                
    except Exception as e:
        print(f"    ✗ {processor_type} - {test_name} exception: {e}")
        return False


def get_processor_test_recipes():
    """Get test recipes for all available processors."""
    
    base_settings = {
        'description': 'Test recipe for processor validation',
        'stages': [
            {'stage_name': 'raw_data', 'description': 'Imported test data', 'protected': False},
            {'stage_name': 'lookup_data', 'description': 'Lookup reference data', 'protected': False},
            {'stage_name': 'processed_data', 'description': 'Processed results', 'protected': False}
        ]
    }
    
    recipes = {
        'import_file': {
            'settings': {
                **base_settings.copy(),
                'stages': [{'stage_name': 'imported_data', 'description': 'Test import', 'protected': False}]
            },
            'recipe': [
                {
                    'step_description': 'Import test data',
                    'processor_type': 'import_file',
                    'input_file': '{input_file}',
                    'save_to_stage': 'imported_data'
                },
                {
                    'step_description': 'Export results',
                    'processor_type': 'export_file',
                    'source_stage': 'imported_data', 
                    'output_file': '{output_file}'
                }
            ]
        },
        
        'filter_data': {
            'settings': base_settings,
            'recipe': [
                {
                    'step_description': 'Import test data',
                    'processor_type': 'import_file',
                    'input_file': '{input_file}',
                    'save_to_stage': 'raw_data'
                },
                {
                    'step_description': 'Filter active products',
                    'processor_type': 'filter_data',
                    'source_stage': 'raw_data',
                    'save_to_stage': 'processed_data',
                    'filters': [
                        {'column': 'Status', 'condition': 'equals', 'value': 'Active'}
                    ]
                },
                {
                    'step_description': 'Export results',
                    'processor_type': 'export_file',
                    'source_stage': 'processed_data',
                    'output_file': '{output_file}'
                }
            ]
        },
        
        'add_calculated_column': {
            'settings': base_settings,
            'recipe': [
                {
                    'step_description': 'Import test data',
                    'processor_type': 'import_file', 
                    'input_file': '{input_file}',
                    'save_to_stage': 'raw_data'
                },
                {
                    'step_description': 'Add revenue calculation',
                    'processor_type': 'add_calculated_column',
                    'source_stage': 'raw_data',
                    'save_to_stage': 'processed_data',
                    'new_column': 'Revenue',
                    'calculation': {
                        'type': 'multiply',
                        'column1': 'Sales',
                        'value': 10
                    }
                },
                {
                    'step_description': 'Export results',
                    'processor_type': 'export_file',
                    'source_stage': 'processed_data',
                    'output_file': '{output_file}'
                }
            ]
        },
        
        'lookup_data': {
            'settings': base_settings,
            'recipe': [
                {
                    'step_description': 'Import main data',
                    'processor_type': 'import_file',
                    'input_file': '{input_file}',
                    'save_to_stage': 'raw_data'
                },
                {
                    'step_description': 'Import lookup data',
                    'processor_type': 'import_file',
                    'input_file': '{lookup_file}',
                    'save_to_stage': 'lookup_data'
                },
                {
                    'step_description': 'Add manager lookup',
                    'processor_type': 'lookup_data',
                    'source_stage': 'raw_data',
                    'save_to_stage': 'processed_data',
                    'lookup_source': {'stage': 'lookup_data'},
                    'lookup_key': 'Region',
                    'source_key': 'Region',
                    'lookup_columns': ['Manager', 'Target']
                },
                {
                    'step_description': 'Export results',
                    'processor_type': 'export_file',
                    'source_stage': 'processed_data',
                    'output_file': '{output_file}'
                }
            ]
        },
        
        'sort_data': {
            'settings': base_settings,
            'recipe': [
                {
                    'step_description': 'Import test data',
                    'processor_type': 'import_file',
                    'input_file': '{input_file}',
                    'save_to_stage': 'raw_data'
                },
                {
                    'step_description': 'Sort by sales descending',
                    'processor_type': 'sort_data',
                    'source_stage': 'raw_data',
                    'save_to_stage': 'processed_data',
                    'columns': [{'column': 'Sales', 'ascending': False}]
                },
                {
                    'step_description': 'Export results',
                    'processor_type': 'export_file',
                    'source_stage': 'processed_data',
                    'output_file': '{output_file}'
                }
            ]
        },
        
        'clean_data': {
            'settings': base_settings,
            'recipe': [
                {
                    'step_description': 'Import test data',
                    'processor_type': 'import_file',
                    'input_file': '{input_file}',
                    'save_to_stage': 'raw_data'
                },
                {
                    'step_description': 'Clean product names',
                    'processor_type': 'clean_data',
                    'source_stage': 'raw_data',
                    'save_to_stage': 'processed_data',
                    'rules': [
                        {
                            'column': 'Product',
                            'operation': 'standardize_text',
                            'standardization': 'uppercase'
                        }
                    ]
                },
                {
                    'step_description': 'Export results',
                    'processor_type': 'export_file',
                    'source_stage': 'processed_data',
                    'output_file': '{output_file}'
                }
            ]
        }
    }
    
    return recipes


def test_all_processors():
    """Test all available processors with valid stage-based recipes."""
    print("🔧 Testing All Processors with Stage-Based Recipes")
    print("=" * 60)
    
    available_processors = registry.get_registered_types()
    test_recipes = get_processor_test_recipes()
    
    print(f"Available processors: {len(available_processors)}")
    print(f"Test recipes created: {len(test_recipes)}")
    
    results = {}
    
    for processor_type in sorted(available_processors):
        print(f"\n📋 Testing {processor_type}...")
        
        if processor_type in test_recipes:
            success = test_processor_with_recipe(
                processor_type, 
                test_recipes[processor_type],
                "full workflow test"
            )
            results[processor_type] = success
        else:
            print(f"  ⚠️  No test recipe available for {processor_type}")
            results[processor_type] = None
    
    return results


def test_multi_processor_workflow():
    """Test a complex workflow using multiple processors."""
    print("\n🔄 Testing Multi-Processor Workflow...")
    
    complex_recipe = {
        'settings': {
            'description': 'Complex multi-step processing workflow',
            'variables': {
                'min_sales': '120',
                'output_prefix': 'processed'
            },
            'stages': [
                {'stage_name': 'raw_data', 'description': 'Raw imported data', 'protected': False},
                {'stage_name': 'filtered_data', 'description': 'Filtered high-sales items', 'protected': False},
                {'stage_name': 'calculated_data', 'description': 'With revenue calculations', 'protected': False},
                {'stage_name': 'sorted_data', 'description': 'Final sorted results', 'protected': False}
            ]
        },
        'recipe': [
            {
                'step_description': 'Import sales data',
                'processor_type': 'import_file',
                'input_file': '{input_file}',
                'save_to_stage': 'raw_data'
            },
            {
                'step_description': 'Filter high-performing products',
                'processor_type': 'filter_data',
                'source_stage': 'raw_data',
                'save_to_stage': 'filtered_data',
                'filters': [
                    {'column': 'Sales', 'condition': 'greater_than', 'value': '{min_sales}'}
                ]
            },
            {
                'step_description': 'Calculate revenue',
                'processor_type': 'add_calculated_column',
                'source_stage': 'filtered_data',
                'save_to_stage': 'calculated_data',
                'new_column': 'Revenue',
                'calculation': {
                    'type': 'multiply',
                    'column1': 'Sales',
                    'value': 12.5
                }
            },
            {
                'step_description': 'Sort by revenue',
                'processor_type': 'sort_data',
                'source_stage': 'calculated_data',
                'save_to_stage': 'sorted_data',
                'columns': [{'column': 'Revenue', 'ascending': False}]
            },
            {
                'step_description': 'Export final results',
                'processor_type': 'export_file',
                'source_stage': 'sorted_data',
                'output_file': '{output_file}'  
            }
        ]
    }
    
    return test_processor_with_recipe('multi_processor', complex_recipe, 'complex workflow')


def main():
    """Run comprehensive processor testing."""
    print("🧪 Comprehensive Recipe Processor Testing")
    print("=" * 60)
    
    # Test individual processors
    processor_results = test_all_processors()
    
    # Test complex workflow
    workflow_success = test_multi_processor_workflow()
    
    # Generate report
    print("\n" + "=" * 60)
    print("📊 TEST RESULTS SUMMARY")
    print("=" * 60)
    
    successful_tests = sum(1 for success in processor_results.values() if success is True)
    failed_tests = sum(1 for success in processor_results.values() if success is False)
    missing_tests = sum(1 for success in processor_results.values() if success is None)
    
    print(f"✅ Successful tests: {successful_tests}")
    print(f"❌ Failed tests: {failed_tests}")
    print(f"⚠️  Missing test recipes: {missing_tests}")
    print(f"🔄 Multi-processor workflow: {'✅ PASSED' if workflow_success else '❌ FAILED'}")
    
    # Show details for failed tests
    if failed_tests > 0:
        print(f"\n❌ Failed Processors:")
        for processor, success in processor_results.items():
            if success is False:
                print(f"   • {processor}")
    
    if missing_tests > 0:
        print(f"\n⚠️  Processors Missing Test Recipes:")
        for processor, success in processor_results.items():
            if success is None:
                print(f"   • {processor}")
    
    # Overall assessment
    total_core_tests = successful_tests + failed_tests
    if total_core_tests > 0:
        success_rate = (successful_tests / total_core_tests) * 100
        print(f"\n🎯 Core Functionality Success Rate: {success_rate:.1f}%")
        
        if success_rate >= 80 and workflow_success:
            print("🎉 SYSTEM STATUS: READY FOR PRODUCTION")
        elif success_rate >= 60:
            print("⚠️  SYSTEM STATUS: MOSTLY FUNCTIONAL - Address failed tests")
        else:
            print("❌ SYSTEM STATUS: NEEDS MAJOR FIXES")
    
    return successful_tests > 0 and workflow_success


if __name__ == '__main__':
    success = main()
    print(f"\nResult: {'PASS' if success else 'FAIL'}")
