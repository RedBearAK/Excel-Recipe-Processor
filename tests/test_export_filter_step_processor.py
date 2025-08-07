#!/usr/bin/env python3
"""
Test module for ExportFilterStepProcessor.

tests/test_export_filter_step_processor.py

Tests filter step generation from reviewed filter terms including YAML/JSON output,
file creation, and various configuration options.
"""

import os
import sys
import json
import yaml
import pandas as pd
import tempfile

from pathlib import Path

# Add project root to Python path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from excel_recipe_processor.core.stage_manager import StageManager
from excel_recipe_processor.core.base_processor import StepProcessorError
from excel_recipe_processor.processors.export_filter_step_processor import ExportFilterStepProcessor


def setup_test_stages():
    """Set up test stages for export filter step tests."""
    StageManager.cleanup_stages()
    StageManager.initialize_stages(max_stages=20)


def create_reviewed_filter_terms():
    """Create test data simulating reviewed filter terms."""
    return pd.DataFrame([
        {
            'Column_Name': 'Status',
            'Filter_Term': 'Cancelled',
            'Term_Type': 'categorical_value',
            'Confidence_Score': 85.0,
            'User_Verified': 'KEEP',
            'Exclusion_Reason': '',
            'Notes': 'Clear filter term'
        },
        {
            'Column_Name': 'Status',  
            'Filter_Term': 'Pending',
            'Term_Type': 'categorical_value',
            'Confidence_Score': 78.0,
            'User_Verified': 'KEEP',
            'Exclusion_Reason': '',
            'Notes': 'Good filter'
        },
        {
            'Column_Name': 'Notes',
            'Filter_Term': 'cancelled',
            'Term_Type': 'text_ngram',
            'Confidence_Score': 92.0,
            'User_Verified': 'KEEP',
            'Exclusion_Reason': '',
            'Notes': 'Strong signal'
        },
        {
            'Column_Name': 'Notes',
            'Filter_Term': 'test data',
            'Term_Type': 'text_ngram', 
            'Confidence_Score': 67.0,
            'User_Verified': 'REJECT',
            'Exclusion_Reason': 'Too generic',
            'Notes': 'False positive'
        },
        {
            'Column_Name': '  Product Name  ',  # Test spaces in column names
            'Filter_Term': 'PROTOTYPE',
            'Term_Type': 'categorical_value',
            'Confidence_Score': 88.0,
            'User_Verified': 'KEEP',
            'Exclusion_Reason': '',
            'Notes': 'Removes test products'
        }
    ])


def test_basic_yaml_generation():
    """Test basic YAML filter step generation."""
    print("\nTesting basic YAML generation...")
    
    setup_test_stages()
    
    # Create and save test data
    test_data = create_reviewed_filter_terms()
    StageManager.save_stage('reviewed_terms', test_data, 'Reviewed filter terms')
    
    with tempfile.TemporaryDirectory() as temp_dir:
        output_file = Path(temp_dir) / "test_filter.yaml"
        
        step_config = {
            'processor_type': 'export_filter_step',
            'step_description': 'Test YAML generation',
            'source_stage': 'reviewed_terms',
            'output_file': str(output_file),
            'target_stage': 'stg_raw_data',
            'output_stage': 'stg_cleaned_data'
        }
        
        try:
            processor = ExportFilterStepProcessor(step_config)
            processor.execute_export()
            
            # Check file was created
            if not output_file.exists():
                print("✗ Output file was not created")
                return False
            
            # Check file content
            with open(output_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Should contain YAML structure
            yaml_data = yaml.safe_load(content)
            
            # Check basic structure
            if 'settings' not in yaml_data or 'recipe' not in yaml_data:
                print("✗ Missing required YAML sections")
                return False
            
            # Check filter step structure
            recipe_step = yaml_data['recipe'][0]
            if recipe_step.get('processor_type') != 'filter_data':
                print("✗ Wrong processor type in generated step")
                return False
            
            # Check filters were generated (should have 4 KEEP terms)
            filters = recipe_step.get('filters', [])
            if len(filters) != 4:
                print(f"✗ Expected 4 filters, got {len(filters)}")
                return False
            
            # Check specific filter conditions
            status_filters = [f for f in filters if f['column'] == 'Status']
            if len(status_filters) != 2:
                print(f"✗ Expected 2 Status filters, got {len(status_filters)}")
                return False
            
            # Check condition types
            categorical_filter = next(f for f in filters if f['value'] == 'Cancelled')
            text_filter = next(f for f in filters if f['value'] == 'cancelled')
            
            if categorical_filter['condition'] != 'not_equals':
                print("✗ Categorical filter should use not_equals")
                return False
            
            if text_filter['condition'] != 'not_contains':
                print("✗ Text filter should use not_contains")
                return False
            
            print("✓ Basic YAML generation passed")
            return True
            
        except Exception as e:
            print(f"✗ YAML generation failed: {e}")
            return False
        finally:
            StageManager.cleanup_stages()


def test_json_format_output():
    """Test JSON format output generation."""
    print("\nTesting JSON format output...")
    
    setup_test_stages()
    
    test_data = create_reviewed_filter_terms()
    StageManager.save_stage('json_test_data', test_data, 'Test data for JSON')
    
    with tempfile.TemporaryDirectory() as temp_dir:
        output_file = Path(temp_dir) / "test_filter.json"
        
        step_config = {
            'processor_type': 'export_filter_step',
            'source_stage': 'json_test_data',
            'output_file': str(output_file),
            'output_format': 'json',
            'include_full_recipe': False  # Test step-only
        }
        
        try:
            processor = ExportFilterStepProcessor(step_config)
            processor.execute_export()
            
            # Check file was created
            if not output_file.exists():
                print("✗ JSON output file was not created")
                return False
            
            # Parse JSON content
            with open(output_file, 'r', encoding='utf-8') as f:
                json_data = json.load(f)
            
            # Should be step-only format (no settings section)
            if 'settings' in json_data:
                print("✗ Step-only format should not have settings section")
                return False
            
            # Check step structure
            if json_data.get('processor_type') != 'filter_data':
                print("✗ Wrong processor type in JSON step")
                return False
            
            # Check filters
            filters = json_data.get('filters', [])
            if len(filters) != 4:
                print(f"✗ Expected 4 filters in JSON, got {len(filters)}")
                return False
            
            print("✓ JSON format output passed")
            return True
            
        except Exception as e:
            print(f"✗ JSON format test failed: {e}")
            return False
        finally:
            StageManager.cleanup_stages()


def test_custom_acceptance_criteria():
    """Test custom acceptance column and values."""
    print("\nTesting custom acceptance criteria...")
    
    setup_test_stages()
    
    # Create data with custom acceptance column
    custom_data = pd.DataFrame([
        {
            'Source_Column': 'Notes',
            'Term_Value': 'error',
            'Filter_Type': 'text_ngram',
            'Keep_Filter': 'APPROVED',
            'Confidence': 85.0
        },
        {
            'Source_Column': 'Status',
            'Term_Value': 'Failed',
            'Filter_Type': 'categorical_value',
            'Keep_Filter': 'REJECTED',
            'Confidence': 90.0
        },
        {
            'Source_Column': 'Priority',
            'Term_Value': 'Low',
            'Filter_Type': 'categorical_value',
            'Keep_Filter': 'APPROVED',
            'Confidence': 75.0
        }
    ])
    
    StageManager.save_stage('custom_review', custom_data, 'Custom review format')
    
    with tempfile.TemporaryDirectory() as temp_dir:
        output_file = Path(temp_dir) / "custom_filter.yaml"
        
        step_config = {
            'processor_type': 'export_filter_step',
            'source_stage': 'custom_review',
            'output_file': str(output_file),
            'acceptance_column': 'Keep_Filter',
            'acceptance_values': ['APPROVED'],
            'column_name_field': 'Source_Column',
            'filter_term_field': 'Term_Value',
            'term_type_field': 'Filter_Type'
        }
        
        try:
            processor = ExportFilterStepProcessor(step_config)
            processor.execute_export()
            
            # Parse generated YAML
            with open(output_file, 'r', encoding='utf-8') as f:
                yaml_data = yaml.safe_load(f)
            
            # Should only include APPROVED terms (2 filters)
            filters = yaml_data['recipe'][0]['filters']
            if len(filters) != 2:
                print(f"✗ Expected 2 approved filters, got {len(filters)}")
                return False
            
            # Check specific filters
            filter_values = {f['value'] for f in filters}
            expected_values = {'error', 'Low'}
            if filter_values != expected_values:
                print(f"✗ Expected values {expected_values}, got {filter_values}")
                return False
            
            print("✓ Custom acceptance criteria passed")
            return True
            
        except Exception as e:
            print(f"✗ Custom acceptance test failed: {e}")
            return False
        finally:
            StageManager.cleanup_stages()


def test_empty_data_handling():
    """Test handling of empty or no-accepted-terms data."""
    print("\nTesting empty data handling...")
    
    setup_test_stages()
    
    # Create data with no accepted terms
    empty_data = pd.DataFrame([
        {
            'Column_Name': 'Notes',
            'Filter_Term': 'test',
            'Term_Type': 'text_ngram',
            'User_Verified': 'REJECT',
            'Confidence_Score': 45.0
        }
    ])
    
    StageManager.save_stage('empty_accepted', empty_data, 'No accepted terms')
    
    with tempfile.TemporaryDirectory() as temp_dir:
        output_file = Path(temp_dir) / "empty_filter.yaml"
        
        step_config = {
            'processor_type': 'export_filter_step',
            'source_stage': 'empty_accepted',
            'output_file': str(output_file)
        }
        
        try:
            processor = ExportFilterStepProcessor(step_config)
            processor.execute_export()
            
            # Should create file with empty filters
            if not output_file.exists():
                print("✗ Output file not created for empty data")
                return False
            
            with open(output_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Should mention no filters generated
            if "No filter terms were accepted" not in content:
                print("✗ Empty data message not found")
                return False
            
            print("✓ Empty data handling passed")
            return True
            
        except Exception as e:
            print(f"✗ Empty data test failed: {e}")
            return False
        finally:
            StageManager.cleanup_stages()


def test_error_handling():
    """Test various error conditions."""
    print("\nTesting error handling...")
    
    # Test missing output_file
    try:
        step_config = {
            'processor_type': 'export_filter_step',
            'source_stage': 'test_stage'
            # Missing output_file
        }
        processor = ExportFilterStepProcessor(step_config)
        print("✗ Should have failed with missing output_file")
        return False
    except StepProcessorError as e:
        if "output_file" in str(e):
            print("✓ Caught expected error for missing output_file")
        else:
            print(f"✗ Wrong error for missing output_file: {e}")
            return False
    
    # Test invalid directory creation
    setup_test_stages()
    test_data = create_reviewed_filter_terms()
    StageManager.save_stage('error_test', test_data, 'Error test data')
    
    try:
        # Try to write to invalid path (should create directories)
        output_file = Path("/nonexistent/deeply/nested/path/test.yaml")
        step_config = {
            'processor_type': 'export_filter_step',
            'source_stage': 'error_test',
            'output_file': str(output_file)
        }
        processor = ExportFilterStepProcessor(step_config)
        processor.execute_export()
        print("✗ Should have failed with invalid path")
        return False
    except (StepProcessorError, PermissionError, OSError):
        print("✓ Caught expected error for invalid path")
    finally:
        StageManager.cleanup_stages()
    
    print("✓ Error handling tests passed")
    return True


def test_capabilities_and_examples():
    """Test capabilities and examples methods."""
    print("\nTesting capabilities and examples...")
    
    step_config = {
        'processor_type': 'export_filter_step',
        'source_stage': 'dummy',
        'output_file': 'dummy.yaml'
    }
    
    processor = ExportFilterStepProcessor(step_config)
    
    # Test capabilities
    capabilities = processor.get_capabilities()
    expected_keys = ['description', 'export_formats', 'export_features', 'stage_integration', 'examples']
    
    if not all(key in capabilities for key in expected_keys):
        print("✗ Missing expected capabilities keys")
        return False
    
    # Test examples
    examples = processor.get_usage_examples()
    if not isinstance(examples, dict) or len(examples) < 3:
        print("✗ get_usage_examples method returned insufficient content")
        return False
    
    if 'error' in examples:
        print(f"✗ Examples method returned error: {examples['error']}")
        return False
    
    if 'description' not in examples:
        print("✗ Examples don't contain description")
        return False
    
    print("✓ Capabilities and examples passed")
    return True


def test_column_name_quoting():
    """Test proper handling of column names with spaces and special characters."""
    print("\nTesting column name quoting...")
    
    setup_test_stages()
    
    # Create data with problematic column names
    special_data = pd.DataFrame([
        {
            'Column_Name': '  Notes  ',  # Leading/trailing spaces
            'Filter_Term': 'cancelled',
            'Term_Type': 'text_ngram',
            'User_Verified': 'KEEP',
            'Confidence_Score': 85.0
        },
        {
            'Column_Name': 'Customer Ref #',  # Spaces and special chars
            'Filter_Term': 'TEST-',
            'Term_Type': 'categorical_value',
            'User_Verified': 'KEEP',
            'Confidence_Score': 90.0
        },
        {
            'Column_Name': 'Price($)',  # Special characters
            'Filter_Term': '0.00',
            'Term_Type': 'categorical_value',
            'User_Verified': 'KEEP',
            'Confidence_Score': 75.0
        }
    ])
    
    StageManager.save_stage('special_columns', special_data, 'Special column names')
    
    with tempfile.TemporaryDirectory() as temp_dir:
        output_file = Path(temp_dir) / "special_columns.yaml"
        
        step_config = {
            'processor_type': 'export_filter_step',
            'source_stage': 'special_columns',
            'output_file': str(output_file)
        }
        
        try:
            processor = ExportFilterStepProcessor(step_config)
            processor.execute_export()
            
            # Read and parse generated YAML
            with open(output_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            yaml_data = yaml.safe_load(content)
            
            # Check that column names are preserved correctly (yaml.dump handles quoting automatically)
            filters = yaml_data['recipe'][0]['filters']
            column_names = {f['column'] for f in filters}
            expected_columns = {'  Notes  ', 'Customer Ref #', 'Price($)'}
            
            if column_names != expected_columns:
                print(f"✗ Column name mismatch. Expected {expected_columns}, got {column_names}")
                return False
            
            # Verify that spaces and special characters are preserved in the parsed YAML
            notes_filter = next((f for f in filters if f['column'] == '  Notes  '), None)
            if not notes_filter:
                print("✗ Notes filter with spaces not found")
                return False
            
            special_filter = next((f for f in filters if f['column'] == 'Customer Ref #'), None)
            if not special_filter:
                print("✗ Special character filter not found")
                return False
            
            # Check that the YAML is valid and parseable (most important test)
            if not isinstance(yaml_data, dict) or 'recipe' not in yaml_data:
                print("✗ Generated YAML is not valid or missing recipe section")
                return False
            
            print("✓ Column name quoting passed")
            return True
            
        except Exception as e:
            print(f"✗ Column name quoting test failed: {e}")
            return False
        finally:
            StageManager.cleanup_stages()


if __name__ == '__main__':
    print("🔧 Testing ExportFilterStepProcessor functionality...")
    print("   Tests YAML/JSON generation, file output, custom configurations")
    print("   Validates filter step generation from reviewed filter terms\n")
    
    success = True
    
    success &= test_basic_yaml_generation()
    success &= test_json_format_output()
    success &= test_custom_acceptance_criteria()
    success &= test_empty_data_handling()
    success &= test_error_handling()
    success &= test_capabilities_and_examples()
    success &= test_column_name_quoting()
    
    if success:
        print("\n✅ All ExportFilterStepProcessor tests passed!")
    else:
        print("\n❌ Some ExportFilterStepProcessor tests failed!")
    
    # Show processor summary
    print(f"\nExportFilterStepProcessor Summary:")
    print(f"✓ Generates copy-paste ready filter_data steps")
    print(f"✓ Supports YAML and JSON output formats") 
    print(f"✓ Handles column names with spaces and special characters")
    print(f"✓ Maps categorical_value→not_equals, text_ngram→not_contains")
    print(f"✓ Produces complete recipes or step-only output")


# End of file #
