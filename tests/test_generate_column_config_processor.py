"""
Test the GenerateColumnConfigProcessor functionality.

tests/test_generate_column_config_processor.py
"""

import os
import pandas as pd
import tempfile

from pathlib import Path

from excel_recipe_processor.core.stage_manager import StageManager
from excel_recipe_processor.processors.generate_column_config_processor import GenerateColumnConfigProcessor


def create_raw_data():
    """Create sample data with raw/technical column names."""
    return pd.DataFrame({
        'cust_id': [1, 2, 3],
        'ord_dt': ['2024-01-01', '2024-01-02', '2024-01-03'], 
        'prod_sku': ['A001', 'B002', 'C003'],
        'qty': [10, 20, 15],
        'amt_usd': [100.50, 250.00, 175.25],
        'ship_st': ['Pending', 'Shipped', 'Delivered']
    })


def create_desired_data():
    """Create sample data with desired business-friendly column names."""
    return pd.DataFrame({
        'Customer Code': [1, 2, 3],
        'Order Date': ['2024-01-01', '2024-01-02', '2024-01-03'],
        'Product SKU': ['A001', 'B002', 'C003'], 
        'Quantity': [10, 20, 15],
        'Amount (USD)': [100.50, 250.00, 175.25],
        'Shipping Status': ['Pending', 'Shipped', 'Delivered'],
        'Sales Rep': ['', '', ''],  # New column to be created
        'Region': ['', '', ''],     # Another new column to be created
        'Notes': ['', '', '']       # Third new column to be created
    })


def test_basic_column_config_generation():
    """Test basic column configuration generation."""
    
    print("Testing basic column configuration generation...")
    
    StageManager.initialize_stages()
    
    try:
        # Create and save test data to stages
        raw_data = create_raw_data()
        desired_data = create_desired_data()
        
        StageManager.save_stage('raw_download', raw_data, description='Raw data from database')
        StageManager.save_stage('target_format', desired_data, description='Desired column format')
        
        # Create temporary output file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as tmp_file:
            output_file = tmp_file.name
        
        try:
            # Test the processor
            step_config = {
                'processor_type': 'generate_column_config',
                'source_stage': 'raw_download',
                'target_stage': 'target_format',
                'output_file': output_file,
                'similarity_threshold': 0.7  # Lower threshold to catch more renames in test
            }
            
            processor = GenerateColumnConfigProcessor(step_config)
            result = processor.execute(None)
            
            # Check that output file was created
            if Path(output_file).exists():
                print("✓ Configuration file created successfully")
            else:
                print("✗ Configuration file was not created")
                return False
            
            # Read and display the generated configuration
            with open(output_file, 'r') as f:
                content = f.read()
                print("\n--- Generated Configuration ---")
                print(content)
                print("--- End Configuration ---\n")
            
            # Check for expected content
            if 'var_columns_raw:' in content and 'var_columns_to_keep:' in content:
                print("✓ Column lists generated correctly")
            else:
                print("✗ Column lists missing from output")
                return False
            
            if 'var_columns_to_create:' in content:
                print("✓ To-create columns list generated")
            else:
                print("✗ To-create columns list missing")
                return False
            
            if 'var_rename_mapping:' in content:
                print("✓ Rename mapping generated")
            else:
                print("✗ Rename mapping missing")
                return False
            
            # Check for specific expected renames (fuzzy matching should catch these)
            expected_renames = ['cust_id', 'ord_dt', 'qty']
            renames_found = sum(1 for rename in expected_renames if rename in content)
            
            if renames_found >= 2:  # At least 2 of the 3 should be detected
                print(f"✓ Fuzzy matching detected {renames_found} potential renames")
            else:
                print(f"✗ Only {renames_found} renames detected, expected more")
                return False
            
            return True
            
        finally:
            # Clean up temp file
            if Path(output_file).exists():
                os.unlink(output_file)
    
    finally:
        StageManager.cleanup_stages()


def test_with_recipe_section():
    """Test configuration generation with optional recipe section."""
    
    print("\nTesting configuration with recipe section...")
    
    StageManager.initialize_stages()
    
    try:
        # Create and save test data
        raw_data = create_raw_data()
        desired_data = create_desired_data()
        
        StageManager.save_stage('source_data', raw_data, description='Source data')
        StageManager.save_stage('target_data', desired_data, description='Target data')
        
        # Create temporary output file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as tmp_file:
            output_file = tmp_file.name
        
        try:
            # Test with recipe section enabled
            step_config = {
                'processor_type': 'generate_column_config',
                'source_stage': 'source_data',
                'target_stage': 'target_data', 
                'output_file': output_file,
                'include_recipe_section': True
            }
            
            processor = GenerateColumnConfigProcessor(step_config)
            result = processor.execute(None)
            
            # Read generated content
            with open(output_file, 'r') as f:
                content = f.read()
            
            # Check for recipe section
            if 'recipe:' in content and 'rename_columns' in content:
                print("✓ Recipe section generated successfully")
                return True
            else:
                print("✗ Recipe section missing or incomplete")
                return False
                
        finally:
            if Path(output_file).exists():
                os.unlink(output_file)
    
    finally:
        StageManager.cleanup_stages()


def test_edge_cases():
    """Test edge cases and error conditions."""
    
    print("\nTesting edge cases...")
    
    # Test with missing target stage
    try:
        step_config = {
            'processor_type': 'generate_column_config',
            'source_stage': 'nonexistent',
            'output_file': 'test.yaml'
            # Missing target_stage
        }
        processor = GenerateColumnConfigProcessor(step_config)
        print("✗ Should have failed with missing target_stage")
        return False
    except Exception:
        print("✓ Properly validates missing target_stage")
    
    # Test with missing output file
    try:
        step_config = {
            'processor_type': 'generate_column_config',
            'source_stage': 'test',
            'target_stage': 'test'
            # Missing output_file
        }
        processor = GenerateColumnConfigProcessor(step_config)
        print("✗ Should have failed with missing output_file")
        return False
    except Exception:
        print("✓ Properly validates missing output_file")
    
    return True


def main():
    """Run all tests and report results."""
    
    print("=== GenerateColumnConfigProcessor Tests ===\n")
    
    tests = [
        test_basic_column_config_generation,
        test_with_recipe_section,
        test_edge_cases
    ]
    
    passed = 0
    
    for test_func in tests:
        try:
            if test_func():
                passed += 1
                print(f"✓ {test_func.__name__} passed\n")
            else:
                print(f"✗ {test_func.__name__} failed\n")
        except Exception as e:
            print(f"✗ {test_func.__name__} failed with error: {e}\n")
    
    print(f"=== Results: {passed}/{len(tests)} tests passed ===")
    
    if passed == len(tests):
        print("\n✅ All GenerateColumnConfigProcessor tests passed!")
        return 1
    else:
        print("\n❌ Some GenerateColumnConfigProcessor tests failed!")
        return 0


if __name__ == "__main__":
    exit(main())


# End of file #
