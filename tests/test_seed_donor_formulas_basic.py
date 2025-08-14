#!/usr/bin/env python3
"""
Basic validation tests for SeedDonorFormulasProcessor.

tests/test_seed_donor_formulas_basic.py

Tests processor initialization, configuration validation, and basic functionality
without requiring complex Excel file setups.
"""

import os
import sys
import tempfile
import pandas as pd

from pathlib import Path

# Add project root to Python path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from excel_recipe_processor.core.base_processor import StepProcessorError
from excel_recipe_processor.processors.seed_donor_formulas_processor import SeedDonorFormulasProcessor


def test_processor_initialization():
    """Test basic processor initialization and configuration validation."""
    print("Testing processor initialization...")
    
    # Test valid minimal configuration
    try:
        step_config = {
            'processor_type': 'seed_donor_formulas',
            'step_description': 'Test initialization',
            'source_file': 'templates/budget_template.xlsx',
            'source_sheet': 'Summary',
            'target_file': 'output/new_budget.xlsx',
            'target_sheet': 'Summary',
            'columns': ['C', 'D'],
            'start_row': 2,
            'row_count': 3
        }
        
        processor = SeedDonorFormulasProcessor(step_config)
        print("✓ Processor initialized with valid config")
        return True
        
    except Exception as e:
        print(f"✗ Processor initialization failed: {e}")
        return False


def test_configuration_validation():
    """Test configuration parameter validation."""
    print("Testing configuration validation...")
    
    # Test missing required fields
    required_fields = ['source_file', 'source_sheet', 'target_file', 'target_sheet', 'columns']
    
    base_config = {
        'processor_type': 'seed_donor_formulas',
        'step_description': 'Validation test',
        'source_file': 'templates/budget_template.xlsx',
        'source_sheet': 'Summary',
        'target_file': 'output/new_budget.xlsx',
        'target_sheet': 'Summary',
        'columns': ['C', 'D'],
        'start_row': 2,
        'row_count': 3
    }
    
    for field in required_fields:
        test_config = base_config.copy()
        del test_config[field]
        
        try:
            processor = SeedDonorFormulasProcessor(test_config)
            print(f"✗ Should have failed without required field: {field}")
            return False
        except StepProcessorError as e:
            if field in str(e):
                print(f"✓ Correctly caught missing field: {field}")
            else:
                print(f"✗ Wrong error for missing {field}: {e}")
                return False
    
    # Test row_count limit
    test_config = base_config.copy()
    test_config['row_count'] = 15  # Over the limit of 10
    
    try:
        processor = SeedDonorFormulasProcessor(test_config)
        print("✗ Should have failed with row_count > 10")
        return False
    except StepProcessorError as e:
        if "exceed 10" in str(e):
            print("✓ Correctly caught row_count limit")
        else:
            print(f"✗ Wrong error for row_count limit: {e}")
            return False
    
    # Test invalid start_row
    test_config = base_config.copy()
    test_config['start_row'] = 0  # Should be 1-indexed
    
    try:
        processor = SeedDonorFormulasProcessor(test_config)
        print("✗ Should have failed with start_row < 1")
        return False
    except StepProcessorError as e:
        if "1 or greater" in str(e):
            print("✓ Correctly caught invalid start_row")
        else:
            print(f"✗ Wrong error for invalid start_row: {e}")
            return False
    
    print("✓ Configuration validation tests passed")
    return True


def test_column_resolution_patterns():
    """Test column specification resolution logic."""
    print("Testing column resolution patterns...")
    
    # This test doesn't need actual Excel files, just tests the pattern matching
    try:
        from excel_recipe_processor.processors._helpers.formula_patterns import excel_column_ref_rgx
        
        # Test valid Excel column references
        valid_refs = ['A', 'B', 'Z', 'AA', 'AB', 'ZZ', 'AAA']
        for ref in valid_refs:
            if not excel_column_ref_rgx.match(ref):
                print(f"✗ Should have matched valid Excel ref: {ref}")
                return False
        
        # Test invalid Excel column references
        invalid_refs = ['1', 'A1', '12A', 'a', 'Total_Amount', '']
        for ref in invalid_refs:
            if excel_column_ref_rgx.match(ref):
                print(f"✗ Should not have matched invalid Excel ref: {ref}")
                return False
        
        print("✓ Column pattern matching works correctly")
        return True
        
    except ImportError as e:
        print(f"✗ Could not import formula patterns: {e}")
        return False


def main():
    """Run all validation tests and report results."""
    
    print("=== SeedDonorFormulasProcessor Basic Validation Tests ===\n")
    
    tests = [
        test_processor_initialization,
        test_configuration_validation,
        test_column_resolution_patterns
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
        print("\n✅ All basic validation tests passed!")
        print("💡 Next: Create Excel test files to test actual formula transplant functionality")
        return 1
    else:
        print("\n❌ Some basic validation tests failed!")
        return 0


if __name__ == "__main__":
    exit(main())


# End of file #
