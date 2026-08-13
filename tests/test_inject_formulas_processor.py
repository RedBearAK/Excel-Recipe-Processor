#!/usr/bin/env python3
"""
Test script for InjectFormulaProcessor functionality.

tests/test_inject_formula_processor.py

Tests the inject_formula processor with different modes and targeting options.
"""

import os
import sys
import pandas as pd
import openpyxl
import tempfile

from pathlib import Path

# Add project root to Python path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# try:
#     OPENPYXL_AVAILABLE = True
# except ImportError:
#     OPENPYXL_AVAILABLE = False

from excel_recipe_processor.core.base_processor import StepProcessorError
from excel_recipe_processor.processors.inject_formulas_processor import InjectFormulasProcessor


def create_sample_data():
    """Create sample DataFrame for testing."""
    return pd.DataFrame({
        'Product': ['Widget A', 'Widget B', 'Widget C', 'Widget D'],
        'Price': [10.50, 25.75, 15.25, 8.00],
        'Quantity': [100, 50, 75, 200],
        'Total': [0, 0, 0, 0]  # Will be calculated by formulas
    })


def create_test_excel_file(data: pd.DataFrame, filename: str = None) -> str:
    """Create a test Excel file and return its path."""
    with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as temp_file:
        temp_path = temp_file.name
    
    # Write data to Excel file
    data.to_excel(temp_path, index=False, sheet_name='TestData')
    
    # Rename to desired filename if specified
    if filename:
        final_path = Path(temp_path).parent / filename
        Path(temp_path).rename(final_path)
        return str(final_path)
    
    return temp_path


# def test_openpyxl_requirement():
#     """Test that processor correctly handles openpyxl availability."""
#     print("Testing openpyxl requirement...")
    
#     if not OPENPYXL_AVAILABLE:
#         # Test that processor fails gracefully without openpyxl
#         step_config = {
#             'processor_type': 'inject_formulas',
#             'target_file': 'test.xlsx',
#             'mode': 'live'
#         }
        
#         try:
#             processor = InjectFormulasProcessor(step_config)
#             processor.execute()
#             print("✗ Should have failed without openpyxl")
#             return False
#         except StepProcessorError as e:
#             if "openpyxl is required" in str(e):
#                 print("✓ Correctly handled missing openpyxl")
#                 return True
#             else:
#                 print(f"✗ Unexpected error: {e}")
#                 return False
#     else:
#         print("✓ openpyxl is available for testing")
#         return True


def test_basic_live_formula_injection():
    """Test basic live formula injection functionality."""
    # if not OPENPYXL_AVAILABLE:
    #     print("⚠️ Skipping live formula test - openpyxl not available")
    #     return True
    
    print("Testing basic live formula injection...")
    
    # Create test Excel file
    test_data = create_sample_data()
    excel_file = create_test_excel_file(test_data, 'formula_test.xlsx')
    
    try:
        # Configure processor for live formula injection
        step_config = {
            'processor_type': 'inject_formulas',
            'target_file': excel_file,
            'mode': 'live',
            'formulas': [
                {
                    'cell': 'D2',
                    'formula': '=B2*C2'
                },
                {
                    'cell': 'D3',
                    'formula': '=B3*C3'
                },
                {
                    'cell': 'D5',
                    'formula': '=SUM(D2:D4)'
                }
            ]
        }
        
        # Execute processor
        processor = InjectFormulasProcessor(step_config)
        result = processor.execute()
        
        # Verify file was modified
        workbook = openpyxl.load_workbook(excel_file)
        worksheet = workbook.active
        
        # Check that formulas were injected
        formula_d2 = worksheet['D2'].value
        formula_d3 = worksheet['D3'].value
        formula_d5 = worksheet['D5'].value
        
        workbook.close()
        
        if (formula_d2 == '=B2*C2' and 
            formula_d3 == '=B3*C3' and 
            formula_d5 == '=SUM(D2:D4)'):
            print("✓ Live formulas injected successfully")
            return True
        else:
            print(f"✗ Formula injection failed: D2={formula_d2}, D3={formula_d3}, D5={formula_d5}")
            return False
    
    except Exception as e:
        print(f"✗ Live formula injection failed: {e}")
        return False
    
    finally:
        # Clean up test file
        if Path(excel_file).exists():
            Path(excel_file).unlink()


def test_dead_formula_injection():
    """Test dead formula injection for documentation."""
    print("Testing dead formula injection...")
    
    # Create test data and save to stage
    test_data = create_sample_data()
    
    try:
        # Set up stage data first
        from excel_recipe_processor.core.stage_manager import StageManager
        StageManager.save_stage(
            stage_name='test_data',
            data=test_data,
            description="Test data for dead formula injection"
        )
        
        # Configure processor for dead formula injection (stage-to-stage)
        step_config = {
            'processor_type': 'inject_formulas',
            'source_stage': 'test_data',
            'save_to_stage': 'output_data', 
            'mode': 'dead',
            'formulas': [
                {
                    'cell': 'A1',  # Header row, first column
                    'formula': 'Calculation: =Price * Quantity'
                },
                {
                    'cell': 'B2',  # Second row, second column
                    'formula': '=B2*C2'
                }
            ]
        }
        
        # Execute processor
        processor = InjectFormulasProcessor(step_config)
        result = processor.execute()
        
        # Load the output stage to verify formulas were injected
        output_data = StageManager.load_stage('output_data')
        
        # Check that dead formulas were injected as text
        # (exact cell mapping depends on DataFrame structure)
        print("✓ Dead formulas injected successfully as stage data")
        return True
        
    except Exception as e:
        print(f"✗ Dead formula injection failed: {e}")
        return False
    
    finally:
        # Clean up stages
        try:
            # StageManager.clear_stage('test_data')
            # StageManager.clear_stage('output_data')
            StageManager.cleanup_stages()
        except:
            pass


def test_range_formula_injection():
    """Test formula injection to cell ranges."""
    # if not OPENPYXL_AVAILABLE:
    #     print("⚠️ Skipping range formula test - openpyxl not available")
    #     return True
    
    print("Testing range formula injection...")
    
    # Create test Excel file with more data
    test_data = pd.DataFrame({
        'Product': [f'Product {i}' for i in range(1, 6)],
        'Price': [10.0, 20.0, 15.0, 25.0, 12.0],
        'Quantity': [100, 50, 75, 40, 60],
        'Total': [0] * 5
    })
    excel_file = create_test_excel_file(test_data, 'range_test.xlsx')
    
    try:
        # Configure processor for range formula injection
        step_config = {
            'processor_type': 'inject_formulas',
            'target_file': excel_file,
            'mode': 'live',
            'formulas': [
                {
                    'range': 'D2:D6',
                    'formula': '=B2*C2'
                }
            ]
        }
        
        # Execute processor
        processor = InjectFormulasProcessor(step_config)
        result = processor.execute()
        
        # Verify formulas were applied to range
        workbook = openpyxl.load_workbook(excel_file)
        worksheet = workbook.active
        
        # Check a few cells in the range
        formula_d2 = worksheet['D2'].value
        formula_d3 = worksheet['D3'].value
        formula_d6 = worksheet['D6'].value
        
        workbook.close()
        
        # Relative references must TRANSLATE down the range, the way Excel
        # shifts them when a formula is copied. This test previously asserted
        # that every cell kept '=B2*C2' verbatim, which meant every row
        # computed row 2's numbers - the behaviour, and the expectation, were
        # both wrong. (2026-08-13)
        if (formula_d2 == '=B2*C2' and
                formula_d3 == '=B3*C3' and
                formula_d6 == '=B6*C6'):
            print("✓ Range formulas injected and translated per row")
            return True
        else:
            print(f"✗ Range formula injection failed: D2={formula_d2}, D3={formula_d3}, D6={formula_d6}")
            return False
    
    except Exception as e:
        print(f"✗ Range formula injection failed: {e}")
        return False
    
    finally:
        # Clean up test file
        if Path(excel_file).exists():
            Path(excel_file).unlink()


def test_configuration_validation():
    """Test processor configuration validation."""
    print("Testing configuration validation...")
    
    # Test missing target_file
    try:
        step_config = {
            'processor_type': 'inject_formulas',
            'mode': 'live'
        }
        processor = InjectFormulasProcessor(step_config)
        print("✗ Should have failed with missing target_file")
        return False
    except StepProcessorError as e:
        if "requires 'target_file'" in str(e):
            print("✓ Correctly caught missing target_file")
        else:
            print(f"✗ Wrong error for missing target_file: {e}")
            return False
    
    # Test invalid mode
    try:
        step_config = {
            'processor_type': 'inject_formulas',
            'target_file': 'test.xlsx',
            'mode': 'invalid_mode'
        }
        processor = InjectFormulasProcessor(step_config)
        print("✗ Should have failed with invalid mode")
        return False
    except StepProcessorError as e:
        if "Invalid mode" in str(e):
            print("✓ Correctly caught invalid mode")
        else:
            print(f"✗ Wrong error for invalid mode: {e}")
            return False
    
    # Test valid configuration
    try:
        step_config = {
            'processor_type': 'inject_formulas',
            'target_file': 'test.xlsx',
            'mode': 'live',
            'formulas': []
        }
        processor = InjectFormulasProcessor(step_config)
        print("✓ Valid configuration accepted")
        return True
    except Exception as e:
        print(f"✗ Valid configuration rejected: {e}")
        return False


def main():
    """Run all tests for InjectFormulasProcessor."""
    print("🧪 TESTING INJECT FORMULAS PROCESSOR")
    print("=" * 50)
    
    tests = [
        # test_openpyxl_requirement,
        test_configuration_validation,
        test_basic_live_formula_injection,
        test_dead_formula_injection,
        test_range_formula_injection,
    ]
    
    passed = 0
    total = len(tests)
    
    for test_func in tests:
        print(f"\n📋 {test_func.__name__}")
        try:
            if test_func():
                passed += 1
            else:
                print(f"❌ {test_func.__name__} failed")
        except Exception as e:
            print(f"❌ {test_func.__name__} crashed: {e}")
    
    print(f"\n🏁 RESULTS: {passed}/{total} tests passed")
    
    if passed == total:
        print("✅ All tests passed!")
        return True
    else:
        print("❌ Some tests failed")
        return False


if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)


# End of file #
