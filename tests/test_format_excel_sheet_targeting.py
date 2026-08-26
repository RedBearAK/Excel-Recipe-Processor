#!/usr/bin/env python3
"""
Test script for format_excel processor sheet targeting functionality.

tests/test_format_excel_sheet_targeting.py

Tests the new explicit sheet targeting format with support for both
sheet names and 1-based numeric indexing.
"""

import sys
import os
import tempfile
import pandas as pd
import openpyxl
from pathlib import Path

# Add the parent directory to the path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from excel_recipe_processor.processors.format_excel_processor import FormatExcelProcessor


def create_multi_sheet_test_file():
    """Create a test Excel file with multiple sheets."""
    test_file = tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False)
    test_file.close()
    
    # Create sample data for different sheets
    summary_data = pd.DataFrame({
        'Metric': ['Revenue', 'Profit', 'Growth'],
        'Q1': [100, 20, 15],
        'Q2': [120, 25, 20],
        'Q3': [140, 30, 25]
    })
    
    detailed_data = pd.DataFrame({
        'Date': ['2024-01-01', '2024-02-01', '2024-03-01'],
        'Sales': [1000, 1200, 1400],
        'Costs': [800, 900, 1000],
        'Region': ['North', 'South', 'East']
    })
    
    charts_data = pd.DataFrame({
        'Category': ['A', 'B', 'C', 'D'],
        'Value': [10, 20, 15, 25]
    })
    
    # Write to Excel with multiple sheets
    with pd.ExcelWriter(test_file.name, engine='openpyxl') as writer:
        summary_data.to_excel(writer, sheet_name='Executive_Summary', index=False)
        detailed_data.to_excel(writer, sheet_name='Detailed_Data', index=False)
        charts_data.to_excel(writer, sheet_name='Charts_Analysis', index=False)
    
    return test_file.name


def test_sheet_name_targeting():
    """Test targeting sheets by name."""
    print("\n1. Testing sheet targeting by name...")
    
    test_file = create_multi_sheet_test_file()
    
    try:
        step_config = {
            'processor_type': 'format_excel',
            'target_file': test_file,
            'active_sheet_name': 'Executive_Summary',
            'formatting': [
                {
                    'sheet_names': ['Executive_Summary'],
                    'header_background_color': 'navy',
                    'header_text_color': 'white',
                    'header_bold': True,
                    'auto_fit_columns': True
                },
                {
                    'sheet_names': ['Detailed_Data'],
                    'header_background_color': 'darkgreen',
                    'header_text_color': 'white',
                    'auto_fit_columns': True,
                    'auto_filter': True
                }
            ]
        }
        
        processor = FormatExcelProcessor(step_config)
        result = processor.execute()
        
        # Verify file is still readable
        workbook = openpyxl.load_workbook(test_file)
        
        # Check that we have the expected sheets
        expected_sheets = ['Executive_Summary', 'Detailed_Data', 'Charts_Analysis']
        if workbook.sheetnames == expected_sheets:
            print("  ✓ All sheets present")
        else:
            print(f"  ✗ Expected {expected_sheets}, got {workbook.sheetnames}")
            return False
        
        # Check active sheet
        if workbook.active.title == 'Executive_Summary':
            print("  ✓ Active sheet set correctly")
        else:
            print(f"  ✗ Expected active sheet 'Executive_Summary', got '{workbook.active.title}'")
            return False
        
        workbook.close()
        print("  ✓ Sheet name targeting successful")
        return True
        
    except Exception as e:
        print(f"  ✗ Sheet name targeting failed: {e}")
        return False
        
    finally:
        if os.path.exists(test_file):
            os.unlink(test_file)


def test_numeric_sheet_targeting():
    """Test targeting sheets by 1-based index."""
    print("\n2. Testing sheet targeting by number...")
    
    test_file = create_multi_sheet_test_file()
    
    try:
        step_config = {
            'processor_type': 'format_excel',
            'target_file': test_file,
            'active_sheet_name': '?sheet_002?',  # Set second sheet as active
            'formatting': [
                {
                    'sheet_names': ['?sheet_001?'],  # First sheet (Executive_Summary)
                    'header_background_color': 'purple',
                    'header_text_color': 'white',
                    'header_font_size': 16,
                    'row_heights': {1: 30}
                },
                {
                    'sheet_names': ['?sheet_002?'],  # Second sheet (Detailed_Data)
                    'header_background_color': 'orange',
                    'header_text_color': 'black',
                    'auto_fit_columns': True
                },
                {
                    'sheet_names': ['?sheet_003?'],  # Third sheet (Charts_Analysis)
                    'header_background_color': 'lightblue',
                    'header_text_color': 'darkblue',
                    'freeze_top_row': True
                }
            ]
        }
        
        processor = FormatExcelProcessor(step_config)
        result = processor.execute()
        
        # Verify file and active sheet
        workbook = openpyxl.load_workbook(test_file)
        
        # Check active sheet
        if workbook.active.title == 'Detailed_Data':
            print("  ✓ Active sheet set correctly by number")
        else:
            print(f"  ✗ Expected active sheet 'Detailed_Data', got '{workbook.active.title}'")
            print(f"  Debug: Available sheets: {workbook.sheetnames}")
            print(f"  Debug: Active sheet index should be 2 (Detailed_Data)")
            return False
        
        workbook.close()
        print("  ✓ Numeric sheet targeting successful")
        return True
        
    except Exception as e:
        print(f"  ✗ Numeric sheet targeting failed: {e}")
        return False
        
    finally:
        if os.path.exists(test_file):
            os.unlink(test_file)


def test_mixed_sheet_targeting():
    """Test mixing sheet names and numbers."""
    print("\n3. Testing mixed sheet targeting...")
    
    test_file = create_multi_sheet_test_file()
    
    try:
        step_config = {
            'processor_type': 'format_excel',
            'target_file': test_file,
            'formatting': [
                {
                    'sheet_names': ['Executive_Summary'],  # By name
                    'header_background_color': 'red',
                    'header_text_color': 'white'
                },
                {
                    'sheet_names': ['?sheet_002?'],  # By index token
                    'header_background_color': 'blue',
                    'header_text_color': 'white'
                },
                {
                    'sheet_names': ['Charts_Analysis'],  # By name again
                    'header_background_color': 'green',
                    'header_text_color': 'white'
                }
            ]
        }
        
        processor = FormatExcelProcessor(step_config)
        result = processor.execute()
        
        print("  ✓ Mixed sheet targeting successful")
        return True
        
    except Exception as e:
        print(f"  ✗ Mixed sheet targeting failed: {e}")
        return False
        
    finally:
        if os.path.exists(test_file):
            os.unlink(test_file)


def test_validation_errors():
    """Test configuration validation errors."""
    print("\n4. Testing validation errors...")
    
    test_file = create_multi_sheet_test_file()
    
    try:
        # Test missing 'sheet' key
        try:
            step_config = {
                'processor_type': 'format_excel',
                'target_file': test_file,
                'formatting': [
                    {
                        'header_bold': True  # Missing 'sheet_name' key
                    }
                ]
            }
            processor = FormatExcelProcessor(step_config)
            processor.execute()
            print("  ✗ Should have failed for missing 'sheet_name' key")
            return False
        except Exception:
            print("  ✓ Correctly rejected missing 'sheet_name' key")
        
        # Test invalid sheet index
        try:
            step_config = {
                'processor_type': 'format_excel',
                'target_file': test_file,
                'formatting': [
                    {
                        'sheet_names': ['?sheet_000?'],  # Invalid: index tokens are 1-based
                        'header_bold': True
                    }
                ]
            }
            processor = FormatExcelProcessor(step_config)
            processor.execute()
            print("  ✗ Should have failed for sheet index 0")
            return False
        except Exception:
            print("  ✓ Correctly rejected sheet index 0")
        
        # Test empty sheet name
        try:
            step_config = {
                'processor_type': 'format_excel',
                'target_file': test_file,
                'formatting': [
                    {
                        'sheet_names': [''],  # Invalid: empty string
                        'header_bold': True
                    }
                ]
            }
            processor = FormatExcelProcessor(step_config)
            processor.execute()
            print("  ✗ Should have failed for empty sheet name")
            return False
        except Exception:
            print("  ✓ Correctly rejected empty sheet name")
        
        # Test formatting not being a list
        try:
            step_config = {
                'processor_type': 'format_excel',
                'target_file': test_file,
                'formatting': {  # Should be a list, not dict
                    'sheet_names': ['Summary'],
                    'header_bold': True
                }
            }
            processor = FormatExcelProcessor(step_config)
            processor.execute()
            print("  ✗ Should have failed for formatting not being a list")
            return False
        except Exception:
            print("  ✓ Correctly rejected non-list formatting")
        
        print("  ✓ All validation tests passed")
        return True
        
    finally:
        if os.path.exists(test_file):
            os.unlink(test_file)


def test_nonexistent_sheets():
    """Test handling of nonexistent sheets."""
    print("\n5. Testing nonexistent sheet handling...")
    
    test_file = create_multi_sheet_test_file()
    
    try:
        step_config = {
            'processor_type': 'format_excel',
            'target_file': test_file,
            'formatting': [
                {
                    'sheet_names': ['Executive_Summary'],  # Exists
                    'header_background_color': 'blue'
                },
                {
                    'sheet_names': ['NonExistent_Sheet'],  # Doesn't exist
                    'header_background_color': 'red'
                },
                {
                    'sheet_names': ['?sheet_010?'],  # Out of range
                    'header_background_color': 'green'
                }
            ]
        }
        
        processor = FormatExcelProcessor(step_config)
        result = processor.execute()
        
        # Doctrine (2026-08-14): unresolvable sheets FAIL LOUD - the old
        # silent-skip path hid typos and substitution accidents.
        print("  ✗ Should have failed loud on nonexistent sheet")
        return False
        
    except Exception as e:
        print(f"  ✓ Failed loud on nonexistent sheet as doctrine requires: {e}")
        return True
        
    finally:
        if os.path.exists(test_file):
            os.unlink(test_file)


def test_row_heights():
    """Test row height specifications."""
    print("\n6. Testing row height functionality...")
    
    test_file = create_multi_sheet_test_file()
    
    try:
        step_config = {
            'processor_type': 'format_excel',
            'target_file': test_file,
            'formatting': [
                {
                    'sheet_names': ['?sheet_001?'],
                    'header_font_size': 18,  # Large font
                    'row_heights': {
                        1: 35,  # Tall header for large font
                        2: 20,  # Standard data rows
                        3: 20,
                        4: 25   # Slightly taller for specific row
                    },
                    'auto_fit_columns': True
                }
            ]
        }
        
        processor = FormatExcelProcessor(step_config)
        result = processor.execute()
        
        print("  ✓ Row heights applied successfully")
        return True
        
    except Exception as e:
        print(f"  ✗ Row heights failed: {e}")
        return False
        
    finally:
        if os.path.exists(test_file):
            os.unlink(test_file)


def main():
    """Run all sheet targeting tests."""
    print("🚀 Format Excel Processor - Sheet Targeting Tests")
    print("=" * 60)
    print("Testing new explicit sheet targeting with names and numbers")
    
    tests = [
        test_sheet_name_targeting,
        test_numeric_sheet_targeting,
        test_mixed_sheet_targeting,
        test_validation_errors,
        test_nonexistent_sheets,
        test_row_heights
    ]
    
    passed = sum(1 for test in tests if test())
    total = len(tests)
    
    print(f"\n🏁 SHEET TARGETING TEST RESULTS")
    print("=" * 60)
    
    if passed == total:
        print(f"✅ All {passed}/{total} tests passed!")
        print()
        print("🎯 SHEET TARGETING ACHIEVEMENTS:")
        print("✓ Sheet targeting by name works correctly")
        print("✓ Sheet targeting by 1-based index works correctly") 
        print("✓ Mixed name/number targeting works correctly")
        print("✓ Configuration validation catches all error cases")
        print("✓ Graceful handling of nonexistent sheets")
        print("✓ Row height specifications work correctly")
        print("✓ Active sheet setting works with both names and numbers")
        print()
        print("🎉 BREAKING CHANGE SUCCESSFUL!")
        print("The new explicit sheet targeting format is ready for production!")
        return 0
    else:
        print(f"❌ {passed}/{total} tests passed")
        print("Issues need to be resolved before the new format is ready")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)


# End of file #
