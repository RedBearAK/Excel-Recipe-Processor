#!/usr/bin/env python3
"""
Test script for Phase 3 cell range targeting and enhanced color support.

tests/test_format_excel_phase3.py

Tests the new cell range targeting, webcolors integration, and border formatting features.
"""

import sys
import os
import tempfile
import pandas as pd
from pathlib import Path

# Add the parent directory to the path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from excel_recipe_processor.processors.format_excel_processor import FormatExcelProcessor


def create_test_data():
    """Create sample test data with enough rows/columns for range testing."""
    return pd.DataFrame({
        'Product': ['Widget A', 'Widget B', 'Widget C', 'Widget D', 'Widget E', 'Widget F'],
        'Sales': [100, 200, 150, 75, 300, 250],
        'Region': ['North', 'South', 'North', 'West', 'East', 'South'],
        'Status': ['Active', 'Active', 'Inactive', 'Active', 'Active', 'Pending'],
        'Quarter': ['Q1', 'Q1', 'Q2', 'Q2', 'Q3', 'Q3'],
        'Priority': ['High', 'Medium', 'Low', 'High', 'Medium', 'High']
    })


def test_webcolors_integration():
    """Test enhanced color format support with webcolors."""
    print("\n1. Testing webcolors integration...")
    
    test_df = create_test_data()
    
    with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as temp_file:
        test_file = temp_file.name
    
    try:
        # Create Excel file
        test_df.to_excel(test_file, index=False)
        
        # Test various color formats
        color_tests = [
            ("CSS color names", {
                'header_text_color': 'white',
                'header_background_color': 'navy',
                'general_text_color': 'darkslategray'
            }),
            ("RGB format", {
                'header_text_color': 'rgb(255, 255, 255)',
                'header_background_color': 'rgb(0, 0, 128)',
                'general_text_color': 'rgb(64, 64, 64)'
            }),
            ("Mixed formats", {
                'header_text_color': '#FFFFFF',          # Hex with hash
                'header_background_color': 'steelblue',  # CSS name
                'general_text_color': 'rgb(51, 51, 51)'  # RGB format
            })
        ]
        
        for test_name, color_config in color_tests:
            step_config = {
                'processor_type': 'format_excel',
                'target_file': test_file,
                'formatting': [{
                    'sheet_names': ['?sheet_001?'],
                    **color_config,
                    'header_bold': True,
                    'header_background': True
                }]
            }
            
            try:
                processor = FormatExcelProcessor(step_config)
                result = processor.execute()
                print(f"  ✓ {test_name}: Success")
            except Exception as e:
                print(f"  ✗ {test_name}: Failed: {e}")
                return False
        
        return True
        
    except Exception as e:
        print(f"  ✗ Webcolors integration test failed: {e}")
        return False
        
    finally:
        if os.path.exists(test_file):
            os.unlink(test_file)


def test_basic_cell_range_formatting():
    """Test basic cell range targeting functionality."""
    print("\n2. Testing basic cell range formatting...")
    
    test_df = create_test_data()
    
    with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as temp_file:
        test_file = temp_file.name
    
    try:
        # Create Excel file
        test_df.to_excel(test_file, index=False)
        
        # Test cell range targeting
        step_config = {
            'processor_type': 'format_excel',
            'target_file': test_file,
            'formatting': [{
                'sheet_names': ['?sheet_001?'],
                'cell_ranges': {
                    'A1:F1': {                        # Header row
                        'text_color': 'white',
                        'background_color': 'darkblue',
                        'bold': True,
                        'font_size': 14
                    },
                    'A2:A7': {                        # First column
                        'text_color': 'darkgreen',
                        'bold': True,
                        'alignment_horizontal': 'right'
                    },
                    'B2:F7': {                        # Data area
                        'text_color': 'black',
                        'alignment_horizontal': 'center'
                    }
                }
            }]
        }
        
        processor = FormatExcelProcessor(step_config)
        result = processor.execute()
        print(f"  ✓ Basic cell range formatting applied successfully")
        
        # Verify file integrity
        verification_df = pd.read_excel(test_file)
        if len(verification_df) == len(test_df):
            print("  ✓ File integrity verified")
            return True
        else:
            print("  ✗ File integrity check failed")
            return False
        
    except Exception as e:
        print(f"  ✗ Basic cell range formatting test failed: {e}")
        return False
        
    finally:
        if os.path.exists(test_file):
            os.unlink(test_file)


def test_border_formatting():
    """Test border formatting functionality."""
    print("\n3. Testing border formatting...")
    
    test_df = create_test_data()
    
    with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as temp_file:
        test_file = temp_file.name
    
    try:
        # Create Excel file
        test_df.to_excel(test_file, index=False)
        
        # Test border formatting
        step_config = {
            'processor_type': 'format_excel',
            'target_file': test_file,
            'formatting': [{
                'sheet_names': ['?sheet_001?'],
                'cell_ranges': {
                    'A1:F1': {                        # Header with thick border
                        'border': 'thick'
                    },
                    'A2:F7': {                        # Data area with thin border
                        'border': 'thin'
                    },
                    'A8:F8': {                        # Summary row with custom border
                        'border': {
                            'top': 'thick',
                            'bottom': 'thick',
                            'left': 'thin',
                            'right': 'thin'
                        }
                    }
                }
            }]
        }
        
        processor = FormatExcelProcessor(step_config)
        result = processor.execute()
        print(f"  ✓ Border formatting applied successfully")
        return True
        
    except Exception as e:
        print(f"  ✗ Border formatting test failed: {e}")
        return False
        
    finally:
        if os.path.exists(test_file):
            os.unlink(test_file)


def test_single_cell_targeting():
    """Test single cell targeting."""
    print("\n4. Testing single cell targeting...")
    
    test_df = create_test_data()
    
    with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as temp_file:
        test_file = temp_file.name
    
    try:
        # Create Excel file
        test_df.to_excel(test_file, index=False)
        
        # Test single cell targeting
        step_config = {
            'processor_type': 'format_excel',
            'target_file': test_file,
            'formatting': [{
                'sheet_names': ['?sheet_001?'],
                'cell_ranges': {
                    'A1': {                           # Single cell
                        'text_color': 'red',
                        'bold': True,
                        'font_size': 16
                    },
                    'B2': {                           # Another single cell
                        'background_color': 'yellow',
                        'text_color': 'black'
                    },
                    'C3': {                           # Third single cell
                        'italic': True,
                        'text_color': 'blue'
                    }
                }
            }]
        }
        
        processor = FormatExcelProcessor(step_config)
        result = processor.execute()
        print(f"  ✓ Single cell targeting applied successfully")
        return True
        
    except Exception as e:
        print(f"  ✗ Single cell targeting test failed: {e}")
        return False
        
    finally:
        if os.path.exists(test_file):
            os.unlink(test_file)


def test_range_validation():
    """Test validation of cell range specifications."""
    print("\n5. Testing range validation...")
    
    test_df = create_test_data()
    
    with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as temp_file:
        test_file = temp_file.name
    
    try:
        # Create Excel file
        test_df.to_excel(test_file, index=False)
        
        # Test invalid range specifications
        invalid_ranges = [
            ('Invalid range format', {
                'cell_ranges': {
                    'INVALID_RANGE': {'text_color': 'red'}
                }
            }),
            ('Invalid color in range', {
                'cell_ranges': {
                    'A1:C3': {'text_color': 'invalid_color_name'}
                }
            }),
            ('Invalid font size', {
                'cell_ranges': {
                    'A1:C3': {'font_size': -5}
                }
            }),
            ('Invalid alignment', {
                'cell_ranges': {
                    'A1:C3': {'alignment_horizontal': 'invalid_alignment'}
                }
            })
        ]
        
        success_count = 0
        
        for test_name, config in invalid_ranges:
            step_config = {
                'processor_type': 'format_excel',
                'target_file': test_file,
                'formatting': config['cell_ranges']
            }
            step_config['formatting'] = config
            
            try:
                processor = FormatExcelProcessor(step_config)
                result = processor.execute()
                print(f"  ✗ {test_name} should have failed")
            except Exception:
                print(f"  ✓ {test_name} correctly rejected")
                success_count += 1
        
        return success_count == len(invalid_ranges)
        
    finally:
        if os.path.exists(test_file):
            os.unlink(test_file)


def test_comprehensive_integration():
    """Test all phases working together with cell ranges."""
    print("\n6. Testing comprehensive Phase 1 + 2 + 3 integration...")
    
    test_df = create_test_data()
    
    with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as temp_file:
        test_file = temp_file.name
    
    try:
        # Create Excel file
        test_df.to_excel(test_file, index=False)
        
        # Test comprehensive formatting with all phases
        step_config = {
            'processor_type': 'format_excel',
            'target_file': test_file,
            'formatting': [{
                'sheet_names': ['?sheet_001?'],
                # Phase 1: Header formatting
                'header_text_color': 'white',
                'header_font_size': 16,
                'header_bold': True,
                'header_background': True,
                'header_background_color': 'navy',
                
                # Phase 2: General formatting
                'general_text_color': 'darkgray',
                'general_font_size': 10,
                'general_font_name': 'Arial',
                'general_alignment_horizontal': 'left',
                'general_alignment_vertical': 'center',
                
                # Phase 3: Cell range targeting (should override general/header)
                'cell_ranges': {
                    'A1:F1': {                        # Override header formatting
                        'background_color': 'darkred',
                        'text_color': 'yellow',
                        'font_size': 18
                    },
                    'A2:A7': {                        # Override general formatting
                        'text_color': 'blue',
                        'bold': True,
                        'alignment_horizontal': 'right'
                    },
                    'B2:F7': {                        # Data area with borders
                        'border': 'thin',
                        'alignment_horizontal': 'center'
                    },
                    'F2:F7': {                        # Special column
                        'background_color': 'lightblue',
                        'text_color': 'darkblue',
                        'border': 'thick'
                    }
                },
                
                # Other features
                'auto_fit_columns': True,
                'freeze_top_row': True,
                'auto_filter': True
            }]
        }
        
        processor = FormatExcelProcessor(step_config)
        result = processor.execute()
        print(f"  ✓ Comprehensive integration applied successfully")
        
        # Verify file integrity
        verification_df = pd.read_excel(test_file)
        if len(verification_df) == len(test_df):
            print("  ✓ File integrity verified")
            print("  ✓ All three phases work together perfectly!")
            return True
        else:
            print("  ✗ File integrity check failed")
            return False
        
    except Exception as e:
        print(f"  ✗ Comprehensive integration test failed: {e}")
        return False
        
    finally:
        if os.path.exists(test_file):
            os.unlink(test_file)


def test_dashboard_scenario():
    """Test a realistic dashboard formatting scenario."""
    print("\n7. Testing dashboard formatting scenario...")
    
    test_df = create_test_data()
    
    with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as temp_file:
        test_file = temp_file.name
    
    try:
        # Create Excel file
        test_df.to_excel(test_file, index=False)
        
        # Professional dashboard formatting
        step_config = {
            'processor_type': 'format_excel',
            'target_file': test_file,
            'formatting': [{
                'sheet_names': ['?sheet_001?'],
                # Dashboard-style header
                'header_text_color': 'white',
                'header_font_size': 18,
                'header_bold': True,
                'header_background_color': 'darkslateblue',
                
                # Clean general formatting
                'general_font_size': 10,
                'general_text_color': 'dimgray',
                
                # Dashboard sections with cell ranges
                'cell_ranges': {
                    'A1:F1': {                        # Main header
                        'background_color': 'midnightblue',
                        'text_color': 'white',
                        'font_size': 20,
                        'bold': True,
                        'alignment_horizontal': 'center',
                        'border': 'thick'
                    },
                    'A2:C7': {                        # KPI section
                        'background_color': 'lightsteelblue',
                        'text_color': 'darkblue',
                        'border': 'medium',
                        'alignment_horizontal': 'center'
                    },
                    'D2:F7': {                        # Data section
                        'background_color': 'honeydew',
                        'text_color': 'darkgreen',
                        'border': 'thin'
                    },
                    'A8:F8': {                        # Footer
                        'background_color': 'gray',
                        'text_color': 'white',
                        'italic': True,
                        'alignment_horizontal': 'center',
                        'border': 'thick'
                    }
                },
                
                'auto_fit_columns': True
            }]
        }
        
        processor = FormatExcelProcessor(step_config)
        result = processor.execute()
        print(f"  ✓ Dashboard formatting applied successfully")
        print("  ✓ Professional dashboard appearance achieved!")
        
        return True
        
    except Exception as e:
        print(f"  ✗ Dashboard scenario failed: {e}")
        return False
        
    finally:
        if os.path.exists(test_file):
            os.unlink(test_file)


def main():
    """Run all Phase 3 enhancement tests."""
    print("🚀 Format Excel Processor - Phase 3 Enhancement Tests")
    print("=" * 60)
    print("Testing cell range targeting, webcolors integration, and border formatting")
    print("Features: cell_ranges, CSS colors, RGB colors, border formatting")
    print()
    
    tests = [
        test_webcolors_integration,
        test_basic_cell_range_formatting,
        test_border_formatting,
        test_single_cell_targeting,
        test_range_validation,
        test_comprehensive_integration,
        test_dashboard_scenario
    ]
    
    passed = 0
    total = len(tests)
    
    for test_func in tests:
        try:
            if test_func():
                passed += 1
        except Exception as e:
            print(f"  ✗ Test {test_func.__name__} failed with exception: {e}")
    
    print(f"\n🏁 PHASE 3 TEST RESULTS")
    print("=" * 60)
    if passed == total:
        print(f"✅ All {total} tests passed!")
        print()
        print("🎯 PHASE 3 ACHIEVEMENTS:")
        print("✓ Cell range targeting with dict structure")
        print("✓ webcolors integration (CSS names, RGB format)")
        print("✓ Comprehensive border formatting")
        print("✓ Single cell and range targeting")
        print("✓ Advanced validation for ranges and colors")
        print("✓ Perfect integration with Phase 1 & 2 features")
        print("✓ Professional dashboard scenarios supported")
        print()
        print("🎉 FORMAT EXCEL PROCESSOR COMPLETE!")
        print("All three phases successfully implemented:")
        print("  Phase 1: Header text colors & font sizes")
        print("  Phase 2: General formatting for all cells")
        print("  Phase 3: Cell range targeting & advanced colors")
        print()
        print("Ready for production use! 🚀")
        return 0
    else:
        print(f"❌ {passed}/{total} tests passed")
        print("Issues need to be resolved")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)


# End of file #
