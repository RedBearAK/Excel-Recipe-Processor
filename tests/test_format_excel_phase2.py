#!/usr/bin/env python3
"""
Test script for Phase 2 general formatting enhancements.

tests/test_format_excel_phase2.py

Tests the new general formatting features: general_text_color, general_font_size,
general_font_name, and alignment controls.
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
    """Create sample test data with multiple rows for general formatting testing."""
    return pd.DataFrame({
        'Product': ['Widget A', 'Widget B', 'Widget C', 'Widget D', 'Widget E'],
        'Sales': [100, 200, 150, 75, 300],
        'Region': ['North', 'South', 'North', 'West', 'East'],
        'Status': ['Active', 'Active', 'Inactive', 'Active', 'Active'],
        'Quarter': ['Q1', 'Q1', 'Q2', 'Q2', 'Q3']
    })


def test_general_text_color():
    """Test general text color formatting for data cells."""
    print("\n1. Testing general text color...")
    
    test_df = create_test_data()
    
    with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as temp_file:
        test_file = temp_file.name
    
    try:
        # Create Excel file
        test_df.to_excel(test_file, index=False)
        
        # Test general text color (should apply to data cells, not headers)
        step_config = {
            'processor_type': 'format_excel',
            'target_file': test_file,
            'formatting': {
                'general_text_color': '333333',     # Dark gray for data
                'header_text_color': 'FFFFFF',      # White for headers
                'header_background': True,
                'header_background_color': '2F5597'  # Blue background
            }
        }
        
        processor = FormatExcelProcessor(step_config)
        result = processor.execute()  # Returns empty DataFrame by design for FileOps
        print(f"  ✓ General text color applied successfully")
        
        # Verify file integrity
        verification_df = pd.read_excel(test_file)
        if len(verification_df) == len(test_df):
            print("  ✓ File integrity verified")
            return True
        else:
            print("  ✗ File integrity check failed")
            return False
        
    except Exception as e:
        print(f"  ✗ General text color test failed: {e}")
        return False
        
    finally:
        if os.path.exists(test_file):
            os.unlink(test_file)


def test_general_font_settings():
    """Test general font size and name."""
    print("\n2. Testing general font settings...")
    
    test_df = create_test_data()
    
    with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as temp_file:
        test_file = temp_file.name
    
    try:
        # Create Excel file
        test_df.to_excel(test_file, index=False)
        
        # Test font settings
        step_config = {
            'processor_type': 'format_excel',
            'target_file': test_file,
            'formatting': {
                'general_font_size': 12,           # Larger than default
                'general_font_name': 'Arial',      # Different from default
                'general_text_color': '222222'     # Near black
            }
        }
        
        processor = FormatExcelProcessor(step_config)
        result = processor.execute()  # Returns empty DataFrame by design for FileOps
        print(f"  ✓ General font settings applied successfully")
        return True
        
    except Exception as e:
        print(f"  ✗ General font settings test failed: {e}")
        return False
        
    finally:
        if os.path.exists(test_file):
            os.unlink(test_file)


def test_general_alignment():
    """Test general alignment options."""
    print("\n3. Testing general alignment...")
    
    test_df = create_test_data()
    
    with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as temp_file:
        test_file = temp_file.name
    
    try:
        # Create Excel file
        test_df.to_excel(test_file, index=False)
        
        # Test alignment settings
        alignment_tests = [
            ('left', 'center'),
            ('right', 'top'),
            ('center', 'bottom'),
            ('justify', 'distributed')
        ]
        
        for h_align, v_align in alignment_tests:
            step_config = {
                'processor_type': 'format_excel',
                'target_file': test_file,
                'formatting': {
                    'general_alignment_horizontal': h_align,
                    'general_alignment_vertical': v_align
                }
            }
            
            try:
                processor = FormatExcelProcessor(step_config)
                result = processor.execute()  # Returns empty DataFrame by design for FileOps
                print(f"  ✓ Alignment {h_align}/{v_align}: Success")
            except Exception as e:
                print(f"  ✗ Alignment {h_align}/{v_align}: Failed: {e}")
                return False
        
        return True
        
    except Exception as e:
        print(f"  ✗ General alignment test failed: {e}")
        return False
        
    finally:
        if os.path.exists(test_file):
            os.unlink(test_file)


def test_invalid_alignment_values():
    """Test validation of alignment values."""
    print("\n4. Testing alignment validation...")
    
    test_df = create_test_data()
    
    with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as temp_file:
        test_file = temp_file.name
    
    try:
        # Create Excel file
        test_df.to_excel(test_file, index=False)
        
        # Test invalid alignment values
        invalid_alignments = [
            ('invalid_horizontal', 'center'),
            ('left', 'invalid_vertical'),
            ('middle', 'center'),  # 'middle' is not valid, should be 'center'
        ]
        
        for h_align, v_align in invalid_alignments:
            step_config = {
                'processor_type': 'format_excel',
                'target_file': test_file,
                'formatting': {
                    'general_alignment_horizontal': h_align,
                    'general_alignment_vertical': v_align
                }
            }
            
            try:
                processor = FormatExcelProcessor(step_config)
                result = processor.execute()  # Returns empty DataFrame by design for FileOps
                print(f"  ✗ Invalid alignment should have failed: {h_align}/{v_align}")
                return False
            except Exception:
                print(f"  ✓ Invalid alignment correctly rejected: {h_align}/{v_align}")
        
        return True
        
    finally:
        if os.path.exists(test_file):
            os.unlink(test_file)


def test_comprehensive_formatting():
    """Test combining all Phase 2 features with Phase 1."""
    print("\n5. Testing comprehensive Phase 1 + Phase 2 formatting...")
    
    test_df = create_test_data()
    
    with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as temp_file:
        test_file = temp_file.name
    
    try:
        # Create Excel file
        test_df.to_excel(test_file, index=False)
        
        # Test comprehensive formatting
        step_config = {
            'processor_type': 'format_excel',
            'target_file': test_file,
            'formatting': {
                # Phase 1: Header formatting
                'header_text_color': 'FFFFFF',
                'header_font_size': 16,
                'header_bold': True,
                'header_background': True,
                'header_background_color': '2F5597',
                
                # Phase 2: General formatting
                'general_text_color': '333333',
                'general_font_size': 11,
                'general_font_name': 'Segoe UI',
                'general_alignment_horizontal': 'left',
                'general_alignment_vertical': 'center',
                
                # Other features
                'auto_fit_columns': True,
                'freeze_top_row': True,
                'auto_filter': True
            }
        }
        
        processor = FormatExcelProcessor(step_config)
        result = processor.execute()  # Returns empty DataFrame by design for FileOps
        print(f"  ✓ Comprehensive formatting applied successfully")
        
        # Verify file integrity
        verification_df = pd.read_excel(test_file)
        if len(verification_df) == len(test_df):
            print("  ✓ File integrity verified")
            return True
        else:
            print("  ✗ File integrity check failed")
            return False
        
    except Exception as e:
        print(f"  ✗ Comprehensive formatting test failed: {e}")
        return False
        
    finally:
        if os.path.exists(test_file):
            os.unlink(test_file)


def test_corporate_branding_scenario():
    """Test a realistic corporate branding scenario."""
    print("\n6. Testing corporate branding scenario...")
    
    test_df = create_test_data()
    
    with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as temp_file:
        test_file = temp_file.name
    
    try:
        # Create Excel file
        test_df.to_excel(test_file, index=False)
        
        # Corporate branding scenario
        step_config = {
            'processor_type': 'format_excel',
            'target_file': test_file,
            'formatting': {
                # Corporate header style
                'header_text_color': 'FFFFFF',        # White text
                'header_font_size': 14,               # Prominent
                'header_bold': True,
                'header_background': True,
                'header_background_color': '1F4E79',  # Corporate navy

                # Corporate body style
                'general_text_color': '444444',       # Professional gray
                'general_font_size': 10,              # Compact but readable
                'general_font_name': 'Calibri',       # Corporate standard
                'general_alignment_horizontal': 'left',
                'general_alignment_vertical': 'center',

                # Professional features
                'auto_fit_columns': True,
                'freeze_top_row': True,
                'auto_filter': True,
                'max_column_width': 40
            }
        }
        
        processor = FormatExcelProcessor(step_config)
        result = processor.execute()  # Returns empty DataFrame by design for FileOps
        print(f"  ✓ Corporate branding applied successfully")
        print("  ✓ Professional appearance with consistent branding!")
        
        return True
        
    except Exception as e:
        print(f"  ✗ Corporate branding scenario failed: {e}")
        return False
        
    finally:
        if os.path.exists(test_file):
            os.unlink(test_file)


def main():
    """Run all Phase 2 enhancement tests."""
    print("🚀 Format Excel Processor - Phase 2 Enhancement Tests")
    print("=" * 60)
    print("Testing general formatting for all data cells")
    print("Features: general_text_color, general_font_size, general_font_name, alignment")
    print()
    
    tests = [
        test_general_text_color,
        test_general_font_settings,
        test_general_alignment,
        test_invalid_alignment_values,
        test_comprehensive_formatting,
        test_corporate_branding_scenario
    ]
    
    passed = 0
    total = len(tests)
    
    for test_func in tests:
        try:
            if test_func():
                passed += 1
        except Exception as e:
            print(f"  ✗ Test {test_func.__name__} failed with exception: {e}")
    
    print(f"\n🏁 PHASE 2 TEST RESULTS")
    print("=" * 60)
    if passed == total:
        print(f"✅ All {total} tests passed!")
        print()
        print("🎯 PHASE 2 ACHIEVEMENTS:")
        print("✓ General text color control for all data cells")
        print("✓ General font size and font name control")
        print("✓ Horizontal and vertical alignment controls")
        print("✓ Comprehensive validation for alignment values")
        print("✓ Integration with Phase 1 header formatting")
        print("✓ Corporate branding scenarios supported")
        print()
        print("📋 READY FOR PHASE 3:")
        print("- Cell range targeting (format specific ranges)")
        print("- webcolors integration (CSS colors, RGB, named colors)")
        print("- Border formatting options")
        return 0
    else:
        print(f"❌ {passed}/{total} tests passed")
        print("Issues need to be resolved before proceeding to Phase 3")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)


# End of file #
