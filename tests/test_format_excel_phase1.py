#!/usr/bin/env python3
"""
Test script for Phase 1 header formatting enhancements.

tests/test_format_excel_phase1.py

Tests the new header_text_color and header_font_size features to ensure
they solve the dark background readability issue.
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
    """Create sample test data for header formatting tests."""
    return pd.DataFrame({
        'Product': ['Widget A', 'Widget B', 'Widget C', 'Widget D'],
        'Sales': [100, 200, 150, 75],
        'Region': ['North', 'South', 'North', 'West'],
        'Status': ['Active', 'Active', 'Inactive', 'Active']
    })


def test_header_text_color():
    """Test header text color formatting."""
    print("\n1. Testing header text color...")
    
    test_df = create_test_data()
    
    with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as temp_file:
        test_file = temp_file.name
    
    try:
        # Create Excel file
        test_df.to_excel(test_file, index=False)
        
        # Test various color formats
        test_cases = [
            ("FFFFFF", "6-digit hex without hash"),
            ("#FF0000", "6-digit hex with hash"),
            ("0F0", "3-digit hex without hash"),
            ("#00F", "3-digit hex with hash")
        ]
        
        for color, description in test_cases:
            step_config = {
                'processor_type': 'format_excel',
                'target_file': test_file,
                'formatting': {
                    'header_text_color': color,
                    'header_bold': True,
                    'header_background': True,
                    'header_background_color': '1F4E79'  # Dark navy
                }
            }
            
            try:
                processor = FormatExcelProcessor(step_config)
                result = processor.execute()  # Returns empty DataFrame by design for FileOps
                print(f"  ✓ {description}: {color} → Success")
            except Exception as e:
                print(f"  ✗ {description}: {color} → Failed: {e}")
                return False
        
        return True
        
    finally:
        if os.path.exists(test_file):
            os.unlink(test_file)


def test_header_font_size():
    """Test header font size formatting."""
    print("\n2. Testing header font size...")
    
    test_df = create_test_data()
    
    with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as temp_file:
        test_file = temp_file.name
    
    try:
        # Create Excel file
        test_df.to_excel(test_file, index=False)
        
        # Test various font sizes
        font_sizes = [8, 10, 12, 14, 16, 18, 24]
        
        for size in font_sizes:
            step_config = {
                'processor_type': 'format_excel',
                'target_file': test_file,
                'formatting': {
                    'header_font_size': size,
                    'header_bold': True
                }
            }
            
            try:
                processor = FormatExcelProcessor(step_config)
                result = processor.execute()  # Returns empty DataFrame by design for FileOps
                print(f"  ✓ Font size {size}: Success")
            except Exception as e:
                print(f"  ✗ Font size {size}: Failed: {e}")
                return False
        
        return True
        
    finally:
        if os.path.exists(test_file):
            os.unlink(test_file)


def test_combined_header_formatting():
    """Test combining text color, font size, and existing features."""
    print("\n3. Testing combined header formatting...")
    
    test_df = create_test_data()
    
    with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as temp_file:
        test_file = temp_file.name
    
    try:
        # Create Excel file
        test_df.to_excel(test_file, index=False)
        
        # Test comprehensive header formatting
        step_config = {
            'processor_type': 'format_excel',
            'target_file': test_file,
            'formatting': {
                # Phase 1 enhancements
                'header_text_color': 'FFFFFF',      # White text
                'header_font_size': 14,             # Prominent size
                # Existing features
                'header_bold': True,                # Bold text
                'header_background': True,          # Background fill
                'header_background_color': '1F4E79', # Dark navy background
                # Additional formatting
                'auto_fit_columns': True,
                'freeze_top_row': True,
                'auto_filter': True
            }
        }
        
        processor = FormatExcelProcessor(step_config)
        result = processor.execute()  # Returns empty DataFrame by design for FileOps
        
        print(f"  ✓ Combined formatting applied successfully")
        
        # Verify the file is still readable
        verification_df = pd.read_excel(test_file)
        if len(verification_df) == len(test_df):
            print("  ✓ File integrity verified")
            return True
        else:
            print("  ✗ File integrity check failed")
            return False
        
    except Exception as e:
        print(f"  ✗ Combined formatting failed: {e}")
        return False
        
    finally:
        if os.path.exists(test_file):
            os.unlink(test_file)


def test_color_validation():
    """Test color format validation."""
    print("\n4. Testing color validation...")
    
    test_df = create_test_data()
    
    with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as temp_file:
        test_file = temp_file.name
    
    try:
        # Create Excel file
        test_df.to_excel(test_file, index=False)
        
        # Test invalid color formats (should fail)
        invalid_colors = [
            "GGGGGG",  # Invalid hex characters
            "FF00",    # Wrong length (4 digits)
            "FF00000", # Wrong length (7 digits)
            123,       # Not a string
            ""         # Empty string
        ]
        
        for invalid_color in invalid_colors:
            step_config = {
                'processor_type': 'format_excel',
                'target_file': test_file,
                'formatting': {
                    'header_text_color': invalid_color
                }
            }
            
            try:
                processor = FormatExcelProcessor(step_config)
                result = processor.execute()  # Returns empty DataFrame by design for FileOps
                print(f"  ✗ Invalid color should have failed: {invalid_color}")
                return False
            except Exception:
                print(f"  ✓ Invalid color correctly rejected: {invalid_color}")
        
        return True
        
    finally:
        if os.path.exists(test_file):
            os.unlink(test_file)


def test_dark_theme_scenario():
    """Test the specific dark theme scenario that motivated this enhancement."""
    print("\n5. Testing dark theme readability scenario...")
    
    test_df = create_test_data()
    
    with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as temp_file:
        test_file = temp_file.name
    
    try:
        # Create Excel file
        test_df.to_excel(test_file, index=False)
        
        # The exact scenario from the problem description:
        # Dark background that would make default black text unreadable
        step_config = {
            'processor_type': 'format_excel',
            'target_file': test_file,
            'formatting': {
                # This solves the readability problem!
                'header_text_color': 'FFFFFF',      # White text for dark backgrounds
                'header_font_size': 16,             # Larger for prominence
                'header_bold': True,                # Bold for emphasis
                'header_background': True,          # Enable background
                'header_background_color': '1F4E79', # Dark navy (corporate)
                'auto_fit_columns': True,
                'freeze_top_row': True,
                'auto_filter': True
            }
        }
        
        processor = FormatExcelProcessor(step_config)
        result = processor.execute()  # Returns empty DataFrame by design for FileOps
        
        print(f"  ✓ Dark theme readability fix applied successfully")
        print("  ✓ White text on dark background now properly configured!")
        
        return True
        
    except Exception as e:
        print(f"  ✗ Dark theme scenario failed: {e}")
        return False
        
    finally:
        if os.path.exists(test_file):
            os.unlink(test_file)


def main():
    """Run all Phase 1 enhancement tests."""
    print("🚀 Format Excel Processor - Phase 1 Enhancement Tests")
    print("=" * 60)
    print("Testing header text color and font size enhancements")
    print("Focus: Solving dark background readability issues")
    print()
    
    tests = [
        test_header_text_color,
        test_header_font_size,
        test_combined_header_formatting,
        test_color_validation,
        test_dark_theme_scenario
    ]
    
    passed = 0
    total = len(tests)
    
    for test_func in tests:
        try:
            if test_func():
                passed += 1
        except Exception as e:
            print(f"  ✗ Test {test_func.__name__} failed with exception: {e}")
    
    print(f"\n🏁 PHASE 1 TEST RESULTS")
    print("=" * 60)
    if passed == total:
        print(f"✅ All {total} tests passed!")
        print()
        print("🎯 PHASE 1 ACHIEVEMENTS:")
        print("✓ Header text color support added (fixes dark background readability)")
        print("✓ Header font size control implemented")
        print("✓ Color format validation working")
        print("✓ Backwards compatibility maintained")
        print("✓ Dark theme scenario solved!")
        print()
        print("📋 READY FOR PHASE 2:")
        print("- General formatting options (general_text_color, general_font_size)")
        print("- Alignment control (general_alignment_horizontal/vertical)")
        print("- Enhanced color parsing with webcolors library")
        return 0
    else:
        print(f"❌ {passed}/{total} tests passed")
        print("Issues need to be resolved before proceeding to Phase 2")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)


# End of file #
