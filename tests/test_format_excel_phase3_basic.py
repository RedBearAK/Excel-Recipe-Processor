#!/usr/bin/env python3
"""
Basic test for Phase 3 functionality to check core features first.

tests/test_format_excel_phase3_basic.py

Simple test to verify Phase 3 enhancements work before running comprehensive tests.
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
    """Create simple test data."""
    return pd.DataFrame({
        'Name': ['Alice', 'Bob', 'Charlie'],
        'Score': [95, 87, 92],
        'Grade': ['A', 'B', 'A']
    })


def test_simple_css_colors():
    """Test basic CSS color names."""
    print("Testing CSS color names...")
    
    test_df = create_test_data()
    
    with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as temp_file:
        test_file = temp_file.name
    
    try:
        # Create Excel file
        test_df.to_excel(test_file, index=False)
        
        step_config = {
            'processor_type': 'format_excel',
            'target_file': test_file,
            'formatting': {
                'header_text_color': 'white',
                'header_background_color': 'blue',
                'general_text_color': 'black'
            }
        }
        
        processor = FormatExcelProcessor(step_config)
        result = processor.execute()
        print("✓ CSS color names work")
        return True
        
    except Exception as e:
        print(f"✗ CSS color names failed: {e}")
        return False
        
    finally:
        if os.path.exists(test_file):
            os.unlink(test_file)


def test_simple_cell_range():
    """Test basic cell range targeting."""
    print("Testing basic cell range...")
    
    test_df = create_test_data()
    
    with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as temp_file:
        test_file = temp_file.name
    
    try:
        # Create Excel file
        test_df.to_excel(test_file, index=False)
        
        step_config = {
            'processor_type': 'format_excel',
            'target_file': test_file,
            'formatting': {
                'cell_ranges': {
                    'A1': {
                        'text_color': 'red',
                        'bold': True
                    }
                }
            }
        }
        
        processor = FormatExcelProcessor(step_config)
        result = processor.execute()
        print("✓ Basic cell range works")
        return True
        
    except Exception as e:
        print(f"✗ Basic cell range failed: {e}")
        return False
        
    finally:
        if os.path.exists(test_file):
            os.unlink(test_file)


def main():
    """Run basic Phase 3 tests."""
    print("🔍 Phase 3 Basic Functionality Test")
    print("=" * 40)
    
    tests = [test_simple_css_colors, test_simple_cell_range]
    passed = sum(1 for test in tests if test())
    
    print(f"\nResult: {passed}/{len(tests)} basic tests passed")
    return 0 if passed == len(tests) else 1


if __name__ == "__main__":
    sys.exit(main())


# End of file #
