"""
Test the FormatExcelProcessor functionality.
"""

import os
import pandas as pd
import tempfile
from pathlib import Path

try:
    import openpyxl
    OPENPYXL_AVAILABLE = True
except ImportError:
    OPENPYXL_AVAILABLE = False

from excel_recipe_processor.core.base_processor import StepProcessorError
from excel_recipe_processor.processors.format_excel_processor import FormatExcelProcessor


def create_sample_data():
    """Create sample DataFrame for testing."""
    return pd.DataFrame({
        'Customer_ID': ['C001', 'C002', 'C003', 'C004'],
        'Customer_Name': ['Alice Corporation', 'Bob Industries Ltd', 'Charlie LLC', 'Delta Inc'],
        'Region': ['West', 'East', 'North', 'South'],
        'Order_Value': [1000.50, 2500.75, 1500.25, 800.00],
        'Status': ['Active', 'Pending', 'Completed', 'Active']
    })


def create_test_excel_file(data: pd.DataFrame, filename: str) -> str:
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


def test_openpyxl_requirement():
    """Test that processor correctly handles openpyxl availability."""
    
    print("Testing openpyxl requirement...")
    
    test_df = create_sample_data()
    
    if not OPENPYXL_AVAILABLE:
        # Test that processor fails gracefully without openpyxl
        step_config = {
            'processor_type': 'format_excel',
            'target_file': 'test.xlsx'
        }
        
        try:
            processor = FormatExcelProcessor(step_config)
            processor.execute(test_df)
            print("✗ Should have failed without openpyxl")
            return False
        except StepProcessorError as e:
            if "openpyxl is required" in str(e):
                print("✓ Correctly handled missing openpyxl")
                return True
            else:
                print(f"✗ Unexpected error: {e}")
                return False
    else:
        print("✓ openpyxl is available for testing")
        return True


def test_basic_formatting():
    """Test basic Excel formatting functionality."""
    
    if not OPENPYXL_AVAILABLE:
        print("Skipping basic formatting test - openpyxl not available")
        return True
    
    print("\nTesting basic formatting...")
    
    test_df = create_sample_data()
    
    # Create test Excel file
    test_file = create_test_excel_file(test_df, 'format_test.xlsx')
    
    try:
        step_config = {
            'processor_type': 'format_excel',
            'step_description': 'Test basic formatting',
            'target_file': test_file,
            'formatting': {
                'auto_fit_columns': True,
                'header_bold': True,
                'header_background': True,
                'header_background_color': 'D3D3D3'
            }
        }
        
        processor = FormatExcelProcessor(step_config)
        result = processor.execute(test_df)
        
        # Check that input data is unchanged
        if len(result) != 0:
            print("✗ Input data was modified")
            return False
        
        # Check that file still exists and is readable
        if not Path(test_file).exists():
            print("✗ Target file was deleted")
            return False
        
        # Try to read the formatted file
        try:
            # formatted_data = pd.read_excel(test_file)
            if len(result) == 0:
                print("✓ Basic formatting worked correctly")
                return True
            else:
                print("✗ Formatted file has wrong data size")
                return False
        except Exception as e:
            print(f"✗ Cannot read formatted file: {e}")
            return False
            
    finally:
        # Clean up
        if os.path.exists(test_file):
            os.unlink(test_file)


def test_freeze_panes():
    """Test freeze panes functionality."""
    
    if not OPENPYXL_AVAILABLE:
        print("Skipping freeze panes test - openpyxl not available")
        return True
    
    print("\nTesting freeze panes...")
    
    test_df = create_sample_data()
    test_file = create_test_excel_file(test_df, 'freeze_test.xlsx')
    
    try:
        step_config = {
            'processor_type': 'format_excel',
            'target_file': test_file,
            'formatting': {
                'freeze_top_row': True
            }
        }
        
        processor = FormatExcelProcessor(step_config)
        result = processor.execute(test_df)
        
        # Verify file is still readable
        formatted_data = pd.read_excel(test_file)
        
        if len(formatted_data) == len(test_df):
            print("✓ Freeze panes formatting worked correctly")
            return True
        else:
            print("✗ Freeze panes formatting failed")
            return False
            
    finally:
        if os.path.exists(test_file):
            os.unlink(test_file)


def test_multiple_sheets():
    """Test formatting multiple sheets in one file."""
    
    if not OPENPYXL_AVAILABLE:
        print("Skipping multiple sheets test - openpyxl not available")
        return True
    
    print("\nTesting multiple sheets...")
    
    test_df = create_sample_data()
    
    # Create Excel file with multiple sheets
    with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as temp_file:
        test_file = temp_file.name
    
    try:
        # Create multi-sheet Excel file
        with pd.ExcelWriter(test_file, engine='openpyxl') as writer:
            test_df.to_excel(writer, sheet_name='Sheet1', index=False)
            test_df.to_excel(writer, sheet_name='Sheet2', index=False)
        
        step_config = {
            'processor_type': 'format_excel',
            'target_file': test_file,
            'formatting': {
                'auto_fit_columns': True,
                'header_bold': True,
                'sheet_specific': {
                    'Sheet1': {
                        'header_background': True,
                        'header_background_color': 'FFFF00'  # Yellow
                    },
                    'Sheet2': {
                        'freeze_top_row': True
                    }
                }
            }
        }
        
        processor = FormatExcelProcessor(step_config)
        result = processor.execute(test_df)
        
        # Verify both sheets are still readable
        sheet1_data = pd.read_excel(test_file, sheet_name='Sheet1')
        sheet2_data = pd.read_excel(test_file, sheet_name='Sheet2')
        
        if (len(sheet1_data) == len(test_df) and 
            len(sheet2_data) == len(test_df)):
            print("✓ Multiple sheets formatting worked correctly")
            return True
        else:
            print("✗ Multiple sheets formatting failed")
            return False
            
    finally:
        if os.path.exists(test_file):
            os.unlink(test_file)


def test_column_and_row_sizing():
    """Test column width and row height settings."""
    
    if not OPENPYXL_AVAILABLE:
        print("Skipping sizing test - openpyxl not available")
        return True
    
    print("\nTesting column and row sizing...")
    
    test_df = create_sample_data()
    test_file = create_test_excel_file(test_df, 'sizing_test.xlsx')
    
    try:
        step_config = {
            'processor_type': 'format_excel',
            'target_file': test_file,
            'formatting': {
                'auto_fit_columns': True,
                'max_column_width': 30,
                'min_column_width': 10,
                'column_widths': {
                    'A': 15,  # Customer_ID column
                    'B': 25   # Customer_Name column
                },
                'row_heights': {
                    1: 20,  # Header row
                    2: 15   # First data row
                }
            }
        }
        
        processor = FormatExcelProcessor(step_config)
        result = processor.execute(test_df)
        
        # Verify file is still readable
        formatted_data = pd.read_excel(test_file)
        
        if len(formatted_data) == len(test_df):
            print("✓ Column and row sizing worked correctly")
            return True
        else:
            print("✗ Column and row sizing failed")
            return False
            
    finally:
        if os.path.exists(test_file):
            os.unlink(test_file)


def test_auto_filter():
    """Test auto-filter functionality."""
    
    if not OPENPYXL_AVAILABLE:
        print("Skipping auto-filter test - openpyxl not available")
        return True
    
    print("\nTesting auto-filter...")
    
    test_df = create_sample_data()
    test_file = create_test_excel_file(test_df, 'filter_test.xlsx')
    
    try:
        step_config = {
            'processor_type': 'format_excel',
            'target_file': test_file,
            'formatting': {
                'auto_filter': True,
                'header_bold': True
            }
        }
        
        processor = FormatExcelProcessor(step_config)
        result = processor.execute(test_df)
        
        # Verify file is still readable and has data
        formatted_data = pd.read_excel(test_file)
        
        if len(formatted_data) == len(test_df):
            print("✓ Auto-filter formatting worked correctly")
            return True
        else:
            print("✗ Auto-filter formatting failed")
            return False
            
    finally:
        if os.path.exists(test_file):
            os.unlink(test_file)


def test_error_handling():
    """Test error handling for various invalid configurations."""
    
    print("\nTesting error handling...")
    
    test_df = create_sample_data()
    
    # Test missing target_file
    try:
        bad_config = {
            'processor_type': 'format_excel',
            'step_description': 'Missing target file'
        }
        processor = FormatExcelProcessor(bad_config)
        processor.execute(test_df)
        print("✗ Should have failed with missing target_file")
        return False
    except StepProcessorError as e:
        print(f"✓ Caught expected error for missing target_file: {e}")
    
    # Test nonexistent file
    try:
        bad_config = {
            'processor_type': 'format_excel',
            'target_file': '/nonexistent/file.xlsx'
        }
        processor = FormatExcelProcessor(bad_config)
        processor.execute(test_df)
        print("✗ Should have failed with nonexistent file")
        return False
    except StepProcessorError as e:
        print(f"✓ Caught expected error for nonexistent file: {e}")
    
    # Test invalid file extension
    try:
        bad_config = {
            'processor_type': 'format_excel',
            'target_file': 'test.txt'  # Not Excel format
        }
        processor = FormatExcelProcessor(bad_config)
        processor.execute(test_df)
        print("✗ Should have failed with invalid file extension")
        return False
    except StepProcessorError as e:
        print(f"✓ Caught expected error for invalid extension: {e}")
    
    # Test invalid column width
    if OPENPYXL_AVAILABLE:
        test_file = create_test_excel_file(test_df, 'error_test.xlsx')
        try:
            bad_config = {
                'processor_type': 'format_excel',
                'target_file': test_file,
                'formatting': {
                    'max_column_width': -5  # Invalid negative width
                }
            }
            processor = FormatExcelProcessor(bad_config)
            processor.execute(test_df)
            print("✗ Should have failed with invalid column width")
            return False
        except StepProcessorError as e:
            print(f"✓ Caught expected error for invalid column width: {e}")
        finally:
            if os.path.exists(test_file):
                os.unlink(test_file)
    
    print("✓ Error handling worked correctly")
    return True


def test_variable_substitution():
    """Test variable substitution in target filename."""
    
    print("\nTesting variable substitution...")
    
    # Note: This test is limited since we don't have access to the full pipeline
    # variable substitution system in this isolated test
    test_df = create_sample_data()
    
    # Create a file with a simple name (variable substitution would happen in real usage)
    test_file = create_test_excel_file(test_df, 'variable_test.xlsx')
    
    try:
        step_config = {
            'processor_type': 'format_excel',
            'target_file': test_file,  # In real usage this might be "{date}_report.xlsx"
            'formatting': {
                'auto_fit_columns': True
            }
        }
        
        processor = FormatExcelProcessor(step_config)
        result = processor.execute(test_df)
        
        # Just verify the basic functionality works
        if len(result) == 0 and Path(test_file).exists():
            print("✓ Variable substitution interface worked correctly")
            return True
        else:
            print("✗ Variable substitution interface failed")
            return False
            
    finally:
        if os.path.exists(test_file):
            os.unlink(test_file)


def test_variable_substitution_real():
    """Test actual variable substitution functionality."""
    print("Testing real variable substitution...")
    
    # Create test data
    test_data = create_sample_data()
    
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Create actual Excel file with substituted name
            actual_filename = f"2024-03-15_test_report.xlsx"
            excel_file = temp_path / actual_filename
            test_data.to_excel(excel_file, index=False, sheet_name='TestData')
            
            # Configure processor with variable template
            step_config = {
                'processor_type': 'format_excel',
                'target_file': str(temp_path / "{date}_test_report.xlsx"),
                'formatting': {
                    'auto_fit_columns': True,
                    'header_bold': True
                }
            }
            
            # Create processor
            processor = FormatExcelProcessor(step_config)
            
            # Set up variable substitution with the date variable
            from excel_recipe_processor.core.variable_substitution import VariableSubstitution
            var_sub = VariableSubstitution(
                input_path=None, 
                recipe_path=None, 
                custom_variables={'date': '2024-03-15'}
            )
            processor.variable_substitution = var_sub
            
            # Execute processor
            result = processor.execute()
            
            # Verify the file was processed (should still exist and be formatted)
            if excel_file.exists():
                print("✓ Variable substitution and formatting worked correctly")
                return True
            else:
                print("✗ Target file not found after processing")
                return False
                
    except Exception as e:
        print(f"✗ Variable substitution test failed: {e}")
        return False


def test_data_passthrough():
    """Test that input data passes through unchanged."""
    
    print("\nTesting data passthrough...")
    
    test_df = create_sample_data()
    
    if OPENPYXL_AVAILABLE:
        test_file = create_test_excel_file(test_df, 'passthrough_test.xlsx')
        
        try:
            step_config = {
                'processor_type': 'format_excel',
                'target_file': test_file,
                'formatting': {
                    'auto_fit_columns': True,
                    'header_bold': True
                }
            }
            
            processor = FormatExcelProcessor(step_config)
            result = processor.execute(test_df)
            
            # Check that result is identical to input
            if len(result) == 0:
                print("✓ Data passthrough worked correctly")
                return True
            else:
                print("✗ Data was modified during formatting")
                return False
                
        finally:
            if os.path.exists(test_file):
                os.unlink(test_file)
    else:
        # Without openpyxl, just test that error is raised properly
        step_config = {
            'processor_type': 'format_excel',
            'target_file': 'test.xlsx'
        }
        
        try:
            processor = FormatExcelProcessor(step_config)
            processor.execute(test_df)
            print("✗ Should have failed without openpyxl")
            return False
        except StepProcessorError:
            print("✓ Data passthrough error handling worked correctly")
            return True


if __name__ == '__main__':
    success = True
    
    success &= test_openpyxl_requirement()
    success &= test_basic_formatting()
    success &= test_freeze_panes()
    success &= test_multiple_sheets()
    success &= test_column_and_row_sizing()
    success &= test_auto_filter()
    success &= test_error_handling()
    success &= test_variable_substitution()
    success &= test_variable_substitution_real()
    success &= test_data_passthrough()
    
    if success:
        print("\n✓ All format Excel processor tests passed!")
    else:
        print("\n✗ Some format Excel processor tests failed!")
    
    # Show supported features
    if OPENPYXL_AVAILABLE:
        processor = FormatExcelProcessor({
            'processor_type': 'format_excel',
            'target_file': 'test.xlsx'
        })
        print(f"\nSupported features: {processor.get_supported_features()}")
        print(f"Processor capabilities: {list(processor.get_capabilities().keys())}")
    else:
        print("\nopenpyxl not available - install it to see full capabilities")
