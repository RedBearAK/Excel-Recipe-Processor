"""
Test the GenerateColumnConfigProcessor (FileOps version) functionality.

tests/test_generate_column_config_fileops.py

Tests the enhanced file-based processor that supports both CSV and Excel files
with openpyxl reading, smart column detection, and improved trimming.
"""

import os
import pandas as pd
import tempfile
import yaml

from pathlib import Path

from excel_recipe_processor.processors.generate_column_config_processor import GenerateColumnConfigProcessor


def create_raw_data():
    """Create sample data with raw/technical column names."""
    return pd.DataFrame({
        'cust_id': [1, 2, 3, 4],
        'ord_dt': ['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04'], 
        'prod_sku': ['A001', 'B002', 'C003', 'D004'],
        'qty': [10, 20, 15, 25],
        'amt_usd': [100.50, 250.00, 175.25, 300.75],
        'ship_st': ['Pending', 'Shipped', 'Delivered', 'Processing']
    })


def create_desired_data():
    """Create sample data with desired business-friendly column names."""
    return pd.DataFrame({
        'Customer Code': [1, 2, 3, 4],
        'Order Date': ['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04'],
        'Product SKU': ['A001', 'B002', 'C003', 'D004'], 
        'Quantity': [10, 20, 15, 25],
        'Amount (USD)': [100.50, 250.00, 175.25, 300.75],
        'Shipping Status': ['Pending', 'Shipped', 'Delivered', 'Processing'],
        'Sales Rep': ['Alice', 'Bob', '', 'Charlie'],    # Some data
        'Region': ['', '', '', ''],                      # No data (ghost column)
        'Notes': ['', '', '', '']                        # No data (ghost column)
    })


def create_excel_with_dates_and_empty_cols():
    """Create Excel data with meaningful columns and TRUE ghost columns at the end."""
    # Create data matrix manually to have exact control over columns
    data_matrix = [
        # Row 1 (data)
        ['P001', 100, '', 'Active', '', '', '', ''],
        # Row 2 (data)  
        ['P002', 200, '', 'Active', '', '', '', ''],
        # Row 3 (data)
        ['P003', 150, '', 'Closed', '', '', '', '']
    ]
    
    # Column headers - this is what openpyxl would read from Excel
    column_headers = [
        'Product_ID',    # Has header + data → Keep
        '8/4/2025',      # Has header + data → Keep  
        '',              # No header, no data, but between meaningful columns → Keep (formatting)
        'Status',        # Has header + data → Keep
        'Notes',         # Has header, no data → Keep (template column)
        '',              # No header, no data, trailing → GHOST (should trim)
        '',              # No header, no data, trailing → GHOST (should trim)
        ''               # No header, no data, trailing → GHOST (should trim)
    ]
    
    # Create DataFrame with explicit column names
    df = pd.DataFrame(data_matrix, columns=column_headers)
    
    return df


def create_temp_csv_file(data: pd.DataFrame, suffix='.csv'):
    """Create a temporary CSV file with the given data."""
    temp_file = tempfile.NamedTemporaryFile(mode='w', suffix=suffix, delete=False)
    temp_file.close()
    data.to_csv(temp_file.name, index=False)
    return temp_file.name


def create_temp_excel_file(data: pd.DataFrame, suffix='.xlsx'):
    """Create a temporary Excel file with the given data."""
    temp_file = tempfile.NamedTemporaryFile(suffix=suffix, delete=False)
    temp_file.close()
    data.to_excel(temp_file.name, index=False, engine='openpyxl')
    return temp_file.name


def test_basic_csv_processing():
    """Test basic CSV file processing."""
    
    print("Testing basic CSV file processing...")
    
    # Create test files
    raw_data = create_raw_data()
    desired_data = create_desired_data()
    
    source_file = create_temp_csv_file(raw_data)
    template_file = create_temp_csv_file(desired_data)
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as tmp_output:
        output_file = tmp_output.name
    
    try:
        # Test the processor
        step_config = {
            'processor_type': 'generate_column_config',
            'source_file': source_file,
            'template_file': template_file,
            'output_file': output_file,
            'similarity_threshold': 0.6  # Lower threshold to catch more renames
        }
        
        processor = GenerateColumnConfigProcessor(step_config)
        result = processor.execute()
        
        # Check that output file was created
        if not Path(output_file).exists():
            print("✗ Configuration file was not created")
            return False
        
        # Read and parse the generated configuration
        with open(output_file, 'r') as f:
            content = f.read()
        
        # Check for expected sections
        required_sections = ['raw_columns:', 'desired_columns:', 'columns_to_create:', 'rename_mapping:']
        missing_sections = [section for section in required_sections if section not in content]
        
        if missing_sections:
            print(f"✗ Missing sections: {missing_sections}")
            return False
        
        # Check that fuzzy matching found some renames
        if 'cust_id' in content and 'ord_dt' in content:
            print("✓ Fuzzy matching detected potential renames")
        else:
            print("✗ Fuzzy matching failed to detect obvious renames")
            return False
        
        # Check that columns to create were identified
        if 'Sales Rep' in content:
            print("✓ Columns to create identified correctly")
        else:
            print("✗ Failed to identify columns to create")
            return False
        
        print("✓ Basic CSV processing test passed")
        return True
        
    finally:
        # Clean up temp files
        for file_path in [source_file, template_file, output_file]:
            if Path(file_path).exists():
                os.unlink(file_path)


def test_excel_processing_with_data_check():
    """Test Excel file processing with smart column detection."""
    
    print("\nTesting Excel processing with data checking...")
    
    # Create test data with ghost columns
    raw_data = create_raw_data()
    template_data = create_excel_with_dates_and_empty_cols()
    
    source_file = create_temp_excel_file(raw_data)
    template_file = create_temp_excel_file(template_data)
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as tmp_output:
        output_file = tmp_output.name
    
    try:
        # Test with data checking enabled (default)
        step_config = {
            'processor_type': 'generate_column_config',
            'source_file': source_file,
            'template_file': template_file,
            'output_file': output_file,
            'check_column_data': True,
            'max_rows': 1000
        }
        
        processor = GenerateColumnConfigProcessor(step_config)
        result = processor.execute()
        
        # Read the generated configuration
        with open(output_file, 'r') as f:
            content = f.read()
        
        # Check that ghost columns (trailing empty columns with no headers) were trimmed
        # Notes should be KEPT (has header, even if no data - it's a template column)
        if 'Notes' in content:
            print("✓ Template column 'Notes' preserved correctly")
        else:
            print("✗ Template column 'Notes' was incorrectly trimmed")
            return False
        
        # The last meaningful column should be 'Notes', so result should end there
        # The 3 trailing empty columns with no headers should be trimmed
        with open(output_file, 'r') as f:
            config_content = f.read()
        
        # Count empty string entries in desired_columns to see if ghost columns were trimmed
        # Should have: Product_ID, 8/4/2025, "", Status, Notes (5 total)
        # Should NOT have the 3 trailing "" ghost columns
        empty_str_count = config_content.count('- ""')
        if empty_str_count == 1:  # Only the formatting column between data
            print("✓ Ghost columns trimmed correctly")
        else:
            print(f"✗ Expected 1 empty column (formatting), found {empty_str_count}")
            return False
        
        # Check that date header was preserved (not converted by pandas)
        if '8/4/2025' in content:
            print("✓ Date format preserved in Excel reading")
        else:
            print("✗ Date format was not preserved")
            return False
        
        # Check that empty column between data was preserved
        if '""' in content:
            print("✓ Empty columns between data preserved")
        else:
            print("✗ Empty columns between data not preserved")
            return False
        
        print("✓ Excel processing with data check test passed")
        return True
        
    finally:
        # Clean up temp files
        for file_path in [source_file, template_file, output_file]:
            if Path(file_path).exists():
                os.unlink(file_path)


def test_headers_only_mode():
    """Test header-only mode (no data checking)."""
    
    print("\nTesting headers-only mode...")
    
    template_data = create_excel_with_dates_and_empty_cols()
    source_file = create_temp_csv_file(create_raw_data())
    template_file = create_temp_excel_file(template_data)
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as tmp_output:
        output_file = tmp_output.name
    
    try:
        step_config = {
            'processor_type': 'generate_column_config',
            'source_file': source_file,
            'template_file': template_file,
            'output_file': output_file,
            'check_column_data': False  # Disable data checking
        }
        
        processor = GenerateColumnConfigProcessor(step_config)
        result = processor.execute()
        
        # Read the generated configuration
        with open(output_file, 'r') as f:
            content = f.read()
        
        # In headers-only mode, columns should still be trimmed based on headers
        # (not data), but the result should be the same: preserve meaningful columns
        if 'Notes' in content:
            print("✓ Headers-only mode preserved columns with headers")
        else:
            print("✗ Headers-only mode incorrectly trimmed columns with headers")
            return False
        
        # Check that trailing empty-header columns were still trimmed
        # Should have the same 5 columns as data-checking mode, just for different reasons
        lines = content.split('\n')
        in_desired_section = False
        desired_section_lines = []
        
        for line in lines:
            if line.startswith('desired_columns:'):
                in_desired_section = True
                continue
            elif line.startswith('columns_to_create:'):
                in_desired_section = False
                break
            elif in_desired_section and line.strip():
                desired_section_lines.append(line)
        
        # Should have 5 columns total: Product_ID, 8/4/2025, "", Status, Notes
        if len(desired_section_lines) == 5:
            print("✓ Headers-only mode trimmed trailing empty-header columns")
        else:
            print(f"✗ Expected 5 columns in headers-only mode, found {len(desired_section_lines)}")
            return False
        
        print("✓ Headers-only mode test passed")
        return True
        
    finally:
        # Clean up temp files
        for file_path in [source_file, template_file, output_file]:
            if Path(file_path).exists():
                os.unlink(file_path)


def test_recipe_section_generation():
    """Test recipe section generation."""
    
    print("\nTesting recipe section generation...")
    
    source_file = create_temp_csv_file(create_raw_data())
    template_file = create_temp_csv_file(create_desired_data())
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as tmp_output:
        output_file = tmp_output.name
    
    try:
        step_config = {
            'processor_type': 'generate_column_config',
            'source_file': source_file,
            'template_file': template_file,
            'output_file': output_file,
            'include_recipe_section': True
        }
        
        processor = GenerateColumnConfigProcessor(step_config)
        result = processor.execute()
        
        # Read the generated configuration
        with open(output_file, 'r') as f:
            content = f.read()
        
        # Check for recipe section
        recipe_indicators = ['recipe_section:', 'rename_columns', 'select_columns', 'columns_to_keep:', 'columns_to_create:']
        missing_indicators = [indicator for indicator in recipe_indicators if indicator not in content]
        
        if missing_indicators:
            print(f"✗ Missing recipe indicators: {missing_indicators}")
            return False
        
        print("✓ Recipe section generation test passed")
        return True
        
    finally:
        # Clean up temp files
        for file_path in [source_file, template_file, output_file]:
            if Path(file_path).exists():
                os.unlink(file_path)


def test_mixed_file_types():
    """Test mixing CSV source with Excel template."""
    
    print("\nTesting mixed file types (CSV + Excel)...")
    
    source_file = create_temp_csv_file(create_raw_data())
    template_file = create_temp_excel_file(create_desired_data())
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as tmp_output:
        output_file = tmp_output.name
    
    try:
        step_config = {
            'processor_type': 'generate_column_config',
            'source_file': source_file,
            'template_file': template_file,
            'output_file': output_file
        }
        
        processor = GenerateColumnConfigProcessor(step_config)
        result = processor.execute()
        
        # Check that output was created successfully
        if Path(output_file).exists():
            print("✓ Mixed file types processing succeeded")
            return True
        else:
            print("✗ Mixed file types processing failed")
            return False
        
    finally:
        # Clean up temp files
        for file_path in [source_file, template_file, output_file]:
            if Path(file_path).exists():
                os.unlink(file_path)


def test_error_handling():
    """Test error handling for various edge cases."""
    
    print("\nTesting error handling...")
    
    # Test missing source_file
    try:
        step_config = {
            'processor_type': 'generate_column_config',
            'template_file': 'template.csv',
            'output_file': 'output.yaml'
            # Missing source_file
        }
        processor = GenerateColumnConfigProcessor(step_config)
        print("✗ Should have failed with missing source_file")
        return False
    except Exception:
        print("✓ Properly validates missing source_file")
    
    # Test missing template_file
    try:
        step_config = {
            'processor_type': 'generate_column_config',
            'source_file': 'source.csv',
            'output_file': 'output.yaml'
            # Missing template_file
        }
        processor = GenerateColumnConfigProcessor(step_config)
        print("✗ Should have failed with missing template_file")
        return False
    except Exception:
        print("✓ Properly validates missing template_file")
    
    # Test missing output_file
    try:
        step_config = {
            'processor_type': 'generate_column_config',
            'source_file': 'source.csv',
            'template_file': 'template.csv'
            # Missing output_file
        }
        processor = GenerateColumnConfigProcessor(step_config)
        print("✗ Should have failed with missing output_file")
        return False
    except Exception:
        print("✓ Properly validates missing output_file")
    
    # Test unsupported file format
    try:
        step_config = {
            'processor_type': 'generate_column_config',
            'source_file': 'source.txt',  # Unsupported format
            'template_file': 'template.csv',
            'output_file': 'output.yaml'
        }
        processor = GenerateColumnConfigProcessor(step_config)
        print("✗ Should have failed with unsupported file format")
        return False
    except Exception:
        print("✓ Properly validates file formats")
    
    return True


def test_configuration_options():
    """Test various configuration options."""
    
    print("\nTesting configuration options...")
    
    source_file = create_temp_csv_file(create_raw_data())
    template_file = create_temp_csv_file(create_desired_data())
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as tmp_output:
        output_file = tmp_output.name
    
    try:
        # Test with custom similarity threshold
        step_config = {
            'processor_type': 'generate_column_config',
            'source_file': source_file,
            'template_file': template_file,
            'output_file': output_file,
            'similarity_threshold': 0.9,  # Very high threshold
            'header_row': 1,              # Default but explicit
            'max_rows': 50000             # Custom limit
        }
        
        processor = GenerateColumnConfigProcessor(step_config)
        result = processor.execute()
        
        # With high similarity threshold, fewer renames should be detected
        with open(output_file, 'r') as f:
            content = f.read()
        
        # Should still create output even with strict matching
        if Path(output_file).exists() and len(content) > 100:
            print("✓ Configuration options processing succeeded")
            return True
        else:
            print("✗ Configuration options processing failed")
            return False
        
    finally:
        # Clean up temp files
        for file_path in [source_file, template_file, output_file]:
            if Path(file_path).exists():
                os.unlink(file_path)


def main():
    """Run all tests and report results."""
    
    print("=== GenerateColumnConfigProcessor FileOps Tests ===\n")
    
    tests = [
        test_basic_csv_processing,
        test_excel_processing_with_data_check,
        test_headers_only_mode,
        test_recipe_section_generation,
        test_mixed_file_types,
        test_error_handling,
        test_configuration_options
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
        print("\n✅ All GenerateColumnConfigProcessor FileOps tests passed!")
        return 1
    else:
        print("\n❌ Some GenerateColumnConfigProcessor FileOps tests failed!")
        return 0


if __name__ == "__main__":
    exit(main())


# End of file #
