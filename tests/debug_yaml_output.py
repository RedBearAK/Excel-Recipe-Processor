"""
Debug the actual YAML output to see what's being generated.

debug_yaml_output.py
"""

import pandas as pd
import tempfile
import os
from pathlib import Path

from excel_recipe_processor.processors.generate_column_config_processor import GenerateColumnConfigProcessor


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


def create_temp_excel_file(data: pd.DataFrame):
    """Create a temporary Excel file with the given data."""
    temp_file = tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False)
    temp_file.close()
    data.to_excel(temp_file.name, index=False, engine='openpyxl')
    return temp_file.name


def debug_excel_with_data_check():
    """Debug Excel processing with data checking enabled."""
    
    print("=== Debug Excel Processing with Data Check ===\n")
    
    # Create test files
    raw_data = create_raw_data()
    template_data = create_excel_with_dates_and_empty_cols()
    
    print("Template data columns:", list(template_data.columns))
    print("Template data shape:", template_data.shape)
    
    source_file = create_temp_excel_file(raw_data)
    template_file = create_temp_excel_file(template_data)
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as tmp_output:
        output_file = tmp_output.name
    
    try:
        # Test with data checking enabled
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
        
        # Read and display the actual YAML content
        with open(output_file, 'r') as f:
            content = f.read()
        
        print("Generated YAML content:")
        print("=" * 50)
        print(content)
        print("=" * 50)
        
        # Parse the desired_columns section manually
        lines = content.split('\n')
        in_desired_section = False
        desired_section_lines = []
        
        print("\nParsing desired_columns section:")
        for i, line in enumerate(lines):
            if line.startswith('desired_columns:'):
                in_desired_section = True
                print(f"Line {i}: Found start of desired_columns section")
                continue
            elif line.startswith('columns_to_create:'):
                in_desired_section = False
                print(f"Line {i}: Found end of desired_columns section")
                break
            elif in_desired_section and line.strip():
                desired_section_lines.append(line)
                print(f"Line {i}: '{line}' (stripped: '{line.strip()}')")
        
        print(f"\nDesired section lines ({len(desired_section_lines)}):")
        for i, line in enumerate(desired_section_lines):
            print(f"  {i+1}: {repr(line)}")
        
        # Count empty string entries
        empty_str_count = sum(1 for line in desired_section_lines if '- ""' in line)
        print(f"\nEmpty string count in desired_columns: {empty_str_count}")
        
        # Count total occurrences in entire file
        total_empty_str_count = content.count('- ""')
        print(f"Empty string count in entire file: {total_empty_str_count}")
        
    finally:
        # Clean up temp files
        for file_path in [source_file, template_file, output_file]:
            if Path(file_path).exists():
                os.unlink(file_path)


def debug_headers_only_mode():
    """Debug headers-only mode."""
    
    print("\n=== Debug Headers-Only Mode ===\n")
    
    # Create test files
    raw_data = create_raw_data()
    template_data = create_excel_with_dates_and_empty_cols()
    
    source_file = create_temp_excel_file(raw_data)
    template_file = create_temp_excel_file(template_data)
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as tmp_output:
        output_file = tmp_output.name
    
    try:
        # Test with data checking disabled
        step_config = {
            'processor_type': 'generate_column_config',
            'source_file': source_file,
            'template_file': template_file,
            'output_file': output_file,
            'check_column_data': False  # Headers only
        }
        
        processor = GenerateColumnConfigProcessor(step_config)
        result = processor.execute()
        
        # Read and display the actual YAML content
        with open(output_file, 'r') as f:
            content = f.read()
        
        print("Generated YAML content (headers-only mode):")
        print("=" * 50)
        print(content)
        print("=" * 50)
        
        # Parse the desired_columns section manually
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
        
        print(f"\nDesired section lines in headers-only mode ({len(desired_section_lines)}):")
        for i, line in enumerate(desired_section_lines):
            print(f"  {i+1}: {repr(line)}")
        
    finally:
        # Clean up temp files
        for file_path in [source_file, template_file, output_file]:
            if Path(file_path).exists():
                os.unlink(file_path)


if __name__ == "__main__":
    debug_excel_with_data_check()
    debug_headers_only_mode()


# End of file #
