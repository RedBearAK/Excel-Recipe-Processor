"""
Debug test for column trimming functionality.

debug_column_trimming.py

Isolates the ghost column detection and trimming to see what's going wrong.
"""

import pandas as pd
import tempfile
import os
from pathlib import Path

from excel_recipe_processor.processors.generate_column_config_processor import GenerateColumnConfigProcessor
from excel_recipe_processor.readers.openpyxl_excel_reader import OpenpyxlExcelReader


def create_excel_with_ghost_cols():
    """Create Excel data with TRUE ghost columns for testing."""
    # Create data matrix to have exact control over structure
    data_matrix = [
        # Row 1 (data)
        ['P001', 100, '', 'Active', '', '', '', ''],
        # Row 2 (data)  
        ['P002', 200, '', 'Active', '', '', '', ''],
        # Row 3 (data)
        ['P003', 150, '', 'Closed', '', '', '', '']
    ]
    
    # Column headers - simulating what openpyxl reads from Excel
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


def create_temp_excel_file(data: pd.DataFrame):
    """Create a temporary Excel file with the given data."""
    temp_file = tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False)
    temp_file.close()
    data.to_excel(temp_file.name, index=False, engine='openpyxl')
    return temp_file.name


def debug_excel_reading():
    """Debug the Excel reading and analysis step by step."""
    
    print("=== Debug Excel Reading and Analysis ===\n")
    
    # Create test data
    test_data = create_excel_with_ghost_cols()
    print("1. Created test data:")
    print(f"   Columns: {list(test_data.columns)}")
    print(f"   Shape: {test_data.shape}")
    for col in test_data.columns:
        non_empty_count = sum(1 for val in test_data[col] if str(val).strip())
        print(f"   '{col}': {non_empty_count} non-empty values")
    
    # Create Excel file
    excel_file = create_temp_excel_file(test_data)
    print(f"\n2. Created Excel file: {excel_file}")
    
    try:
        # Test direct openpyxl reading
        print("\n3. Testing direct openpyxl reading...")
        
        # Headers only
        headers_only = OpenpyxlExcelReader.read_headers(excel_file)
        print(f"   Headers only: {headers_only}")
        
        # With data check
        analysis = OpenpyxlExcelReader.read_headers_with_data_check(
            file_path=excel_file,
            max_rows=1000
        )
        
        print(f"\n4. Data analysis results:")
        print(f"   Headers: {analysis['headers']}")
        print(f"   Column has data: {analysis['column_has_data']}")
        print(f"   Empty column indices: {analysis['empty_column_indices']}")
        print(f"   Scanned data rows: {analysis['scanned_data_rows']}")
        
        # Show which columns are considered empty
        empty_cols = []
        for i, (header, has_data) in enumerate(zip(analysis['headers'], analysis['column_has_data'])):
            status = "HAS DATA" if has_data else "EMPTY"
            print(f"   Column {i+1}: '{header}' → {status}")
            if not has_data:
                empty_cols.append(header)
        
        print(f"\n5. Expected behavior:")
        print(f"   Should KEEP: Product_ID, 8/4/2025, '' (formatting), Status, Notes (template)")
        print(f"   Should TRIM: 3 trailing '' columns (true ghost columns)")
        print(f"   Detected empty columns: {empty_cols}")
        
        # Test processor trimming
        print("\n6. Testing processor trimming...")
        
        step_config = {
            'processor_type': 'generate_column_config',
            'source_file': excel_file,
            'template_file': excel_file,  # Use same file as template for simplicity
            'output_file': 'debug_output.yaml',
            'check_column_data': True
        }
        
        processor = GenerateColumnConfigProcessor(step_config)
        
        # Directly test the trimming method
        print("\n7. Testing trimming logic...")
        
        # Simulate what happens in the processor
        template_columns = analysis['headers']
        print(f"   Template columns before trimming: {template_columns}")
        
        # Set the analysis data like the processor does
        processor._last_excel_analysis = analysis
        
        # Call the trimming method
        trimmed_columns = processor._trim_trailing_empty_columns(template_columns)
        print(f"   Template columns after trimming: {trimmed_columns}")
        
        # Check what got trimmed - should keep first 5 columns, trim last 3
        expected_result = ['Product_ID', '8/4/2025', '', 'Status', 'Notes']
        if trimmed_columns == expected_result:
            print("✓ Ghost columns successfully trimmed")
        else:
            print("✗ Ghost columns were NOT trimmed correctly")
            print(f"   Expected: {expected_result}")
            print(f"   Got: {trimmed_columns}")
            
        # Show the difference
        removed = [col for col in template_columns if col not in trimmed_columns]
        print(f"   Removed columns: {removed}")
        print(f"   Removed count: {len(removed)} (expected: 3)")
        
        # Verify Notes is preserved
        if 'Notes' in trimmed_columns:
            print("✓ Template column 'Notes' correctly preserved")
        else:
            print("✗ Template column 'Notes' was incorrectly trimmed")
        
    finally:
        # Clean up
        if Path(excel_file).exists():
            os.unlink(excel_file)


def debug_csv_comparison():
    """Debug CSV vs Excel behavior for comparison."""
    
    print("\n=== Debug CSV vs Excel Comparison ===\n")
    
    # Create same data as CSV
    test_data = create_excel_with_ghost_cols()
    
    # Create CSV file
    csv_file = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
    csv_file.close()
    test_data.to_csv(csv_file.name, index=False)
    
    try:
        print("1. Testing CSV reading (no data checking)...")
        
        # Test CSV headers
        df = pd.read_csv(csv_file.name, dtype=str, nrows=0)
        csv_headers = [str(col) for col in df.columns]
        print(f"   CSV headers: {csv_headers}")
        
        # CSV doesn't have data checking, so all columns would be preserved
        print("   CSV would preserve all columns (no smart trimming)")
        
    finally:
        if Path(csv_file.name).exists():
            os.unlink(csv_file.name)


if __name__ == "__main__":
    debug_excel_reading()
    debug_csv_comparison()


# End of file #
