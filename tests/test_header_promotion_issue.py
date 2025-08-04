"""
Test to demonstrate header promotion issues with import_file_processor.

tests/test_header_promotion_issue.py

Shows what happens when blank rows get promoted to column names and 
how this breaks downstream processors that reference columns by name.
"""

import pandas as pd
import tempfile
from pathlib import Path

from excel_recipe_processor.core.stage_manager import StageManager
from excel_recipe_processor.core.base_processor import StepProcessorError
from excel_recipe_processor.processors.import_file_processor import ImportFileProcessor
from excel_recipe_processor.processors.filter_data_processor import FilterDataProcessor


def create_problematic_excel_file(file_path):
    """Create an Excel file with blank first row and headers in second row."""
    
    # This mimics the user's real file structure
    data = pd.DataFrame({
        'A': ['', 'Customer_ID', 'C001', 'C002', 'C003'],      # Blank, Header, Data, Data, Data
        'B': ['', 'Product_Name', 'Widget', 'Gadget', 'Tool'], # Blank, Header, Data, Data, Data
        'C': ['', 'Status', 'Active', 'Active', 'Cancelled']   # Blank, Header, Data, Data, Data
    })
    
    # Save to Excel - pandas will write it exactly as structured
    data.to_excel(file_path, index=False, header=False)
    print(f"✓ Created Excel file with structure:")
    print(f"  Row 1: {list(data.iloc[0])}")  # Blank row
    print(f"  Row 2: {list(data.iloc[1])}")  # Headers
    print(f"  Row 3: {list(data.iloc[2])}")  # Data
    
    return data


def test_default_import_behavior():
    """Test what happens with default import settings."""
    
    print("\n" + "="*60)
    print("🔍 Testing Default Import Behavior")
    print("="*60)
    
    with tempfile.TemporaryDirectory() as temp_dir:
        excel_file = Path(temp_dir) / "problematic_file.xlsx"
        original_data = create_problematic_excel_file(excel_file)
        
        # Initialize stage manager
        StageManager.initialize_stages(max_stages=10)
        
        try:
            # Import with default settings (auto header promotion)
            import_config = {
                'processor_type': 'import_file',
                'input_file': str(excel_file),
                'save_to_stage': 'imported_data'
            }
            
            processor = ImportFileProcessor(import_config)
            imported_data = processor.execute(None)  # Import doesn't need input data
            
            print(f"\n📊 Import Results:")
            print(f"✓ Imported shape: {imported_data.shape}")
            print(f"✓ Column names: {list(imported_data.columns)}")
            print(f"✓ First row data: {list(imported_data.iloc[0])}")
            if len(imported_data) > 1:
                print(f"✓ Second row data: {list(imported_data.iloc[1])}")
            
            # Check what happened to the headers
            blank_row_promoted = any('Unnamed' in str(col) for col in imported_data.columns)
            headers_in_data = any('Customer_ID' in str(val) for val in imported_data.iloc[0] if pd.notna(val))
            
            print(f"\n🔍 Analysis:")
            print(f"✓ Blank row promoted to column names: {blank_row_promoted}")
            print(f"✓ Real headers found in data rows: {headers_in_data}")
            
            return imported_data, blank_row_promoted, headers_in_data
            
        finally:
            StageManager.cleanup_stages()


def test_filter_with_wrong_column_names():
    """Test what happens when filter_data tries to use header names that aren't column names."""
    
    print("\n" + "="*60)
    print("🔍 Testing Filter with Expected Column Names")
    print("="*60)
    
    with tempfile.TemporaryDirectory() as temp_dir:
        excel_file = Path(temp_dir) / "problematic_file.xlsx"
        create_problematic_excel_file(excel_file)
        
        # Initialize stage manager
        StageManager.initialize_stages(max_stages=10)
        
        try:
            # Import the file
            import_config = {
                'processor_type': 'import_file',
                'input_file': str(excel_file),
                'save_to_stage': 'raw_data'
            }
            
            processor = ImportFileProcessor(import_config)
            imported_data = processor.execute(None)
            
            print(f"✓ Available columns: {list(imported_data.columns)}")
            
            # Now try to filter using the expected header name
            filter_config = {
                'processor_type': 'filter_data',
                'source_stage': 'raw_data',
                'save_to_stage': 'filtered_data',
                'filters': [
                    {
                        'column': 'Customer_ID',  # This is what user expects to work
                        'condition': 'equals',
                        'value': 'C001'
                    }
                ]
            }
            
            print(f"\n🔍 Attempting to filter on column 'Customer_ID'...")
            
            try:
                filter_processor = FilterDataProcessor(filter_config)
                filtered_data = filter_processor.execute(imported_data)
                
                print(f"✓ Filter succeeded: {len(filtered_data)} rows")
                return True, "Filter worked unexpectedly"
                
            except StepProcessorError as e:
                print(f"❌ Filter failed as expected: {e}")
                
                # Check if error mentions available columns
                error_msg = str(e)
                mentions_available = "Available columns" in error_msg
                
                print(f"✓ Error mentions available columns: {mentions_available}")
                
                return False, error_msg
                
        finally:
            StageManager.cleanup_stages()


def test_filter_with_actual_column_names():
    """Test filtering with the actual column names (Unnamed: 0, etc.)."""
    
    print("\n" + "="*60)
    print("🔍 Testing Filter with Actual Column Names")
    print("="*60)
    
    with tempfile.TemporaryDirectory() as temp_dir:
        excel_file = Path(temp_dir) / "problematic_file.xlsx"
        create_problematic_excel_file(excel_file)
        
        # Initialize stage manager
        StageManager.initialize_stages(max_stages=10)
        
        try:
            # Import the file
            import_config = {
                'processor_type': 'import_file',
                'input_file': str(excel_file),
                'save_to_stage': 'raw_data'
            }
            
            processor = ImportFileProcessor(import_config)
            imported_data = processor.execute(None)
            
            actual_col_name = imported_data.columns[0]  # Usually "Unnamed: 0"
            print(f"✓ Using actual column name: '{actual_col_name}'")
            
            # Try to filter on the actual column name
            filter_config = {
                'processor_type': 'filter_data', 
                'source_stage': 'raw_data',
                'save_to_stage': 'filtered_data',
                'filters': [
                    {
                        'column': actual_col_name,  # Use the actual column name
                        'condition': 'equals',
                        'value': 'Customer_ID'  # Looking for the header row
                    }
                ]
            }
            
            print(f"🔍 Filtering for rows where '{actual_col_name}' = 'Customer_ID'...")
            
            try:
                filter_processor = FilterDataProcessor(filter_config)
                filtered_data = filter_processor.execute(imported_data)
                
                print(f"✓ Filter succeeded: {len(filtered_data)} rows")
                if len(filtered_data) > 0:
                    print(f"✓ Found header row: {list(filtered_data.iloc[0])}")
                
                return True, filtered_data
                
            except StepProcessorError as e:
                print(f"❌ Filter failed: {e}")
                return False, str(e)
                
        finally:
            StageManager.cleanup_stages()


def demonstrate_workaround_with_slice():
    """Show how slice_data can be used to work around the header promotion issue."""
    
    print("\n" + "="*60)
    print("🔍 Demonstrating Slice Workaround")
    print("="*60)
    
    with tempfile.TemporaryDirectory() as temp_dir:
        excel_file = Path(temp_dir) / "problematic_file.xlsx"
        create_problematic_excel_file(excel_file)
        
        # Initialize stage manager
        StageManager.initialize_stages(max_stages=10)
        
        try:
            # Import the file (gets wrong headers)
            import_config = {
                'processor_type': 'import_file',
                'input_file': str(excel_file),
                'save_to_stage': 'raw_import'
            }
            
            processor = ImportFileProcessor(import_config)
            imported_data = processor.execute(None)
            
            print(f"✓ Raw import columns: {list(imported_data.columns)}")
            print(f"✓ Raw import data preview:")
            for i in range(min(3, len(imported_data))):
                print(f"  Row {i}: {list(imported_data.iloc[i])}")
            
            # Use slice_data to extract from row 2 onward with header promotion
            # This is similar to what the user had to do
            from excel_recipe_processor.processors.slice_data_processor import SliceDataProcessor
            
            slice_config = {
                'processor_type': 'slice_data',
                'source_stage': 'raw_import',
                'slice_type': 'row_range',
                'start_row': 2,  # Start from row where headers are
                'slice_result_contains_headers': True,
                'save_to_stage': 'clean_data'
            }
            
            slice_processor = SliceDataProcessor(slice_config)
            clean_data = slice_processor.execute(imported_data)
            
            print(f"\n✓ After slice processing:")
            print(f"✓ Clean data columns: {list(clean_data.columns)}")
            print(f"✓ Clean data shape: {clean_data.shape}")
            if len(clean_data) > 0:
                print(f"✓ First data row: {list(clean_data.iloc[0])}")
            
            # Now try the filter that should work
            filter_config = {
                'processor_type': 'filter_data',
                'source_stage': 'clean_data', 
                'save_to_stage': 'filtered_result',
                'filters': [
                    {
                        'column': 'Customer_ID',  # This should work now!
                        'condition': 'equals',
                        'value': 'C001'
                    }
                ]
            }
            
            print(f"\n🔍 Testing filter with proper column names...")
            
            filter_processor = FilterDataProcessor(filter_config)
            filtered_result = filter_processor.execute(clean_data)
            
            print(f"✅ Filter succeeded! Found {len(filtered_result)} matching rows")
            if len(filtered_result) > 0:
                print(f"✓ Filtered result: {list(filtered_result.iloc[0])}")
            
            return True
            
        except Exception as e:
            print(f"❌ Workaround failed: {e}")
            return False
            
        finally:
            StageManager.cleanup_stages()


def main():
    """Run all header promotion tests."""
    
    print("🔍 TESTING HEADER PROMOTION ISSUES")
    print("="*80)
    print("This test demonstrates the fundamental problem with files that have")
    print("blank rows before headers, and how it breaks downstream processors.")
    
    # Test 1: Show what default import does
    import_result = test_default_import_behavior()
    
    # Test 2: Show that filters fail with expected column names  
    filter_result = test_filter_with_wrong_column_names()
    
    # Test 3: Show that filters work with actual (wrong) column names
    actual_filter_result = test_filter_with_actual_column_names()
    
    # Test 4: Show the slice workaround
    workaround_result = demonstrate_workaround_with_slice()
    
    print("\n" + "="*80)
    print("📋 SUMMARY")
    print("="*80)
    
    if import_result:
        imported_data, blank_promoted, headers_in_data = import_result
        print(f"✓ Import promoted blank row to columns: {blank_promoted}")
        print(f"✓ Real headers ended up in data: {headers_in_data}")
    
    if filter_result:
        filter_failed, error_msg = filter_result
        print(f"✓ Filter with expected column names failed: {filter_failed}")
    
    if actual_filter_result:
        actual_worked, result = actual_filter_result
        print(f"✓ Filter with actual column names worked: {actual_worked}")
    
    print(f"✓ Slice workaround succeeded: {workaround_result}")
    
    print("\n🚨 CONCLUSION:")
    print("This demonstrates why we need proper header parameter support")
    print("in import_file_processor to handle files with metadata/blank rows.")
    
    return workaround_result


if __name__ == '__main__':
    main()


# End of file #
