"""
Test the FileReader functionality.
"""

import os
import pandas as pd
import tempfile

from pathlib import Path
from datetime import datetime

from excel_recipe_processor.core.file_reader import FileReader, FileReaderError


def create_sample_data():
    """Create sample data for testing."""
    return pd.DataFrame({
        'ID': [1, 2, 3, 4, 5],
        'Name': ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'],
        'Department': ['Engineering', 'Sales', 'Marketing', 'Engineering', 'Sales'],
        'Salary': [75000, 65000, 70000, 80000, 60000],
        'Start_Date': ['2020-01-15', '2019-03-22', '2021-07-10', '2018-11-05', '2022-02-28']
    })


def create_sample_multi_sheet_data():
    """Create sample data for multi-sheet testing."""
    employees = create_sample_data()
    
    departments = pd.DataFrame({
        'Department': ['Engineering', 'Sales', 'Marketing'],
        'Manager': ['John Smith', 'Jane Doe', 'Mike Johnson'],
        'Budget': [500000, 300000, 200000]
    })
    
    return {'Employees': employees, 'Departments': departments}


def test_excel_file_reading():
    """Test reading Excel files (.xlsx)."""
    
    print("Testing Excel file reading...")
    
    # Create temporary Excel file
    sample_data = create_sample_data()
    
    with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as temp_file:
        sample_data.to_excel(temp_file.name, index=False, engine='openpyxl')
        temp_excel_path = temp_file.name
    
    try:
        # Test basic Excel reading
        result = FileReader.read_file(temp_excel_path)
        
        print(f"✓ Read Excel file: {result.shape} shape")
        print(f"✓ Columns: {list(result.columns)}")
        
        # Verify data integrity
        if len(result) == len(sample_data) and list(result.columns) == list(sample_data.columns):
            print("✓ Excel reading preserved data integrity")
            return True
        else:
            print("✗ Excel reading failed data integrity check")
            return False
            
    finally:
        # Clean up temp file
        os.unlink(temp_excel_path)


def test_multi_sheet_excel():
    """Test reading multi-sheet Excel files and getting sheet names."""
    
    print("\nTesting multi-sheet Excel functionality...")
    
    # Create temporary multi-sheet Excel file
    sheets_data = create_sample_multi_sheet_data()
    
    with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as temp_file:
        with pd.ExcelWriter(temp_file.name, engine='openpyxl') as writer:
            for sheet_name, df in sheets_data.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)
        temp_excel_path = temp_file.name
    
    try:
        # Test getting sheet names
        sheet_names = FileReader.get_excel_sheets(temp_excel_path)
        expected_sheets = list(sheets_data.keys())
        
        print(f"✓ Found sheets: {sheet_names}")
        print(f"✓ Expected sheets: {expected_sheets}")
        
        if set(sheet_names) == set(expected_sheets):
            print("✓ Sheet names match expected")
        else:
            print("✗ Sheet names don't match expected")
            return False
        
        # Test reading specific sheet by name
        employees_data = FileReader.read_file(temp_excel_path, sheet='Employees')
        print(f"✓ Read 'Employees' sheet: {employees_data.shape}")
        
        # Test reading specific sheet by index
        departments_data = FileReader.read_file(temp_excel_path, sheet=1)
        print(f"✓ Read sheet index 1: {departments_data.shape}")
        
        # Verify correct data was read
        if 'Name' in employees_data.columns and 'Manager' in departments_data.columns:
            print("✓ Multi-sheet reading worked correctly")
            return True
        else:
            print("✗ Multi-sheet reading failed")
            return False
            
    finally:
        # Clean up temp file
        os.unlink(temp_excel_path)


def test_csv_file_reading():
    """Test reading CSV files."""
    
    print("\nTesting CSV file reading...")
    
    # Create temporary CSV file
    sample_data = create_sample_data()
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as temp_file:
        sample_data.to_csv(temp_file.name, index=False)
        temp_csv_path = temp_file.name
    
    try:
        # Test basic CSV reading
        result = FileReader.read_file(temp_csv_path)
        
        print(f"✓ Read CSV file: {result.shape} shape")
        print(f"✓ Columns: {list(result.columns)}")
        
        # Test custom separator
        # Create CSV with semicolon separator
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as temp_file2:
            sample_data.to_csv(temp_file2.name, index=False, sep=';')
            temp_csv_semicolon = temp_file2.name
        
        try:
            result_semicolon = FileReader.read_file(temp_csv_semicolon, separator=';')
            print(f"✓ Read CSV with semicolon separator: {result_semicolon.shape}")
            
            if len(result_semicolon) == len(sample_data):
                print("✓ CSV reading with custom separator worked")
            else:
                print("✗ CSV reading with custom separator failed")
                return False
                
        finally:
            os.unlink(temp_csv_semicolon)
        
        # Verify data integrity
        if len(result) == len(sample_data):
            print("✓ CSV reading preserved data integrity")
            return True
        else:
            print("✗ CSV reading failed data integrity check")
            return False
            
    finally:
        # Clean up temp file
        os.unlink(temp_csv_path)


def test_tsv_file_reading():
    """Test reading TSV files."""
    
    print("\nTesting TSV file reading...")
    
    # Create temporary TSV file
    sample_data = create_sample_data()
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.tsv', delete=False) as temp_file:
        sample_data.to_csv(temp_file.name, index=False, sep='\t')
        temp_tsv_path = temp_file.name
    
    try:
        # Test basic TSV reading
        result = FileReader.read_file(temp_tsv_path)
        
        print(f"✓ Read TSV file: {result.shape} shape")
        print(f"✓ Columns: {list(result.columns)}")
        
        # Test .txt file (should be read as TSV)
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as temp_file2:
            sample_data.to_csv(temp_file2.name, index=False, sep='\t')
            temp_txt_path = temp_file2.name
        
        try:
            result_txt = FileReader.read_file(temp_txt_path)
            print(f"✓ Read .txt file as TSV: {result_txt.shape}")
            
            if len(result_txt) == len(sample_data):
                print("✓ .txt file reading as TSV worked")
            else:
                print("✗ .txt file reading as TSV failed")
                return False
                
        finally:
            os.unlink(temp_txt_path)
        
        # Verify data integrity
        if len(result) == len(sample_data):
            print("✓ TSV reading preserved data integrity")
            return True
        else:
            print("✗ TSV reading failed data integrity check")
            return False
            
    finally:
        # Clean up temp file
        os.unlink(temp_tsv_path)


def test_variable_substitution():
    """Test variable substitution in filenames."""
    
    print("\nTesting variable substitution...")
    
    # Create temporary file with today's date in name
    sample_data = create_sample_data()
    now = datetime.now()
    
    # Create file with actual date
    actual_filename = f"test_data_{now.strftime('%Y%m%d')}.xlsx"
    temp_dir = Path(tempfile.gettempdir())
    actual_path = temp_dir / actual_filename
    
    sample_data.to_excel(actual_path, index=False)
    
    try:
        # Test reading with variable substitution
        template_filename = str(temp_dir / "test_data_{date}.xlsx")
        
        result = FileReader.read_file(template_filename)
        print(f"✓ Read file with variable substitution: {result.shape}")
        
        # Test with custom variables
        custom_variables = {
            'data_type': 'employees',
            'version': 'v1'
        }
        
        # Create file matching custom variables
        custom_filename = f"test_employees_v1.xlsx"
        custom_path = temp_dir / custom_filename
        sample_data.to_excel(custom_path, index=False, engine='openpyxl')
        
        try:
            custom_template = str(temp_dir / "test_{data_type}_{version}.xlsx")
            custom_result = FileReader.read_file(custom_template, variables=custom_variables)
            print(f"✓ Read file with custom variables: {custom_result.shape}")
            
            if len(result) == len(sample_data) and len(custom_result) == len(sample_data):
                print("✓ Variable substitution worked correctly")
                return True
            else:
                print("✗ Variable substitution failed")
                return False
                
        finally:
            if custom_path.exists():
                custom_path.unlink()
            
    finally:
        # Clean up temp file
        if actual_path.exists():
            actual_path.unlink()


def test_file_existence_checking():
    """Test file existence checking with variable substitution."""
    
    print("\nTesting file existence checking...")
    
    # Create temporary file
    sample_data = create_sample_data()
    
    with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as temp_file:
        sample_data.to_excel(temp_file.name, index=False, engine='openpyxl')
        temp_excel_path = temp_file.name
    
    try:
        # Test file exists
        exists = FileReader.file_exists(temp_excel_path)
        print(f"✓ File exists check: {exists}")
        
        # Test non-existent file
        fake_path = temp_excel_path.replace('.xlsx', '_fake.xlsx')
        not_exists = FileReader.file_exists(fake_path)
        print(f"✓ Non-existent file check: {not_exists}")
        
        # Test with variable substitution
        now = datetime.now()
        actual_filename = f"exists_test_{now.strftime('%Y%m%d')}.xlsx"
        temp_dir = Path(tempfile.gettempdir())
        actual_path = temp_dir / actual_filename
        
        sample_data.to_excel(actual_path, index=False, engine='openpyxl')
        
        try:
            template_filename = str(temp_dir / "exists_test_{date}.xlsx")
            exists_with_vars = FileReader.file_exists(template_filename)
            print(f"✓ File exists with variables: {exists_with_vars}")
            
            if exists and not not_exists and exists_with_vars:
                print("✓ File existence checking worked correctly")
                return True
            else:
                print("✗ File existence checking failed")
                return False
                
        finally:
            if actual_path.exists():
                actual_path.unlink()
            
    finally:
        # Clean up temp file
        os.unlink(temp_excel_path)


def test_file_info():
    """Test getting file information."""
    
    print("\nTesting file info retrieval...")
    
    # Create temporary file
    sample_data = create_sample_data()
    
    with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as temp_file:
        sample_data.to_excel(temp_file.name, index=False)
        temp_excel_path = temp_file.name
    
    try:
        # Test file info for existing file
        info = FileReader.get_file_info(temp_excel_path)
        
        print(f"✓ File info: {info}")
        
        expected_keys = ['original_filename', 'final_filename', 'exists', 'size_bytes', 'extension', 'detected_format']
        has_all_keys = all(key in info for key in expected_keys)
        
        if has_all_keys and info['exists'] and info['detected_format'] == 'xlsx':
            print("✓ File info retrieval worked correctly")
        else:
            print("✗ File info retrieval failed")
            print(f"  Missing keys: {[k for k in expected_keys if k not in info]}")
            print(f"  File exists: {info.get('exists')}")
            print(f"  Detected format: {info.get('detected_format')} (expected 'xlsx')")
            return False
        
        # Test file info for non-existent file
        fake_path = temp_excel_path.replace('.xlsx', '_fake.xlsx')
        fake_info = FileReader.get_file_info(fake_path)
        
        print(f"✓ Non-existent file info: {fake_info}")
        
        if not fake_info['exists'] and 'error' in fake_info:
            print("✓ Non-existent file info handled correctly")
            return True
        else:
            print("✗ Non-existent file info handling failed")
            return False
            
    finally:
        # Clean up temp file
        os.unlink(temp_excel_path)


def test_format_detection():
    """Test automatic format detection."""
    
    print("\nTesting format detection...")
    
    sample_data = create_sample_data()
    
    # Test different file extensions → logical formats (no dots)
    format_tests = [
        ('.xlsx', 'xlsx'),
        ('.csv', 'csv'),
        ('.tsv', 'tsv'),
        ('.txt', 'tsv'),  # .txt files should be detected as TSV format
    ]
    
    all_passed = True
    
    for extension, expected_format in format_tests:
        with tempfile.NamedTemporaryFile(suffix=extension, delete=False) as temp_file:
            temp_path = temp_file.name
        
        try:
            # Write file in appropriate format
            if extension == '.xlsx':
                sample_data.to_excel(temp_path, index=False, engine='openpyxl')
            elif extension == '.csv':
                sample_data.to_csv(temp_path, index=False)
            else:  # tsv/txt
                sample_data.to_csv(temp_path, index=False, sep='\t')
            
            # Get file info to check detected format
            info = FileReader.get_file_info(temp_path)
            detected = info.get('detected_format')
            
            print(f"✓ {extension} → detected as {detected} (expected {expected_format})")
            
            if detected == expected_format:
                print(f"  ✓ Format detection correct for {extension}")
            else:
                print(f"  ✗ Format detection failed for {extension}")
                all_passed = False
            
        except Exception as e:
            print(f"  ✗ Error testing {extension}: {e}")
            all_passed = False
            
        finally:
            if Path(temp_path).exists():
                os.unlink(temp_path)
    
    if all_passed:
        print("✓ All format detection tests passed")
    else:
        print("✗ Some format detection tests failed")
    
    return all_passed


def test_error_handling():
    """Test error handling for various failure cases."""
    
    print("\nTesting error handling...")
    
    # Test reading non-existent file
    try:
        FileReader.read_file('/nonexistent/path/file.xlsx')
        print("✗ Should have failed for non-existent file")
        return False
    except FileReaderError as e:
        print(f"✓ Caught expected error for non-existent file: {e}")
    
    # Test getting sheets from non-Excel file
    sample_data = create_sample_data()
    
    with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as temp_file:
        sample_data.to_csv(temp_file.name, index=False)
        temp_csv_path = temp_file.name
    
    try:
        FileReader.get_excel_sheets(temp_csv_path)
        print("✗ Should have failed for non-Excel file")
        return False
    except FileReaderError as e:
        print(f"✓ Caught expected error for non-Excel file: {e}")
    finally:
        os.unlink(temp_csv_path)
    
    # Test reading non-existent sheet
    with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as temp_file:
        sample_data.to_excel(temp_file.name, index=False, engine='openpyxl')
        temp_excel_path = temp_file.name
    
    try:
        FileReader.read_file(temp_excel_path, sheet='NonExistentSheet')
        print("✗ Should have failed for non-existent sheet")
        return False
    except FileReaderError as e:
        print(f"✓ Caught expected error for non-existent sheet: {e}")
    finally:
        os.unlink(temp_excel_path)
    
    print("✓ Error handling tests passed")
    return True


def test_supported_formats():
    """Test getting supported formats information."""
    
    print("\nTesting supported formats info...")
    
    formats_info = FileReader.get_supported_formats()
    
    print(f"✓ Supported formats info: {formats_info}")
    
    expected_keys = ['excel_formats', 'csv_formats', 'tsv_formats', 'all_formats', 
                     'supported_extensions', 'extension_mapping', 'format_descriptions']
    has_all_keys = all(key in formats_info for key in expected_keys)
    
    # Check some expected formats are present (logical formats without dots)
    has_xlsx = 'xlsx' in formats_info['excel_formats']
    has_csv = 'csv' in formats_info['csv_formats'] 
    has_tsv = 'tsv' in formats_info['tsv_formats']
    
    # Check extension mapping works correctly
    has_txt_mapping = formats_info['extension_mapping'].get('.txt') == 'tsv'
    has_xlsx_mapping = formats_info['extension_mapping'].get('.xlsx') == 'xlsx'
    
    if has_all_keys and has_xlsx and has_csv and has_tsv and has_txt_mapping and has_xlsx_mapping:
        print("✓ Supported formats info is complete and correct")
        return True
    else:
        print("✗ Supported formats info is incomplete")
        print(f"  Has all keys: {has_all_keys}")
        print(f"  Has xlsx: {has_xlsx}")
        print(f"  Has csv: {has_csv}")
        print(f"  Has tsv: {has_tsv}")
        print(f"  .txt → tsv mapping: {has_txt_mapping}")
        print(f"  .xlsx → xlsx mapping: {has_xlsx_mapping}")
        return False


def test_encoding_handling():
    """Test handling of different text encodings."""
    
    print("\nTesting encoding handling...")
    
    # Create sample data with unicode characters
    unicode_data = pd.DataFrame({
        'Name': ['Café', 'Naïve', 'Résumé', 'François'],
        'City': ['Zürich', 'São Paulo', 'Köln', 'Montréal'],
        'Value': [100, 200, 150, 300]
    })
    
    # Test UTF-8 encoding (default)
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as temp_file:
        unicode_data.to_csv(temp_file.name, index=False, encoding='utf-8')
        temp_utf8_path = temp_file.name
    
    try:
        result_utf8 = FileReader.read_file(temp_utf8_path, encoding='utf-8')
        print(f"✓ Read UTF-8 CSV: {result_utf8.shape}")
        print(f"✓ Sample names: {list(result_utf8['Name'].head(2))}")
        
        # Verify unicode characters preserved
        if 'Café' in result_utf8['Name'].values:
            print("✓ UTF-8 encoding preserved unicode characters")
            return True
        else:
            print("✗ UTF-8 encoding failed to preserve unicode characters")
            return False
            
    finally:
        os.unlink(temp_utf8_path)


if __name__ == '__main__':
    print("🧪 Testing FileReader functionality...")
    print("   Now uses logical formats without dots (e.g., 'xlsx', 'csv', 'tsv')")
    print("   .txt files are correctly mapped to 'tsv' format for processing")
    print("   Note: FutureWarnings about numeric conversion are from pandas version changes")
    print("   These warnings have been addressed in the refactored version\n")
    
    success = True
    
    success &= test_excel_file_reading()
    success &= test_multi_sheet_excel()
    success &= test_csv_file_reading()
    success &= test_tsv_file_reading()
    success &= test_variable_substitution()
    success &= test_file_existence_checking()
    success &= test_file_info()
    success &= test_format_detection()
    success &= test_error_handling()
    success &= test_supported_formats()
    success &= test_encoding_handling()
    
    if success:
        print("\n✅ All FileReader tests passed!")
    else:
        print("\n❌ Some FileReader tests failed!")
    
    # Show supported formats summary
    try:
        formats = FileReader.get_supported_formats()
        print(f"\nSupported file formats: {formats['all_formats']}")
        print(f"Total formats supported: {len(formats['all_formats'])}")
    except Exception as e:
        print(f"\n⚠️  Could not get supported formats: {e}")
