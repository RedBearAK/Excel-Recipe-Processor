"""
Test the FileWriter functionality.
"""

import os
import sys

import pandas as pd
import tempfile
from pathlib import Path
from datetime import datetime

from excel_recipe_processor.core.file_writer import FileWriter, FileWriterError


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


def create_unicode_data():
    """Create sample data with unicode characters for encoding tests."""
    return pd.DataFrame({
        'Name': ['Café', 'Naïve', 'Résumé', 'François'],
        'City': ['Zürich', 'São Paulo', 'Köln', 'Montréal'],
        'Value': [100, 200, 150, 300]
    })


def test_excel_file_writing():
    """Test writing Excel files (.xlsx)."""
    
    print("Testing Excel file writing...")
    
    sample_data = create_sample_data()
    
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir) / "test_output.xlsx"
        
        # Test basic Excel writing
        final_path = FileWriter.write_file(sample_data, str(temp_path))
        
        print(f"✓ Wrote Excel file: {final_path}")
        
        # Verify file was created
        if Path(final_path).exists():
            print("✓ Excel file exists after writing")
        else:
            print("✗ Excel file not found after writing")
            return False
        
        # Verify file can be read back
        try:
            verify_data = pd.read_excel(final_path)
            
            if len(verify_data) == len(sample_data) and list(verify_data.columns) == list(sample_data.columns):
                print("✓ Excel file content verification passed")
                return True
            else:
                print("✗ Excel file content verification failed")
                return False
        except Exception as e:
            print(f"✗ Error reading back Excel file: {e}")
            return False


def test_csv_file_writing():
    """Test writing CSV files."""
    
    print("\nTesting CSV file writing...")
    
    sample_data = create_sample_data()
    
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir) / "test_output.csv"
        
        # Test basic CSV writing
        final_path = FileWriter.write_file(sample_data, str(temp_path))
        
        print(f"✓ Wrote CSV file: {final_path}")
        
        # Test custom separator
        temp_path_semicolon = Path(temp_dir) / "test_semicolon.csv"
        final_path_semicolon = FileWriter.write_file(
            sample_data, str(temp_path_semicolon), separator=';'
        )
        
        print(f"✓ Wrote CSV with semicolon separator: {final_path_semicolon}")
        
        # Verify files exist and have correct content
        if Path(final_path).exists() and Path(final_path_semicolon).exists():
            print("✓ Both CSV files exist after writing")
        else:
            print("✗ CSV files not found after writing")
            return False
        
        # Verify content by reading back
        try:
            verify_data = pd.read_csv(final_path)
            verify_data_semicolon = pd.read_csv(final_path_semicolon, sep=';')
            
            same_shape = (len(verify_data) == len(sample_data) and 
                         len(verify_data_semicolon) == len(sample_data))
            
            if same_shape:
                print("✓ CSV file content verification passed")
                return True
            else:
                print("✗ CSV file content verification failed")
                return False
                
        except Exception as e:
            print(f"✗ Error reading back CSV files: {e}")
            return False


def test_tsv_file_writing():
    """Test writing TSV files."""
    
    print("\nTesting TSV file writing...")
    
    sample_data = create_sample_data()
    
    with tempfile.TemporaryDirectory() as temp_dir:
        # Test .tsv extension
        temp_path_tsv = Path(temp_dir) / "test_output.tsv"
        final_path_tsv = FileWriter.write_file(sample_data, str(temp_path_tsv))
        
        print(f"✓ Wrote TSV file: {final_path_tsv}")
        
        # Test .txt extension (should be written as TSV)
        temp_path_txt = Path(temp_dir) / "test_output.txt"
        final_path_txt = FileWriter.write_file(sample_data, str(temp_path_txt))
        
        print(f"✓ Wrote .txt file as TSV: {final_path_txt}")
        
        # Verify files exist
        if Path(final_path_tsv).exists() and Path(final_path_txt).exists():
            print("✓ Both TSV files exist after writing")
        else:
            print("✗ TSV files not found after writing")
            return False
        
        # Verify content by reading back
        try:
            verify_data_tsv = pd.read_csv(final_path_tsv, sep='\t')
            verify_data_txt = pd.read_csv(final_path_txt, sep='\t')
            
            same_shape = (len(verify_data_tsv) == len(sample_data) and 
                         len(verify_data_txt) == len(sample_data))
            
            if same_shape:
                print("✓ TSV file content verification passed")
                return True
            else:
                print("✗ TSV file content verification failed")
                return False
                
        except Exception as e:
            print(f"✗ Error reading back TSV files: {e}")
            return False


def test_multi_sheet_excel():
    """Test writing multi-sheet Excel files."""
    
    print("\nTesting multi-sheet Excel writing...")
    
    sheets_data = create_sample_multi_sheet_data()
    
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir) / "multi_sheet_test.xlsx"
        
        # Test multi-sheet writing
        final_path = FileWriter.write_multi_sheet_excel(
            sheets_data, str(temp_path), active_sheet='Departments'
        )
        
        print(f"✓ Wrote multi-sheet Excel: {final_path}")
        
        # Verify file exists
        if not Path(final_path).exists():
            print("✗ Multi-sheet Excel file not found after writing")
            return False
        
        # Verify sheets by reading back
        try:
            excel_file = pd.ExcelFile(final_path)
            found_sheets = excel_file.sheet_names
            expected_sheets = list(sheets_data.keys())
            
            print(f"✓ Found sheets: {found_sheets}")
            print(f"✓ Expected sheets: {expected_sheets}")
            
            if set(found_sheets) == set(expected_sheets):
                print("✓ All sheets found in multi-sheet file")
            else:
                print("✗ Sheet mismatch in multi-sheet file")
                return False
            
            # Verify content of each sheet
            for sheet_name in expected_sheets:
                sheet_data = pd.read_excel(final_path, sheet_name=sheet_name)
                original_data = sheets_data[sheet_name]
                
                if len(sheet_data) == len(original_data):
                    print(f"  ✓ Sheet '{sheet_name}' content verified")
                else:
                    print(f"  ✗ Sheet '{sheet_name}' content mismatch")
                    return False
            
            print("✓ Multi-sheet Excel writing worked correctly")
            return True
            
        except Exception as e:
            print(f"✗ Error verifying multi-sheet Excel: {e}")
            return False


def test_variable_substitution():
    """Test variable substitution in output filenames."""
    
    print("\nTesting variable substitution layer boundary...")

    sample_data = create_sample_data()

    with tempfile.TemporaryDirectory() as temp_dir:
        # FileWriter does NOT substitute variables (2026 doctrine): the
        # processor layer resolves {date}-style templates BEFORE calling
        # FileWriter. Braces are legal filename characters, so a template
        # reaching this layer unresolved is written literally - visible
        # evidence of an upstream bug rather than a silent wrong file.
        template_filename = str(Path(temp_dir) / "output_{date}.xlsx")

        FileWriter.write_file(sample_data, template_filename)

        literal_path = Path(template_filename)
        if literal_path.exists():
            print("✓ Unresolved template written literally, braces visible on disk")
        else:
            print("✗ Literal-braces file not found where written")
            return False

        # The retired variables= parameter must not be silently accepted
        try:
            FileWriter.write_file(
                sample_data, str(Path(temp_dir) / "x.csv"),
                variables={'project': 'p'}
            )
            print("✗ Retired variables= parameter was accepted")
            return False
        except TypeError:
            print("✓ Retired variables= parameter rejected")
            return True


def test_format_detection():
    """Test automatic format detection from file extensions."""
    
    print("\nTesting format detection...")
    
    sample_data = create_sample_data()
    
    # Test different extensions and verify correct format processing
    format_tests = [
        ('.xlsx', 'xlsx'),
        ('.csv', 'csv'),
        ('.tsv', 'tsv'),
        ('.txt', 'tsv'),  # .txt should be processed as TSV
    ]
    
    all_passed = True
    
    with tempfile.TemporaryDirectory() as temp_dir:
        for extension, expected_format in format_tests:
            temp_path = Path(temp_dir) / f"test{extension}"
            
            try:
                # Write file and get info
                final_path = FileWriter.write_file(sample_data, str(temp_path))
                info = FileWriter.get_output_info(str(temp_path))
                
                detected_format = info.get('detected_format')
                
                print(f"✓ {extension} → detected as {detected_format} (expected {expected_format})")
                
                if detected_format == expected_format and Path(final_path).exists():
                    print(f"  ✓ Format detection correct for {extension}")
                else:
                    print(f"  ✗ Format detection failed for {extension}")
                    all_passed = False
                
            except Exception as e:
                print(f"  ✗ Error testing {extension}: {e}")
                all_passed = False
    
    if all_passed:
        print("✓ All format detection tests passed")
    else:
        print("✗ Some format detection tests failed")
    
    return all_passed


def test_backup_creation():
    """Test backup file creation."""
    
    print("\nTesting backup creation...")
    
    sample_data = create_sample_data()
    
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir) / "test_backup.xlsx"
        
        # Create initial file
        FileWriter.write_file(sample_data, str(temp_path))
        
        if not temp_path.exists():
            print("✗ Initial file creation failed")
            return False
        
        print("✓ Created initial file for backup test")
        
        # Write again with backup enabled
        modified_data = sample_data.copy()
        modified_data['NewColumn'] = ['A', 'B', 'C', 'D', 'E']
        
        final_path = FileWriter.write_file(
            modified_data, str(temp_path), create_backup=True
        )
        
        print(f"✓ Wrote file with backup enabled: {final_path}")
        
        # Check for backup file
        # Backup naming: {stem}_erpbkup_{timestamp}{ext}
        backup_files = list(temp_path.parent.glob(
            f"{temp_path.stem}_erpbkup_*{temp_path.suffix}"))
        
        if backup_files:
            print(f"✓ Backup file created: {backup_files[0]}")
            
            # Verify backup content (should be original data)
            try:
                backup_data = pd.read_excel(backup_files[0])
                if len(backup_data.columns) == len(sample_data.columns):  # Original columns
                    print("✓ Backup contains original data")
                    
                    # Verify new file has modified data
                    new_data = pd.read_excel(final_path)
                    if 'NewColumn' in new_data.columns:
                        print("✓ New file contains modified data")
                        return True
                    else:
                        print("✗ New file doesn't contain modifications")
                        return False
                else:
                    print("✗ Backup data verification failed")
                    return False
                    
            except Exception as e:
                print(f"✗ Error verifying backup: {e}")
                return False
        else:
            print("✗ No backup file found")
            return False


def test_encoding_handling():
    """Test handling of different text encodings."""
    
    print("\nTesting encoding handling...")
    
    unicode_data = create_unicode_data()
    
    with tempfile.TemporaryDirectory() as temp_dir:
        # Test UTF-8 encoding (default)
        temp_path_utf8 = Path(temp_dir) / "unicode_test.csv"
        
        final_path = FileWriter.write_file(
            unicode_data, str(temp_path_utf8), encoding='utf-8'
        )
        
        print(f"✓ Wrote UTF-8 CSV: {final_path}")
        
        # Verify by reading back
        try:
            verify_data = pd.read_csv(final_path, encoding='utf-8')
            
            print(f"✓ Read back UTF-8 CSV: {verify_data.shape}")
            print(f"✓ Sample names: {list(verify_data['Name'].head(2))}")
            
            # Check if unicode characters are preserved
            if 'Café' in verify_data['Name'].values:
                print("✓ UTF-8 encoding preserved unicode characters")
                return True
            else:
                print("✗ UTF-8 encoding failed to preserve unicode characters")
                return False
                
        except Exception as e:
            print(f"✗ Error verifying UTF-8 encoding: {e}")
            return False


def test_file_info():
    """Test getting output file information."""
    
    print("\nTesting output file info...")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir) / "info_test.xlsx"
        
        # Get info before file exists
        info_before = FileWriter.get_output_info(str(temp_path))
        
        print(f"✓ Info before writing: {info_before}")
        
        # Write file
        sample_data = create_sample_data()
        FileWriter.write_file(sample_data, str(temp_path))
        
        # Get info after file exists
        info_after = FileWriter.get_output_info(str(temp_path))
        
        print(f"✓ Info after writing: {info_after}")
        
        # Verify info structure
        expected_keys = ['original_filename', 'final_filename', 'directory', 
                        'extension', 'detected_format', 'directory_exists', 'file_exists']
        
        has_all_keys = all(key in info_after for key in expected_keys)
        
        if (has_all_keys and 
            info_after['file_exists'] and 
            info_after['detected_format'] == 'xlsx'):
            print("✓ File info retrieval worked correctly")
            return True
        else:
            print("✗ File info retrieval failed")
            print(f"  Missing keys: {[k for k in expected_keys if k not in info_after]}")
            print(f"  File exists: {info_after.get('file_exists')}")
            print(f"  Detected format: {info_after.get('detected_format')}")
            return False


def test_writable_check():
    """Test checking if a location is writable."""
    
    print("\nTesting writable location check...")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        # Test writable location
        writable_path = Path(temp_dir) / "writable_test.xlsx"
        is_writable = FileWriter.file_writable(str(writable_path))
        
        print(f"✓ Writable location check: {is_writable}")
        
        if is_writable:
            print("✓ Writable location correctly identified")
        else:
            print("✗ Failed to identify writable location")
            return False
        
        # Test with variable substitution
        template_path = str(Path(temp_dir) / "test_{date}.xlsx")
        is_writable_with_vars = FileWriter.file_writable(template_path)
        
        print(f"✓ Writable with variables: {is_writable_with_vars}")
        
        if is_writable_with_vars:
            print("✓ Writable check with variables worked correctly")
            return True
        else:
            print("✗ Writable check with variables failed")
            return False


def test_error_handling():
    """Test error handling for various failure cases."""
    
    print("\nTesting error handling...")
    
    sample_data = create_sample_data()
    
    # Test writing to non-existent directory (should work - creates directory)
    with tempfile.TemporaryDirectory() as temp_dir:
        nested_path = Path(temp_dir) / "nested" / "deep" / "test.xlsx"
        
        try:
            FileWriter.write_file(sample_data, str(nested_path))
            print("✓ Successfully created nested directories")
        except Exception as e:
            print(f"✗ Failed to create nested directories: {e}")
            return False
    
    # Test invalid DataFrame
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            invalid_path = Path(temp_dir) / "invalid.xlsx"
            FileWriter.write_file("not a dataframe", str(invalid_path))
        print("✗ Should have failed with invalid DataFrame")
        return False
    except FileWriterError as e:
        print(f"✓ Caught expected error for invalid DataFrame: {e}")
    
    # Test invalid explicit format
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            invalid_path = Path(temp_dir) / "test.xlsx"
            FileWriter.write_file(sample_data, str(invalid_path), explicit_format="invalid")
        print("✗ Should have failed with invalid format")
        return False
    except FileWriterError as e:
        print(f"✓ Caught expected error for invalid format: {e}")
    
    # Test empty sheets data for multi-sheet
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            empty_path = Path(temp_dir) / "empty.xlsx"
            FileWriter.write_multi_sheet_excel({}, str(empty_path))
        print("✗ Should have failed with empty sheets data")
        return False
    except FileWriterError as e:
        print(f"✓ Caught expected error for empty sheets data: {e}")
    
    print("✓ Error handling tests passed")
    return True


def test_supported_formats():
    """Test getting supported formats information."""
    
    print("\nTesting supported formats info...")
    
    formats_info = FileWriter.get_supported_formats()
    
    print(f"✓ Supported formats info: {formats_info}")
    
    expected_keys = ['excel_formats', 'csv_formats', 'tsv_formats', 'all_formats',
                     'supported_extensions', 'extension_mapping', 'format_descriptions', 'features']
    has_all_keys = all(key in formats_info for key in expected_keys)
    
    # Check some expected formats are present (logical formats without dots)
    has_xlsx = 'xlsx' in formats_info['excel_formats']
    has_csv = 'csv' in formats_info['csv_formats']
    has_tsv = 'tsv' in formats_info['tsv_formats']
    
    # Check extension mapping works correctly
    has_txt_mapping = formats_info['extension_mapping'].get('.txt') == 'tsv'
    has_xlsx_mapping = formats_info['extension_mapping'].get('.xlsx') == 'xlsx'
    
    # Check features
    has_features = 'excel' in formats_info['features'] and 'csv' in formats_info['features']
    
    if (has_all_keys and has_xlsx and has_csv and has_tsv and 
        has_txt_mapping and has_xlsx_mapping and has_features):
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
        print(f"  Has features: {has_features}")
        return False


if __name__ == '__main__':
    print("📝 Testing FileWriter functionality...")
    print("   Uses logical formats without dots (e.g., 'xlsx', 'csv', 'tsv')")
    print("   .txt files are written as TSV format")
    print("   Includes comprehensive backup, encoding, and error handling tests\n")
    
    success = True
    
    success &= test_excel_file_writing()
    success &= test_csv_file_writing()
    success &= test_tsv_file_writing()
    success &= test_multi_sheet_excel()
    success &= test_variable_substitution()
    success &= test_format_detection()
    success &= test_backup_creation()
    success &= test_encoding_handling()
    success &= test_file_info()
    success &= test_writable_check()
    success &= test_error_handling()
    success &= test_supported_formats()
    
    if success:
        print("\n✅ All FileWriter tests passed!")
    else:
        print("\n❌ Some FileWriter tests failed!")
    
    # Show supported formats summary
    try:
        formats = FileWriter.get_supported_formats()
        print(f"\nSupported output formats: {formats['all_formats']}")
        print(f"Supported extensions: {formats['supported_extensions']}")
        print(f"Total formats supported: {len(formats['all_formats'])}")
    except Exception as e:
        print(f"\n⚠️  Could not get supported formats: {e}")

    sys.exit(0 if success else 1)
