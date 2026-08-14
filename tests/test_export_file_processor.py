"""
Test the ExportFileProcessor functionality.
"""

import pandas as pd
import tempfile

from pathlib import Path

from excel_recipe_processor.core.stage_manager import StageManager, StageError
from excel_recipe_processor.core.base_processor import StepProcessorError
from excel_recipe_processor.processors.export_file_processor import ExportFileProcessor


def create_sample_data():
    """Create sample data for testing."""
    return pd.DataFrame({
        'ID': [1, 2, 3, 4, 5],
        'Name': ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'],
        'Department': ['Engineering', 'Sales', 'Marketing', 'Engineering', 'Sales'],
        'Salary': [75000, 65000, 70000, 80000, 60000]
    })


def create_different_data():
    """Create different sample data for multi-sheet testing."""
    return pd.DataFrame({
        'ProductID': [101, 102, 103],
        'ProductName': ['Widget A', 'Widget B', 'Gadget X'],
        'Category': ['Electronics', 'Electronics', 'Hardware'],
        'Price': [19.99, 24.99, 15.50]
    })


def test_basic_excel_export():
    """Test basic Excel file export."""
    
    print("Testing basic Excel export...")
    
    sample_data = create_sample_data()
    
    with tempfile.TemporaryDirectory() as temp_dir:
        output_path = Path(temp_dir) / "export_test.xlsx"
        
        step_config = {
            'processor_type': 'export_file',
            'step_description': 'Test Excel export',
            'output_file': str(output_path),
            'sheet_name': 'TestData'
        }
        
        processor = ExportFileProcessor(step_config)
        result = processor.execute(sample_data)
        
        print(f"✓ Export completed, result shape: {result.shape}")
        
        # Verify data passed through unchanged
        if (len(result) == len(sample_data) and 
            list(result.columns) == list(sample_data.columns)):
            print("✓ Data passed through unchanged")
        else:
            print("✗ Data was modified unexpectedly")
            return False
        
        # Verify file was created
        if output_path.exists():
            print("✓ Excel file was created")
            
            # Verify file content
            try:
                exported_data = pd.read_excel(output_path, sheet_name='TestData')
                
                if (len(exported_data) == len(sample_data) and
                    list(exported_data.columns) == list(sample_data.columns)):
                    print("✓ Exported data matches original")
                    return True
                else:
                    print("✗ Exported data doesn't match original")
                    return False
                    
            except Exception as e:
                print(f"✗ Error reading exported file: {e}")
                return False
        else:
            print("✗ Excel file was not created")
            return False


def test_csv_export():
    """Test CSV file export."""
    
    print("\nTesting CSV export...")
    
    sample_data = create_sample_data()
    
    with tempfile.TemporaryDirectory() as temp_dir:
        output_path = Path(temp_dir) / "export_test.csv"
        
        step_config = {
            'processor_type': 'export_file',
            'step_description': 'Test CSV export',
            'output_file': str(output_path),
            'separator': ';',
            'encoding': 'utf-8'
        }
        
        processor = ExportFileProcessor(step_config)
        result = processor.execute(sample_data)
        
        print(f"✓ CSV export completed: {result.shape}")
        
        # Verify file was created
        if output_path.exists():
            print("✓ CSV file was created")
            
            # Verify file content
            try:
                exported_data = pd.read_csv(output_path, sep=';')
                
                if len(exported_data) == len(sample_data):
                    print("✓ CSV export worked correctly")
                    return True
                else:
                    print("✗ CSV data size mismatch")
                    return False
                    
            except Exception as e:
                print(f"✗ Error reading CSV file: {e}")
                return False
        else:
            print("✗ CSV file was not created")
            return False


def test_multi_sheet_export_current_data():
    """Test multi-sheet export with current data only."""
    
    print("\nTesting multi-sheet export (current data)...")
    
    sample_data = create_sample_data()
    
    with tempfile.TemporaryDirectory() as temp_dir:
        output_path = Path(temp_dir) / "multi_sheet_test.xlsx"
        
        step_config = {
            'processor_type': 'export_file',
            'step_description': 'Test multi-sheet export',
            'output_file': str(output_path),
            'sheets_to_create': [
                {
                    'sheet_name': 'All_Data',
                    'data_source': 'current',
                    'active': True
                },
                {
                    'sheet_name': 'Summary',
                    'data_source': 'current'
                }
            ]
        }
        
        processor = ExportFileProcessor(step_config)
        result = processor.execute(sample_data)
        
        print(f"✓ Multi-sheet export completed: {result.shape}")
        
        # Verify file was created
        if output_path.exists():
            print("✓ Multi-sheet Excel file was created")
            
            # Verify sheets
            try:
                excel_file = pd.ExcelFile(output_path)
                sheet_names = excel_file.sheet_names
                
                print(f"✓ Found sheets: {sheet_names}")
                
                if 'All_Data' in sheet_names and 'Summary' in sheet_names:
                    print("✓ All expected sheets found")
                    
                    # Verify sheet content
                    all_data = pd.read_excel(output_path, sheet_name='All_Data')
                    summary_data = pd.read_excel(output_path, sheet_name='Summary')
                    
                    if (len(all_data) == len(sample_data) and 
                        len(summary_data) == len(sample_data)):
                        print("✓ Multi-sheet export worked correctly")
                        return True
                    else:
                        print("✗ Sheet data size mismatch")
                        return False
                else:
                    print("✗ Missing expected sheets")
                    return False
                    
            except Exception as e:
                print(f"✗ Error reading multi-sheet file: {e}")
                return False
        else:
            print("✗ Multi-sheet file was not created")
            return False


def test_multi_sheet_export_with_stages():
    """Test multi-sheet export using saved stages."""
    
    print("\nTesting multi-sheet export with stages...")
    
    # Initialize StageManager for testing
    StageManager.initialize_stages()
    
    try:
        sample_data = create_sample_data()
        different_data = create_different_data()
        
        # Save some data to stages
        StageManager.save_stage('Employee Data', sample_data, description='Employee information')
        StageManager.save_stage('Product Data', different_data, description='Product catalog')
        
        print("✓ Created test stages")
        
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "stage_export_test.xlsx"
            
            step_config = {
                'processor_type': 'export_file',
                'step_description': 'Test stage export',
                'output_file': str(output_path),
                'sheets_to_create': [
                    {
                        'sheet_name': 'Current_Data',
                        'data_source': 'current'
                    },
                    {
                        'sheet_name': 'Employees',
                        'data_source': 'Employee Data'
                    },
                    {
                        'sheet_name': 'Products',
                        'data_source': 'Product Data',
                        'active': True
                    }
                ]
            }
            
            processor = ExportFileProcessor(step_config)
            
            # Execute with current data
            current_data = pd.DataFrame({'Current': [1, 2, 3]})
            result = processor.execute(current_data)
            
            print(f"✓ Stage export completed: {result.shape}")
            
            # Verify file was created
            if output_path.exists():
                print("✓ Stage export file was created")
                
                # Verify all sheets and their content
                try:
                    excel_file = pd.ExcelFile(output_path)
                    sheet_names = excel_file.sheet_names
                    
                    print(f"✓ Found sheets: {sheet_names}")
                    
                    expected_sheets = ['Current_Data', 'Employees', 'Products']
                    if all(sheet in sheet_names for sheet in expected_sheets):
                        print("✓ All expected sheets found")
                        
                        # Verify sheet content
                        current_sheet = pd.read_excel(output_path, sheet_name='Current_Data')
                        employee_sheet = pd.read_excel(output_path, sheet_name='Employees')
                        product_sheet = pd.read_excel(output_path, sheet_name='Products')
                        
                        if (len(current_sheet) == 3 and 
                            len(employee_sheet) == len(sample_data) and
                            len(product_sheet) == len(different_data)):
                            print("✓ Stage export with mixed data sources worked correctly")
                            return True
                        else:
                            print("✗ Sheet content size mismatch")
                            print(f"  Current: {len(current_sheet)} (expected 3)")
                            print(f"  Employee: {len(employee_sheet)} (expected {len(sample_data)})")
                            print(f"  Product: {len(product_sheet)} (expected {len(different_data)})")
                            return False
                    else:
                        print("✗ Missing expected sheets")
                        return False
                        
                except Exception as e:
                    print(f"✗ Error reading stage export file: {e}")
                    return False
            else:
                print("✗ Stage export file was not created")
                return False
                
    finally:
        StageManager.cleanup_stages()


def test_variable_substitution():
    """Test variable substitution in filenames."""
    
    print("\nTesting variable substitution...")
    
    sample_data = create_sample_data()
    
    with tempfile.TemporaryDirectory() as temp_dir:
        # Template filename with variables
        template_path = str(Path(temp_dir) / "report_{date}.xlsx")
        
        step_config = {
            'processor_type': 'export_file',
            'step_description': 'Test variable substitution',
            'output_file': template_path
        }
        
        processor = ExportFileProcessor(step_config)
        result = processor.execute(sample_data)
        
        print(f"✓ Variable substitution export completed: {result.shape}")
        
        # Check for files with substituted names
        from datetime import datetime
        date_str = datetime.now().strftime('%Y%m%d')
        expected_filename = f"report_{date_str}.xlsx"
        expected_path = Path(temp_dir) / expected_filename
        
        if expected_path.exists():
            print(f"✓ Variable substitution worked: {expected_filename}")
            return True
        else:
            # Check what files were actually created
            created_files = list(Path(temp_dir).glob("*.xlsx"))
            print(f"✗ Expected file not found: {expected_filename}")
            print(f"  Created files: {[f.name for f in created_files]}")
            return False


def test_backup_creation():
    """Test backup file creation."""
    
    print("\nTesting backup creation...")
    
    sample_data = create_sample_data()
    
    with tempfile.TemporaryDirectory() as temp_dir:
        output_path = Path(temp_dir) / "backup_test.xlsx"
        
        # First export to create initial file
        step_config_1 = {
            'processor_type': 'export_file',
            'step_description': 'Initial export',
            'output_file': str(output_path)
        }
        
        processor_1 = ExportFileProcessor(step_config_1)
        processor_1.execute(sample_data)
        
        print("✓ Created initial file")
        
        # Second export with backup enabled
        modified_data = sample_data.copy()
        modified_data['NewColumn'] = ['A', 'B', 'C', 'D', 'E']
        
        step_config_2 = {
            'processor_type': 'export_file',
            'step_description': 'Export with backup',
            'output_file': str(output_path),
            'create_backup': True
        }
        
        processor_2 = ExportFileProcessor(step_config_2)
        processor_2.execute(modified_data)
        
        print("✓ Exported with backup enabled")
        
        # Check for backup file
        backup_files = list(output_path.parent.glob(f"{output_path.stem}*.backup*"))
        
        if backup_files:
            print(f"✓ Backup file created: {backup_files[0].name}")
            
            # Verify backup contains original data
            try:
                backup_data = pd.read_excel(backup_files[0])
                new_data = pd.read_excel(output_path)
                
                if (len(backup_data.columns) == len(sample_data.columns) and
                    'NewColumn' in new_data.columns):
                    print("✓ Backup contains original data, new file has modifications")
                    return True
                else:
                    print("✗ Backup content verification failed")
                    return False
                    
            except Exception as e:
                print(f"✗ Error verifying backup: {e}")
                return False
        else:
            print("✗ No backup file found")
            return False


def test_error_handling():
    """Test error handling for various failure cases."""
    
    print("\nTesting error handling...")
    
    sample_data = create_sample_data()
    
    # Test missing output file
    try:
        step_config = {
            'processor_type': 'export_file',
            'step_description': 'Missing output file test'
            # Missing 'output_file'
        }
        processor = ExportFileProcessor(step_config)
        processor.execute(sample_data)
        print("✗ Should have failed with missing output_file")
        return False
    except StepProcessorError as e:
        print(f"✓ Caught expected error for missing output_file: {e}")
    
    # Test invalid format
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "test.xlsx"
            
            step_config = {
                'processor_type': 'export_file',
                'step_description': 'Invalid format test',
                'output_file': str(output_path),
                'format': 'invalid_format'
            }
            processor = ExportFileProcessor(step_config)
            processor.execute(sample_data)
        print("✗ Should have failed with invalid format")
        return False
    except StepProcessorError as e:
        print(f"✓ Caught expected error for invalid format: {e}")
    
    # Test non-existent stage reference
    StageManager.initialize_stages()
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "test.xlsx"
            
            step_config = {
                'processor_type': 'export_file',
                'step_description': 'Non-existent stage test',
                'output_file': str(output_path),
                'sheets_to_create': [
                    {
                        'sheet_name': 'Test',
                        'data_source': 'NonExistentStage'
                    }
                ]
            }
            processor = ExportFileProcessor(step_config)
            processor.execute(sample_data)
        print("✗ Should have failed with non-existent stage")
        return False
    except StepProcessorError as e:
        if "Cannot access data source" in str(e):
            print(f"✓ Caught expected error for non-existent stage: {e}")
        else:
            print(f"✗ Wrong error for non-existent stage: {e}")
            return False
    finally:
        StageManager.cleanup_stages()
    
    # Test invalid data input
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "test.xlsx"
            
            step_config = {
                'processor_type': 'export_file',
                'step_description': 'Invalid data test',
                'output_file': str(output_path)
            }
            processor = ExportFileProcessor(step_config)
            processor.execute("not a dataframe")
        print("✗ Should have failed with invalid data")
        return False
    except StepProcessorError as e:
        print(f"✓ Caught expected error for invalid data: {e}")
    
    print("✓ Error handling tests passed")
    return True


def test_capabilities_info():
    """Test getting processor capabilities information."""
    
    print("\nTesting capabilities info...")
    
    step_config = {
        'processor_type': 'export_file',
        'step_description': 'Capabilities test',
        'output_file': 'dummy.xlsx'
    }
    
    processor = ExportFileProcessor(step_config)
    capabilities = processor.get_capabilities()
    
    print(f"✓ Capabilities info retrieved")
    
    # Check expected keys
    expected_keys = ['description', 'export_formats', 'export_features', 'stage_integration', 'examples']
    has_all_keys = all(key in capabilities for key in expected_keys)
    
    # Check some expected features
    has_stage_integration = 'export_from_multiple_stages' in capabilities.get('stage_integration', [])
    has_multi_sheet = 'multi_sheet_excel' in capabilities.get('export_features', [])
    
    if has_all_keys and has_stage_integration and has_multi_sheet:
        print("✓ Capabilities info is complete")
        return True
    else:
        print("✗ Capabilities info is incomplete")
        print(f"  Has all keys: {has_all_keys}")
        print(f"  Has stage integration: {has_stage_integration}")
        print(f"  Has multi-sheet: {has_multi_sheet}")
        return False


if __name__ == '__main__':
    print("📤 Testing ExportFileProcessor functionality...")
    print("   Tests single/multi-sheet export, stage integration, variable substitution")
    print("   Leverages FileWriter for file operations and StageManager for stage access\n")
    
    success = True
    
    success &= test_basic_excel_export()
    success &= test_csv_export()
    success &= test_multi_sheet_export_current_data()
    success &= test_multi_sheet_export_with_stages()
    success &= test_variable_substitution()
    success &= test_backup_creation()
    success &= test_error_handling()
    success &= test_capabilities_info()
    
    if success:
        print("\n✅ All ExportFileProcessor tests passed!")
    else:
        print("\n❌ Some ExportFileProcessor tests failed!")
    
    # Show processor summary
    print(f"\nExportFileProcessor Summary:")
    print(f"✓ Integrates FileWriter for all file operations")
    print(f"✓ Integrates StageManager for accessing saved stages")
    print(f"✓ Supports all FileWriter formats: Excel, CSV, TSV")
    print(f"✓ Enables multi-sheet exports with mixed data sources")
    print(f"✓ Provides variable substitution and backup functionality")
