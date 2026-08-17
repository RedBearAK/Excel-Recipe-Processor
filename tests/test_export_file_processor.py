"""
Test the ExportFileProcessor functionality.

tests/test_export_file_processor.py

Exercises the stage-only export contract (2026-08 doctrine): every export
reads from a declared source_stage, and every sheets_to_create entry names
a stage in data_source. The retired current-data pathway is gone; tests
that used it now stage their frames first.

Runnable standalone or under pytest; exit code carries the verdict.
"""

import sys
import tempfile
import pandas as pd

from pathlib import Path

from excel_recipe_processor.core.stage_manager import StageManager
from excel_recipe_processor.core.base_processor import StepProcessorError
from excel_recipe_processor.core.variable_substitution import VariableSubstitution
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
    """Test basic Excel file export from a stage."""

    print("Testing basic Excel export...")

    sample_data = create_sample_data()
    StageManager.initialize_stages()

    try:
        StageManager.save_stage('stg_test_export_basic_excel_source', sample_data,
                                description='Basic Excel export source')

        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "export_test.xlsx"

            step_config = {
                'processor_type': 'export_file',
                'step_description': 'Test Excel export',
                'source_stage': 'stg_test_export_basic_excel_source',
                'output_file': str(output_path),
                'sheet_name': 'TestData'
            }

            processor = ExportFileProcessor(step_config)
            result = processor.execute()

            print(f"✓ Export completed, result shape: {result.shape}")

            if not output_path.exists():
                print("✗ Excel file was not created")
                return False
            print("✓ Excel file was created")

            exported_data = pd.read_excel(output_path, sheet_name='TestData')

            if (len(exported_data) == len(sample_data) and
                    list(exported_data.columns) == list(sample_data.columns)):
                print("✓ Exported data matches staged source")
                return True

            print("✗ Exported data does not match staged source")
            return False

    finally:
        StageManager.cleanup_stages()


def test_csv_export():
    """Test CSV file export with a custom separator."""

    print("\nTesting CSV export...")

    sample_data = create_sample_data()
    StageManager.initialize_stages()

    try:
        StageManager.save_stage('stg_test_export_basic_csv_source', sample_data,
                                description='Basic CSV export source')

        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "export_test.csv"

            step_config = {
                'processor_type': 'export_file',
                'step_description': 'Test CSV export',
                'source_stage': 'stg_test_export_basic_csv_source',
                'output_file': str(output_path),
                'separator': ';',
                'encoding': 'utf-8'
            }

            processor = ExportFileProcessor(step_config)
            result = processor.execute()

            print(f"✓ CSV export completed: {result.shape}")

            if not output_path.exists():
                print("✗ CSV file was not created")
                return False
            print("✓ CSV file was created")

            exported_data = pd.read_csv(output_path, sep=';')

            if (len(exported_data) == len(sample_data) and
                    len(exported_data.columns) == len(sample_data.columns)):
                print("✓ CSV export honored the configured separator")
                return True

            print("✗ CSV separator was not honored")
            print(f"  Read back {len(exported_data.columns)} columns "
                  f"(expected {len(sample_data.columns)})")
            return False

    finally:
        StageManager.cleanup_stages()


def test_multi_sheet_export():
    """Test multi-sheet export where both sheets come from one stage."""

    print("\nTesting multi-sheet export (single stage, two views)...")

    sample_data = create_sample_data()
    StageManager.initialize_stages()

    try:
        StageManager.save_stage('stg_test_export_multi_shared_source', sample_data,
                                description='Shared source for two tabs')

        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "multi_sheet_test.xlsx"

            step_config = {
                'processor_type': 'export_file',
                'step_description': 'Test multi-sheet export',
                'source_stage': 'stg_test_export_multi_shared_source',
                'output_file': str(output_path),
                'sheets_to_create': [
                    {
                        'sheet_name': 'All_Data',
                        'data_source': 'stg_test_export_multi_shared_source',
                        'active': True
                    },
                    {
                        'sheet_name': 'Summary',
                        'data_source': 'stg_test_export_multi_shared_source'
                    }
                ]
            }

            processor = ExportFileProcessor(step_config)
            processor.execute()

            if not output_path.exists():
                print("✗ Multi-sheet file was not created")
                return False
            print("✓ Multi-sheet Excel file was created")

            excel_file = pd.ExcelFile(output_path)
            sheet_names = excel_file.sheet_names
            print(f"✓ Found sheets: {sheet_names}")

            if 'All_Data' not in sheet_names or 'Summary' not in sheet_names:
                print("✗ Missing expected sheets")
                return False
            print("✓ All expected sheets found")

            all_data = pd.read_excel(output_path, sheet_name='All_Data')
            summary_data = pd.read_excel(output_path, sheet_name='Summary')

            if (len(all_data) == len(sample_data) and
                    len(summary_data) == len(sample_data)):
                print("✓ Multi-sheet export worked correctly")
                return True

            print("✗ Sheet data size mismatch")
            return False

    finally:
        StageManager.cleanup_stages()


def test_multi_sheet_export_with_stages():
    """Test multi-sheet export mixing three distinct stages."""

    print("\nTesting multi-sheet export with mixed stages...")

    StageManager.initialize_stages()

    try:
        sample_data = create_sample_data()
        different_data = create_different_data()
        current_data = pd.DataFrame({'Current': [1, 2, 3]})

        StageManager.save_stage('stg_test_export_mixed_employees', sample_data,
                                description='Employee information')
        StageManager.save_stage('stg_test_export_mixed_products', different_data,
                                description='Product catalog')
        StageManager.save_stage('stg_test_export_mixed_current', current_data,
                                description='Pipeline snapshot frame')

        print("✓ Created test stages")

        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "stage_export_test.xlsx"

            step_config = {
                'processor_type': 'export_file',
                'step_description': 'Test stage export',
                'source_stage': 'stg_test_export_mixed_current',
                'output_file': str(output_path),
                'sheets_to_create': [
                    {
                        'sheet_name': 'Current_Data',
                        'data_source': 'stg_test_export_mixed_current'
                    },
                    {
                        'sheet_name': 'Employees',
                        'data_source': 'stg_test_export_mixed_employees'
                    },
                    {
                        'sheet_name': 'Products',
                        'data_source': 'stg_test_export_mixed_products',
                        'active': True
                    }
                ]
            }

            processor = ExportFileProcessor(step_config)
            processor.execute()

            if not output_path.exists():
                print("✗ Stage export file was not created")
                return False
            print("✓ Stage export file was created")

            excel_file = pd.ExcelFile(output_path)
            sheet_names = excel_file.sheet_names
            print(f"✓ Found sheets: {sheet_names}")

            expected_sheets = ['Current_Data', 'Employees', 'Products']
            if not all(sheet in sheet_names for sheet in expected_sheets):
                print("✗ Missing expected sheets")
                return False
            print("✓ All expected sheets found")

            current_sheet = pd.read_excel(output_path, sheet_name='Current_Data')
            employee_sheet = pd.read_excel(output_path, sheet_name='Employees')
            product_sheet = pd.read_excel(output_path, sheet_name='Products')

            if (len(current_sheet) == 3 and
                    len(employee_sheet) == len(sample_data) and
                    len(product_sheet) == len(different_data)):
                print("✓ Stage export with mixed data sources worked correctly")
                return True

            print("✗ Sheet content size mismatch")
            print(f"  Current: {len(current_sheet)} (expected 3)")
            print(f"  Employee: {len(employee_sheet)} (expected {len(sample_data)})")
            print(f"  Product: {len(product_sheet)} (expected {len(different_data)})")
            return False

    finally:
        StageManager.cleanup_stages()


def test_variable_substitution():
    """Test variable substitution in output filenames."""

    print("\nTesting variable substitution...")

    sample_data = create_sample_data()
    StageManager.initialize_stages()

    try:
        StageManager.save_stage('stg_test_export_variable_sub_source', sample_data,
                                description='Variable substitution source')

        with tempfile.TemporaryDirectory() as temp_dir:
            template_path = str(Path(temp_dir) / "report_{date}.xlsx")

            step_config = {
                'processor_type': 'export_file',
                'step_description': 'Test variable substitution',
                'source_stage': 'stg_test_export_variable_sub_source',
                'output_file': template_path
            }

            processor = ExportFileProcessor(step_config)
            # Substitution moved to the pipeline layer, which injects this
            # object onto each processor; standalone tests inject it here.
            processor.variable_substitution = VariableSubstitution()
            processor.execute()

            from datetime import datetime
            date_str = datetime.now().strftime('%Y%m%d')
            expected_filename = f"report_{date_str}.xlsx"
            expected_path = Path(temp_dir) / expected_filename

            if expected_path.exists():
                print(f"✓ Variable substitution worked: {expected_filename}")
                return True

            created_files = list(Path(temp_dir).glob("*.xlsx"))
            print(f"✗ Expected file not found: {expected_filename}")
            print(f"  Created files: {[f.name for f in created_files]}")
            return False

    finally:
        StageManager.cleanup_stages()


def test_backup_creation():
    """Test timestamped backup creation on overwrite."""

    print("\nTesting backup creation...")

    sample_data = create_sample_data()
    modified_data = sample_data.copy()
    modified_data['NewColumn'] = ['A', 'B', 'C', 'D', 'E']

    StageManager.initialize_stages()

    try:
        StageManager.save_stage('stg_test_export_backup_original', sample_data,
                                description='Original frame')
        StageManager.save_stage('stg_test_export_backup_modified', modified_data,
                                description='Modified frame')

        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "backup_test.xlsx"

            step_config_1 = {
                'processor_type': 'export_file',
                'step_description': 'Initial export',
                'source_stage': 'stg_test_export_backup_original',
                'output_file': str(output_path)
            }
            ExportFileProcessor(step_config_1).execute()
            print("✓ Created initial file")

            step_config_2 = {
                'processor_type': 'export_file',
                'step_description': 'Export with backup',
                'source_stage': 'stg_test_export_backup_modified',
                'output_file': str(output_path),
                'create_backup': True
            }
            ExportFileProcessor(step_config_2).execute()
            print("✓ Exported with backup enabled")

            # Backup naming: {stem}_erpbkup_{timestamp}{ext}
            backup_files = list(output_path.parent.glob(
                f"{output_path.stem}_erpbkup_*{output_path.suffix}"))

            if not backup_files:
                print("✗ No backup file found")
                print(f"  Directory contents: "
                      f"{[p.name for p in output_path.parent.iterdir()]}")
                return False
            print(f"✓ Backup file created: {backup_files[0].name}")

            backup_data = pd.read_excel(backup_files[0])
            new_data = pd.read_excel(output_path)

            if (len(backup_data.columns) == len(sample_data.columns) and
                    'NewColumn' in new_data.columns):
                print("✓ Backup holds original data, new file has modifications")
                return True

            print("✗ Backup content verification failed")
            return False

    finally:
        StageManager.cleanup_stages()


def test_error_handling():
    """Test error handling for various failure cases."""

    print("\nTesting error handling...")

    sample_data = create_sample_data()

    # Missing source_stage fails at construction (ExportBaseProcessor)
    try:
        step_config = {
            'processor_type': 'export_file',
            'step_description': 'Missing source stage test',
            'output_file': 'dummy.xlsx'
        }
        ExportFileProcessor(step_config)
        print("✗ Should have failed with missing source_stage")
        return False
    except StepProcessorError as e:
        print(f"✓ Caught expected error for missing source_stage: {e}")

    StageManager.initialize_stages()

    try:
        StageManager.save_stage('stg_test_export_error_probe_source', sample_data,
                                description='Error handling source')

        # Missing output_file fails guided, not with a downstream TypeError
        try:
            step_config = {
                'processor_type': 'export_file',
                'step_description': 'Missing output file test',
                'source_stage': 'stg_test_export_error_probe_source'
            }
            ExportFileProcessor(step_config).execute()
            print("✗ Should have failed with missing output_file")
            return False
        except StepProcessorError as e:
            print(f"✓ Caught expected error for missing output_file: {e}")

        # Invalid explicit format
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                step_config = {
                    'processor_type': 'export_file',
                    'step_description': 'Invalid format test',
                    'source_stage': 'stg_test_export_error_probe_source',
                    'output_file': str(Path(temp_dir) / "test.xlsx"),
                    'format': 'invalid_format'
                }
                ExportFileProcessor(step_config).execute()
            print("✗ Should have failed with invalid format")
            return False
        except StepProcessorError as e:
            print(f"✓ Caught expected error for invalid format: {e}")

        # Nonexistent data_source stage in sheets_to_create
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                step_config = {
                    'processor_type': 'export_file',
                    'step_description': 'Non-existent stage test',
                    'source_stage': 'stg_test_export_error_probe_source',
                    'output_file': str(Path(temp_dir) / "test.xlsx"),
                    'sheets_to_create': [
                        {
                            'sheet_name': 'Test',
                            'data_source': 'stg_nonexistent_probe_stage'
                        }
                    ]
                }
                ExportFileProcessor(step_config).execute()
            print("✗ Should have failed with non-existent stage")
            return False
        except StepProcessorError as e:
            if "Cannot load data_source" in str(e):
                print(f"✓ Caught expected error for non-existent stage: {e}")
            else:
                print(f"✗ Wrong error for non-existent stage: {e}")
                return False

        # Retired multi-sheet key fails with the guided rename error
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                step_config = {
                    'processor_type': 'export_file',
                    'step_description': 'Retired sheets key test',
                    'source_stage': 'stg_test_export_error_probe_source',
                    'output_file': str(Path(temp_dir) / "test.xlsx"),
                    'sheets': [{'sheet_name': 'Test',
                                'data_source': 'stg_test_export_error_probe_source'}]
                }
                ExportFileProcessor(step_config).execute()
            print("✗ Should have failed with retired sheets key")
            return False
        except StepProcessorError as e:
            if "sheets_to_create" in str(e):
                print(f"✓ Caught expected error for retired sheets key: {e}")
            else:
                print(f"✗ Wrong error for retired sheets key: {e}")
                return False

    finally:
        StageManager.cleanup_stages()

    print("✓ Error handling tests passed")
    return True


def test_capabilities_info():
    """Test getting processor capabilities information."""

    print("\nTesting capabilities info...")

    step_config = {
        'processor_type': 'export_file',
        'step_description': 'Capabilities test',
        'source_stage': 'stg_test_export_capabilities_probe',
        'output_file': 'dummy.xlsx'
    }

    processor = ExportFileProcessor(step_config)
    capabilities = processor.get_capabilities()

    print("✓ Capabilities info retrieved")

    # Check expected keys per the current capabilities shape
    expected_keys = ['description', 'file_formats', 'excel_options', 'safety']
    has_all_keys = all(key in capabilities for key in expected_keys)

    has_xlsx = 'xlsx' in capabilities.get('file_formats', [])
    excel_options_text = ' '.join(capabilities.get('excel_options', []))
    has_multi_sheet = 'multi-sheet' in excel_options_text

    if has_all_keys and has_xlsx and has_multi_sheet:
        print("✓ Capabilities info is complete")
        return True

    print("✗ Capabilities info is incomplete")
    print(f"  Has all keys: {has_all_keys}")
    print(f"  Has xlsx format: {has_xlsx}")
    print(f"  Has multi-sheet option: {has_multi_sheet}")
    return False


if __name__ == '__main__':
    print("📤 Testing ExportFileProcessor functionality...")
    print("   Tests stage-sourced single/multi-sheet export, variable substitution")
    print("   Leverages FileWriter for file operations and StageManager for stage access\n")

    success = True

    success &= test_basic_excel_export()
    success &= test_csv_export()
    success &= test_multi_sheet_export()
    success &= test_multi_sheet_export_with_stages()
    success &= test_variable_substitution()
    success &= test_backup_creation()
    success &= test_error_handling()
    success &= test_capabilities_info()

    if success:
        print("\n✅ All ExportFileProcessor tests passed!")
    else:
        print("\n❌ Some ExportFileProcessor tests failed!")

    print("\nExportFileProcessor Summary:")
    print("✓ Reads every export from a declared source_stage")
    print("✓ Multi-sheet exports name a stage per sheets_to_create entry")
    print("✓ Honors separator/encoding for CSV, backs up replaced files")
    print("✓ Provides variable substitution via pipeline-injected object")

    sys.exit(0 if success else 1)

# End of file #
