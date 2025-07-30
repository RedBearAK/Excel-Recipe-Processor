"""
Test the ImportFileProcessor functionality.
"""

import pandas as pd
import tempfile

from pathlib import Path

from excel_recipe_processor.core.base_processor import StepProcessorError
from excel_recipe_processor.core.stage_manager import StageManager, StageError
from excel_recipe_processor.processors.import_file_processor import ImportFileProcessor


def create_sample_data():
    """Create sample data for testing."""
    return pd.DataFrame({
        'ID': [1, 2, 3, 4, 5],
        'Name': ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'],
        'Department': ['Engineering', 'Sales', 'Marketing', 'Engineering', 'Sales'],
        'Salary': [75000, 65000, 70000, 80000, 60000],
        'Start_Date': ['2020-01-15', '2019-03-22', '2021-07-10', '2018-11-05', '2022-02-28']
    })


def create_different_data():
    """Create different sample data for import testing."""
    return pd.DataFrame({
        'ProductID': [101, 102, 103, 104],
        'ProductName': ['Widget A', 'Widget B', 'Gadget X', 'Tool Y'],
        'Category': ['Electronics', 'Electronics', 'Hardware', 'Tools'],
        'Price': [19.99, 24.99, 15.50, 35.00]
    })


def setup_test_files(temp_dir):
    """Set up test files for import testing."""
    sample_data = create_different_data()
    
    # Create Excel file
    excel_path = Path(temp_dir) / "import_test.xlsx"
    sample_data.to_excel(excel_path, index=False, engine='openpyxl')
    
    # Create CSV file  
    csv_path = Path(temp_dir) / "import_test.csv"
    sample_data.to_csv(csv_path, index=False)
    
    # Create TSV file
    tsv_path = Path(temp_dir) / "import_test.tsv"
    sample_data.to_csv(tsv_path, index=False, sep='\t')
    
    return {
        'excel': str(excel_path),
        'csv': str(csv_path), 
        'tsv': str(tsv_path),
        'expected_data': sample_data
    }


def test_basic_import_excel():
    """Test basic Excel file import without stage saving."""
    
    print("Testing basic Excel import...")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        # Set up test files
        test_files = setup_test_files(temp_dir)
        
        # Configure processor
        step_config = {
            'processor_type': 'import_file',
            'step_description': 'Test Excel import',
            'input_file': test_files['excel'],
            'replace_current_data': True
        }
        
        processor = ImportFileProcessor(step_config)
        
        # Execute with dummy input data (should be replaced)
        original_data = create_sample_data()
        result = processor.execute(original_data)
        
        print(f"✓ Imported data: {result.shape}")
        print(f"✓ Result columns: {list(result.columns)}")
        
        # Verify data was replaced with imported data
        expected_data = test_files['expected_data']
        
        if (len(result) == len(expected_data) and 
            list(result.columns) == list(expected_data.columns) and
            'ProductID' in result.columns):  # Key column from imported data
            print("✓ Basic Excel import worked correctly")
            return True
        else:
            print("✗ Basic Excel import failed")
            print(f"  Expected shape: {expected_data.shape}")
            print(f"  Actual shape: {result.shape}")
            return False


def test_basic_import_csv():
    """Test basic CSV file import."""
    
    print("\nTesting basic CSV import...")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        test_files = setup_test_files(temp_dir)
        
        step_config = {
            'processor_type': 'import_file',
            'step_description': 'Test CSV import',
            'input_file': test_files['csv'],
            'replace_current_data': True
        }
        
        processor = ImportFileProcessor(step_config)
        original_data = create_sample_data()
        result = processor.execute(original_data)
        
        print(f"✓ Imported CSV data: {result.shape}")
        
        expected_data = test_files['expected_data']
        
        if (len(result) == len(expected_data) and 
            'ProductName' in result.columns):
            print("✓ Basic CSV import worked correctly")
            return True
        else:
            print("✗ Basic CSV import failed")
            return False


def test_import_with_stage_saving():
    """Test importing with stage saving functionality."""
    
    print("\nTesting import with stage saving...")
    
    # Initialize StageManager for testing
    StageManager.initialize_stages()
    
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            test_files = setup_test_files(temp_dir)
            
            step_config = {
                'processor_type': 'import_file',
                'step_description': 'Test import with stage',
                'input_file': test_files['excel'],
                'save_to_stage': 'Imported Product Data',
                'stage_description': 'Product catalog from Excel import',
                'replace_current_data': True
            }
            
            processor = ImportFileProcessor(step_config)
            original_data = create_sample_data()
            result = processor.execute(original_data)
            
            print(f"✓ Import with stage saving completed: {result.shape}")
            
            # Verify stage was created
            if StageManager.stage_exists('Imported Product Data'):
                print("✓ Stage was created successfully")
                
                # Verify stage contains correct data
                stage_data = StageManager.load_stage('Imported Product Data')
                
                if (len(stage_data) == len(result) and 
                    list(stage_data.columns) == list(result.columns)):
                    print("✓ Stage contains correct data")
                    
                    # Verify stage metadata
                    stage_info = StageManager.list_stages()
                    stage_meta = stage_info['Imported Product Data']
                    
                    if (stage_meta['description'] == 'Product catalog from Excel import' and
                        stage_meta['step_name'] == 'Test import with stage'):
                        print("✓ Stage metadata is correct")
                        return True
                    else:
                        print("✗ Stage metadata is incorrect")
                        print(f"  Description: {stage_meta['description']}")
                        print(f"  Step name: {stage_meta['step_name']}")
                        return False
                else:
                    print("✗ Stage data doesn't match result data")
                    return False
            else:
                print("✗ Stage was not created")
                return False
                
    finally:
        StageManager.cleanup_stages()


def test_stage_overwrite_protection():
    """Test stage overwrite protection."""
    
    print("\nTesting stage overwrite protection...")
    
    StageManager.initialize_stages()
    
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            test_files = setup_test_files(temp_dir)
            
            # First import - create stage
            step_config_1 = {
                'processor_type': 'import_file',
                'step_description': 'First import',
                'input_file': test_files['excel'],
                'save_to_stage': 'Test Stage',
                'replace_current_data': True
            }
            
            processor_1 = ImportFileProcessor(step_config_1)
            processor_1.execute(create_sample_data())
            
            print("✓ Created initial stage")
            
            # Second import - should fail without overwrite
            step_config_2 = {
                'processor_type': 'import_file',
                'step_description': 'Second import',
                'input_file': test_files['csv'],
                'save_to_stage': 'Test Stage',  # Same name
                # stage_overwrite: false (default)
                'replace_current_data': True
            }
            
            processor_2 = ImportFileProcessor(step_config_2)
            
            try:
                processor_2.execute(create_sample_data())
                print("✗ Should have failed with overwrite protection")
                return False
            except StepProcessorError as e:
                if "already exists" in str(e):
                    print(f"✓ Overwrite protection worked: {e}")
                else:
                    print(f"✗ Wrong error message: {e}")
                    return False
            
            # Third import - should succeed with overwrite=true
            step_config_3 = {
                'processor_type': 'import_file',
                'step_description': 'Third import',
                'input_file': test_files['csv'],
                'save_to_stage': 'Test Stage',
                'stage_overwrite': True,
                'replace_current_data': True
            }
            
            processor_3 = ImportFileProcessor(step_config_3)
            processor_3.execute(create_sample_data())
            
            print("✓ Overwrite with permission worked")
            return True
            
    finally:
        StageManager.cleanup_stages()


def test_variable_substitution():
    """Test variable substitution in filenames."""
    
    print("\nTesting variable substitution...")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create file with today's date
        from datetime import datetime
        date_str = datetime.now().strftime('%Y%m%d')
        actual_filename = f"data_{date_str}.csv"
        
        sample_data = create_different_data()
        actual_path = Path(temp_dir) / actual_filename
        sample_data.to_csv(actual_path, index=False)
        
        print(f"✓ Created test file: {actual_filename}")
        
        # Import using variable template
        template_filename = str(Path(temp_dir) / "data_{date}.csv")
        
        step_config = {
            'processor_type': 'import_file',
            'step_description': 'Test variable substitution',
            'input_file': template_filename,
            'replace_current_data': True
        }
        
        processor = ImportFileProcessor(step_config)
        result = processor.execute(create_sample_data())
        
        print(f"✓ Variable substitution result: {result.shape}")
        
        if len(result) == len(sample_data) and 'ProductID' in result.columns:
            print("✓ Variable substitution worked correctly")
            return True
        else:
            print("✗ Variable substitution failed")
            return False


def test_explicit_format_override():
    """Test explicit format specification."""
    
    print("\nTesting explicit format override...")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        test_files = setup_test_files(temp_dir)
        
        step_config = {
            'processor_type': 'import_file',
            'step_description': 'Test explicit format',
            'input_file': test_files['tsv'],
            'format': 'tsv',  # Explicit format
            'replace_current_data': True
        }
        
        processor = ImportFileProcessor(step_config)
        result = processor.execute(create_sample_data())
        
        print(f"✓ Explicit format import: {result.shape}")
        
        if len(result) == len(test_files['expected_data']):
            print("✓ Explicit format override worked correctly")
            return True
        else:
            print("✗ Explicit format override failed")
            return False


def test_replace_current_data_safety():
    """Test replace_current_data safety mechanism."""
    
    print("\nTesting replace_current_data safety...")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        test_files = setup_test_files(temp_dir)
        
        # Try without replace_current_data (should fail)
        step_config = {
            'processor_type': 'import_file',
            'input_file': test_files['excel']
            # Testing for missing replace_current_data
        }
        
        try:
            processor = ImportFileProcessor(step_config)
            processor.execute(create_sample_data())
            print("✗ Should have failed without replace_current_data")
            return False
        except StepProcessorError as e:
            print(f"✓ Correctly enforced replace_current_data requirement: {e}")
        
        # Try with replace_current_data=false (should fail)  
        step_config['replace_current_data'] = False
        
        try:
            processor = ImportFileProcessor(step_config)
            processor.execute(create_sample_data())
            print("✗ Should have failed with replace_current_data=false")
            return False
        except StepProcessorError as e:
            print(f"✓ Correctly rejected replace_current_data=false: {e}")
        
        return True


def test_error_handling():
    """Test error handling for various failure cases."""
    
    print("\nTesting error handling...")
    
    # Test missing input file
    try:
        step_config = {
            'processor_type': 'import_file',
            'step_description': 'Missing input file test',
            'replace_current_data': True
            # Missing 'input_file'
        }
        processor = ImportFileProcessor(step_config)
        processor.execute(create_sample_data())
        print("✗ Should have failed with missing input_file")
        return False
    except StepProcessorError as e:
        print(f"✓ Caught expected error for missing input_file: {e}")
    
    # Test non-existent file
    try:
        step_config = {
            'processor_type': 'import_file',
            'step_description': 'Non-existent file test',
            'input_file': '/nonexistent/path/file.xlsx',
            'replace_current_data': True
        }
        processor = ImportFileProcessor(step_config)
        processor.execute(create_sample_data())
        print("✗ Should have failed with non-existent file")
        return False
    except StepProcessorError as e:
        print(f"✓ Caught expected error for non-existent file: {e}")
    
    # Test invalid format
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            test_files = setup_test_files(temp_dir)
            
            step_config = {
                'processor_type': 'import_file',
                'step_description': 'Invalid format test',
                'input_file': test_files['excel'],
                'format': 'invalid_format',
                'replace_current_data': True
            }
            processor = ImportFileProcessor(step_config)
            processor.execute(create_sample_data())
        print("✗ Should have failed with invalid format")
        return False
    except StepProcessorError as e:
        print(f"✓ Caught expected error for invalid format: {e}")
    
    # Test invalid stage name (using StageManager validation)
    StageManager.initialize_stages()
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            test_files = setup_test_files(temp_dir)
            
            step_config = {
                'processor_type': 'import_file',
                'step_description': 'Invalid stage name test',
                'input_file': test_files['excel'],
                'save_to_stage': 'clean_data',  # Conflicts with processor name
                'replace_current_data': True
            }
            processor = ImportFileProcessor(step_config)
            processor.execute(create_sample_data())
        print("✗ Should have failed with invalid stage name")
        return False
    except StepProcessorError as e:
        if "too similar to processor type" in str(e):
            print(f"✓ Caught expected error for invalid stage name: {e}")
        else:
            print(f"✗ Wrong error for invalid stage name: {e}")
            return False
    finally:
        StageManager.cleanup_stages()
    
    print("✓ Error handling tests passed")
    return True


def test_configuration_validation():
    """Test configuration parameter validation."""
    
    print("\nTesting configuration validation...")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        test_files = setup_test_files(temp_dir)
        
        # Test invalid sheet parameter
        try:
            step_config = {
                'processor_type': 'import_file',
                'step_description': 'Invalid sheet test',
                'input_file': test_files['excel'],
                'sheet': ['invalid'],  # Should be string or int
                'replace_current_data': True
            }
            processor = ImportFileProcessor(step_config)
            processor.execute(create_sample_data())
            print("✗ Should have failed with invalid sheet parameter")
            return False
        except StepProcessorError as e:
            print(f"✓ Caught expected error for invalid sheet: {e}")
        
        # Test invalid encoding
        try:
            step_config = {
                'processor_type': 'import_file',
                'step_description': 'Invalid encoding test',
                'input_file': test_files['csv'],
                'encoding': 123,  # Should be string
                'replace_current_data': True
            }
            processor = ImportFileProcessor(step_config)
            processor.execute(create_sample_data())
            print("✗ Should have failed with invalid encoding")
            return False
        except StepProcessorError as e:
            print(f"✓ Caught expected error for invalid encoding: {e}")
        
        print("✓ Configuration validation tests passed")
        return True


def test_capabilities_info():
    """Test getting processor capabilities information."""
    
    print("\nTesting capabilities info...")
    
    step_config = {
        'processor_type': 'import_file',
        'step_description': 'Capabilities test',
        'input_file': 'dummy.xlsx',
        'replace_current_data': True
    }
    
    processor = ImportFileProcessor(step_config)
    capabilities = processor.get_capabilities()
    
    print(f"✓ Capabilities info: {capabilities}")
    
    # Check expected keys
    expected_keys = ['description', 'import_formats', 'import_features', 'stage_features', 'examples']
    has_all_keys = all(key in capabilities for key in expected_keys)
    
    # Check some expected features
    has_stage_features = 'save_to_named_stage' in capabilities.get('stage_features', [])
    has_format_detection = 'automatic_format_detection' in capabilities.get('import_features', [])
    
    if has_all_keys and has_stage_features and has_format_detection:
        print("✓ Capabilities info is complete")
        return True
    else:
        print("✗ Capabilities info is incomplete")
        print(f"  Has all keys: {has_all_keys}")
        print(f"  Has stage features: {has_stage_features}")
        print(f"  Has format detection: {has_format_detection}")
        return False


if __name__ == '__main__':
    print("📥 Testing ImportFileProcessor functionality...")
    print("   Tests basic import, stage saving, variable substitution, and error handling")
    print("   Leverages FileReader for file operations and StageManager for stage operations\n")
    
    success = True
    
    success &= test_basic_import_excel()
    success &= test_basic_import_csv()
    success &= test_import_with_stage_saving()
    success &= test_stage_overwrite_protection()
    success &= test_variable_substitution()
    success &= test_explicit_format_override()
    success &= test_replace_current_data_safety()
    success &= test_error_handling()
    success &= test_configuration_validation()
    success &= test_capabilities_info()
    
    if success:
        print("\n✅ All ImportFileProcessor tests passed!")
    else:
        print("\n❌ Some ImportFileProcessor tests failed!")
    
    # Show processor summary
    print(f"\nImportFileProcessor Summary:")
    print(f"✓ Integrates FileReader for all file operations")
    print(f"✓ Integrates StageManager for optional stage saving")
    print(f"✓ Supports all FileReader formats: Excel, CSV, TSV")
    print(f"✓ Provides variable substitution and format auto-detection")
    print(f"✓ Validates stage names and prevents conflicts")
