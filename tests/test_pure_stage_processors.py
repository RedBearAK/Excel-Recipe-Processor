import pandas as pd
import tempfile

from pathlib import Path

from excel_recipe_processor.core.stage_manager import StageManager
from excel_recipe_processor.processors.import_file_processor import ImportFileProcessor
from excel_recipe_processor.processors.export_file_processor import ExportFileProcessor


def create_test_data():
    """Create test data for processor testing."""
    return pd.DataFrame({
        'ID': [1, 2, 3],
        'Name': ['Alice', 'Bob', 'Charlie'],
        'Status': ['Active', 'Active', 'Inactive']
    })


def test_import_processor_pure_stage():
    """Test ImportFileProcessor with pure stage architecture."""
    
    print("Testing pure stage ImportFileProcessor...")
    
    StageManager.initialize_stages()
    
    try:
        # Declare stages
        recipe_config = {
            'settings': {
                'stages': [
                    {'stage_name': 'imported_data', 'description': 'Imported test data'}
                ]
            }
        }
        StageManager.declare_recipe_stages(recipe_config)
        
        # Create test file
        test_data = create_test_data()
        
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as f:
            test_file = Path(f.name)
            test_data.to_excel(test_file, index=False)
        
        try:
            # Test import processor
            step_config = {
                'processor_type': 'import_file',
                'input_file': str(test_file),
                'save_to_stage': 'imported_data'
            }
            
            processor = ImportFileProcessor(step_config)
            result = processor.execute_import()
            
            # Check that stage was created
            if not StageManager.stage_exists('imported_data'):
                print("✗ Import processor didn't create stage")
                return False
            
            # Check data integrity
            stage_data = StageManager.load_stage('imported_data')
            if len(stage_data) == 3 and 'Name' in stage_data.columns:
                print("✓ Import processor working with pure stage architecture")
                return True
            else:
                print("✗ Import processor data integrity failed")
                return False
        
        finally:
            test_file.unlink()  # Cleanup
    
    finally:
        StageManager.cleanup_stages()


def test_export_processor_pure_stage():
    """Test ExportFileProcessor with pure stage architecture."""
    
    print("Testing pure stage ExportFileProcessor...")
    
    StageManager.initialize_stages()
    
    try:
        # Declare stages and create test data
        recipe_config = {
            'settings': {
                'stages': [
                    {'stage_name': 'export_data', 'description': 'Data to export'}
                ]
            }
        }
        StageManager.declare_recipe_stages(recipe_config)
        
        # Create stage with test data
        test_data = create_test_data()
        StageManager.save_stage('export_data', test_data, 'Test export data')
        
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as f:
            output_file = Path(f.name)
        
        try:
            # Test export processor
            step_config = {
                'processor_type': 'export_file',
                'source_stage': 'export_data',
                'output_file': str(output_file)
            }
            
            processor = ExportFileProcessor(step_config)
            processor.execute_export()
            
            # Check that file was created
            if not output_file.exists():
                print("✗ Export processor didn't create file")
                return False
            
            # Check file content
            exported_data = pd.read_excel(output_file)
            if len(exported_data) == 3 and 'Name' in exported_data.columns:
                print("✓ Export processor working with pure stage architecture")
                return True
            else:
                print("✗ Export processor file content failed")
                return False
        
        finally:
            if output_file.exists():
                output_file.unlink()  # Cleanup
    
    finally:
        StageManager.cleanup_stages()


def test_complete_import_export_workflow():
    """Test complete import -> process -> export workflow."""
    
    print("Testing complete pure stage workflow...")
    
    StageManager.initialize_stages()
    
    try:
        # Declare all stages
        recipe_config = {
            'settings': {
                'stages': [
                    {'stage_name': 'raw_data', 'description': 'Imported data'},
                    {'stage_name': 'filtered_data', 'description': 'Filtered data'}
                ]
            }
        }
        StageManager.declare_recipe_stages(recipe_config)
        
        # Create test file
        test_data = create_test_data()
        
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as f:
            input_file = Path(f.name)
            test_data.to_excel(input_file, index=False)
        
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as f:
            output_file = Path(f.name)
        
        try:
            # Step 1: Import
            import_config = {
                'processor_type': 'import_file',
                'input_file': str(input_file),
                'save_to_stage': 'raw_data'
            }
            import_processor = ImportFileProcessor(import_config)
            import_processor.execute_import()
            
            # Step 2: Filter (using existing processor with stage-to-stage)
            from excel_recipe_processor.processors.filter_data_processor import FilterDataProcessor
            filter_config = {
                'processor_type': 'filter_data',
                'source_stage': 'raw_data',
                'save_to_stage': 'filtered_data',
                'filters': [
                    {'column': 'Status', 'condition': 'equals', 'value': 'Active'}
                ]
            }
            filter_processor = FilterDataProcessor(filter_config)
            filter_processor.execute_stage_to_stage()
            
            # Step 3: Export
            export_config = {
                'processor_type': 'export_file',
                'source_stage': 'filtered_data',
                'output_file': str(output_file)
            }
            export_processor = ExportFileProcessor(export_config)
            export_processor.execute_export()
            
            # Verify end result
            final_data = pd.read_excel(output_file)
            active_count = len(final_data[final_data['Status'] == 'Active'])
            
            if active_count == 2 and len(final_data) == 2:
                print("✓ Complete pure stage workflow successful")
                return True
            else:
                print(f"✗ Workflow failed: expected 2 active records, got {active_count}")
                return False
        
        finally:
            # Cleanup
            if input_file.exists():
                input_file.unlink()
            if output_file.exists():
                output_file.unlink()
    
    finally:
        StageManager.cleanup_stages()


if __name__ == '__main__':
    print("🚀 Pure Stage Processor Tests")
    print("=" * 50)
    
    tests = [
        test_import_processor_pure_stage,
        test_export_processor_pure_stage,
        test_complete_import_export_workflow
    ]
    
    results = []
    for test_func in tests:
        print(f"\nRunning {test_func.__name__}...")
        try:
            result = test_func()
            results.append(result)
        except Exception as e:
            print(f"✗ Test {test_func.__name__} failed with exception: {e}")
            results.append(False)
    
    # Summary
    passed = sum(results)
    total = len(results)
    
    print(f"\n{'='*50}")
    print(f"Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All pure stage processor tests passed!")
        print("Import/Export processors are ready for pure stage architecture!")
    else:
        print("❌ Some tests failed - check processor implementations")
    
    exit(0 if passed == total else 1)
