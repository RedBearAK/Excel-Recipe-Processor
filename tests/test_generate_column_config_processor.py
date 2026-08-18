"""
Basic validation tests for GenerateColumnConfigProcessor.

tests/test_generate_column_config_processor.py

Simple validation and configuration tests for the file-based processor.
Comprehensive functionality tests are in test_generate_column_config_fileops.py.
"""

import tempfile
from pathlib import Path

from excel_recipe_processor.processors.generate_column_config_processor import GenerateColumnConfigProcessor


def test_processor_initialization():
    """Test basic processor initialization and configuration validation."""
    
    print("Testing processor initialization...")
    
    # Test valid configuration
    try:
        step_config = {
            'processor_type': 'generate_column_config',
            'source_file': 'data/source.csv',
            'template_file': 'data/template.csv',
            'output_file': 'configs/output.yaml'
        }
        processor = GenerateColumnConfigProcessor(step_config)
        print("✓ Valid configuration accepted")
    except Exception as e:
        print(f"✗ Valid configuration rejected: {e}")
        return False
    
    # Test configuration with optional parameters
    try:
        step_config = {
            'processor_type': 'generate_column_config',
            'source_file': 'data/source.xlsx',
            'template_file': 'data/template.xlsx', 
            'output_file': 'configs/output.yaml',
            'source_sheet': 'Data',
            'template_sheet': 2,
            'header_row': 3,
            'max_rows': 50000,
            'similarity_threshold': 0.8,
            'include_recipe_section': True
        }
        processor = GenerateColumnConfigProcessor(step_config)
        print("✓ Configuration with all optional parameters accepted")
    except Exception as e:
        print(f"✗ Full configuration rejected: {e}")
        return False
    
    return True


def test_required_configuration_validation():
    """Test validation of required configuration parameters."""
    
    print("\nTesting required configuration validation...")
    
    # Test missing source_file
    try:
        step_config = {
            'processor_type': 'generate_column_config',
            'template_file': 'template.csv',
            'output_file': 'output.yaml'
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
        }
        processor = GenerateColumnConfigProcessor(step_config)
        print("✗ Should have failed with missing output_file")
        return False
    except Exception:
        print("✓ Properly validates missing output_file")
    
    return True


def test_file_format_validation():
    """Test file format validation."""
    
    print("\nTesting file format validation...")
    
    # Test unsupported source file format
    try:
        step_config = {
            'processor_type': 'generate_column_config',
            'source_file': 'data.txt',  # Unsupported
            'template_file': 'template.csv',
            'output_file': 'output.yaml'
        }
        processor = GenerateColumnConfigProcessor(step_config)
        print("✗ Should have failed with unsupported source file format")
        return False
    except Exception:
        print("✓ Properly validates source file format")
    
    # Test unsupported template file format
    try:
        step_config = {
            'processor_type': 'generate_column_config',
            'source_file': 'source.csv',
            'template_file': 'template.json',  # Unsupported
            'output_file': 'output.yaml'
        }
        processor = GenerateColumnConfigProcessor(step_config)
        print("✗ Should have failed with unsupported template file format")
        return False
    except Exception:
        print("✓ Properly validates template file format")
    
    # Test valid file formats
    valid_formats = [
        ('source.csv', 'template.csv'),
        ('source.xlsx', 'template.xlsx'),
        ('source.xls', 'template.xlsm'),
        ('source.csv', 'template.xlsx')  # Mixed formats
    ]
    
    for source_file, template_file in valid_formats:
        try:
            step_config = {
                'processor_type': 'generate_column_config',
                'source_file': source_file,
                'template_file': template_file,
                'output_file': 'output.yaml'
            }
            processor = GenerateColumnConfigProcessor(step_config)
        except Exception as e:
            print(f"✗ Valid format combination rejected: {source_file}, {template_file} - {e}")
            return False
    
    print("✓ All valid file format combinations accepted")
    return True


def test_minimal_config():
    """Test the get_minimal_config class method."""
    
    print("\nTesting minimal configuration...")
    
    try:
        minimal_config = GenerateColumnConfigProcessor.get_minimal_config()
        
        # Check that minimal config has required keys
        required_keys = {'source_file', 'template_file', 'output_file'}
        if not required_keys.issubset(minimal_config.keys()):
            missing_keys = required_keys - minimal_config.keys()
            print(f"✗ Minimal config missing required keys: {missing_keys}")
            return False
        
        # Check that minimal config can create a processor
        processor = GenerateColumnConfigProcessor(minimal_config)
        print("✓ Minimal configuration works correctly")
        return True
        
    except Exception as e:
        print(f"✗ Minimal configuration failed: {e}")
        return False


def test_processor_type():
    """Test that processor reports correct operation type."""
    
    print("\nTesting processor operation type...")
    
    try:
        step_config = GenerateColumnConfigProcessor.get_minimal_config()
        processor = GenerateColumnConfigProcessor(step_config)
        
        operation_type = processor.get_operation_type()
        if operation_type == "column_config_generation":
            print("✓ Processor reports correct operation type")
            return True
        else:
            print(f"✗ Unexpected operation type: {operation_type}")
            return False
            
    except Exception as e:
        print(f"✗ Operation type test failed: {e}")
        return False


def main():
    """Run all validation tests and report results."""
    
    print("=== GenerateColumnConfigProcessor Validation Tests ===\n")
    
    tests = [
        test_processor_initialization,
        test_required_configuration_validation,
        test_file_format_validation,
        test_minimal_config,
        test_processor_type
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
        print("\n✅ All GenerateColumnConfigProcessor validation tests passed!")
        print("💡 Run test_generate_column_config_fileops.py for comprehensive functionality tests")
        return 1
    else:
        print("\n❌ Some GenerateColumnConfigProcessor validation tests failed!")
        return 0


if __name__ == "__main__":
    exit(0 if main() else 1)
# End of file #
