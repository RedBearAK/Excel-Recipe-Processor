"""
Debug test to identify fundamental issues with GenerateColumnConfigProcessor.

debug_generate_column_config.py

This test attempts to isolate what's going wrong with the processor initialization.
"""

import sys
import os
import inspect
from pathlib import Path

print("=== GenerateColumnConfigProcessor Debug Test ===\n")

# Check Python path and current directory
print("1. Environment Check:")
print(f"   Current working directory: {os.getcwd()}")
print(f"   Python path includes:")
for path in sys.path[:5]:  # Show first 5 paths
    print(f"     {path}")
print(f"   Script location: {__file__}")

# Try to import the processor and check what we actually get
print("\n2. Import Test:")
try:
    from excel_recipe_processor.processors.generate_column_config_processor import GenerateColumnConfigProcessor
    print("✓ Successfully imported GenerateColumnConfigProcessor")
    
    # Check the file location of what we imported
    module_file = inspect.getfile(GenerateColumnConfigProcessor)
    print(f"   Imported from: {module_file}")
    
    # Check if it's the right class
    print(f"   Class name: {GenerateColumnConfigProcessor.__name__}")
    print(f"   Module name: {GenerateColumnConfigProcessor.__module__}")
    
except ImportError as e:
    print(f"✗ Failed to import GenerateColumnConfigProcessor: {e}")
    sys.exit(1)

# Check base class
print("\n3. Base Class Check:")
base_classes = [cls.__name__ for cls in GenerateColumnConfigProcessor.__mro__]
print(f"   Method Resolution Order: {' -> '.join(base_classes)}")

# Try to import the base class separately
try:
    from excel_recipe_processor.core.base_processor import FileOpsBaseProcessor
    print("✓ Successfully imported FileOpsBaseProcessor")
except ImportError as e:
    print(f"✗ Failed to import FileOpsBaseProcessor: {e}")

# Check if the processor has the expected methods and attributes
print("\n4. Class Structure Check:")
processor_attrs = [attr for attr in dir(GenerateColumnConfigProcessor) if not attr.startswith('_')]
print(f"   Public methods/attributes: {len(processor_attrs)}")
for attr in sorted(processor_attrs)[:10]:  # Show first 10
    print(f"     {attr}")

# Check __init__ method specifically
print("\n5. __init__ Method Check:")
try:
    init_signature = inspect.signature(GenerateColumnConfigProcessor.__init__)
    print(f"   __init__ signature: {init_signature}")
    
    # Get the source code of __init__ if possible
    try:
        init_source = inspect.getsource(GenerateColumnConfigProcessor.__init__)
        print(f"   __init__ source length: {len(init_source)} characters")
        # Look for source_file assignment
        if "self.source_file" in init_source:
            print("✓ Found 'self.source_file' assignment in __init__")
        else:
            print("✗ 'self.source_file' assignment NOT found in __init__")
    except Exception as e:
        print(f"   Could not get __init__ source: {e}")
        
except Exception as e:
    print(f"   Error checking __init__: {e}")

# Try creating a minimal processor to see where it fails
print("\n6. Minimal Creation Test:")
try:
    # First try with minimal config
    minimal_config = {
        'processor_type': 'generate_column_config',
        'source_file': 'test.csv',
        'template_file': 'test.csv', 
        'output_file': 'test.yaml'
    }
    
    print(f"   Attempting to create processor with config: {minimal_config}")
    processor = GenerateColumnConfigProcessor(minimal_config)
    print("✓ Processor created successfully")
    
    # Check if it has the expected attributes
    if hasattr(processor, 'source_file'):
        print(f"✓ processor.source_file = '{processor.source_file}'")
    else:
        print("✗ processor.source_file attribute missing!")
        print(f"   Available attributes: {[attr for attr in dir(processor) if not attr.startswith('_')]}")
    
    if hasattr(processor, 'template_file'):
        print(f"✓ processor.template_file = '{processor.template_file}'")
    else:
        print("✗ processor.template_file attribute missing!")
        
    if hasattr(processor, 'output_file'):
        print(f"✓ processor.output_file = '{processor.output_file}'")
    else:
        print("✗ processor.output_file attribute missing!")

except Exception as e:
    print(f"✗ Failed to create processor: {e}")
    print(f"   Exception type: {type(e)}")
    
    # Try to get more details about the error
    import traceback
    print("\n   Full traceback:")
    traceback.print_exc()

# Check dependent imports
print("\n7. Dependency Import Check:")

dependencies = [
    ('excel_recipe_processor.core.base_processor', 'FileOpsBaseProcessor'),
    ('excel_recipe_processor.core.file_reader', 'FileReader'),
    ('excel_recipe_processor.readers.openpyxl_excel_reader', 'OpenpyxlExcelReader'),
    ('excel_recipe_processor.processors._helpers.column_patterns', 'empty_or_whitespace_rgx')
]

for module_name, item_name in dependencies:
    try:
        module = __import__(module_name, fromlist=[item_name])
        item = getattr(module, item_name)
        print(f"✓ {module_name}.{item_name}")
    except ImportError as e:
        print(f"✗ {module_name}.{item_name} - ImportError: {e}")
    except AttributeError as e:
        print(f"✗ {module_name}.{item_name} - AttributeError: {e}")
    except Exception as e:
        print(f"✗ {module_name}.{item_name} - {type(e).__name__}: {e}")

# Check if we can see the processor file directly
print("\n8. File System Check:")
expected_processor_path = Path("excel_recipe_processor/processors/generate_column_config_processor.py")
if expected_processor_path.exists():
    print(f"✓ Processor file exists at: {expected_processor_path.absolute()}")
    file_size = expected_processor_path.stat().st_size
    print(f"   File size: {file_size} bytes")
    
    # Quick check of file contents
    with open(expected_processor_path, 'r') as f:
        content = f.read()
        if "class GenerateColumnConfigProcessor(FileOpsBaseProcessor):" in content:
            print("✓ File contains expected class definition")
        else:
            print("✗ File does not contain expected class definition")
            
        if "self.source_file = self.get_config_value('source_file')" in content:
            print("✓ File contains source_file assignment")
        else:
            print("✗ File does not contain source_file assignment")
else:
    print(f"✗ Processor file not found at: {expected_processor_path.absolute()}")

print("\n=== Debug Test Complete ===")

# End of file #
