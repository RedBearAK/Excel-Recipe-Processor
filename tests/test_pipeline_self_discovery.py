# Quick test of the self-discovery system
from excel_recipe_processor.core.pipeline import get_system_capabilities

print("Testing self-discovery system...")
capabilities = get_system_capabilities()

print(f"Total processors: {capabilities['system_info']['total_processors']}")
print(f"Processor types: {capabilities['system_info']['processor_types']}")

# Check if the processors with get_minimal_config() are working
test_processors = ['add_calculated_column', 'clean_data', 'rename_columns']

for proc_type in test_processors:
    if proc_type in capabilities['processors']:
        proc_info = capabilities['processors'][proc_type]
        if 'error' in proc_info:
            print(f"✗ {proc_type}: {proc_info['error']}")
        else:
            print(f"✓ {proc_type}: {proc_info.get('description', 'OK')}")
    else:
        print(f"✗ {proc_type}: Not found")

print("\nDone!")
