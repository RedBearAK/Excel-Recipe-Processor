"""
Debug script to check processor registration.
"""

def debug_registration():
    print("🔧 Debug: Checking processor registration...")
    
    # Check before any imports
    try:
        from excel_recipe_processor.core.base_processor import registry
        print(f"Before pipeline import: {registry.get_registered_types()}")
    except Exception as e:
        print(f"Error importing registry: {e}")
    
    # Import pipeline to trigger registration
    try:
        from excel_recipe_processor.core.pipeline import ExcelPipeline
        print(f"After pipeline import: {registry.get_registered_types()}")
    except Exception as e:
        print(f"Error importing pipeline: {e}")
    
    # Check if lookup_data_processor can be imported
    try:
        from excel_recipe_processor.processors.lookup_data_processor import LookupDataProcessor
        print("✓ LookupDataProcessor imports successfully")
    except Exception as e:
        print(f"✗ Error importing LookupDataProcessor: {e}")
    
    # Try manual registration
    try:
        registry.register('lookup_data_manual', LookupDataProcessor)
        print(f"After manual registration: {registry.get_registered_types()}")
    except Exception as e:
        print(f"Error in manual registration: {e}")


if __name__ == '__main__':
    debug_registration()
