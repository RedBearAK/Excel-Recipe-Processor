"""
Demo script showing the capability discovery system.
"""

def demo_capabilities():
    """Demonstrate the capability discovery system."""
    
    print("🔍 Excel Recipe Processor - Capability Discovery Demo\n")
    
    # Import the system
    from excel_recipe_processor.core.base_processor import registry
    from excel_recipe_processor.core.recipe_pipeline import RecipePipeline
    
    print("📋 Available Processors:")
    available_types = registry.get_registered_types()
    for i, processor_type in enumerate(available_types, 1):
        print(f"  {i}. {processor_type}")
    print()
    
    # Demo individual processor capabilities
    print("🚀 Individual Processor Capabilities:\n")
    
    # Test lookup processor (which we know has capabilities)
    try:
        lookup_config = {
            'processor_type': 'lookup_data',
            'step_description': 'Test lookup',
            'lookup_source': {},
            'lookup_key': 'key',
            'source_key': 'key',
            'lookup_columns': ['col']
        }
        lookup_processor = registry.create_processor(lookup_config)
        
        if hasattr(lookup_processor, 'get_capabilities'):
            capabilities = lookup_processor.get_capabilities()
            print("🔗 LookupDataProcessor Capabilities:")
            print(f"   Description: {capabilities['description']}")
            print(f"   Join Types: {capabilities['join_types']}")
            print(f"   Data Sources: {capabilities['data_sources']}")
            print(f"   Features: {len(capabilities['lookup_features'])} features")
            print()
    except Exception as e:
        print(f"❌ Error getting lookup capabilities: {e}")
    
    # Test sort processor (which has capabilities)
    try:
        sort_config = {
            'processor_type': 'sort_data',
            'step_description': 'Test sort',
            'columns': ['test']
        }
        sort_processor = registry.create_processor(sort_config)
        
        if hasattr(sort_processor, 'get_capabilities'):
            capabilities = sort_processor.get_capabilities()
            print("📊 SortDataProcessor Capabilities:")
            print(f"   Description: {capabilities['description']}")
            print(f"   Options: {capabilities['supported_options']}")
            print(f"   Special Methods: {capabilities['special_methods']}")
            print()
    except Exception as e:
        print(f"❌ Error getting sort capabilities: {e}")
    
    # Demo recipe capability checking
    print("📜 Recipe Capability Analysis:\n")
    
    sample_recipe = {
        'recipe': [
            {
                'step_description': 'Filter canned products',
                'processor_type': 'filter_data',
                'filters': [{'column': 'Product_Name', 'condition': 'contains', 'value': 'CANNED'}]
            },
            {
                'step_description': 'Lookup product details',
                'processor_type': 'lookup_data',
                'lookup_source': 'products.xlsx',
                'lookup_key': 'Code',
                'source_key': 'Product_Code',
                'lookup_columns': ['Category', 'Price']
            },
            {
                'step_description': 'Group by region',
                'processor_type': 'group_data',
                'source_column': 'Origin',
                'groups': {'Region1': ['City1', 'City2']}
            }
        ]
    }
    
    print("Sample Recipe Analysis:")
    print(f"  📊 Total Steps: {len(sample_recipe['recipe'])}")
    
    available_steps = 0
    for step in sample_recipe['recipe']:
        step_type = step.get('processor_type')
        is_available = step_type in available_types
        status = "✅" if is_available else "❌"
        print(f"  {status} {step.get('step_description', 'Unnamed')} ({step_type})")
        if is_available:
            available_steps += 1
    
    print(f"\n  📈 Recipe Compatibility: {available_steps}/{len(sample_recipe['recipe'])} steps available")
    
    if available_steps == len(sample_recipe['recipe']):
        print("  🎉 Recipe is fully supported!")
    else:
        print("  ⚠️  Some processors missing - recipe may not run completely")
    
    print("\n" + "="*60)
    print("💡 Next Steps:")
    print("   1. Add get_capabilities() methods to all processors")
    print("   2. Run: python -c \"from your_module import get_system_capabilities; print(get_system_capabilities())\"")
    print("   3. Use capability info to build recipe validation tools")
    print("   4. Create documentation from capability information")


if __name__ == '__main__':
    demo_capabilities()
