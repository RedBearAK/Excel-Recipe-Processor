"""
Test script for the capabilities and validation tools.
Creates sample recipes and demonstrates the tools.
"""

import yaml

from pathlib import Path


def create_sample_recipes():
    """Create sample recipe files for testing."""
    
    # Sample 1: Valid van report style recipe
    valid_recipe = {
        'recipe': [
            {
                'step_description': 'Filter for canned products',
                'processor_type': 'filter_data',
                'filters': [
                    {
                        'column': 'Product_Name',
                        'condition': 'contains',
                        'value': 'CANNED'
                    }
                ]
            },
            {
                'step_description': 'Replace FLESH with CANS',
                'processor_type': 'clean_data',
                'rules': [
                    {
                        'column': 'Component',
                        'action': 'replace',
                        'old_value': 'FLESH',
                        'new_value': 'CANS'
                    }
                ]
            },
            {
                'step_description': 'Lookup product details',
                'processor_type': 'lookup_data',
                'lookup_source': 'products.xlsx',
                'lookup_key': 'Product_Code',
                'source_key': 'Product_Code',
                'lookup_columns': ['Category', 'Price']
            },
            {
                'step_description': 'Group by region',
                'processor_type': 'group_data',
                'source_column': 'Product_Origin',
                'target_column': 'Region',
                'groups': {
                    'Bristol Bay': ['Dillingham', 'Naknek', 'Wood River'],
                    'Kodiak': ['Kodiak', 'Kodiak West'],
                    'PWS': ['Cordova', 'Seward', 'Valdez'],
                    'SE': ['Craig', 'Ketchikan', 'Petersburg', 'Sitka']
                }
            },
            {
                'step_description': 'Create pivot table',
                'processor_type': 'pivot_table',
                'index': ['Region', 'Carrier'],
                'values': ['Van_Number'],
                'aggfunc': 'count'
            }
        ],
        'settings': {
            'output_filename': 'van_report.xlsx'
        }
    }
    
    # Sample 2: Recipe with missing processor
    invalid_recipe = {
        'recipe': [
            {
                'step_description': 'Valid filter step',
                'processor_type': 'filter_data',
                'filters': [{'column': 'step_description', 'condition': 'equals', 'value': 'Test'}]
            },
            {
                'step_description': 'Invalid processor',
                'processor_type': 'nonexistent_processor',
                'some_config': 'value'
            },
            {
                'step_description': 'Another invalid processor',
                'processor_type': 'machine_learning_magic',
                'algorithm': 'predict_future'
            }
        ]
    }
    
    # Sample 3: Recipe with config errors
    broken_config_recipe = {
        'recipe': [
            {
                'step_description': 'Missing required fields',
                'processor_type': 'lookup_data'
                # Missing lookup_source, lookup_key, source_key, lookup_columns
            },
            {
                'step_description': 'Invalid filter',
                'processor_type': 'filter_data',
                'filters': 'this should be a list, not a string'
            }
        ]
    }
    
    # Save test recipes
    recipes = [
        ('valid_van_report.yaml', valid_recipe),
        ('invalid_processors.yaml', invalid_recipe),
        ('broken_config.yaml', broken_config_recipe)
    ]
    
    created_files = []
    for filename, recipe_data in recipes:
        filepath = Path(filename)
        with open(filepath, 'w') as f:
            yaml.dump(recipe_data, f, default_flow_style=False, sort_keys=False)
        created_files.append(filepath)
        print(f"✓ Created: {filename}")
    
    return created_files


def demo_capabilities_tools():
    """Demonstrate the capabilities and validation tools."""
    
    print("🔧 Capabilities & Validation Tools Demo")
    print("=" * 50)
    
    # Create sample recipes
    print("\n📝 Creating sample recipes...")
    recipe_files = create_sample_recipes()
    
    # Demo 1: System capabilities overview
    print("\n" + "="*50)
    print("🔍 DEMO 1: System Capabilities Overview")
    print("="*50)
    
    try:
        # Import the comprehensive viewer we created
        import sys
        sys.path.append('.')  # Make sure we can import our tools
        
        from test_comprehensive_capabilities import print_system_overview
        print_system_overview()
        
    except ImportError:
        print("⚠️  Capabilities viewer not found - run from correct directory")
        # Show basic capabilities instead
        from excel_recipe_processor.core.base_processor import registry
        available = registry.get_registered_types()
        print(f"Available processors: {len(available)}")
        for i, proc_type in enumerate(available, 1):
            print(f"  {i}. {proc_type}")
    
    # Demo 2: Recipe validation
    print("\n" + "="*50)
    print("🔍 DEMO 2: Recipe Validation")
    print("="*50)
    
    try:
        from test_recipe_validator import RecipeValidator
        validator = RecipeValidator()
        
        for recipe_file in recipe_files:
            print(f"\n📋 Validating: {recipe_file.name}")
            print("-" * 40)
            
            result = validator.validate_recipe_file(str(recipe_file))
            validator.print_validation_report(result)
            
    except ImportError:
        print("⚠️  Recipe validator not found")
        
        # Basic validation fallback
        from excel_recipe_processor.core.base_processor import registry
        from excel_recipe_processor.config.recipe_loader import RecipeLoader
        
        available_types = registry.get_registered_types()
        
        for recipe_file in recipe_files:
            print(f"\n📋 Basic validation: {recipe_file.name}")
            try:
                loader = RecipeLoader()
                recipe_data = loader.load_file(str(recipe_file))
                steps = loader.get_steps()
                
                valid_steps = 0
                for step in steps:
                    if step.get('processor_type') in available_types:
                        valid_steps += 1
                
                print(f"   Steps: {valid_steps}/{len(steps)} valid")
                if valid_steps == len(steps):
                    print("   ✅ Recipe is compatible")
                else:
                    print("   ⚠️  Recipe has issues")
                    
            except Exception as e:
                print(f"   ❌ Error: {e}")
    
    # Demo 3: Usage examples
    print("\n" + "="*50)
    print("💡 USAGE EXAMPLES")
    print("="*50)
    
    print("\n🔧 To view full system capabilities:")
    print("   python tests/comprehensive_capabilities.py")
    print("   python tests/comprehensive_capabilities.py --detailed")
    print("   python tests/comprehensive_capabilities.py --matrix")
    print("   python tests/comprehensive_capabilities.py --json")
    
    print("\n🔍 To validate recipes:")
    print("   python tests/recipe_validator.py valid_van_report.yaml")
    print("   python tests/recipe_validator.py invalid_processors.yaml")
    print("   python tests/recipe_validator.py --list")
    
    print("\n📊 To get processor-specific capabilities:")
    print("   from excel_recipe_processor.processors.lookup_data_processor import LookupDataProcessor")
    print("   processor = LookupDataProcessor({'processor_type': 'lookup_data', ...})")
    print("   print(processor.get_capabilities())")
    
    # Cleanup
    print(f"\n🧹 Cleaning up test files...")
    for file_path in recipe_files:
        if file_path.exists():
            file_path.unlink()
            print(f"   Removed: {file_path.name}")
    
    print(f"\n🎉 Demo complete! Your system has comprehensive self-documentation capabilities.")


if __name__ == '__main__':
    demo_capabilities_tools()
