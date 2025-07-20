"""
Comprehensive system capabilities viewer for Excel Recipe Processor.
Shows detailed capabilities of all processors and system-wide features.
"""

import json

from excel_recipe_processor.processors.base_processor import registry


def get_all_capabilities():
    """Get comprehensive capabilities of all processors."""
    
    capabilities = {
        'system_overview': {
            'step_description': 'Excel Recipe Processor',
            'description': 'Automated Excel data processing system with YAML/JSON recipes',
            'total_processors': len(registry.get_registered_types()),
            'version': 'Development Build'
        },
        'processors': {}
    }
    
    # Get capabilities for each processor
    for processor_type in sorted(registry.get_registered_types()):
        try:
            # Create processor with minimal config
            config = {'processor_type': processor_type, 'step_description': f'Capabilities check'}
            
            # Add required fields for each processor type
            if processor_type == 'add_calculated_column':
                config.update({'new_column': 'test', 'calculation': {}})
            elif processor_type == 'clean_data':
                config.update({'rules': []})
            elif processor_type == 'filter_data':
                config.update({'filters': []})
            elif processor_type == 'group_data':
                config.update({'source_column': 'test', 'groups': {}})
            elif processor_type == 'lookup_data':
                config.update({
                    'lookup_source': {}, 'lookup_key': 'test',
                    'source_key': 'test', 'lookup_columns': ['test']
                })
            elif processor_type == 'rename_columns':
                # Note: rename_columns has a design issue - it uses 'processor_type' for both processor and operation type
                # We'll create the config without the operation type to avoid conflict
                config.update({'mapping': {'old_col': 'new_col'}})
            elif processor_type == 'sort_data':
                config.update({'columns': ['test']})
            
            processor = registry.create_processor(config)
            
            if hasattr(processor, 'get_capabilities'):
                capabilities['processors'][processor_type] = processor.get_capabilities()
            else:
                capabilities['processors'][processor_type] = {
                    'description': f'{processor_type} processor',
                    'status': 'capabilities method not implemented'
                }
                
        except Exception as e:
            capabilities['processors'][processor_type] = {
                'error': f'Could not load capabilities: {str(e)}'
            }
    
    return capabilities


def print_system_overview():
    """Print a formatted overview of the entire system."""
    
    print("=" * 80)
    print("🏭 EXCEL RECIPE PROCESSOR - SYSTEM CAPABILITIES REPORT")
    print("=" * 80)
    
    capabilities = get_all_capabilities()
    
    # System Overview
    system = capabilities['system_overview']
    print(f"\n📊 System Overview:")
    print(f"   Name: {system['step_description']}")
    print(f"   Description: {system['description']}")
    print(f"   Total Processors: {system['total_processors']}")
    print(f"   Version: {system['version']}")
    
    # Processor Summary
    print(f"\n🔧 Available Processors:")
    for i, (processor_type, info) in enumerate(capabilities['processors'].items(), 1):
        description = info.get('description', 'No description available')
        print(f"   {i:2d}. {processor_type:<20} - {description}")
    
    return capabilities


def print_detailed_capabilities():
    """Print detailed capabilities for each processor."""
    
    capabilities = get_all_capabilities()
    
    print(f"\n🚀 DETAILED PROCESSOR CAPABILITIES:")
    print("=" * 80)
    
    for processor_type, info in capabilities['processors'].items():
        print(f"\n📋 {processor_type.upper().replace('_', ' ')} PROCESSOR")
        print("-" * 60)
        
        if 'error' in info:
            print(f"   ❌ Error: {info['error']}")
            continue
        
        # Description
        if 'description' in info:
            print(f"   📝 Description: {info['description']}")
        
        # Main features/capabilities
        feature_keys = [
            'supported_actions', 'calculation_types', 'supported_conditions',
            'lookup_features', 'pivot_features', 'rename_types', 'supported_options',
            'filter_operations', 'grouping_features', 'transformation_features'
        ]
        
        for key in feature_keys:
            if key in info:
                print(f"   🎯 {key.replace('_', ' ').title()}: {len(info[key])} available")
        
        # Special capabilities
        if 'join_types' in info:
            print(f"   🔗 Join Types: {', '.join(info['join_types'])}")
        
        if 'data_sources' in info:
            print(f"   📁 Data Sources: {', '.join(info['data_sources'])}")
        
        if 'aggregation_functions' in info:
            print(f"   📊 Aggregation Functions: {len(info['aggregation_functions'])} available")
        
        if 'case_conversions' in info:
            print(f"   🔤 Case Conversions: {', '.join(info['case_conversions'])}")
        
        # Helper methods
        if 'helper_methods' in info:
            print(f"   🔧 Helper Methods: {len(info['helper_methods'])} available")
        
        if 'special_methods' in info:
            print(f"   ⚡ Special Methods: {len(info['special_methods'])} available")
        
        # Examples
        if 'examples' in info:
            print(f"   💡 Example Uses:")
            for example_type, example_desc in list(info['examples'].items())[:2]:
                print(f"      • {example_desc}")


def print_feature_matrix():
    """Print a feature matrix showing what each processor can do."""
    
    capabilities = get_all_capabilities()
    
    print(f"\n📈 FEATURE MATRIX:")
    print("=" * 120)
    
    # Collect all unique features across processors
    all_features = set()
    processor_features = {}
    
    for processor_type, info in capabilities['processors'].items():
        features = set()
        
        # Extract features from different capability fields
        for key in ['supported_actions', 'calculation_types', 'filter_operations', 
                   'lookup_features', 'pivot_features', 'grouping_features']:
            if key in info and isinstance(info[key], list):
                features.update(info[key])
        
        processor_features[processor_type] = features
        all_features.update(features)
    
    # Sort features
    sorted_features = sorted(all_features)
    
    print(f"Legend: ✅ = Supported, ❌ = Not Supported\n")
    
    # Print header with better spacing - use full processor names if short enough
    processors = list(capabilities['processors'].keys())
    header_names = []
    
    for proc in processors:
        if len(proc) <= 15:
            header_names.append(proc[:15])
        else:
            # Smart abbreviation
            parts = proc.split('_')
            if len(parts) == 2:
                header_names.append(f"{parts[0][:7]}_{parts[1][:6]}")
            elif len(parts) >= 3:
                header_names.append(f"{parts[0][:5]}_{parts[1][:3]}_{parts[2][:3]}")
            else:
                header_names.append(proc[:15])
    
    # Calculate dynamic column width
    col_width = max(15, max(len(name) for name in header_names))
    
    header = f"{'Feature':<30} " + " ".join(f"{name:<{col_width}}" for name in header_names)
    print(header)
    print("-" * len(header))
    
    # Print feature matrix (show first 25 features)
    for feature in sorted_features[:25]:
        feature_name = feature[:29]  # Truncate long feature names
        row = f"{feature_name:<30} "
        
        for processor_type in processors:
            has_feature = feature in processor_features.get(processor_type, set())
            symbol = "✅" + " " * (col_width - 1) if has_feature else "❌" + " " * (col_width - 1)
            row += symbol
        
        print(row)
    
    if len(sorted_features) > 25:
        print(f"\n... and {len(sorted_features) - 25} more features")
    
    print(f"\n💡 Total unique features across all processors: {len(sorted_features)}")
    print(f"📊 Matrix shows top 25 features across {len(processors)} processors")


def validate_recipe_file(recipe_path):
    """Validate a recipe file against system capabilities."""
    
    try:
        from excel_recipe_processor.config.recipe_loader import RecipeLoader
        
        print(f"\n🔍 RECIPE VALIDATION: {recipe_path}")
        print("=" * 80)
        
        loader = RecipeLoader()
        recipe_data = loader.load_file(recipe_path)
        
        steps = loader.get_steps()
        available_types = registry.get_registered_types()
        
        print(f"📋 Recipe Summary:")
        print(f"   Total Steps: {len(steps)}")
        print(f"   Recipe File: {recipe_path}")
        
        print(f"\n🔍 Step Analysis:")
        
        valid_steps = 0
        for i, step in enumerate(steps, 1):
            step_type = step.get('processor_type', 'unknown')
            step_name = step.get('step_description', f'Step {i}')
            
            if step_type in available_types:
                print(f"   ✅ Step {i}: {step_name} ({step_type})")
                valid_steps += 1
            else:
                print(f"   ❌ Step {i}: {step_name} ({step_type}) - PROCESSOR NOT FOUND")
        
        print(f"\n📊 Validation Results:")
        print(f"   Valid Steps: {valid_steps}/{len(steps)}")
        print(f"   Compatibility: {valid_steps/len(steps)*100:.1f}%")
        
        if valid_steps == len(steps):
            print(f"   🎉 Recipe is fully supported and ready to run!")
        else:
            print(f"   ⚠️  Recipe has unsupported processors - may not run completely")
        
        return valid_steps == len(steps)
        
    except Exception as e:
        print(f"   ❌ Error validating recipe: {e}")
        return False


def main():
    """Main capability viewer interface."""
    
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == '--detailed':
            print_system_overview()
            print_detailed_capabilities()
        elif sys.argv[1] == '--matrix':
            print_system_overview()
            print_feature_matrix()
        elif sys.argv[1] == '--validate' and len(sys.argv) > 2:
            print_system_overview()
            validate_recipe_file(sys.argv[2])
        elif sys.argv[1] == '--json':
            capabilities = get_all_capabilities()
            print(json.dumps(capabilities, indent=2))
        else:
            print("Usage:")
            print("  python capability_viewer.py                 # Basic overview")
            print("  python capability_viewer.py --detailed      # Detailed capabilities")
            print("  python capability_viewer.py --matrix        # Feature matrix")
            print("  python capability_viewer.py --json          # JSON export")
            print("  python capability_viewer.py --validate recipe.yaml  # Validate recipe")
    else:
        # Default: show overview
        print_system_overview()
        print("\n💡 For more details, run with --detailed, --matrix, or --json")


if __name__ == '__main__':
    main()
