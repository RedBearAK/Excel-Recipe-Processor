"""
Recipe validation tool for Excel Recipe Processor.
Validates recipes against available processors and provides detailed feedback.
"""

import json
import yaml

from pathlib import Path

from excel_recipe_processor.core.base_processor import registry
from excel_recipe_processor.config.recipe_loader import RecipeLoader, RecipeValidationError


# Universal step keys never counted as processor-specific requirements:
# identity/labeling keys, plus the stage keys the recipe-structure
# validator already enforces.
UNIVERSAL_STEP_KEYS = frozenset({
    'processor_type', 'step_description', 'source_stage', 'save_to_stage',
})

# Keys present in a processor's minimal config for ILLUSTRATION but
# actually optional (defaulted or conditional) in its execute path. Only
# OPTIONALITY lives here - key names still come from the processor - so
# an entry errs toward under-flagging, never toward a wrong name. Each
# entry cites the default that makes it optional.
OPTIONAL_IN_MINIMAL = {
    'slice_data': frozenset({'start_row', 'end_row'}),   # default 1 / None
    'fill_data': frozenset({'fill_value'}),              # default None (method-conditional)
    'debug_breakpoint': frozenset({'message'}),          # default text supplied
    'filter_terms_detector': frozenset({'text_columns'}),  # auto-detection covers it
    'rename_columns': frozenset({'mapping', 'rename_type'}),  # any-of shape, checked separately
}


def derive_required_fields(processor_cls, step_type):
    """Required fields per the processor's own get_minimal_config()."""
    minimal_config_fn = getattr(processor_cls, 'get_minimal_config', None)
    if minimal_config_fn is None:
        return []
    try:
        minimal_keys = set(minimal_config_fn().keys())
    except Exception:
        return []
    optional_keys = OPTIONAL_IN_MINIMAL.get(step_type, frozenset())
    return sorted(minimal_keys - UNIVERSAL_STEP_KEYS - optional_keys)


class RecipeValidator:
    """Validates recipes against system capabilities."""
    
    def __init__(self):
        self.available_processors = registry.get_registered_types()
    
    def validate_recipe_file(self, recipe_path: str) -> dict:
        """Validate a recipe file and return detailed results."""
        
        validation_result = {
            'file_path': recipe_path,
            'file_exists': False,
            'parseable': False,
            'valid_structure': False,
            'compatible': False,
            'total_steps': 0,
            'valid_steps': 0,
            'issues': [],
            'warnings': [],
            'step_details': []
        }
        
        # Check file exists
        recipe_file = Path(recipe_path)
        if not recipe_file.exists():
            validation_result['issues'].append(f"Recipe file not found: {recipe_path}")
            return validation_result
        
        validation_result['file_exists'] = True
        
        try:
            # Try to parse the recipe
            recipe_loader = RecipeLoader()
            recipe_data = recipe_loader.load_recipe_file(recipe_path)
            validation_result['parseable'] = True
            validation_result['valid_structure'] = True
            
            # Analyze steps
            steps = recipe_loader.get_steps()
            validation_result['total_steps'] = len(steps)
            
            for i, step in enumerate(steps, 1):
                step_analysis = self._analyze_step(step, i)
                validation_result['step_details'].append(step_analysis)
                
                if step_analysis['processor_available']:
                    validation_result['valid_steps'] += 1
                    if not step_analysis['config_valid']:
                        for issue in step_analysis['issues']:
                            validation_result['issues'].append(f"Step {i}: {issue}")
                else:
                    validation_result['issues'].append(
                        f"Step {i}: Processor '{step_analysis['step_type']}' not available"
                    )
            
            # Check overall compatibility
            validation_result['compatible'] = (
                validation_result['valid_steps'] == validation_result['total_steps'] and
                all(step['config_valid'] for step in validation_result['step_details'])
            )
            
            # Add compatibility warnings
            if validation_result['compatible']:
                validation_result['warnings'].append("Recipe is fully compatible!")
            else:
                missing_count = validation_result['total_steps'] - validation_result['valid_steps']
                validation_result['warnings'].append(
                    f"{missing_count} step(s) use unavailable processors"
                )
            
        except RecipeValidationError as e:
            validation_result['issues'].append(f"Recipe structure error: {e}")
        except yaml.YAMLError as e:
            validation_result['issues'].append(f"YAML parsing error: {e}")
        except json.JSONDecodeError as e:
            validation_result['issues'].append(f"JSON parsing error: {e}")
        except Exception as e:
            validation_result['issues'].append(f"Unexpected error: {e}")
        
        return validation_result
    
    def _analyze_step(self, step: dict, step_number: int) -> dict:
        """Analyze a single recipe step."""
        
        analysis = {
            'step_number': step_number,
            'step_name': step.get('step_description', f'Step {step_number}'),
            'step_type': step.get('processor_type', 'unknown'),
            'processor_available': False,
            'config_valid': False,
            'capabilities': None,
            'issues': []
        }
        
        # Check if processor type is available
        step_type = analysis['step_type']
        if step_type in self.available_processors:
            analysis['processor_available'] = True
            
            # Try to get processor capabilities and validate config
            try:
                # Create processor to test config and get capabilities
                processor = registry.create_processor(step)
                analysis['config_valid'] = True
                
                # Additional validation - try to check required fields
                if hasattr(processor, 'validate_required_fields'):
                    # Required fields are DERIVED from the processor's own
                    # get_minimal_config() (2026-08-17 redesign): key NAMES
                    # come from the framework's single source of truth, so
                    # a vocabulary change surfaces here automatically
                    # instead of rotting in a hand-maintained table (the
                    # lookup_data row did exactly that).
                    step_type = step.get('processor_type', '')
                    required_fields = derive_required_fields(
                        type(processor), step_type)

                    if step_type == 'rename_columns':
                        # Semantic, not vocabulary: rename accepts mapping
                        # OR pattern OR transform options; require any one.
                        has_mapping = 'mapping' in step and step['mapping']
                        has_pattern = 'pattern' in step
                        has_transform = any(k in step for k in ['case_conversion', 'add_prefix', 'add_suffix'])
                        if not (has_mapping or has_pattern or has_transform):
                            analysis['issues'].append("Missing rename configuration (mapping, pattern, or transform options)")
                            analysis['config_valid'] = False
                    
                    # Validate required fields exist
                    for field in required_fields:
                        if field not in step:
                            analysis['issues'].append(f"Missing required field: {field}")
                            analysis['config_valid'] = False
                        elif field == 'filters' and not isinstance(step[field], list):
                            analysis['issues'].append(f"Field '{field}' must be a list")
                            analysis['config_valid'] = False
                        elif field == 'rules' and not isinstance(step[field], list):
                            analysis['issues'].append(f"Field '{field}' must be a list")
                            analysis['config_valid'] = False
                        elif field == 'mapping' and not isinstance(step[field], dict):
                            analysis['issues'].append(f"Field '{field}' must be a dictionary")
                            analysis['config_valid'] = False
                        elif field == 'groups' and not isinstance(step[field], dict):
                            analysis['issues'].append(f"Field '{field}' must be a dictionary")
                            analysis['config_valid'] = False
                        elif field == 'columns' and not isinstance(step[field], (list, str)):
                            analysis['issues'].append(f"Field '{field}' must be a string or list")
                            analysis['config_valid'] = False
                        elif field == 'lookup_columns' and not isinstance(step[field], (list, str)):
                            analysis['issues'].append(f"Field '{field}' must be a string or list")
                            analysis['config_valid'] = False
                
                if hasattr(processor, 'get_capabilities'):
                    analysis['capabilities'] = processor.get_capabilities()
                    
            except Exception as e:
                analysis['issues'].append(f"Configuration error: {e}")
                analysis['config_valid'] = False
        else:
            analysis['issues'].append(f"Unknown processor type: {step_type}")
        
        return analysis
    
    def print_validation_report(self, validation_result: dict):
        """Print a formatted validation report."""
        
        print("🔍 RECIPE VALIDATION REPORT")
        print("=" * 60)
        
        # File info
        print(f"📁 File: {validation_result['file_path']}")
        print(f"📊 Status: ", end="")
        
        if validation_result['compatible']:
            print("✅ VALID - Ready to run")
        elif validation_result['valid_structure'] and validation_result['valid_steps'] > 0:
            config_issues = sum(1 for step in validation_result['step_details'] if not step['config_valid'])
            if config_issues > 0:
                print("⚠️  PARTIAL - Configuration errors found")
            else:
                print("⚠️  PARTIAL - Some processors missing")
        else:
            print("❌ INVALID - Cannot be processed")
        
        # Summary stats
        if validation_result['valid_structure']:
            total = validation_result['total_steps']
            valid = validation_result['valid_steps']
            print(f"📈 Compatibility: {valid}/{total} steps ({valid/total*100:.1f}%)")
        
        # Issues
        if validation_result['issues']:
            print(f"\n❌ Issues Found:")
            for issue in validation_result['issues']:
                print(f"   • {issue}")
        
        # Warnings
        if validation_result['warnings']:
            print(f"\n⚠️  Warnings:")
            for warning in validation_result['warnings']:
                print(f"   • {warning}")
        
        # Step details
        if validation_result['step_details']:
            print(f"\n📋 Step Analysis:")
            for step in validation_result['step_details']:
                status = "✅" if step['processor_available'] else "❌"
                config_status = "✅" if step['config_valid'] else "⚠️ "
                
                print(f"   {status} Step {step['step_number']}: {step['step_name']}")
                print(f"      Type: {step['step_type']} {config_status}")
                
                if step['issues']:
                    for issue in step['issues']:
                        print(f"      ⚠️  {issue}")
        
        # Recommendations
        print(f"\n💡 Recommendations:")
        if validation_result['compatible']:
            print("   • Recipe is ready to run!")
            print("   • Consider testing with sample data first")
        else:
            print("   • Install missing processors or replace with available alternatives")
            print("   • Check processor documentation for required configuration")
            print("   • Use --list to see available processors")
    
    def list_available_processors(self):
        """List all available processors with brief descriptions."""
        
        print("🔧 AVAILABLE PROCESSORS")
        print("=" * 60)
        
        for i, processor_type in enumerate(sorted(self.available_processors), 1):
            try:
                # Get basic info
                if processor_type == 'add_calculated_column':
                    config = {'processor_type': processor_type, 'new_column': 'test', 'calculation': {}}
                elif processor_type == 'clean_data':
                    config = {'processor_type': processor_type, 'rules': []}
                elif processor_type == 'filter_data':
                    config = {'processor_type': processor_type, 'filters': []}
                elif processor_type == 'group_data':
                    config = {'processor_type': processor_type, 'source_column': 'test', 'groups': {}}
                elif processor_type == 'lookup_data':
                    config = {'processor_type': processor_type, 'lookup_source': {}, 'lookup_key': 'test', 'source_key': 'test', 'lookup_columns': ['test']}
                elif processor_type == 'rename_columns':
                    config = {'processor_type': processor_type, 'mapping': {'old_col': 'new_col'}}
                elif processor_type == 'sort_data':
                    config = {'processor_type': processor_type, 'columns': ['test']}
                else:
                    config = {'processor_type': processor_type}
                
                processor = registry.create_processor(config)
                
                if hasattr(processor, 'get_capabilities'):
                    capabilities = processor.get_capabilities()
                    description = capabilities.get('description', 'No description available')
                else:
                    description = f'{processor_type} processor'
                
                print(f"{i:2d}. {processor_type:<20} - {description}")
                
            except Exception as e:
                print(f"{i:2d}. {processor_type:<20} - Error: {e}")


def main():
    """Main CLI interface for recipe validation."""
    
    import sys
    
    validator = RecipeValidator()
    
    if len(sys.argv) < 2:
        print("Recipe Validation Tool")
        print("=" * 40)
        print("Usage:")
        print("  python recipe_validator.py <recipe_file>     # Validate recipe")
        print("  python recipe_validator.py --list           # List processors")
        print("  python recipe_validator.py --help           # Show help")
        return
    
    if sys.argv[1] == '--list':
        validator.list_available_processors()
    elif sys.argv[1] == '--help':
        print("Recipe Validation Tool Help")
        print("=" * 40)
        print("This tool validates Excel Recipe Processor recipe files.")
        print("\nFeatures:")
        print("  • Checks recipe syntax and structure")
        print("  • Validates processor availability")
        print("  • Tests step configuration")
        print("  • Provides detailed error reporting")
        print("\nExample recipes should have this structure:")
        print("""
recipe:
  - step_description: "Filter products"
    processor_type: "filter_data"
    filters:
      - column: "Product_Name"
        condition: "contains"
        value: "CANNED"
        
  - step_description: "Add categories"
    processor_type: "lookup_data"
    lookup_source: "products.xlsx"
    lookup_key: "Code"
    source_key: "Product_Code"
    lookup_columns: ["Category"]
        """)
    else:
        recipe_file = sys.argv[1]
        result = validator.validate_recipe_file(recipe_file)
        validator.print_validation_report(result)


if __name__ == '__main__':
    main()
