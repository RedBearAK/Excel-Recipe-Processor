import sys
import logging

from pathlib import Path
from argparse import Namespace

from excel_recipe_processor.core.pipeline import (
    ExcelPipeline,
    PipelineError,
    get_system_capabilities
)
from excel_recipe_processor.config.recipe_loader import RecipeLoader


# Set up logging
logger = logging.getLogger(__name__)


def run_main(args: Namespace) -> int:
    """
    Main entry point for the package functionality.
    
    Args:
        args: Parsed command line arguments
        
    Returns:
        Exit code (0 for success, non-zero for error)
    """
    try:
        # Handle special commands first (before setting up logging)
        if hasattr(args, 'list_capabilities') and args.list_capabilities:
            # Check for output format flags
            detailed = getattr(args, 'detailed', False)
            json_output = getattr(args, 'json', False)
            yaml_output = getattr(args, 'yaml', False)
            detailed_yaml = getattr(args, 'detailed_yaml', False)
            matrix = getattr(args, 'matrix', False)
            
            if json_output:
                return list_system_capabilities_json()
            elif yaml_output:
                return list_system_capabilities_yaml()
            elif detailed_yaml:
                return list_system_capabilities_detailed_yaml()
            elif detailed:
                return list_system_capabilities_detailed()
            elif matrix:
                return list_system_capabilities_matrix()
            else:
                return list_system_capabilities()  # Basic format
        
        if hasattr(args, 'validate_recipe') and args.validate_recipe:
            return validate_recipe_file(args.validate_recipe)
        
        # Set up logging only for actual processing
        if args.verbose:
            logging.basicConfig(
                level=logging.DEBUG,
                format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
        else:
            logging.basicConfig(
                level=logging.INFO,
                format='%(levelname)s: %(message)s'
            )
            
        logger.info(f"Starting Excel Recipe Processor v{get_version()}")
        
        # Main processing requires input file and recipe
        if not args.input_file:
            print("Error: Input file is required for processing")
            print("Use --help for usage information")
            return 1
            
        if not args.config:
            print("Error: Recipe file is required (use --config)")
            print("Use --help for usage information")
            return 1
        
        # Process the files
        return process_excel_file(
            input_file=args.input_file,
            recipe_file=args.config,
            output_file=args.output,
            input_sheet=getattr(args, 'sheet', 0),
            output_sheet=getattr(args, 'output_sheet', 'ProcessedData'),
            verbose=args.verbose
        )
        
    except Exception as e:
        # For unexpected errors, always show them clearly
        print(f"Error: {e}")
        if hasattr(args, 'verbose') and args.verbose:
            import traceback
            traceback.print_exc()
        return 1


def process_excel_file(input_file: str, recipe_file: str, output_file = None,
                        input_sheet = 0, output_sheet: str = 'ProcessedData', 
                        verbose: bool = False) -> int:
    """
    Process an Excel file using a recipe.
    
    Args:
        input_file: Path to input Excel file
        recipe_file: Path to YAML recipe file
        output_file: Path to output Excel file (optional)
        input_sheet: Sheet name or index to process
        output_sheet: Name for the output sheet
        verbose: Enable verbose logging
        
    Returns:
        Exit code (0 for success, 1 for error)
    """
    try:
        # Validate input files exist
        input_path = Path(input_file)
        recipe_path = Path(recipe_file)
        
        if not input_path.exists():
            print(f"Error: Input file not found: {input_file}")
            return 1
            
        if not recipe_path.exists():
            print(f"Error: Recipe file not found: {recipe_file}")
            return 1
        
        # Create and run pipeline
        pipeline = ExcelPipeline()
        
        # Run the complete pipeline
        result = pipeline.run_complete_pipeline(
            recipe_path=recipe_file,
            input_path=input_file,
            output_path=output_file,
            input_sheet=input_sheet,
            output_sheet=output_sheet
        )
        
        logger.info(f"Processing completed successfully")
        if output_file:
            logger.info(f"Results saved to: {output_file}")
        
        return 0
        
    except PipelineError as e:
        print(f"Pipeline error: {e}")
        return 1
    except Exception as e:
        print(f"Error processing file: {e}")
        if verbose:
            import traceback
            traceback.print_exc()
        return 1


def validate_recipe_file(recipe_file: str) -> int:
    """
    Validate a recipe file.
    
    Args:
        recipe_file: Path to YAML recipe file
        
    Returns:
        Exit code (0 for valid, 1 for invalid)
    """
    try:
        print(f"Validating recipe: {recipe_file}")
        print("=" * 50)
        
        recipe_path = Path(recipe_file)
        if not recipe_path.exists():
            print(f"Error: Recipe file not found: {recipe_file}")
            return 1
        
        # Try to load and validate the recipe
        loader = RecipeLoader()
        recipe_data = loader.load_file(recipe_file)
        
        print("✅ Recipe syntax is valid")
        
        # Get steps and check processor availability
        steps = loader.get_steps()
        print(f"📋 Recipe contains {len(steps)} steps")
        
        # Import registry to check processor availability
        from excel_recipe_processor.processors.base_processor import registry
        available_types = registry.get_registered_types()
        
        valid_steps = 0
        for i, step in enumerate(steps, 1):
            step_type = step.get('processor_type', 'unknown')
            step_name = step.get('step_description', f'Step {i}')
            
            if step_type in available_types:
                print(f"   ✅ Step {i}: {step_name} ({step_type})")
                valid_steps += 1
            else:
                print(f"   ❌ Step {i}: {step_name} ({step_type}) - PROCESSOR NOT AVAILABLE")
        
        print(f"\n📊 Validation Results:")
        print(f"   Valid Steps: {valid_steps}/{len(steps)}")
        print(f"   Compatibility: {valid_steps/len(steps)*100:.1f}%")
        
        if valid_steps == len(steps):
            print(f"   🎉 Recipe is fully supported and ready to run!")
            return 0
        else:
            print(f"   ⚠️  Recipe has unsupported processors")
            return 1
            
    except Exception as e:
        print(f"❌ Recipe validation failed: {e}")
        return 1
            
    except Exception as e:
        print(f"Error validating recipe: {e}")
        return 1


def get_version() -> str:
    """Get the package version."""
    try:
        from excel_recipe_processor._version import __version__
        return __version__
    except ImportError:
        return "unknown"


def list_system_capabilities() -> int:
    """
    List basic system capabilities.
    
    Returns:
        Exit code (always 0)
    """
    try:
        print("Excel Recipe Processor - System Capabilities")
        print("=" * 50)
        
        capabilities = get_system_capabilities()
        system_info = capabilities['system_info']
        
        print(f"System: {system_info['description']}")
        print(f"Total Processors: {system_info['total_processors']}")
        print()
        
        print("Available Processors:")
        print("-" * 20)
        
        for proc_type in sorted(system_info['processor_types']):
            proc_info = capabilities['processors'].get(proc_type, {})
            
            if 'error' in proc_info:
                print(f"  {proc_type}: ERROR - {proc_info['error']}")
            else:
                description = proc_info.get('description', 'No description available')
                print(f"  {proc_type}: {description}")
        
        print()
        print("Use --detailed for more information or --json/--yaml for machine-readable output")
        print("Use 'python -m excel_recipe_processor --help' for usage information")
        
        return 0
        
    except Exception as e:
        print(f"Error listing capabilities: {e}")
        return 1


def list_system_capabilities_json() -> int:
    """
    Output system capabilities as JSON.
    
    Returns:
        Exit code (always 0)
    """
    try:
        import json
        capabilities = get_system_capabilities()
        print(json.dumps(capabilities, indent=2))
        return 0
        
    except Exception as e:
        print(f"Error generating JSON capabilities: {e}")
        return 1


def list_system_capabilities_yaml() -> int:
    """
    Output system capabilities as YAML.
    
    Returns:
        Exit code (always 0)
    """
    try:
        import yaml
        capabilities = get_system_capabilities()
        print(yaml.dump(capabilities, default_flow_style=False, sort_keys=True, indent=2))
        return 0
        
    except ImportError:
        print("Error: PyYAML is required for YAML output. Install with: pip install PyYAML")
        return 1
    except Exception as e:
        print(f"Error generating YAML capabilities: {e}")
        return 1


def list_system_capabilities_detailed() -> int:
    """
    List detailed capabilities for each processor.
    
    Returns:
        Exit code (always 0)
    """
    try:
        print("=" * 80)
        print("🏭 EXCEL RECIPE PROCESSOR - DETAILED CAPABILITIES REPORT")
        print("=" * 80)
        
        capabilities = get_system_capabilities()
        
        # System Overview
        system = capabilities['system_info']
        print(f"\n📊 System Overview:")
        print(f"   Description: {system['description']}")
        print(f"   Total Processors: {system['total_processors']}")
        
        # Processor Summary
        print(f"\n🔧 Available Processors:")
        for i, (processor_type, info) in enumerate(capabilities['processors'].items(), 1):
            description = info.get('description', 'No description available')
            print(f"   {i:2d}. {processor_type:<20} - {description}")
        
        # Detailed capabilities for each processor
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
                    if isinstance(info[key], list):
                        count = len(info[key])
                    else:
                        count = 'N/A'
                    print(f"   🎯 {key.replace('_', ' ').title()}: {count} available")
            
            # Special capabilities
            if 'join_types' in info:
                if isinstance(info['join_types'], list):
                    print(f"   🔗 Join Types: {', '.join(info['join_types'])}")
            
            if 'data_sources' in info:
                if isinstance(info['data_sources'], list):
                    print(f"   📁 Data Sources: {', '.join(info['data_sources'])}")
            
            if 'aggregation_functions' in info:
                if isinstance(info['aggregation_functions'], list):
                    count = len(info['aggregation_functions'])
                else:
                    count = 'N/A'
                print(f"   📊 Aggregation Functions: {count} available")
            
            if 'case_conversions' in info:
                if isinstance(info['case_conversions'], list):
                    print(f"   🔤 Case Conversions: {', '.join(info['case_conversions'])}")
            
            # Helper methods
            if 'helper_methods' in info:
                if isinstance(info['helper_methods'], list):
                    count = len(info['helper_methods'])
                else:
                    count = 'N/A'
                print(f"   🔧 Helper Methods: {count} available")
            
            if 'special_methods' in info:
                if isinstance(info['special_methods'], list):
                    count = len(info['special_methods'])
                else:
                    count = 'N/A'
                print(f"   ⚡ Special Methods: {count} available")
            
            # Examples
            if 'examples' in info:
                print(f"   💡 Example Uses:")
                if isinstance(info['examples'], dict):
                    for example_type, example_desc in list(info['examples'].items())[:2]:
                        print(f"      • {example_desc}")
        
        print(f"\n💡 Use --json or --yaml for machine-readable output or --matrix for feature comparison")
        return 0
        
    except Exception as e:
        print(f"Error generating detailed capabilities: {e}")
        return 1


def list_system_capabilities_detailed_yaml() -> int:
    """
    Show detailed capabilities with YAML listings - hybrid format.
    
    Returns:
        Exit code (always 0)
    """
    try:
        import yaml
        import textwrap
        
        print("=" * 80)
        print("🏭 EXCEL RECIPE PROCESSOR - DETAILED CAPABILITIES WITH YAML")
        print("=" * 80)
        
        capabilities = get_system_capabilities()
        
        # System Overview (formatted)
        system = capabilities['system_info']
        print(f"\n📊 System Overview:")
        
        # Wrap the description if it's too long
        description = system['description']
        if len(description) > 60:
            wrapped_desc = textwrap.fill(description, width=76, initial_indent="   ", subsequent_indent="   ")
            print(f"   Description:")
            print(wrapped_desc)
        else:
            print(f"   Description: {description}")
        
        print(f"   Total Processors: {system['total_processors']}")
        
        # Wrap the available types list to 80 characters
        types_list = ', '.join(sorted(system['processor_types']))
        if len(types_list) > 60:
            wrapped_types = textwrap.fill(types_list, width=76, initial_indent="   ", subsequent_indent="   ")
            print(f"   Available Types:")
            print(wrapped_types)
        else:
            print(f"   Available Types: {types_list}")
        
        # Detailed capabilities for each processor with YAML structure
        print(f"\n🚀 DETAILED PROCESSOR CAPABILITIES:")
        print("=" * 80)
        
        for processor_type, info in capabilities['processors'].items():
            print(f"\n📋 {processor_type.upper().replace('_', ' ')} PROCESSOR")
            print("-" * 60)
            
            if 'error' in info:
                print(f"   ❌ Error: {info['error']}")
                continue
            
            # Basic info with text wrapping
            if 'description' in info:
                description = info['description']
                if len(description) > 60:
                    wrapped_desc = textwrap.fill(description, width=76, initial_indent="   ", subsequent_indent="   ")
                    print(f"   📝 Description:")
                    print(wrapped_desc)
                else:
                    print(f"   📝 Description: {description}")
            
            # Show key capabilities as summary counts
            capability_counts = {}
            feature_keys = [
                'supported_actions', 'calculation_types', 'supported_conditions',
                'lookup_features', 'pivot_features', 'rename_types', 'supported_options',
                'filter_operations', 'grouping_features', 'transformation_features',
                'aggregation_functions', 'helper_methods', 'special_methods'
            ]
            
            for key in feature_keys:
                if key in info and isinstance(info[key], list):
                    capability_counts[key] = len(info[key])
            
            if capability_counts:
                print(f"   📊 Capability Summary:")
                for key, count in capability_counts.items():
                    key_name = key.replace('_', ' ').title()
                    print(f"      {key_name}: {count}")
            
            # YAML listing of actual capabilities
            yaml_data = {}
            for key in feature_keys:
                if key in info and isinstance(info[key], list) and info[key]:
                    yaml_data[key] = info[key]
            
            # Add other important fields
            for key in ['join_types', 'data_sources', 'case_conversions', 'examples']:
                if key in info:
                    if isinstance(info[key], (list, dict)) and info[key]:
                        yaml_data[key] = info[key]
            
            if yaml_data:
                print()  # Add blank line before YAML section
                print(f"   🔧 Full Capabilities (YAML):")
                yaml_output = yaml.dump(yaml_data, default_flow_style=False, sort_keys=True, indent=2)
                # Indent the YAML output and ensure it fits within 80 characters
                for line in yaml_output.split('\n'):
                    if line.strip():
                        indented_line = f"      {line}"
                        # If line is too long, we'll let YAML handle it since it's structured data
                        print(indented_line)
            else:
                print(f"   ℹ️  No detailed capabilities available")
        
        # Wrap the final informational messages
        print(f"\n💡 This format combines structured overview with complete YAML capability")
        print(f"    listings for each processor.")
        print(f"📝 Use --yaml for pure YAML output or --detailed for formatted text only.")
        return 0
        
    except ImportError:
        print("Error: PyYAML is required for detailed YAML output. Install with:")
        print("       pip install PyYAML")
        return 1
    except Exception as e:
        print(f"Error generating detailed YAML capabilities: {e}")
        return 1


def list_system_capabilities_matrix() -> int:
    """
    Print a feature matrix showing what each processor can do.
    
    Returns:
        Exit code (always 0)
    """
    try:
        capabilities = get_system_capabilities()
        
        print("=" * 80)
        print("🏭 EXCEL RECIPE PROCESSOR - FEATURE MATRIX")
        print("=" * 80)
        
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
        
        if not sorted_features:
            print("No features found in capabilities data")
            return 0
        
        print(f"Legend: ✅ = Supported, ❌ = Not Supported\n")
        
        # Print header with better spacing
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
        col_width = max(15, max(len(name) for name in header_names) if header_names else 15)
        
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
        return 0
        
    except Exception as e:
        print(f"Error generating feature matrix: {e}")
        return 1
