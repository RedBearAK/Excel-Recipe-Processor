"""Main functionality for excel_recipe_processor package."""

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
from excel_recipe_processor.core.interactive_variables import (
    InteractiveVariablePrompt,
    InteractiveVariableError,
    parse_cli_variables
)

# Set up logging
logger = logging.getLogger(__name__)

# Global variable for CLI variables (temporary storage)
_current_cli_variables = {}


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
        
        # Parse CLI variable overrides
        cli_variables = {}
        if hasattr(args, 'variable_overrides') and args.variable_overrides:
            try:
                cli_variables = parse_cli_variables(args.variable_overrides)
                if cli_variables:
                    logger.info(f"Parsed {len(cli_variables)} variable overrides from CLI")
            except InteractiveVariableError as e:
                print(f"Error parsing variable overrides: {e}")
                return 1
        
        # Process the files with CLI variables passed via a wrapper
        _current_cli_variables = cli_variables  # Store globally for process_excel_file
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
    Process an Excel file using a recipe with interactive variable support.
    
    Args:
        input_file: Path to input Excel file
        recipe_file: Path to YAML recipe file
        output_file: Path for output file (optional, can be specified in recipe)
        input_sheet: Sheet to read from input file
        output_sheet: Sheet name for output
        verbose: Whether to show detailed error information
        
    Returns:
        Exit code (0 for success, non-zero for error)
    """
    cli_variables = _current_cli_variables or {}
    
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
        
        logger.info(f"Processing: {input_file}")
        logger.info(f"Using recipe: {recipe_file}")
        
        # Initialize pipeline
        pipeline = ExcelPipeline()
        
        # Load recipe to check for required external variables
        pipeline.load_recipe(recipe_path)
        required_external_vars = pipeline.recipe_loader.get_required_external_vars()
        
        # Collect external variables if needed
        external_variables = {}
        if required_external_vars:
            logger.info(f"Recipe requires {len(required_external_vars)} external variables")
            
            try:
                # Create variable substitution for resolving defaults
                from excel_recipe_processor.core.variable_substitution import VariableSubstitution
                var_sub = VariableSubstitution(input_path, recipe_path)
                
                # Collect variables interactively
                prompt = InteractiveVariablePrompt(var_sub)
                external_variables = prompt.collect_variables(required_external_vars, cli_variables)
                
                logger.info(f"Collected {len(external_variables)} external variables")
                
            except InteractiveVariableError as e:
                print(f"Error collecting variables: {e}")
                return 1
        elif cli_variables:
            # No external variables required, but CLI variables provided
            logger.warning("CLI variables provided but recipe doesn't require external variables")
            print("Warning: Recipe doesn't require external variables but --var arguments were provided")
        
        # Add external variables to pipeline
        for name, value in external_variables.items():
            pipeline.add_custom_variable(name, value)
        
        # Determine output file
        if output_file:
            final_output = output_file
            logger.info(f"Output will be saved to: {output_file}")
        else:
            # Let the pipeline determine output from recipe settings
            final_output = None
            logger.info("Output filename will be determined from recipe settings")
        
        # Run the complete pipeline
        if final_output:
            result = pipeline.run_complete_pipeline(
                recipe_path=recipe_path,
                input_path=input_path,
                output_path=final_output,
                input_sheet=input_sheet,
                output_sheet=output_sheet
            )
        else:
            # Load recipe first to get output filename
            settings = pipeline.recipe_loader.get_settings()
            
            if 'output_filename' not in settings:
                print("Error: No output filename specified in recipe or command line")
                return 1
            
            final_output = settings['output_filename']
            
            result = pipeline.run_complete_pipeline(
                recipe_path=recipe_path,
                input_path=input_path,
                output_path=final_output,
                input_sheet=input_sheet,
                output_sheet=output_sheet
            )
        
        # Report results
        if result is not None:
            print(f"✓ Processing completed successfully")
            print(f"  - Processed {len(result)} rows with {len(result.columns)} columns")
            
            # Show final output path (with variable substitution applied)
            final_output_resolved = pipeline.substitute_template(str(final_output))
            print(f"  - Output saved to: {final_output_resolved}")
            
            return 0
        else:
            print("✗ Processing failed: No result data")
            return 1
            
    except PipelineError as e:
        print(f"Pipeline error: {e}")
        return 1
    except FileNotFoundError as e:
        print(f"File error: {e}")
        return 1
    except Exception as e:
        print(f"Unexpected error: {e}")
        if verbose:
            import traceback
            traceback.print_exc()
        return 1


def get_version() -> str:
    """Get the package version."""
    try:
        from excel_recipe_processor._version import __version__
        return __version__
    except ImportError:
        return "unknown"


def list_system_capabilities() -> int:
    """List basic system capabilities."""
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


def list_system_capabilities_detailed() -> int:
    """List detailed capabilities for each processor."""
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


def list_system_capabilities_json() -> int:
    """List system capabilities in JSON format."""
    try:
        import json
        capabilities = get_system_capabilities()
        print(json.dumps(capabilities, indent=2))
        return 0
        
    except Exception as e:
        print(f"Error listing JSON capabilities: {e}")
        return 1


def list_system_capabilities_yaml() -> int:
    """Output system capabilities as YAML."""
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


def list_system_capabilities_detailed_yaml() -> int:
    """Show detailed capabilities with YAML listings - hybrid format."""
    
    def print_wrapped_description(description: str) -> None:
        """Print processor description with proper wrapping."""
        if len(description) > 60:
            wrapped_desc = textwrap.fill(description, width=76, 
                                        initial_indent="   📝 Description: ", 
                                        subsequent_indent="                    ")
            print(wrapped_desc)
        else:
            print(f"   📝 Description: {description}")
    
    def print_wrapped_yaml_line(line: str) -> None:
        """Print a YAML line with intelligent wrapping."""
        indented_line = f"      {line}"
        
        if len(indented_line) <= 78:
            print(indented_line)
            return
        
        # Handle long key: value pairs
        if ': ' in line and not line.strip().startswith('-'):
            parts = line.split(': ', 1)
            if len(parts) == 2:
                key_part = f"      {parts[0]}: "
                value_part = parts[1]
                
                if len(key_part) + len(value_part) > 78:
                    print(key_part)
                    wrapped_value = textwrap.fill(value_part, width=72, 
                                                initial_indent="        ", 
                                                subsequent_indent="        ")
                    print(wrapped_value)
                else:
                    print(indented_line)
            else:
                print(indented_line)
        else:
            # For list items and other content, preserve YAML structure
            print(indented_line)
    
    def print_processor_info(processor_type: str, info: dict) -> None:
        """Print detailed information for a single processor."""
        print(f"\n📋 {processor_type.upper().replace('_', ' ')} PROCESSOR")
        print("-" * 60)
        
        if 'error' in info:
            print(f"   ❌ Error: {info['error']}")
            return
        
        # Description with text wrapping
        if 'description' in info:
            print_wrapped_description(info['description'])
        
        # Feature summary
        print_feature_summary(info)
        
        # YAML capabilities
        print_yaml_capabilities(info)
    
    def print_feature_summary(info: dict) -> None:
        """Print feature summary counts."""
        feature_keys = [
            'supported_actions', 'calculation_types', 'supported_conditions',
            'lookup_features', 'pivot_features', 'rename_types', 'supported_options',
            'filter_operations', 'grouping_features', 'transformation_features'
        ]
        
        for key in feature_keys:
            if key in info:
                count = len(info[key]) if isinstance(info[key], list) else 'N/A'
                key_name = key.replace('_', ' ').title()
                print(f"      {key_name}: {count}")
    
    def print_yaml_capabilities(info: dict) -> None:
        """Print YAML capabilities section."""
        feature_keys = [
            'supported_actions', 'calculation_types', 'supported_conditions',
            'lookup_features', 'pivot_features', 'rename_types', 'supported_options',
            'filter_operations', 'grouping_features', 'transformation_features'
        ]
        
        yaml_data = {}
        
        # Collect feature data
        for key in feature_keys:
            if key in info and isinstance(info[key], list) and info[key]:
                yaml_data[key] = info[key]
        
        # Add other important fields
        for key in ['join_types', 'data_sources', 'case_conversions', 'examples']:
            if key in info and isinstance(info[key], (list, dict)) and info[key]:
                yaml_data[key] = info[key]
        
        if not yaml_data:
            print(f"   ℹ️  No detailed capabilities available")
            return
        
        print()  # Add blank line before YAML section
        print(f"   🔧 Full Capabilities (YAML):")
        
        yaml_output = yaml.dump(yaml_data, default_flow_style=False, sort_keys=True, indent=2)
        for line in yaml_output.split('\n'):
            if line.strip():
                print_wrapped_yaml_line(line)
    
    def print_system_overview(system: dict) -> None:
        """Print system overview with wrapping."""
        print(f"\n📊 System Overview:")
        
        description = system['description']
        if len(description) > 60:
            wrapped_desc = textwrap.fill(description, width=76, 
                                        initial_indent="   ", 
                                        subsequent_indent="   ")
            print(f"   Description:")
            print(wrapped_desc)
        else:
            print(f"   Description: {description}")
        
        print(f"   Total Processors: {system['total_processors']}")
        
        # Wrap processor types list
        types_list = ', '.join(sorted(system['processor_types']))
        if len(types_list) > 60:
            wrapped_types = textwrap.fill(types_list, width=76, 
                                        initial_indent="   ", 
                                        subsequent_indent="   ")
            print(f"   Available Types:")
            print(wrapped_types)
        else:
            print(f"   Available Types: {types_list}")
    
    # Main function body
    try:
        import yaml
        import textwrap
        
        print("=" * 80)
        print("🏭 EXCEL RECIPE PROCESSOR - DETAILED CAPABILITIES WITH YAML")
        print("=" * 80)
        
        capabilities = get_system_capabilities()
        
        # System overview
        print_system_overview(capabilities['system_info'])
        
        # Processor details
        print(f"\n🚀 DETAILED PROCESSOR CAPABILITIES:")
        print("=" * 80)
        
        for processor_type, info in capabilities['processors'].items():
            print_processor_info(processor_type, info)
        
        # Footer
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
    """Print a feature matrix showing what each processor can do."""
    try:
        print("Excel Recipe Processor - Feature Matrix")
        print("=" * 50)
        
        capabilities = get_system_capabilities()
        processors = capabilities.get('processors', {})
        
        # Get all unique capabilities
        all_capabilities = set()
        for info in processors.values():
            # Collect various capability types
            for key in ['supported_actions', 'calculation_types', 'supported_conditions',
                       'lookup_features', 'pivot_features', 'filter_operations']:
                if key in info and isinstance(info[key], list):
                    all_capabilities.update(info[key])
        
        all_capabilities = sorted(list(all_capabilities))
        
        if not all_capabilities:
            print("No detailed capabilities available for matrix display")
            return 0
        
        # Print header
        print(f"{'Processor':<20} " + " ".join(f"{cap[:8]:<8}" for cap in all_capabilities))
        print("-" * (20 + len(all_capabilities) * 9))
        
        # Print matrix
        for proc_type, info in processors.items():
            if 'error' in info:
                continue
                
            proc_caps = set()
            # Collect capabilities from all sources
            for key in ['supported_actions', 'calculation_types', 'supported_conditions',
                       'lookup_features', 'pivot_features', 'filter_operations']:
                if key in info and isinstance(info[key], list):
                    proc_caps.update(info[key])
            
            row = f"{proc_type[:19]:<20} "
            
            for cap in all_capabilities:
                marker = "✓" if cap in proc_caps else "·"
                row += f"{marker:<8} "
            
            print(row)
        
        return 0
        
    except Exception as e:
        print(f"Error listing matrix capabilities: {e}")
        return 1


def validate_recipe_file(recipe_file: str) -> int:
    """Validate a recipe file."""
    try:
        loader = RecipeLoader()
        loader.load_file(recipe_file)
        
        print(f"✓ Recipe validation successful: {recipe_file}")
        print(f"  {loader.summary()}")
        
        # Check for external variables
        external_vars = loader.get_required_external_vars()
        if external_vars:
            print(f"  External variables required: {', '.join(external_vars.keys())}")
        
        return 0
        
    except Exception as e:
        print(f"✗ Recipe validation failed: {e}")
        return 1
