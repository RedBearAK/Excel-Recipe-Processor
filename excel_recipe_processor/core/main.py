"""Main functionality for excel_recipe_processor package."""

import sys
import logging

from pathlib import Path
from argparse import Namespace

from excel_recipe_processor.core.pipeline import get_system_capabilities  # Keep for compatibility
from excel_recipe_processor.core.recipe_pipeline import RecipePipeline, RecipePipelineError
from excel_recipe_processor.config.recipe_loader import RecipeLoader
from excel_recipe_processor.core.interactive_variables import (
    InteractiveVariablePrompt,
    InteractiveVariableError,
    parse_cli_variables
)

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
        
        # Handle settings examples command
        if hasattr(args, 'get_settings_examples') and args.get_settings_examples:
            format_type = getattr(args, 'format_examples', 'yaml')
            return get_settings_examples(format_type)
        
        # Handle usage examples command
        if hasattr(args, 'get_usage_examples') and args.get_usage_examples:
            processor_name = args.get_usage_examples
            format_type = getattr(args, 'format_examples', 'yaml')
            return get_usage_examples(processor_name, format_type)
        
        # Handle recipe validation
        if hasattr(args, 'validate_recipe') and args.validate_recipe:
            return validate_recipe_file(args.validate_recipe)
        
        # Set up logging level
        if hasattr(args, 'verbose') and args.verbose:
            logging.basicConfig(level=logging.DEBUG, format='%(levelname)s: %(message)s')
        else:
            logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
        
        # Main processing workflow
        if hasattr(args, 'recipe_file') and args.recipe_file:
            return process_recipe(args)
        else:
            # No recipe specified - show help
            print("Error: Recipe file is required")
            print("Usage: python -m excel_recipe_processor recipe.yaml [--var name=value ...]")
            print("Use --help for full usage information")
            return 1
            
    except Exception as e:
        # For unexpected errors, always show them clearly
        print(f"Error: {e}")
        if hasattr(args, 'verbose') and args.verbose:
            import traceback
            traceback.print_exc()
        return 1


def process_recipe(args: Namespace) -> int:
    """
    Process a recipe using the new RecipePipeline system.
    
    Args:
        args: Parsed command line arguments
        
    Returns:
        Exit code (0 for success, non-zero for error)
    """
    recipe_file = args.recipe_file
    verbose = getattr(args, 'verbose', False)
    
    if verbose:
        logger.info(f"Processing recipe: {recipe_file}")
    
    try:
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
        
        # Create and initialize pipeline
        pipeline = RecipePipeline()
        
        # Load recipe first to check for required external variables
        recipe_data = pipeline.load_recipe(recipe_file)
        
        # Handle external variables
        external_variables = {}
        recipe_loader = RecipeLoader()
        required_external_vars = recipe_loader.get_required_external_vars()
        
        if required_external_vars:
            try:
                # Initialize variable substitution for default resolution
                from excel_recipe_processor.core.variable_substitution import VariableSubstitution
                var_sub = VariableSubstitution(recipe_path=recipe_file)
                
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
            external_variables = cli_variables  # Use them anyway for flexibility
        
        # Add external variables to pipeline
        for name, value in external_variables.items():
            pipeline.add_external_variable(name, value)
        
        # Execute the complete recipe
        completion_report = pipeline.execute_recipe()
        
        # Report completion
        steps_executed = completion_report.get('steps_executed', 0)
        stages_created = len(completion_report.get('stages_created', []))
        
        print(f"✓ Recipe completed successfully")
        print(f"  Steps executed: {steps_executed}")
        print(f"  Data stages created: {stages_created}")
        
        if verbose:
            stages = completion_report.get('stages_created', [])
            if stages:
                print("  Stages created:")
                for stage_name in stages:
                    print(f"    - {stage_name}")
        
        return 0
        
    except RecipePipelineError as e:
        print(f"Recipe processing failed: {e}")
        return 1
    except FileNotFoundError:
        print(f"Recipe file not found: {recipe_file}")
        return 1
    except Exception as e:
        print(f"Unexpected error: {e}")
        if verbose:
            import traceback
            traceback.print_exc()
        return 1


# Keep existing special command functions for compatibility
def list_system_capabilities() -> int:
    """List available processors in basic format."""
    try:
        capabilities = get_system_capabilities()
        
        print("Available Excel Recipe Processors")
        print("=" * 40)
        
        for processor_name, info in capabilities.items():
            description = info.get('description', 'No description available')
            print(f"{processor_name:20} {description}")
        
        print(f"\nTotal: {len(capabilities)} processors available")
        return 0
        
    except Exception as e:
        print(f"Error listing capabilities: {e}")
        return 1


def list_system_capabilities_detailed() -> int:
    """List capabilities with detailed information."""
    try:
        capabilities = get_system_capabilities()
        
        print("Detailed Excel Recipe Processor Capabilities")
        print("=" * 50)
        
        for processor_name, info in capabilities.items():
            print(f"\n{processor_name}")
            print("-" * len(processor_name))
            print(f"Description: {info.get('description', 'No description available')}")
            
            # Show parameters if available
            if 'parameters' in info:
                print("Parameters:")
                for param_name, param_info in info['parameters'].items():
                    required = "Required" if param_info.get('required', False) else "Optional"
                    print(f"  {param_name} ({required}): {param_info.get('description', 'No description')}")
        
        print(f"\nTotal: {len(capabilities)} processors available")
        return 0
        
    except Exception as e:
        print(f"Error listing detailed capabilities: {e}")
        return 1


def list_system_capabilities_json() -> int:
    """List capabilities in JSON format."""
    try:
        import json
        capabilities = get_system_capabilities()
        print(json.dumps(capabilities, indent=2))
        return 0
    except Exception as e:
        print(f"Error generating JSON capabilities: {e}")
        return 1


def list_system_capabilities_yaml() -> int:
    """List capabilities in YAML format."""
    try:
        import yaml
        capabilities = get_system_capabilities()
        print(yaml.dump(capabilities, default_flow_style=False))
        return 0
    except Exception as e:
        print(f"Error generating YAML capabilities: {e}")
        return 1


def list_system_capabilities_detailed_yaml() -> int:
    """List capabilities with detailed YAML format."""
    try:
        import yaml
        capabilities = get_system_capabilities()
        
        print("# Excel Recipe Processor - Detailed Capabilities")
        print("# Generated automatically")
        print()
        print(yaml.dump(capabilities, default_flow_style=False))
        return 0
    except Exception as e:
        print(f"Error generating detailed YAML capabilities: {e}")
        return 1


def list_system_capabilities_matrix() -> int:
    """Show capabilities in matrix format."""
    try:
        capabilities = get_system_capabilities()
        
        print("Excel Recipe Processor - Feature Matrix")
        print("=" * 50)
        
        # This would need more sophisticated matrix logic
        # For now, show a simple categorized view
        categories = {}
        for processor_name, info in capabilities.items():
            category = info.get('category', 'Data Processing')
            if category not in categories:
                categories[category] = []
            categories[category].append(processor_name)
        
        for category, processors in categories.items():
            print(f"\n{category}:")
            for processor in processors:
                print(f"  ✓ {processor}")
        
        return 0
    except Exception as e:
        print(f"Error generating capability matrix: {e}")
        return 1


def validate_recipe_file(recipe_path: str) -> int:
    """Validate a recipe file."""
    try:
        from excel_recipe_processor.config.recipe_loader import RecipeLoader, RecipeValidationError
        from excel_recipe_processor.core.stage_manager import StageManager
        
        # Load and validate recipe
        loader = RecipeLoader()
        recipe_data = loader.load_recipe(recipe_path)
        
        # Validate stages
        StageManager.declare_recipe_stages(recipe_data)
        stage_errors = StageManager.validate_recipe_stages(recipe_data)
        
        if stage_errors:
            print(f"Recipe validation failed for: {recipe_path}")
            for error in stage_errors:
                print(f"  ❌ {error}")
            return 1
        else:
            print(f"✓ Recipe validation successful: {recipe_path}")
            
            # Show summary
            recipe_steps = recipe_data.get('recipe', [])
            settings = recipe_data.get('settings', {})
            external_vars = settings.get('required_external_vars', {})
            custom_vars = settings.get('variables', {})
            
            print(f"  Steps: {len(recipe_steps)}")
            print(f"  External variables: {len(external_vars)}")
            print(f"  Custom variables: {len(custom_vars)}")
            
            return 0
            
    except RecipeValidationError as e:
        print(f"Recipe validation error: {e}")
        return 1
    except FileNotFoundError:
        print(f"Recipe file not found: {recipe_path}")
        return 1
    except Exception as e:
        print(f"Error validating recipe: {e}")
        return 1


def get_settings_examples(format_type: str = 'yaml') -> int:
    """Show recipe settings examples."""
    # This would be implemented based on your existing settings examples
    print("Recipe Settings Usage Examples")
    print("=" * 40)
    print("# Example settings configurations")
    print()
    
    example_settings = """
settings:
  # External variables for dynamic file names
  required_external_vars:
    batch_id:
      description: "Batch identifier (e.g., A47, B23)"
      validation: "^[A-Z]\\d+$"
      example: "A47"
    
    region:
      description: "Processing region"
      choices: ["west", "east", "central"]
      default_value: "west"
  
  # Custom variables for reuse
  variables:
    output_prefix: "processed"
    version: "v1.0"
    
  # Optional global settings
  create_backup: true
  encoding: "utf-8"
"""
    
    print(example_settings)
    return 0


def get_usage_examples(processor_name: str = None, format_type: str = 'yaml') -> int:
    """Show usage examples for processors."""
    # This would integrate with your existing usage examples system
    if processor_name:
        print(f"Usage Examples for: {processor_name}")
    else:
        print("Usage Examples - All Processors")
    
    print("=" * 40)
    print("# Example processor configurations")
    print()
    
    # This would be implemented to show actual examples
    example_usage = """
# Example import_file step
- step_description: "Import daily sales data"
  processor_type: "import_file"
  input_file: "data/sales_{batch_id}_{date}.xlsx"
  sheet: 0

# Example export_file step  
- step_description: "Export processed results"
  processor_type: "export_file"
  output_file: "output/results_{batch_id}_{date}.xlsx"
  create_backup: true
"""
    
    print(example_usage)
    return 0
