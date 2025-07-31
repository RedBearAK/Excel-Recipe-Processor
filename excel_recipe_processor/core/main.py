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


# Replace the existing list_system_capabilities_* functions in main.py with these corrected versions

def list_system_capabilities() -> int:
    """List available processors in basic format."""
    try:
        capabilities = get_system_capabilities()
        
        print("Available Excel Recipe Processors")
        print("=" * 40)
        
        system_info = capabilities.get('system_info', {})
        processors = capabilities.get('processors', {})
        
        print(f"System: {system_info.get('description', 'Excel Recipe Processor')}")
        print(f"Total Processors: {system_info.get('total_processors', len(processors))}")
        print()
        
        for processor_name, info in sorted(processors.items()):
            if 'error' in info:
                print(f"{processor_name:<25} ❌ {info['error']}")
            else:
                description = info.get('description', 'No description available')
                print(f"{processor_name:<25} {description}")
        
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
        
        system_info = capabilities.get('system_info', {})
        processors = capabilities.get('processors', {})
        
        print(f"System: {system_info.get('description', 'Excel Recipe Processor')}")
        print(f"Total Processors: {system_info.get('total_processors', len(processors))}")
        print()
        
        for processor_name, info in sorted(processors.items()):
            print(f"\n{processor_name}")
            print("-" * len(processor_name))
            
            if 'error' in info:
                print(f"❌ Error: {info['error']}")
                continue
                
            print(f"Description: {info.get('description', 'No description available')}")
            
            # Show parameters if available
            if 'parameters' in info:
                print("Parameters:")
                for param_name, param_info in info['parameters'].items():
                    required = "Required" if param_info.get('required', False) else "Optional"
                    param_desc = param_info.get('description', 'No description')
                    print(f"  {param_name} ({required}): {param_desc}")
            
            # Show capabilities if available
            if 'supported_actions' in info:
                print(f"Supported Actions: {', '.join(info['supported_actions'])}")
            
            if 'calculation_types' in info:
                print(f"Calculation Types: {', '.join(info['calculation_types'])}")
            
            if 'supported_conditions' in info:
                print(f"Filter Conditions: {', '.join(info['supported_conditions'])}")
            
            if 'join_types' in info:
                print(f"Join Types: {', '.join(info['join_types'])}")
            
            if 'aggregation_functions' in info:
                print(f"Aggregation Functions: {', '.join(info['aggregation_functions'])}")
            
            # Show feature counts
            feature_counts = []
            for key, value in info.items():
                if isinstance(value, list) and key not in ['parameters']:
                    feature_counts.append(f"{key}: {len(value)}")
            
            if feature_counts:
                print(f"Features: {', '.join(feature_counts)}")
        
        print(f"\nTotal: {len(processors)} processors available")
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
        
        system_info = capabilities.get('system_info', {})
        processors = capabilities.get('processors', {})
        
        print(f"System: {system_info.get('description', 'Excel Recipe Processor')}")
        print(f"Total Processors: {len(processors)}")
        print()
        
        # Collect all feature types across processors
        all_features = set()
        processor_features = {}
        
        for processor_name, info in processors.items():
            if 'error' not in info:
                features = set()
                for key, value in info.items():
                    if isinstance(value, list) and key not in ['parameters']:
                        features.update([f"{key}:{item}" for item in value])
                        all_features.update([f"{key}:{item}" for item in value])
                    elif key in ['join_types', 'calculation_types', 'supported_conditions', 'aggregation_functions']:
                        if isinstance(value, list):
                            features.update([f"{key}:{item}" for item in value])
                            all_features.update([f"{key}:{item}" for item in value])
                
                processor_features[processor_name] = features
        
        # Group features by category
        feature_categories = {}
        for feature in sorted(all_features):
            if ':' in feature:
                category, item = feature.split(':', 1)
                if category not in feature_categories:
                    feature_categories[category] = []
                feature_categories[category].append(item)
        
        # Display matrix by category
        for category, items in feature_categories.items():
            print(f"\n{category.replace('_', ' ').title()}:")
            print("-" * 30)
            
            for item in sorted(items):
                supporting_processors = []
                for proc_name, proc_features in processor_features.items():
                    if f"{category}:{item}" in proc_features:
                        supporting_processors.append(proc_name)
                
                if supporting_processors:
                    print(f"  {item:<20} → {', '.join(supporting_processors)}")
        
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


# Add these missing CLI functions to excel_recipe_processor/core/main.py

def get_settings_examples(format_type: str = 'yaml') -> int:
    """
    Get and display recipe settings configuration examples.
    
    Args:
        format_type: Output format ('yaml', 'text', 'json')
        
    Returns:
        Exit code (0 for success, non-zero for error)
    """
    try:
        # Import the settings examples loading function
        from excel_recipe_processor.utils.processor_examples_loader import load_settings_examples
        
        # Try to load settings examples from YAML file
        settings_examples = load_settings_examples()
        
        if 'error' in settings_examples:
            # No YAML file found, use built-in examples
            return _display_builtin_settings_examples(format_type)
        
        # Successfully loaded YAML file
        return _display_yaml_settings_examples(settings_examples, format_type)
        
    except Exception as e:
        print(f"Error getting settings examples: {e}")
        return 1


def get_usage_examples(processor_name: str, format_type: str = 'yaml') -> int:
    """
    Get and display usage examples for a specific processor or all processors.
    
    Args:
        processor_name: Name of the processor to get examples for, or 'all' for all processors
        format_type: Output format ('yaml', 'text', 'json')
        
    Returns:
        Exit code (0 for success, non-zero for error)
    """
    try:
        # Special case: handle 'settings' as processor name
        if processor_name == 'settings':
            return get_settings_examples(format_type)
        
        # Import the YAML loading function and pipeline functions
        from excel_recipe_processor.utils.processor_examples_loader import load_processor_examples
        from excel_recipe_processor.core.pipeline import get_processor_usage_examples, get_all_usage_examples
        
        # Handle 'all' processors case
        if processor_name == 'all':
            return _get_all_usage_examples(format_type)
        
        # First try to load from YAML file
        examples_data = load_processor_examples(processor_name)
        
        if 'error' not in examples_data:
            # Successfully loaded YAML file
            return _display_yaml_examples(processor_name, examples_data, format_type)
        
        # Fallback to processor method
        method_examples = get_processor_usage_examples(processor_name)
        
        if method_examples and 'error' not in method_examples:
            # Successfully got examples from processor method
            return _display_method_examples(processor_name, method_examples, format_type)
        
        # No examples available
        print(f"No usage examples available for processor: {processor_name}")
        print()
        print("Available processors:")
        
        # Show available processors
        capabilities = get_system_capabilities()
        processor_names = [name for name in capabilities['processors'].keys() if name != 'base_processor']
        
        for name in sorted(processor_names):
            print(f"  - {name}")
        
        return 1
        
    except Exception as e:
        print(f"Error getting usage examples: {e}")
        return 1


def _display_yaml_settings_examples(settings_examples: dict, format_type: str) -> int:
    """Display settings examples loaded from YAML file."""
    try:
        print("Recipe Settings Usage Examples")
        print("=" * 40)
        
        if format_type == 'json':
            import json
            print(json.dumps(settings_examples, indent=2))
            return 0
        
        if format_type == 'text':
            print(f"Description: {settings_examples.get('description', 'No description available')}")
            print()
            
            # List examples
            example_keys = [key for key in settings_examples.keys() if key.endswith('_example')]
            for example_key in example_keys:
                example = settings_examples[example_key]
                print(f"Example: {example_key}")
                print(f"  Description: {example.get('description', 'No description')}")
                print()
            
            return 0
        
        # YAML format (default)
        print(f"# {settings_examples.get('description', 'Recipe settings configuration examples')}")
        print()
        
        # Extract and display the actual YAML examples
        example_keys = [key for key in settings_examples.keys() if key.endswith('_example')]
        
        for example_key in example_keys:
            example = settings_examples[example_key]
            if 'yaml' in example:
                print(f"# {example.get('description', example_key)}")
                print(example['yaml'])
                print()
        
        return 0
        
    except Exception as e:
        print(f"Error displaying settings examples: {e}")
        return 1


def _display_builtin_settings_examples(format_type: str) -> int:
    """Display built-in settings examples when no YAML file is available."""
    try:
        print("Recipe Settings Usage Examples")
        print("=" * 40)
        
        if format_type == 'json':
            builtin_examples = {
                "description": "Recipe settings configuration examples",
                "basic_example": {
                    "description": "Minimal settings section",
                    "yaml": "settings:\n  description: \"Process daily sales data\""
                },
                "variables_example": {
                    "description": "Settings with custom variables",
                    "yaml": "settings:\n  description: \"Process with variables\"\n  variables:\n    region: \"west\"\n    batch_id: \"A47\""
                }
            }
            import json
            print(json.dumps(builtin_examples, indent=2))
            return 0
        
        if format_type == 'text':
            print("Description: Recipe settings configuration options")
            print()
            print("Available settings:")
            print("  - description: Recipe description")
            print("  - variables: Custom variables")
            print("  - required_external_vars: External variable definitions")
            print("  - stages: Stage declarations")
            print()
            return 0
        
        # YAML format (default)
        print("# Recipe settings configuration examples")
        print()
        
        print("# Minimal settings section")
        print("settings:")
        print("  description: \"Process daily sales data\"")
        print()
        
        print("# Settings with custom variables")
        print("settings:")
        print("  description: \"Process with dynamic variables\"")
        print("  variables:")
        print("    region: \"west\"")
        print("    batch_id: \"A47\"")
        print("    output_prefix: \"processed\"")
        print()
        
        print("# Advanced settings with external variables and stages")
        print("settings:")
        print("  description: \"Complete recipe configuration\"")
        print("  ")
        print("  # Custom variables for reuse")
        print("  variables:")
        print("    region: \"west\"")
        print("    output_prefix: \"processed\"")
        print("  ")
        print("  # External variables with validation")
        print("  required_external_vars:")
        print("    batch_id:")
        print("      description: \"Batch identifier\"")
        print("      validation: \"^[A-Z]\\\\d+$\"")
        print("      example: \"A47\"")
        print("  ")
        print("  # Stage declarations")
        print("  stages:")
        print("    - stage_name: \"raw_data\"")
        print("      description: \"Raw imported data\"")
        print("      protected: false")
        print("    - stage_name: \"processed_data\"")
        print("      description: \"Final processed results\"")
        print("      protected: false")
        print()
        
        return 0
        
    except Exception as e:
        print(f"Error displaying built-in settings examples: {e}")
        return 1


def _get_all_usage_examples(format_type: str) -> int:
    """Get usage examples for all processors."""
    try:
        from excel_recipe_processor.core.pipeline import get_all_usage_examples
        
        all_examples = get_all_usage_examples()
        
        if format_type == 'json':
            import json
            print(json.dumps(all_examples, indent=2))
        elif format_type == 'text':
            _display_all_examples_text(all_examples)
        else:  # yaml format
            _display_all_examples_yaml(all_examples)
        
        return 0
        
    except Exception as e:
        print(f"Error getting all usage examples: {e}")
        return 1


def _display_yaml_examples(processor_name: str, examples_data: dict, format_type: str) -> int:
    """Display examples loaded from YAML file."""
    try:
        print(f"Usage Examples for: {processor_name}")
        print("=" * 40)
        
        if format_type == 'json':
            import json
            print(json.dumps(examples_data, indent=2))
            return 0
        
        if format_type == 'text':
            print(f"Processor: {processor_name}")
            print(f"Description: {examples_data.get('description', 'No description available')}")
            print()
            
            # List examples
            example_keys = [key for key in examples_data.keys() if key.endswith('_example')]
            for example_key in example_keys:
                example = examples_data[example_key]
                print(f"Example: {example_key}")
                print(f"  Description: {example.get('description', 'No description')}")
                print()
            
            return 0
        
        # YAML format (default)
        print(f"# {examples_data.get('description', 'Usage examples')}")
        print()
        
        # Extract and display the actual YAML examples
        example_keys = [key for key in examples_data.keys() if key.endswith('_example')]
        
        for example_key in example_keys:
            example = examples_data[example_key]
            if 'yaml' in example:
                print(f"# {example.get('description', example_key)}")
                print(example['yaml'])
                print()
        
        return 0
        
    except Exception as e:
        print(f"Error displaying YAML examples: {e}")
        return 1


def _display_method_examples(processor_name: str, method_examples: dict, format_type: str) -> int:
    """Display examples from processor method."""
    try:
        print(f"Usage Examples for: {processor_name}")
        print("=" * 40)
        
        if format_type == 'json':
            print(method_examples.get('formatted_json', '{}'))
        elif format_type == 'text':
            print(method_examples.get('formatted_text', 'No text format available'))
        else:  # yaml format
            print(method_examples.get('formatted_yaml', '# No YAML format available'))
        
        return 0
        
    except Exception as e:
        print(f"Error displaying method examples: {e}")
        return 1


def _display_all_examples_yaml(all_examples: dict) -> None:
    """Display all examples in YAML format."""
    print("# Complete Usage Examples for Excel Recipe Processor")
    print("# =" * 50)
    print()
    
    system_info = all_examples.get('system_info', {})
    print(f"# Total processors: {system_info.get('total_processors', 0)}")
    print(f"# Processors with examples: {system_info.get('processors_with_examples', 0)}")
    print(f"# Processors missing examples: {system_info.get('processors_missing_examples', 0)}")
    print()
    
    processors = all_examples.get('processors', {})
    
    for processor_name in sorted(processors.keys()):
        processor_data = processors[processor_name]
        
        if 'error' not in processor_data:
            print(f"# {processor_name.upper()} PROCESSOR")
            print(f"# {'-' * 20}")
            
            if 'formatted_yaml' in processor_data:
                print(processor_data['formatted_yaml'])
            else:
                print(f"# Examples available but not formatted for {processor_name}")
        else:
            print(f"# {processor_name.upper()} PROCESSOR - {processor_data['error']}")
        
        print()


def _display_all_examples_text(all_examples: dict) -> None:
    """Display all examples in text format."""
    print("Complete Usage Examples for Excel Recipe Processor")
    print("=" * 50)
    
    system_info = all_examples.get('system_info', {})
    print(f"Total processors: {system_info.get('total_processors', 0)}")
    print(f"Processors with examples: {system_info.get('processors_with_examples', 0)}")
    print(f"Processors missing examples: {system_info.get('processors_missing_examples', 0)}")
    print()
    
    processors = all_examples.get('processors', {})
    
    for processor_name in sorted(processors.keys()):
        processor_data = processors[processor_name]
        
        print(f"Processor: {processor_name}")
        
        if 'error' not in processor_data:
            if 'formatted_text' in processor_data:
                print(processor_data['formatted_text'])
            else:
                print("  Examples available but not formatted for text display")
        else:
            print(f"  Error: {processor_data['error']}")
        
        print("-" * 30)
