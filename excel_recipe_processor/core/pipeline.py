"""
Pipeline orchestrator for Excel automation recipes.

Coordinates loading recipes, reading files, executing steps, and saving results
with enhanced FileReader/FileWriter integration, comprehensive variable substitution,
and external variable support.
"""

import pandas as pd
import logging

from pathlib import Path

from excel_recipe_processor.core.file_reader import FileReader, FileReaderError
from excel_recipe_processor.core.file_writer import FileWriter, FileWriterError
from excel_recipe_processor.core.stage_manager import StageManager, StageError
from excel_recipe_processor.core.variable_substitution import VariableSubstitution
from excel_recipe_processor.processors.base_processor import registry, StepProcessorError
from excel_recipe_processor.config.recipe_loader import RecipeLoader, RecipeValidationError


logger = logging.getLogger(__name__)

step_desc = 'step_description'
proc_type = 'processor_type'


class PipelineError(Exception):
    """Raised when pipeline execution fails."""
    pass


class ExcelPipeline:
    """
    Orchestrates the execution of Excel data processing recipes.
    
    Handles the complete workflow from loading recipes and input files
    to executing processing steps and saving results with enhanced:
    - FileReader/FileWriter integration for consistent file handling
    - Comprehensive variable substitution across all operations
    - StageManager coordination for intermediate data storage
    - Processor variable injection for dynamic configurations
    - External variable support with CLI overrides and interactive prompts
    """
    
    def __init__(self):
        """Initialize the pipeline orchestrator."""
        self.recipe_loader              = RecipeLoader()
        
        self.recipe_data                = None
        self.input_data                 = None
        self.current_data               = None
        self.steps_executed             = 0
        self.variable_substitution      = None
        
        # Track pipeline state for enhanced integration
        self._input_path                = None
        self._recipe_path               = None
        self._custom_variables          = {}
        self._external_variables        = {}

    def load_recipe(self, recipe_path) -> dict:
        """
        Load a recipe file with variable substitution support.
        
        Args:
            recipe_path: Path to the recipe file
            
        Returns:
            Loaded recipe data
            
        Raises:
            PipelineError: If recipe loading fails
        """
        try:
            self._recipe_path = Path(recipe_path)
            self.recipe_data = self.recipe_loader.load_file(recipe_path)
            
            # Extract custom variables from recipe settings
            settings = self.recipe_loader.get_settings()
            self._custom_variables = settings.get('variables', {})
            
            logger.info(f"Loaded recipe: {self.recipe_loader.summary()}")
            
            # Log variable information
            if self._custom_variables:
                logger.info(f"Found {len(self._custom_variables)} custom variables in recipe")
            
            # Log external variable requirements
            external_vars = self.recipe_loader.get_required_external_vars()
            if external_vars:
                logger.info(f"Recipe requires {len(external_vars)} external variables")
            
            return self.recipe_data
        except (RecipeValidationError, FileNotFoundError) as e:
            raise PipelineError(f"Failed to load recipe: {e}")
    
    def load_input_file(self, input_path, sheet_name=0) -> pd.DataFrame:
        """
        Load input file using FileReader with variable substitution.
        
        Args:
            input_path: Path to input file (supports variable substitution)
            sheet_name: Sheet to read (name or index)
            
        Returns:
            Loaded DataFrame
            
        Raises:
            PipelineError: If file loading fails
        """
        try:
            # Store original input path for variable context
            self._input_path = Path(input_path)
            
            # Initialize or update variable substitution
            self._initialize_variable_substitution()
            
            # Apply variable substitution to input path
            substituted_input_path = self.variable_substitution.substitute(str(input_path))
            if substituted_input_path != str(input_path):
                logger.info(f"Substituted variables in input filename: {input_path} → {substituted_input_path}")
            
            # Use FileReader for enhanced file handling
            self.input_data = FileReader.read_file(
                substituted_input_path,
                variables=self._get_current_variables(),
                sheet=sheet_name
            )
            
            self.current_data = self.input_data.copy()
            
            logger.info(f"Loaded input file: {substituted_input_path} ({len(self.input_data)} rows, {len(self.input_data.columns)} columns)")
            return self.input_data
            
        except FileReaderError as e:
            raise PipelineError(f"Failed to load input file: {e}")
    
    def execute_recipe(self) -> pd.DataFrame:
        """
        Execute all recipe steps with enhanced processor integration.
        
        Returns:
            Final processed DataFrame
            
        Raises:
            PipelineError: If recipe execution fails
        """
        if self.recipe_data is None:
            raise PipelineError("No recipe loaded. Call load_recipe() first.")
        
        if self.input_data is None:
            raise PipelineError("No input data loaded. Call load_input_file() first.")
        
        steps = self.recipe_loader.get_steps()
        logger.info(f"Executing recipe with {len(steps)} steps")
        
        try:
            for i, step in enumerate(steps, 1):
                step_name = step.get(step_desc, f"Step {i}")
                step_type = step[proc_type]
                
                logger.info(f"Executing step {i}/{len(steps)}: {step_name} ({step_type})")
                
                # Create processor with variable injection
                processor = self._create_processor(step)
                
                # Execute step
                self.current_data = processor.execute(self.current_data)
                self.steps_executed += 1

                logger.debug(f"Step {i} completed: {len(self.current_data)} rows, {len(self.current_data.columns)} columns")
            
            logger.info(f"Recipe execution completed successfully: {self.steps_executed} steps executed")
            return self.current_data
            
        except (StepProcessorError, StageError) as e:
            logger.error(f"Step execution failed at step {self.steps_executed + 1}: {e}")
            raise PipelineError(f"Recipe execution failed: {e}")
    
    def save_result(self, output_path, sheet_name='ProcessedData') -> str:
        """
        Save processed data using FileWriter with variable substitution.
        
        Args:
            output_path: Path for output file (supports variable substitution)
            sheet_name: Sheet name for output
            
        Returns:
            Final output file path (with variables substituted)
            
        Raises:
            PipelineError: If saving fails
        """
        if self.current_data is None:
            raise PipelineError("No data to save. Execute recipe first.")
        
        try:
            # Apply variable substitution to output path
            substituted_output_path = self.variable_substitution.substitute(str(output_path))
            if substituted_output_path != str(output_path):
                logger.info(f"Substituted variables in output filename: {output_path} → {substituted_output_path}")
            
            # Use FileWriter for enhanced file handling
            final_path = FileWriter.write_file(
                self.current_data,
                substituted_output_path,
                variables=self._get_current_variables(),
                sheet_name=sheet_name
            )
            
            logger.info(f"Saved result to: {final_path}")
            return final_path
            
        except FileWriterError as e:
            raise PipelineError(f"Failed to save result: {e}")
    
    def run_complete_pipeline(self, recipe_path, input_path, output_path, 
                            input_sheet=0, output_sheet='ProcessedData') -> pd.DataFrame:
        """
        Run the complete pipeline: load recipe, load input, execute, and save.
        
        Args:
            recipe_path: Path to recipe file
            input_path: Path to input file (supports variable substitution)
            output_path: Path to output file (supports variable substitution)
            input_sheet: Sheet to read from input file
            output_sheet: Sheet name for output
            
        Returns:
            Final processed DataFrame
            
        Raises:
            PipelineError: If pipeline execution fails
        """
        logger.info("Starting complete pipeline execution")
        
        try:
            # Load recipe first to get variables
            self.load_recipe(recipe_path)
            
            # Load input file with variable support
            self.load_input_file(input_path, sheet_name=input_sheet)
            
            # Execute recipe steps
            result = self.execute_recipe()
            
            # Save result with variable support
            self.save_result(output_path, sheet_name=output_sheet)
            
            logger.info("Pipeline execution completed successfully")
            return result
            
        except Exception as e:
            logger.error(f"Pipeline execution failed: {e}")
            raise
    
    def _initialize_variable_substitution(self) -> None:
        """Initialize or update variable substitution with current context."""
        
        # Combine all variable sources
        all_variables = {}
        all_variables.update(self._custom_variables)  # Recipe variables
        all_variables.update(self._external_variables)  # External variables
        
        self.variable_substitution = VariableSubstitution(
            input_path=self._input_path,
            recipe_path=self._recipe_path,
            custom_variables=all_variables
        )
    
    def _get_current_variables(self) -> dict:
        """Get current variable dictionary for file operations."""
        
        if self.variable_substitution:
            return self.variable_substitution.get_available_variables()
        else:
            # Fallback: combine variables manually
            all_variables = {}
            all_variables.update(self._custom_variables)
            all_variables.update(self._external_variables)
            return all_variables
    
    def _inject_variables_into_processor(self, processor) -> None:
        """Inject variables into processor for dynamic configurations."""
        
        # Set variables on processor for use in dynamic configurations
        processor._variables = self._get_current_variables()
        
        # Also set variable substitution object if processor expects it
        if hasattr(processor, '_variable_substitution'):
            processor._variable_substitution = self.variable_substitution
    
    def _create_processor(self, step_config: dict):
        """
        Create a processor instance for a step configuration.
        
        Args:
            step_config: Step configuration dictionary
            
        Returns:
            Configured processor instance
            
        Raises:
            PipelineError: If processor creation fails
        """
        try:
            processor = registry.create_processor(step_config)
            
            # Inject variables for dynamic configurations
            self._inject_variables_into_processor(processor)
            
            return processor
            
        except Exception as e:
            step_type = step_config.get(proc_type, 'unknown')
            raise PipelineError(f"Failed to create processor '{step_type}': {e}")
    
    def _create_backup_file(self, file_path) -> Path:
        """
        Create a backup of an existing file.
        
        Args:
            file_path: Path to file to backup
            
        Returns:
            Path to backup file
            
        Raises:
            PipelineError: If backup creation fails
        """
        try:
            # Use FileWriter's backup capability instead of excel_writer directly
            backup_path = Path(str(file_path) + '.backup')
            
            # Copy file to backup location
            import shutil
            shutil.copy2(file_path, backup_path)
            
            logger.info(f"Created backup: {backup_path}")
            return backup_path
            
        except Exception as e:
            raise PipelineError(f"Failed to create backup: {e}")
    
    def get_available_variables(self) -> dict:
        """
        Get all available variables for the current pipeline context.
        
        Returns:
            Dictionary of available variables
        """
        if self.variable_substitution:
            return self.variable_substitution.get_available_variables()
        else:
            # Fallback: combine variables manually
            all_variables = {}
            all_variables.update(self._custom_variables)
            all_variables.update(self._external_variables)
            return all_variables
    
    def add_custom_variable(self, name: str, value: str) -> None:
        """
        Add a custom variable to the pipeline context.
        
        Args:
            name: Variable name
            value: Variable value
        """
        # Store in external variables (higher priority than recipe variables)
        self._external_variables[name] = value
        
        # Update variable substitution if initialized
        if self.variable_substitution:
            self.variable_substitution.add_custom_variable(name, value)
        
        logger.debug(f"Added external variable: {name} = {value}")
    
    def add_external_variables(self, variables: dict) -> None:
        """
        Add multiple external variables to the pipeline context.
        
        Args:
            variables: Dictionary of variable name-value pairs
        """
        for name, value in variables.items():
            self.add_custom_variable(name, value)
        
        if variables:
            logger.info(f"Added {len(variables)} external variables")
    
    def validate_variable_templates(self, templates: list) -> dict:
        """
        Validate variable templates and return validation results.
        
        Args:
            templates: List of template strings to validate
            
        Returns:
            Dictionary with validation results
        """
        if not self.variable_substitution:
            self._initialize_variable_substitution()
        
        results = {}
        for template in templates:
            unknown_vars = self.variable_substitution.validate_template(template)
            results[template] = {
                'valid': len(unknown_vars) == 0,
                'unknown_variables': unknown_vars
            }
        
        return results
    
    def substitute_template(self, template: str) -> str:
        """
        Substitute variables in a template string.
        
        Args:
            template: Template string with variable placeholders
            
        Returns:
            String with variables substituted
        """
        if not self.variable_substitution:
            self._initialize_variable_substitution()
        
        return self.variable_substitution.substitute(template)
    
    def cleanup_pipeline(self) -> None:
        """Clean up pipeline resources and stages."""
        self.recipe_data = None
        self.input_data = None
        self.current_data = None
        self.steps_executed = 0
        self.variable_substitution = None
        self._custom_variables.clear()
        self._external_variables.clear()
        
        logger.debug("Pipeline resources cleaned up")


# =============================================================================
# MODULE-LEVEL CONVENIENCE FUNCTIONS
# =============================================================================

def get_system_capabilities() -> dict:
    """
    Get comprehensive system capabilities including all processors and their features.
    Uses self-reported minimal configs from processors for accurate capability discovery.
    
    Returns:
        Dictionary with system overview and detailed processor capabilities
    """
    try:
        processor_types = registry.get_registered_types()
        
        capabilities = {
            'system_info': {
                'description': 'Excel Recipe Processor - Automated Excel data processing system',
                'total_processors': len(processor_types),
                'processor_types': sorted(processor_types)
            },
            'processors': {}
        }
        
        # Get capabilities for each registered processor using self-reported minimal config
        for processor_type in processor_types:
            try:
                # Get the processor class from registry
                processor_class = registry._processors.get(processor_type)
                
                if processor_class is None:
                    capabilities['processors'][processor_type] = {
                        'error': f'Could not get processor class for {processor_type}'
                    }
                    continue
                
                # Get minimal configuration from the processor class (if it has the method)
                if hasattr(processor_class, 'get_minimal_config'):
                    minimal_config = processor_class.get_minimal_config()
                    
                    # Add the processor_type field that's required by BaseStepProcessor
                    minimal_config['processor_type'] = processor_type
                    minimal_config['step_description'] = f'Capabilities check for {processor_type}'

                    # Create processor instance using minimal config
                    processor = registry.create_processor(minimal_config)
                    
                    # Get capabilities if available
                    if hasattr(processor, 'get_capabilities'):
                        capabilities['processors'][processor_type] = processor.get_capabilities()
                    else:
                        # Basic info for processors without get_capabilities method
                        capabilities['processors'][processor_type] = {
                            'description': f'{processor_type.replace("_", " ").title()} processor',
                            'status': 'available',
                            'capabilities_method': False
                        }
                else:
                    capabilities['processors'][processor_type] = {
                        'error': f'Processor class {processor_class.__name__} missing get_minimal_config() method. '
                                f'Add this method to enable self-discovery.'
                    }
                    
            except Exception as e:
                # Handle processors that can't be instantiated
                capabilities['processors'][processor_type] = {
                    'error': f'Could not load capabilities: {str(e)}',
                    'status': 'error'
                }
        
        return capabilities
        
    except Exception as e:
        # Fallback if entire capability discovery fails
        return {
            'system_info': {
                'description': 'Excel Recipe Processor',
                'total_processors': 0,
                'processor_types': [],
                'error': f'Capability discovery failed: {str(e)}'
            },
            'processors': {}
        }


def get_settings_usage_examples() -> dict:
    """
    Get usage examples for recipe settings section.
    
    Returns:
        Dictionary with settings usage examples or error information
    """
    try:
        # Try to load from external YAML file first
        from excel_recipe_processor.utils.processor_examples_loader import load_settings_examples
        
        examples = load_settings_examples()
        
        if 'error' not in examples:
            # Format the examples for different output types
            formatted_examples = _format_settings_examples(examples)
            
            return {
                'status': 'available',
                'examples': examples,
                'formatted_yaml': formatted_examples['yaml'],
                'formatted_text': formatted_examples['text'],
                'formatted_json': formatted_examples['json']
            }
        else:
            # If external YAML file doesn't exist, try fallback method
            from excel_recipe_processor.config.recipe_loader import RecipeLoader
            
            recipe_loader = RecipeLoader()
            if hasattr(recipe_loader, 'get_settings_examples'):
                fallback_examples = recipe_loader.get_settings_examples()
                formatted_examples = _format_settings_examples(fallback_examples)
                
                return {
                    'status': 'available',
                    'source': 'fallback_method',
                    'examples': fallback_examples,
                    'formatted_yaml': formatted_examples['yaml'],
                    'formatted_text': formatted_examples['text'],
                    'formatted_json': formatted_examples['json']
                }
            else:
                return {
                    'error': f'Settings examples not available. {examples["error"]}',
                    'status': 'not_available',
                    'suggestion': 'Create config/examples/recipe_settings_examples.yaml file'
                }
                
    except Exception as e:
        return {
            'error': f'Unexpected error getting settings examples: {str(e)}',
            'status': 'error'
        }


def _format_settings_examples(settings_examples: dict) -> dict:
    """
    Format settings examples for different output types.
    
    Args:
        settings_examples: Raw settings examples from YAML file
        
    Returns:
        Dictionary with formatted examples for yaml, text, and json
    """
    formatted = {
        'yaml': '',
        'text': '',
        'json': settings_examples
    }
    
    try:
        # Format YAML output
        yaml_lines = []
        yaml_lines.append("# Recipe Settings Usage Examples")
        yaml_lines.append("# Settings section configures recipe behavior")
        yaml_lines.append("")
        
        if 'description' in settings_examples:
            yaml_lines.append(f"# {settings_examples['description']}")
            yaml_lines.append("")
        
        # Add basic example
        if 'basic_example' in settings_examples:
            yaml_lines.append("# Basic settings:")
            if 'description' in settings_examples['basic_example']:
                yaml_lines.append(f"# {settings_examples['basic_example']['description']}")
            yaml_lines.append("")
            yaml_lines.append(settings_examples['basic_example'].get('yaml', '# No YAML example provided'))
            yaml_lines.append("")
        
        # Add variables example
        if 'variables_example' in settings_examples:
            yaml_lines.append("# Settings with custom variables:")
            if 'description' in settings_examples['variables_example']:
                yaml_lines.append(f"# {settings_examples['variables_example']['description']}")
            yaml_lines.append("")
            yaml_lines.append(settings_examples['variables_example'].get('yaml', '# No YAML example provided'))
            yaml_lines.append("")
        
        # Add comprehensive example
        if 'comprehensive_example' in settings_examples:
            yaml_lines.append("# Complete settings configuration:")
            if 'description' in settings_examples['comprehensive_example']:
                yaml_lines.append(f"# {settings_examples['comprehensive_example']['description']}")
            yaml_lines.append("")
            yaml_lines.append(settings_examples['comprehensive_example'].get('yaml', '# No YAML example provided'))
            yaml_lines.append("")
        
        # Add additional examples
        for key, example in settings_examples.items():
            if key not in ['description', 'basic_example', 'variables_example', 'comprehensive_example', 'parameter_details'] and isinstance(example, dict):
                if 'yaml' in example:
                    yaml_lines.append(f"# {key.replace('_', ' ').title()}:")
                    if 'description' in example:
                        yaml_lines.append(f"# {example['description']}")
                    yaml_lines.append("")
                    yaml_lines.append(example['yaml'])
                    yaml_lines.append("")
        
        formatted['yaml'] = '\n'.join(yaml_lines)
        
        # Format text output
        text_lines = []
        text_lines.append("Recipe Settings Configuration")
        text_lines.append("")
        
        if 'description' in settings_examples:
            text_lines.append(settings_examples['description'])
            text_lines.append("")
        
        # Add parameter details
        if 'parameter_details' in settings_examples:
            text_lines.append("Available Parameters:")
            text_lines.append("")
            
            for param_name, details in settings_examples['parameter_details'].items():
                if isinstance(details, dict):
                    text_lines.append(f"  {param_name}:")
                    text_lines.append(f"    Type: {details.get('type', 'unknown')}")
                    text_lines.append(f"    Required: {details.get('required', 'unknown')}")
                    if 'default' in details:
                        text_lines.append(f"    Default: {details['default']}")
                    if 'description' in details:
                        text_lines.append(f"    Description: {details['description']}")
                    text_lines.append("")
        
        formatted['text'] = '\n'.join(text_lines)
        
    except Exception as e:
        formatted['yaml'] = f"# Error formatting settings examples: {e}"
        formatted['text'] = f"Error formatting settings examples: {e}"
    
    return formatted


def get_processor_usage_examples(processor_name: str) -> dict:
    """
    Get usage examples for a specific processor.
    
    Args:
        processor_name: Name of the processor to get examples for
        
    Returns:
        Dictionary with usage examples or error information
    """
    try:
        from excel_recipe_processor.processors.base_processor import registry
        
        # Check if processor exists
        if processor_name not in registry.get_registered_types():
            return None
        
        # Get the processor class
        processor_class = registry._processors.get(processor_name)
        if processor_class is None:
            return {'error': f'Could not get processor class for {processor_name}'}
        
        # Try to get minimal config and create instance
        if not hasattr(processor_class, 'get_minimal_config'):
            return {
                'error': f'Processor class {processor_class.__name__} missing get_minimal_config() method. '
                        f'Add this method to enable usage example discovery.'
            }
        
        try:
            minimal_config = processor_class.get_minimal_config()
            minimal_config['processor_type'] = processor_name
            minimal_config['step_description'] = f'Usage examples for {processor_name}'
            
            # Create processor instance
            processor = registry.create_processor(minimal_config)
            
            # Check for get_usage_examples method
            if not hasattr(processor, 'get_usage_examples'):
                return {
                    'error': f'Processor {processor_class.__name__} missing get_usage_examples() method. '
                            f'Add this method to provide complete usage examples.',
                    'status': 'method_missing'
                }
            
            # Get the usage examples
            usage_examples = processor.get_usage_examples()
            
            # Format the examples for different output types
            formatted_examples = _format_usage_examples(processor_name, usage_examples)
            
            return {
                'processor_name': processor_name,
                'status': 'available',
                'examples': usage_examples,
                'formatted_yaml': formatted_examples['yaml'],
                'formatted_text': formatted_examples['text'],
                'formatted_json': formatted_examples['json']
            }
            
        except Exception as e:
            return {
                'error': f'Could not get usage examples: {str(e)}',
                'status': 'error'
            }
            
    except Exception as e:
        return {
            'error': f'Unexpected error getting usage examples for {processor_name}: {str(e)}',
            'status': 'error'
        }


def get_all_usage_examples() -> dict:
    """
    Get usage examples for all processors.
    
    Returns:
        Dictionary with usage examples for all processors and system info
    """
    try:
        from excel_recipe_processor.processors.base_processor import registry
        
        processor_types = registry.get_registered_types()
        
        examples_data = {
            'system_info': {
                'description': 'Complete usage examples for Excel Recipe Processor',
                'total_processors': len(processor_types),
                'processors_with_examples': 0,
                'processors_missing_examples': 0,
                'processors_with_errors': 0
            },
            'processors': {}
        }
        
        # Get examples for each processor
        for processor_type in sorted(processor_types):
            examples = get_processor_usage_examples(processor_type)
            
            if examples is None:
                # This shouldn't happen if processor is registered
                examples = {'error': 'Processor not found'}
                examples_data['system_info']['processors_with_errors'] += 1
            elif 'error' in examples:
                if examples.get('status') == 'method_missing':
                    examples_data['system_info']['processors_missing_examples'] += 1
                else:
                    examples_data['system_info']['processors_with_errors'] += 1
            else:
                examples_data['system_info']['processors_with_examples'] += 1
            
            examples_data['processors'][processor_type] = examples
        
        return examples_data
        
    except Exception as e:
        return {
            'system_info': {
                'error': f'Failed to get usage examples: {str(e)}',
                'total_processors': 0,
                'processors_with_examples': 0,
                'processors_missing_examples': 0,
                'processors_with_errors': 0
            },
            'processors': {}
        }


def _format_usage_examples(processor_name: str, usage_examples: dict) -> dict:
    """
    Format usage examples for different output types.
    
    Args:
        processor_name: Name of the processor
        usage_examples: Raw usage examples from processor
        
    Returns:
        Dictionary with formatted examples for yaml, text, and json
    """
    formatted = {
        'yaml': '',
        'text': '',
        'json': usage_examples
    }
    
    try:
        # Format YAML output
        yaml_lines = []
        yaml_lines.append(f"# {processor_name} processor usage examples")
        yaml_lines.append("")
        
        if 'description' in usage_examples:
            yaml_lines.append(f"# {usage_examples['description']}")
            yaml_lines.append("")
        
        # Add basic example
        if 'basic_example' in usage_examples:
            yaml_lines.append("# Basic usage:")
            if 'description' in usage_examples['basic_example']:
                yaml_lines.append(f"# {usage_examples['basic_example']['description']}")
            yaml_lines.append("")
            yaml_lines.append(usage_examples['basic_example'].get('yaml', '# No YAML example provided'))
            yaml_lines.append("")
        
        # Add advanced example
        if 'advanced_example' in usage_examples:
            yaml_lines.append("# Advanced usage:")
            if 'description' in usage_examples['advanced_example']:
                yaml_lines.append(f"# {usage_examples['advanced_example']['description']}")
            yaml_lines.append("")
            yaml_lines.append(usage_examples['advanced_example'].get('yaml', '# No YAML example provided'))
            yaml_lines.append("")
        
        # Add additional examples
        for key, example in usage_examples.items():
            if key not in ['description', 'basic_example', 'advanced_example', 'parameter_details'] and isinstance(example, dict):
                if 'yaml' in example:
                    yaml_lines.append(f"# {key.replace('_', ' ').title()}:")
                    if 'description' in example:
                        yaml_lines.append(f"# {example['description']}")
                    yaml_lines.append("")
                    yaml_lines.append(example['yaml'])
                    yaml_lines.append("")
        
        formatted['yaml'] = '\n'.join(yaml_lines)
        
        # Format text output
        text_lines = []
        text_lines.append(f"Processor: {processor_name}")
        
        if 'description' in usage_examples:
            text_lines.append(f"Description: {usage_examples['description']}")
        
        text_lines.append("")
        text_lines.append("Available Examples:")
        
        for key, example in usage_examples.items():
            if isinstance(example, dict) and 'description' in example:
                text_lines.append(f"  - {key}: {example['description']}")
        
        # Add parameter details if available
        if 'parameter_details' in usage_examples:
            text_lines.append("")
            text_lines.append("Parameters:")
            for param, details in usage_examples['parameter_details'].items():
                required = "required" if details.get('required', False) else "optional"
                text_lines.append(f"  - {param} ({required}): {details.get('description', 'No description')}")
        
        formatted['text'] = '\n'.join(text_lines)
        
    except Exception as e:
        formatted['yaml'] = f"# Error formatting examples: {e}"
        formatted['text'] = f"Error formatting examples: {e}"
    
    return formatted


# Import processor classes
from excel_recipe_processor.processors.add_calculated_column_processor  import AddCalculatedColumnProcessor
from excel_recipe_processor.processors.add_subtotals_processor          import AddSubtotalsProcessor
from excel_recipe_processor.processors.aggregate_data_processor         import AggregateDataProcessor
from excel_recipe_processor.processors.clean_data_processor             import CleanDataProcessor
from excel_recipe_processor.processors.combine_data_processor           import CombineDataProcessor
from excel_recipe_processor.processors.create_stage_processor           import CreateStageProcessor
from excel_recipe_processor.processors.debug_breakpoint_processor       import DebugBreakpointProcessor
from excel_recipe_processor.processors.export_file_processor            import ExportFileProcessor
from excel_recipe_processor.processors.fill_data_processor              import FillDataProcessor
from excel_recipe_processor.processors.filter_data_processor            import FilterDataProcessor
from excel_recipe_processor.processors.format_excel_processor           import FormatExcelProcessor
from excel_recipe_processor.processors.group_data_processor             import GroupDataProcessor
from excel_recipe_processor.processors.import_file_processor            import ImportFileProcessor
from excel_recipe_processor.processors.load_stage_processor             import LoadStageProcessor
from excel_recipe_processor.processors.lookup_data_processor            import LookupDataProcessor
from excel_recipe_processor.processors.merge_data_processor             import MergeDataProcessor
from excel_recipe_processor.processors.pivot_table_processor            import PivotTableProcessor
from excel_recipe_processor.processors.rename_columns_processor         import RenameColumnsProcessor
from excel_recipe_processor.processors.save_stage_processor             import SaveStageProcessor
from excel_recipe_processor.processors.slice_data_processor             import SliceDataProcessor
from excel_recipe_processor.processors.sort_data_processor              import SortDataProcessor
from excel_recipe_processor.processors.split_column_processor           import SplitColumnProcessor


def register_standard_processors():
    """Register all standard processors with the registry."""
    
    # Register existing processors
    registry.register('add_calculated_column',          AddCalculatedColumnProcessor        )
    registry.register('add_subtotals',                  AddSubtotalsProcessor               )
    registry.register('aggregate_data',                 AggregateDataProcessor              )
    registry.register('clean_data',                     CleanDataProcessor                  )
    registry.register('combine_data',                   CombineDataProcessor                )
    registry.register('create_stage',                   CreateStageProcessor                )
    registry.register('debug_breakpoint',               DebugBreakpointProcessor            )
    registry.register('export_file',                    ExportFileProcessor                 )
    registry.register('fill_data',                      FillDataProcessor                   )
    registry.register('filter_data',                    FilterDataProcessor                 )
    registry.register('format_excel',                   FormatExcelProcessor                )
    registry.register('group_data',                     GroupDataProcessor                  )
    registry.register('import_file',                    ImportFileProcessor                 )
    registry.register('load_stage',                     LoadStageProcessor                  )
    registry.register('lookup_data',                    LookupDataProcessor                 )
    registry.register('merge_data',                     MergeDataProcessor                  )
    registry.register('pivot_table',                    PivotTableProcessor                 )
    registry.register('rename_columns',                 RenameColumnsProcessor              )
    registry.register('save_stage',                     SaveStageProcessor                  )
    registry.register('slice_data',                     SliceDataProcessor                  )
    registry.register('sort_data',                      SortDataProcessor                   )
    registry.register('split_column',                   SplitColumnProcessor                )
    
    logger.debug("Registered standard processors")


# Auto-register processors when module is imported
register_standard_processors()
