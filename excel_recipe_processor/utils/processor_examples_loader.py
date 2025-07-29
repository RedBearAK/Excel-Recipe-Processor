"""
Utility for loading processor usage examples from external YAML files.

This module provides functions to load usage examples for processors from
external YAML files located in processors/examples/ directory. It handles
error cases gracefully and provides consistent error messaging.
"""

from typing import Any
from pathlib import Path


def load_processor_examples(processor_name: str) -> dict[str, Any]:
    """
    Load usage examples for a processor from external YAML file.
    
    Args:
        processor_name: Name of the processor (e.g., 'export_file')
        
    Returns:
        Dictionary with usage examples or error information
        
    Example:
        >>> examples = load_processor_examples('export_file')
        >>> if 'error' not in examples:
        ...     print(examples['description'])
    """
    try:
        import yaml
    except ImportError:
        return {
            'error': 'PyYAML package required for loading usage examples. '
                    'Install with: pip install PyYAML',
            'status': 'missing_dependency'
        }
    
    try:
        # Get path to examples file
        # Navigate from utils/ back to processors/examples/
        current_file = Path(__file__)
        examples_dir = current_file.parent.parent / 'processors' / 'examples'
        example_file = examples_dir / f'{processor_name}_examples.yaml'
        
        if not example_file.exists():
            return {
                'error': f'Usage examples file not found for {processor_name}. '
                        f'Create processors/examples/{processor_name}_examples.yaml to add examples.',
                'status': 'file_missing',
                'expected_path': str(example_file)
            }
        
        # Load and parse YAML file
        with open(example_file, 'r', encoding='utf-8') as f:
            examples_data = yaml.safe_load(f)
        
        if not examples_data:
            return {
                'error': f'Usage examples file for {processor_name} is empty or invalid.',
                'status': 'file_empty',
                'file_path': str(example_file)
            }
        
        # Validate basic structure
        validation_result = _validate_examples_structure(processor_name, examples_data)
        if validation_result['valid']:
            return examples_data
        else:
            return {
                'error': f'Invalid structure in {processor_name}_examples.yaml: {validation_result["error"]}',
                'status': 'invalid_structure',
                'file_path': str(example_file)
            }
        
    except yaml.YAMLError as e:
        return {
            'error': f'YAML syntax error in {processor_name}_examples.yaml: {e}',
            'status': 'yaml_error',
            'file_path': str(example_file) if 'example_file' in locals() else 'unknown'
        }
    except FileNotFoundError:
        return {
            'error': f'Usage examples file not found for {processor_name}. '
                    f'Create processors/examples/{processor_name}_examples.yaml to add examples.',
            'status': 'file_missing'
        }
    except PermissionError:
        return {
            'error': f'Permission denied reading {processor_name}_examples.yaml. '
                    f'Check file permissions.',
            'status': 'permission_error'
        }
    except Exception as e:
        return {
            'error': f'Unexpected error loading usage examples for {processor_name}: {e}',
            'status': 'error'
        }


def _validate_examples_structure(processor_name: str, examples_data: dict[str, Any]) -> dict[str, Any]:
    """
    Validate that the examples data has the expected structure.
    
    Args:
        processor_name: Name of the processor
        examples_data: Loaded YAML data to validate
        
    Returns:
        Dictionary with validation results
    """
    if not isinstance(examples_data, dict):
        return {
            'valid': False,
            'error': 'Root level must be a dictionary'
        }
    
    # Check for required top-level keys
    required_keys = ['description']
    missing_keys = [key for key in required_keys if key not in examples_data]
    if missing_keys:
        return {
            'valid': False,
            'error': f'Missing required keys: {missing_keys}'
        }
    
    # Check that description is a string
    if not isinstance(examples_data['description'], str):
        return {
            'valid': False,
            'error': 'Description must be a string'
        }
    
    # Check for at least one example
    example_keys = [key for key in examples_data.keys() 
                    if key.endswith('_example') and isinstance(examples_data[key], dict)]
    
    if not example_keys:
        return {
            'valid': False,
            'error': 'Must contain at least one example (key ending with "_example")'
        }
    
    # Validate each example structure
    for example_key in example_keys:
        example = examples_data[example_key]
        
        if 'description' not in example:
            return {
                'valid': False,
                'error': f'Example "{example_key}" missing required "description" field'
            }
        
        if 'yaml' not in example:
            return {
                'valid': False,
                'error': f'Example "{example_key}" missing required "yaml" field'
            }
        
        if not isinstance(example['yaml'], str):
            return {
                'valid': False,
                'error': f'Example "{example_key}" yaml field must be a string'
            }
    
    return {'valid': True}


def get_examples_file_path(processor_name: str) -> Path:
    """
    Get the expected path to a processor's examples file.
    
    Args:
        processor_name: Name of the processor
        
    Returns:
        Path object pointing to the examples file
    """
    current_file = Path(__file__)
    examples_dir = current_file.parent.parent / 'processors' / 'examples'
    return examples_dir / f'{processor_name}_examples.yaml'


def list_available_example_files() -> dict[str, Any]:
    """
    List all available processor example files.
    
    Returns:
        Dictionary with information about available example files
    """
    try:
        current_file = Path(__file__)
        examples_dir = current_file.parent.parent / 'processors' / 'examples'
        
        if not examples_dir.exists():
            return {
                'examples_directory': str(examples_dir),
                'exists': False,
                'files': [],
                'total_files': 0
            }
        
        # Find all *_examples.yaml files
        example_files = list(examples_dir.glob('*_examples.yaml'))
        
        file_info = []
        for file_path in example_files:
            # Extract processor name from filename
            processor_name = file_path.stem.replace('_examples', '')
            
            file_info.append({
                'processor_name': processor_name,
                'filename': file_path.name,
                'file_path': str(file_path),
                'file_size': file_path.stat().st_size if file_path.exists() else 0
            })
        
        return {
            'examples_directory': str(examples_dir),
            'exists': True,
            'files': sorted(file_info, key=lambda x: x['processor_name']),
            'total_files': len(file_info)
        }
        
    except Exception as e:
        return {
            'examples_directory': 'unknown',
            'exists': False,
            'files': [],
            'total_files': 0,
            'error': f'Error listing example files: {e}'
        }


def validate_example_file(processor_name: str) -> dict[str, Any]:
    """
    Validate a specific processor's example file.
    
    Args:
        processor_name: Name of the processor to validate
        
    Returns:
        Dictionary with validation results
    """
    # Try to load the file
    examples_data = load_processor_examples(processor_name)
    
    if 'error' in examples_data:
        return {
            'valid': False,
            'processor_name': processor_name,
            'error': examples_data['error'],
            'status': examples_data.get('status', 'unknown')
        }
    
    # Additional validation checks
    validation_issues = []
    
    # Check for recommended examples
    recommended_examples = ['basic_example', 'advanced_example']
    for example_name in recommended_examples:
        if example_name not in examples_data:
            validation_issues.append(f'Missing recommended example: {example_name}')
    
    # Check for parameter_details
    if 'parameter_details' not in examples_data:
        validation_issues.append('Missing recommended section: parameter_details')
    
    # Check YAML content for processor_type
    for key, example in examples_data.items():
        if key.endswith('_example') and isinstance(example, dict):
            yaml_content = example.get('yaml', '')
            if f'processor_type: "{processor_name}"' not in yaml_content:
                validation_issues.append(f'Example {key} may not contain correct processor_type')
    
    return {
        'valid': len(validation_issues) == 0,
        'processor_name': processor_name,
        'validation_issues': validation_issues,
        'examples_count': len([k for k in examples_data.keys() if k.endswith('_example')]),
        'has_parameter_details': 'parameter_details' in examples_data
    }


def load_settings_examples() -> dict[str, Any]:
    """
    Load usage examples for recipe settings section from external YAML file.
    
    Returns:
        Dictionary with settings usage examples or error information
        
    Example:
        >>> examples = load_settings_examples()
        >>> if 'error' not in examples:
        ...     print(examples['description'])
    """
    try:
        import yaml
    except ImportError:
        return {
            'error': 'PyYAML package required for loading settings examples. '
                    'Install with: pip install PyYAML',
            'status': 'missing_dependency'
        }
    
    try:
        # Get path to settings examples file
        # Navigate from utils/ to config/examples/
        current_file = Path(__file__)
        examples_dir = current_file.parent.parent / 'config' / 'examples'
        example_file = examples_dir / 'recipe_settings_examples.yaml'
        
        if not example_file.exists():
            return {
                'error': f'Settings examples file not found. '
                        f'Create config/examples/recipe_settings_examples.yaml to add examples.',
                'status': 'file_missing',
                'expected_path': str(example_file)
            }
        
        # Load and parse YAML file
        with open(example_file, 'r', encoding='utf-8') as f:
            examples_data = yaml.safe_load(f)
        
        if not examples_data:
            return {
                'error': f'Settings examples file is empty or invalid.',
                'status': 'file_empty',
                'file_path': str(example_file)
            }
        
        # Validate basic structure
        validation_result = _validate_settings_examples_structure(examples_data)
        if validation_result['valid']:
            return examples_data
        else:
            return {
                'error': f'Invalid structure in recipe_settings_examples.yaml: {validation_result["error"]}',
                'status': 'invalid_structure',
                'file_path': str(example_file)
            }
        
    except yaml.YAMLError as e:
        return {
            'error': f'YAML syntax error in recipe_settings_examples.yaml: {e}',
            'status': 'yaml_error',
            'file_path': str(example_file) if 'example_file' in locals() else 'unknown'
        }
    except FileNotFoundError:
        return {
            'error': f'Settings examples file not found. '
                    f'Create config/examples/recipe_settings_examples.yaml to add examples.',
            'status': 'file_missing'
        }
    except PermissionError:
        return {
            'error': f'Permission denied reading recipe_settings_examples.yaml. '
                    f'Check file permissions.',
            'status': 'permission_error'
        }
    except Exception as e:
        return {
            'error': f'Unexpected error loading settings examples: {e}',
            'status': 'error'
        }


def _validate_settings_examples_structure(examples_data: dict[str, Any]) -> dict[str, Any]:
    """
    Validate that the settings examples data has the expected structure.
    
    Args:
        examples_data: Loaded YAML data to validate
        
    Returns:
        Dictionary with validation results
    """
    if not isinstance(examples_data, dict):
        return {
            'valid': False,
            'error': 'Root level must be a dictionary'
        }
    
    # Check for required top-level keys
    required_keys = ['description']
    missing_keys = [key for key in required_keys if key not in examples_data]
    if missing_keys:
        return {
            'valid': False,
            'error': f'Missing required keys: {missing_keys}'
        }
    
    # Check that description is a string
    if not isinstance(examples_data['description'], str):
        return {
            'valid': False,
            'error': 'Description must be a string'
        }
    
    # Check for at least one example
    example_keys = [key for key in examples_data.keys() 
                    if key.endswith('_example') and isinstance(examples_data[key], dict)]
    
    if not example_keys:
        return {
            'valid': False,
            'error': 'Must contain at least one example (key ending with "_example")'
        }
    
    # Validate each example structure
    for example_key in example_keys:
        example = examples_data[example_key]
        
        if 'description' not in example:
            return {
                'valid': False,
                'error': f'Example "{example_key}" missing required "description" field'
            }
        
        if 'yaml' not in example:
            return {
                'valid': False,
                'error': f'Example "{example_key}" missing required "yaml" field'
            }
        
        if not isinstance(example['yaml'], str):
            return {
                'valid': False,
                'error': f'Example "{example_key}" yaml field must be a string'
            }
    
    return {'valid': True}


def get_settings_examples_file_path() -> Path:
    """
    Get the expected path to the settings examples file.
    
    Returns:
        Path object pointing to the settings examples file
    """
    current_file = Path(__file__)
    examples_dir = current_file.parent.parent / 'config' / 'examples'
    return examples_dir / 'recipe_settings_examples.yaml'
