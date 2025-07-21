"""Main functionality for excel_recipe_processor package."""

import sys
import logging

from typing import Any, Optional
from pathlib import Path
from argparse import Namespace

from excel_recipe_processor.core.pipeline import (
    ExcelPipeline, PipelineError, get_system_capabilities)

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
            return list_system_capabilities()
        
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


def process_excel_file(input_file: str, recipe_file: str, output_file: Optional[str] = None,
                      input_sheet: Any = 0, output_sheet: str = 'ProcessedData', 
                      verbose: bool = False) -> int:
    """
    Process an Excel file using a recipe.
    
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
            pipeline.load_recipe(recipe_path)
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
        rows_processed = len(result)
        columns_processed = len(result.columns)
        
        print(f"✓ Processing completed successfully")
        print(f"✓ Final result: {rows_processed} rows, {columns_processed} columns")
        print(f"✓ Executed {pipeline.steps_executed} processing steps")
        
        # Show final output path (after variable substitution)
        if hasattr(pipeline, 'variable_substitution') and pipeline.variable_substitution:
            final_path = pipeline.variable_substitution.substitute_variables(final_output)
            if final_path != final_output:
                print(f"✓ Output saved to: {final_path}")
            else:
                print(f"✓ Output saved to: {final_output}")
        else:
            print(f"✓ Output saved to: {final_output}")
        
        return 0
        
    except PipelineError as e:
        print(f"Pipeline error: {e}")
        return 1
    except Exception as e:
        print(f"Unexpected error during processing: {e}")
        if verbose:
            import traceback
            traceback.print_exc()
        return 1


def list_system_capabilities() -> int:
    """
    List all available processors and their capabilities.
    
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
        print("Use 'python -m excel_recipe_processor --help' for usage information")
        
        return 0
        
    except Exception as e:
        print(f"Error listing capabilities: {e}")
        return 1


def validate_recipe_file(recipe_file: str) -> int:
    """
    Validate a recipe file without processing data.
    
    Args:
        recipe_file: Path to YAML recipe file
        
    Returns:
        Exit code (0 for valid, non-zero for invalid)
    """
    try:
        recipe_path = Path(recipe_file)
        
        if not recipe_path.exists():
            print(f"Error: Recipe file not found: {recipe_file}")
            return 1
        
        print(f"Validating recipe: {recipe_file}")
        
        # Try to load the recipe
        pipeline = ExcelPipeline()
        recipe_data = pipeline.load_recipe(recipe_path)
        
        # Get recipe summary
        summary = pipeline.recipe_loader.summary()
        settings = pipeline.recipe_loader.get_settings()
        steps = pipeline.recipe_loader.get_steps()
        
        print("✓ Recipe file is valid")
        print(f"✓ {summary}")
        print(f"✓ Settings: {len(settings)} configured")
        print(f"✓ Steps: {len(steps)} processing steps defined")
        
        # Check processor availability
        from excel_recipe_processor.core.pipeline import check_recipe_capabilities
        capabilities_report = check_recipe_capabilities(recipe_data)
        
        if capabilities_report['recipe_valid']:
            print("✓ All processors are available")
        else:
            print("⚠ Some processors may not be available:")
            for missing in capabilities_report['missing_processors']:
                print(f"  - {missing}")
        
        # Show output filename if specified
        if 'output_filename' in settings:
            print(f"✓ Output filename: {settings['output_filename']}")
        
        return 0 if capabilities_report['recipe_valid'] else 2
        
    except Exception as e:
        print(f"Recipe validation failed: {e}")
        return 1


def get_version() -> str:
    """Get the package version."""
    try:
        from excel_recipe_processor._version import __version__
        return __version__
    except ImportError:
        return "unknown"


def process_file(input_file: str, config: Optional[dict[str, Any]] = None) -> bool:
    """
    Legacy function for backward compatibility.
    
    Args:
        input_file: Path to input file
        config: Optional configuration dictionary
        
    Returns:
        True if successful, False otherwise
    """
    print("Warning: process_file() is deprecated. Use process_excel_file() instead.")
    
    try:
        if config and 'recipe_file' in config:
            result = process_excel_file(
                input_file=input_file,
                recipe_file=config['recipe_file'],
                output_file=config.get('output_file'),
                verbose=config.get('verbose', False)
            )
            return result == 0
        else:
            print("Error: Config must include 'recipe_file'")
            return False
    except Exception as e:
        print(f"Error in process_file: {e}")
        return False
