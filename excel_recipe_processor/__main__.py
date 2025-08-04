#!/usr/bin/env python3
"""
Excel Recipe Processor - Command Line Interface

Process data using YAML-defined recipes with import_file and export_file steps.
Supports external variables for dynamic file names and interactive prompting.
"""

import sys
import argparse

from excel_recipe_processor import __version__, __description__
from excel_recipe_processor.core.main import run_main


def create_argument_parser() -> argparse.ArgumentParser:
    """Create and configure the argument parser for the new RecipePipeline system."""
    
    # Comprehensive epilog with usage examples
    epilog_for_argparse = """
examples:
  BASIC RECIPE PROCESSING:
    # Process recipe with external variables from CLI
    %(prog)s recipe.yaml --var batch_id=A47 --var region=west
    
    # Process recipe with interactive prompting for missing variables
    %(prog)s daily_report.yaml
    
    # Combine CLI variables with interactive prompting for others
    %(prog)s report.yaml --var batch_id=A47
    
    # Complex variables with spaces and special characters
    %(prog)s recipe.yaml --var "description=Q4 Sales Report" --var dept=FINANCE

  DEBUGGING AND DEVELOPMENT:
    # Verbose output for debugging recipe execution
    %(prog)s recipe.yaml --var date=20250729 --verbose
    
    # Validate recipe syntax before processing
    %(prog)s --validate-recipe recipe.yaml
    
    # Validate multiple recipes
    %(prog)s --validate-recipe sales.yaml
    %(prog)s --validate-recipe finance.yaml

  SYSTEM INFORMATION:
    # List all available processors
    %(prog)s --list-capabilities
    
    # Detailed processor information
    %(prog)s --list-capabilities --detailed
    
    # Output capabilities in different formats
    %(prog)s --list-capabilities --json
    %(prog)s --list-capabilities --yaml
    %(prog)s --list-capabilities --detailed-yaml
    
    # Feature comparison matrix
    %(prog)s --list-capabilities --matrix
    
    # Save capabilities to files for documentation
    %(prog)s --list-capabilities --json > capabilities.json
    %(prog)s --list-capabilities --yaml > capabilities.yaml

  USAGE EXAMPLES AND HELP:
    # Get examples for specific processor
    %(prog)s --get-usage-examples import_file
    %(prog)s --get-usage-examples export_file
    %(prog)s --get-usage-examples filter_data
    
    # Get examples for all processors
    %(prog)s --get-usage-examples
    
    # Get examples in different formats
    %(prog)s --get-usage-examples import_file --format-examples yaml
    %(prog)s --get-usage-examples export_file --format-examples text
    %(prog)s --get-usage-examples --format-examples json
    
    # Get recipe settings configuration examples
    %(prog)s --get-settings-examples

  ADVANCED SCENARIOS:
    # Process recipe with date-based variables
    %(prog)s monthly.yaml --var month=12 --var year=2024
    
    # Process with multiple batch identifiers
    %(prog)s batch.yaml --var batch_id=A47 --var sub_batch=001
    
    # Process with region-specific settings
    %(prog)s regional.yaml --var region=west --var timezone=PST
    
    # Debug complex recipes with verbose output
    %(prog)s complex.yaml --var env=prod --verbose

  RECIPE EXAMPLES:
    # Simple data processing recipe
    %(prog)s simple_filter.yaml --var input_date=20250729
    
    # Multi-file processing with lookups
    %(prog)s lookup_report.yaml --var quarter=Q4 --var dept=sales
    
    # Automated daily report generation
    %(prog)s daily_report.yaml --var region=west --var format=xlsx

note: External variables can be defined in recipes with validation, defaults, and choices.
      If required variables are missing from CLI, you'll be prompted interactively.
      Use --validate-recipe to check recipe syntax before processing.

For detailed documentation and more examples:
  https://github.com/yourusername/excel-recipe-processor
"""

    parser = argparse.ArgumentParser(
        description=f"{__description__}\n\nProcess data using YAML recipes with dynamic variables and stage-based architecture.",
        prog="excel-recipe-processor",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=epilog_for_argparse
    )
    
    parser.add_argument(
        '--version', 
        action='version', 
        version=f'excel_recipe_processor {__version__}'
    )
    
    # Main recipe file (primary argument)
    parser.add_argument(
        'recipe_file',
        nargs='?',
        metavar='RECIPE.yaml',
        help='YAML recipe file defining processing steps with import_file and export_file processors'
    )
    
    # Variable overrides for dynamic file names
    parser.add_argument(
        '--var',
        action='append',
        dest='variable_overrides',
        metavar='NAME=VALUE',
        help='Override external variable (repeatable). Example: --var batch_id=A47 --var region=west'
    )
    
    # Verbose logging
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose output and debug logging'
    )
    
    # System information commands
    parser.add_argument(
        '--list-capabilities',
        action='store_true',
        help='List all available processors and their capabilities'
    )
    
    parser.add_argument(
        '--detailed',
        action='store_true',
        help='Show detailed capabilities (use with --list-capabilities)'
    )

    parser.add_argument(
        '--json',
        action='store_true',
        help='Output capabilities as JSON (use with --list-capabilities)'
    )

    parser.add_argument(
        '--yaml',
        action='store_true',
        help='Output capabilities as YAML (use with --list-capabilities)'
    )

    parser.add_argument(
        '--detailed-yaml',
        action='store_true',
        help='Show detailed capabilities with YAML listings (use with --list-capabilities)'
    )

    parser.add_argument(
        '--matrix',
        action='store_true',
        help='Show feature matrix (use with --list-capabilities)'
    )

    # Recipe validation
    parser.add_argument(
        '--validate-recipe',
        metavar='RECIPE.yaml',
        help='Validate recipe file syntax and processor availability'
    )

    # Usage examples
    parser.add_argument(
        '--get-usage-examples',
        metavar='PROCESSOR_NAME',
        nargs='?',
        const='all',  # Default value when flag is used without argument
        help='Show usage examples for specific processor or all processors'
    )

    parser.add_argument(
        '--format-examples',
        choices=['yaml', 'text', 'json'],
        default='yaml',
        help='Format for usage examples output (default: yaml)'
    )

    # Settings examples
    parser.add_argument(
        '--get-settings-examples',
        action='store_true',
        help='Show recipe settings configuration examples'
    )

    return parser


def main() -> int:
    """Main entry point for the command line interface."""
    
    parser = create_argument_parser()
    
    # Special case: no arguments shows help instead of error
    if len(sys.argv) == 1:
        parser.print_help()
        return 0
    
    try:
        args = parser.parse_args()
        return run_main(args)
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        return 1
    except Exception as e:
        print(f"Argument parsing error: {e}")
        return 1


if __name__ == '__main__':
    sys.exit(main())
