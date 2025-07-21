"""
Entry point for excel_recipe_processor package.
Allows the package to be executed with: python -m excel_recipe_processor
"""

import sys
import argparse

from excel_recipe_processor._version import __version__, __description__


def main():
    """Main entry point for the excel_recipe_processor package."""
    parser = argparse.ArgumentParser(
        description=f"{__description__}\n\nProcess Excel files using YAML recipes for automated data transformation.",
        prog="python -m excel_recipe_processor",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process Excel file with recipe
  python -m excel_recipe_processor data.xlsx --config recipe.yaml
  
  # Specify output file
  python -m excel_recipe_processor data.xlsx --config recipe.yaml --output result.xlsx
  
  # Process specific sheet with verbose logging
  python -m excel_recipe_processor data.xlsx --config recipe.yaml --sheet "Sheet2" --verbose
  
  # List available processors
  python -m excel_recipe_processor --list-capabilities
  
  # Validate a recipe file
  python -m excel_recipe_processor --validate-recipe recipe.yaml
        """
    )
    
    parser.add_argument(
        '--version', 
        action='version', 
        version=f'excel_recipe_processor {__version__}'
    )
    
    # Main input file
    parser.add_argument(
        'input_file',
        nargs='?',
        help='Excel file to process (.xlsx, .xls)'
    )
    
    # Recipe configuration
    parser.add_argument(
        '--config', '-c',
        metavar='RECIPE.yaml',
        help='YAML recipe file defining processing steps'
    )
    
    # Output file
    parser.add_argument(
        '--output', '-o',
        metavar='OUTPUT.xlsx',
        help='Output Excel file (optional if specified in recipe)'
    )
    
    # Input sheet selection
    parser.add_argument(
        '--sheet', '-s',
        metavar='SHEET',
        default=0,
        help='Input sheet name or index (default: first sheet)'
    )
    
    # Output sheet name
    parser.add_argument(
        '--output-sheet',
        metavar='SHEET_NAME',
        default='ProcessedData',
        help='Output sheet name (default: ProcessedData)'
    )
    
    # Verbose logging
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose output and debug logging'
    )
    
    # System information
    parser.add_argument(
        '--list-capabilities',
        action='store_true',
        help='List all available processors and their capabilities'
    )
    
    # Recipe validation
    parser.add_argument(
        '--validate-recipe',
        metavar='RECIPE.yaml',
        help='Validate a recipe file without processing data'
    )
    
    # Parse arguments
    args = parser.parse_args()
    
    # Special case: if no arguments provided, show help
    if len(sys.argv) == 1:
        parser.print_help()
        return 0
    
    # Import main function to avoid circular imports
    try:
        from excel_recipe_processor.core.main import run_main
        return run_main(args)
    except ImportError as e:
        print(f"Error importing main functionality: {e}")
        print("Please check that the excel_recipe_processor package is properly installed.")
        return 1
    except Exception as e:
        print(f"Unexpected error: {e}")
        return 1


if __name__ == '__main__':
    sys.exit(main())
