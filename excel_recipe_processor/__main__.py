"""
Entry point for excel_recipe_processor package.
Allows the package to be executed with: python -m excel_recipe_processor
"""

import sys
import argparse
from ._version import __version__, __description__


def main():
    """Main entry point for the excel_recipe_processor package."""
    parser = argparse.ArgumentParser(
        description=__description__,
        prog="excel_recipe_processor"
    )
    
    parser.add_argument(
        '--version', 
        action='version', 
        version=f'excel_recipe_processor {__version__}'
    )
    
    # Add your main arguments here
    parser.add_argument(
        'input_file',
        nargs='?',
        help='Input file to process'
    )
    
    parser.add_argument(
        '--config', '-c',
        help='Configuration file path'
    )
    
    parser.add_argument(
        '--output', '-o',
        help='Output file path'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose output'
    )
    
    args = parser.parse_args()
    
    # Import your main function here to avoid circular imports
    try:
        from .core.main import run_main
        return run_main(args)
    except ImportError:
        print(f"Main functionality not yet implemented for {pkg_name}")
        print("Please implement the main function in {pkg_name}/core/main.py")
        return 1


if __name__ == '__main__':
    sys.exit(main())
