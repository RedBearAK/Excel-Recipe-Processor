"""
excel_recipe_processor: A Python package
"""

from ._version import __version__, __author__, __email__, __description__

# Add this to ensure processors are registered on import
from excel_recipe_processor.core.pipeline import register_standard_processors
register_standard_processors()


# Package-level imports
# Add your main classes/functions here for easy importing
# from .core.main import MainClass
# from .utils.helpers import helper_function

__all__ = [
    '__version__',
    '__author__',
    '__email__',
    '__description__',
    # Add your exported symbols here
    # 'MainClass',
    # 'helper_function',
]
