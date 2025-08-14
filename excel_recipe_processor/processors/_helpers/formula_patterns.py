"""
Regex patterns for formula transplant operations.

excel_recipe_processor/processors/_helpers/formula_patterns.py
"""

import re


# Pattern to detect Excel column references (A, B, AA, AB, etc.)
excel_column_ref_rgx = re.compile(r'^[A-Z]{1,3}$')

# Pattern to validate Excel cell coordinates (A1, B5, AA10, etc.)  
excel_cell_coord_rgx = re.compile(r'^[A-Z]{1,3}\d+$')

# Pattern to validate Excel range references (A1:B5, A:A, 1:1, etc.)
excel_range_ref_rgx = re.compile(r'^[A-Z]{1,3}\d*:[A-Z]{1,3}\d*$')


# End of file #
