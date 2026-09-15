"""
Patterns for infer_column_types.

excel_recipe_processor/processors/_helpers/infer_column_types_rgx.py
"""

import re


# a plain number as an export writes it: optional sign, digits with an
# optional fractional part (a leading '.' as in .000000 is how some exports
# write zero), optional exponent. No thousands separators, no currency,
# no percent - those are text until a person says otherwise.
numeric_text_rgx = re.compile(r'[+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?')

# what makes a numeric value a DECIMAL rather than a whole number
decimal_marker_rgx = re.compile(r'[.eE]')


# End of file #
