"""
Regex patterns for formula injection.

excel_recipe_processor/processors/_helpers/inject_formulas_rgx.py

Patterns live in their own module so editing the surrounding logic can never
corrupt them.

A formula may name a column instead of lettering it:

    =IF({col:Test Dest}2=1, XLOOKUP({col:Destination}2, rng_EXdest, rng_country), "")

The placeholder is replaced with that column's letter, read from the sheet's
header row at injection time, so inserting a column upstream cannot silently
repoint a formula at its neighbour.
"""

import re


# {col:Header Name} - the header may contain spaces and punctuation, so the
# pattern stops at the closing brace rather than at whitespace.
column_placeholder_rgx = re.compile(r'\{col:([^}]+)\}')

# End of file #
