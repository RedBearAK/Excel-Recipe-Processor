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

# A function call in a formula: a name followed by an opening parenthesis.
#
# The negative lookbehind for a dot or a word character keeps an
# already-prefixed name (_xlfn.IFS) and a longer name that merely ends with
# a shorter one (MYIFS) from matching. String literals containing a
# function-name-plus-paren would be rewritten too, which is why the
# substitution is limited to names in the known-function map rather than
# applied to every identifier.
function_call_rgx = re.compile(r'(?<![A-Za-z0-9_.])([A-Za-z][A-Za-z0-9_.]*)\s*\(')

# End of file #
