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

# An Excel string literal, "" being the escaped quote. Storage transforms
# split the formula on these and rewrite only the segments BETWEEN them,
# so text like "See tab Z1#" or "use SUM here" can never be mangled.
string_literal_rgx = re.compile(r'"(?:[^"]|"")*"')

# A spilled-range reference: cell ref (optionally sheet-prefixed) or a
# defined name, wearing the trailing '#'. Excel STORES these wrapped in
# _xlfn.ANCHORARRAY(...) - a literal '#' in a stored formula is invalid
# and triggers Excel's repair dialog (harvested 2026-08-14 from real
# Excel output; see dev_notes/NOTES_spill_storage_forms.md).
# Matches: "Z1#", "$Z$1#", "Lookups!$Z$2#", "'Look Ups'!A2#", "rng_pick#"
spill_reference_rgx = re.compile(
    r"((?:(?:'[^']+'|[A-Za-z_][A-Za-z0-9_.]*)!)?"
    r"(?:\$?[A-Z]{1,3}\$?\d{1,7}|[A-Za-z_][A-Za-z0-9_.]+))#"
)

# A bare aggregation-function name in value position (NOT followed by an
# opening parenthesis): an eta-reduced lambda reference, as in
# GROUPBY(a, b, SUM). Excel STORES these with an _xleta. prefix. The
# lookbehind skips names already carrying a prefix; the lookahead skips
# ordinary calls like SUM(...).
eta_reference_rgx = re.compile(
    r'(?<![A-Za-z0-9_.])'
    r'(SUM|AVERAGE|MEDIAN|COUNT|COUNTA|MAX|MIN|PRODUCT'
    r'|STDEV\.S|STDEV\.P|VAR\.S|VAR\.P|MODE\.SNGL'
    r'|PERCENTOF|ARRAYTOTEXT|CONCAT)'
    r'(?![A-Za-z0-9_.]|\s*\()'
)

# A LAMBDA call. Its parameters must be STORED with an _xlpm. prefix
# (declaration and every body occurrence), which this injector does not
# yet implement - a stored bare parameter is grammatically invalid and
# Excel's repair strips the whole formula. Live formulas containing this
# fail loud instead.
lambda_call_rgx = re.compile(r'(?<![A-Za-z0-9_.])LAMBDA\s*\(')

# End of file #
