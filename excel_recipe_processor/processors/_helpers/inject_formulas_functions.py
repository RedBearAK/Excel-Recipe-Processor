"""
Excel "future function" name prefixes for formula injection.

excel_recipe_processor/processors/_helpers/inject_formulas_functions.py

Functions added to Excel after the 2007 file format was standardised must be
STORED with an _xlfn. prefix, even though the user never sees it: Excel
writes _xlfn.XLOOKUP into the file and displays XLOOKUP in the formula bar.
A tool that writes the plain name produces a file where Excel does not
recognise the function - it shows #NAME? and renders the formula with an
implicit-intersection marker, as in:

    =@IFS(AND(BB2="Export", ...))

so a recipe can write ordinary Excel syntax and have it stored correctly.

A handful of dynamic-array functions take a further _xlws sub-prefix; those
are listed explicitly rather than derived, because the rule is not
predictable from the name.

The map is deliberately extensible: if a formula ever comes back as #NAME?
with a function not listed here, add it. Everything absent from the map is
left exactly as written, so older functions (SUM, COUNTIF, VLOOKUP) and any
name this module has not heard of pass through untouched.
"""

from excel_recipe_processor.processors._helpers.inject_formulas_rgx import function_call_rgx


# Function name -> the prefix it must carry when stored in the file.
FUTURE_FUNCTION_PREFIXES = {
    # Logical and lookup, Excel 2016 / 365
    'IFS':          '_xlfn.',
    'SWITCH':       '_xlfn.',
    'XLOOKUP':      '_xlfn.',
    'XMATCH':       '_xlfn.',
    'LET':          '_xlfn.',
    'LAMBDA':       '_xlfn.',

    # Text
    'TEXTJOIN':     '_xlfn.',
    'CONCAT':       '_xlfn.',
    'TEXTBEFORE':   '_xlfn.',
    'TEXTAFTER':    '_xlfn.',
    'TEXTSPLIT':    '_xlfn.',

    # Conditional aggregates
    'MAXIFS':       '_xlfn.',
    'MINIFS':       '_xlfn.',

    # Dynamic arrays. FILTER and SORT carry the worksheet sub-prefix; the
    # others do not, which is why this is a lookup rather than a rule.
    'FILTER':       '_xlfn._xlws.',
    'SORT':         '_xlfn._xlws.',
    'SORTBY':       '_xlfn.',
    'UNIQUE':       '_xlfn.',
    'SEQUENCE':     '_xlfn.',
    'RANDARRAY':    '_xlfn.',

    # Array shaping, Excel 365
    'VSTACK':       '_xlfn.',
    'HSTACK':       '_xlfn.',
    'TOCOL':        '_xlfn.',
    'TOROW':        '_xlfn.',
    'CHOOSECOLS':   '_xlfn.',
    'CHOOSEROWS':   '_xlfn.',
    'TAKE':         '_xlfn.',
    'DROP':         '_xlfn.',
    'EXPAND':       '_xlfn.',

    # Conversion
    'ARRAYTOTEXT':  '_xlfn.',
    'VALUETOTEXT':  '_xlfn.',
    'ISOMITTED':    '_xlfn.',
}

def prefix_future_functions(formula: str) -> str:
    """
    Give post-2007 Excel functions the storage prefix they require.

    Excel stores XLOOKUP as _xlfn.XLOOKUP and displays it as XLOOKUP; a
    plain name is read as an unknown defined name (#NAME?). Only names in
    the prefix map are touched, and already-prefixed names are left alone,
    so re-running is safe. Shared by inject_formulas (cell formulas) and
    conditional_format (rule formulas - openpyxl stores those verbatim, so
    an unprefixed modern function there would make the rule silently
    never fire).
    """
    def substitute(match):
        name = match.group(1)
        prefix = FUTURE_FUNCTION_PREFIXES.get(name.upper())
        if prefix is None:
            return match.group(0)
        return match.group(0).replace(name, f"{prefix}{name}", 1)

    return function_call_rgx.sub(substitute, formula)


# End of file #
