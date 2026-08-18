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

from excel_recipe_processor.processors._helpers.inject_formulas_rgx import (
    function_call_rgx,
    string_literal_rgx,
    eta_reference_rgx,
    spill_reference_rgx,
)


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

    # Grouping aggregators, Excel 365 (2024). Their aggregation argument
    # is an eta-reduced lambda (bare SUM) which Excel STORES with an
    # _xleta. prefix - handled by transform_storage_forms below, NOT by
    # this call-form map. (Harvested from real Excel output 2026-08-14;
    # see dev_notes/NOTES_spill_storage_forms.md.)
    'GROUPBY':      '_xlfn.',
    'PIVOTBY':      '_xlfn.',
    'PERCENTOF':    '_xlfn.',

    # Lambda-helper family. MAP and REDUCE harvested from real Excel
    # output (2026-08-14, test-named-lambdas-lets.xlsx: _xlfn.MAP,
    # _xlfn.REDUCE); the siblings ride the xlsxwriter differential test,
    # which verifies every map entry against an Excel-validated table.
    'MAP':          '_xlfn.',
    'REDUCE':       '_xlfn.',
    'SCAN':         '_xlfn.',
    'BYROW':        '_xlfn.',
    'BYCOL':        '_xlfn.',
    'MAKEARRAY':    '_xlfn.',

    # The stored name behind spilled-range references (A1# display form).
    # transform_storage_forms emits it when rewriting '#'; listed here so
    # a recipe writing ANCHORARRAY(...) explicitly is prefixed too, and
    # so grammar audits know the name is legitimate.
    'ANCHORARRAY':  '_xlfn.',
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
    never fire). String literals are never touched (string-blindness
    here was latent until 2026-08-14, masked by the LAMBDA guard that
    refused such formulas before prefixing could reach them).
    """
    def substitute(match):
        name = match.group(1)
        prefix = FUTURE_FUNCTION_PREFIXES.get(name.upper())
        if prefix is None:
            return match.group(0)
        return match.group(0).replace(name, f"{prefix}{name}", 1)

    return apply_outside_strings(
        formula,
        lambda segment: function_call_rgx.sub(substitute, segment)
    )


def apply_outside_strings(formula: str, transform) -> str:
    """
    Apply a text transform to the parts of a formula OUTSIDE string
    literals, so "text like Z1# or SUM" can never be rewritten. The
    split preserves the literals verbatim, "" escapes included.
    """
    pieces = []
    last_end = 0
    for match in string_literal_rgx.finditer(formula):
        pieces.append(transform(formula[last_end:match.start()]))
        pieces.append(match.group(0))
        last_end = match.end()
    pieces.append(transform(formula[last_end:]))
    return ''.join(pieces)


def transform_storage_forms(formula: str) -> str:
    """
    Rewrite display syntax into the forms Excel actually STORES, beyond
    the simple call-name prefixes. Both harvested verbatim from real
    Excel output (2026-08-14, data-validation-test.xlsx):

    - Spilled-range references lose the '#' and gain ANCHORARRAY:
        SUM(D1#)            -> SUM(_xlfn.ANCHORARRAY(D1))
      A stored literal '#' is invalid and triggers Excel's repair.

    - Eta-reduced lambda references (bare aggregation names in value
      position) gain the _xleta. prefix:
        GROUPBY(a, b, SUM)  -> _xlfn.GROUPBY(a, b, _xleta.SUM)
      (the GROUPBY call itself is prefixed by prefix_future_functions).

    LAMBDA/LET declared names are NOT this function's business: the
    xlpm_name_storage transformer handles them, and it must run FIRST
    (the 2026-08-14 fail-loud guard that lived here is retired - the
    capability replaced the refusal).

    String literals are never touched. Idempotent: already-transformed
    text contains no bare '#' references or unprefixed eta names.
    """
    def rewrite(segment: str) -> str:
        segment = spill_reference_rgx.sub(r'_xlfn.ANCHORARRAY(\1)', segment)
        segment = eta_reference_rgx.sub(r'_xleta.\1', segment)
        return segment

    return apply_outside_strings(formula, rewrite)


# End of file #
