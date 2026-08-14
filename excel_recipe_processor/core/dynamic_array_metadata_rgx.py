"""
Regex patterns for the dynamic-array metadata pass.

excel_recipe_processor/core/dynamic_array_metadata_rgx.py

Patterns live in their own module so editing the surrounding logic can never
corrupt them.

These patterns operate on RAW WORKSHEET XML, not on formulas the user wrote.
Sheet XML is machine-generated (by openpyxl or Excel), which is what makes
targeted regex surgery defensible here: element shapes are uniform, `<` never
appears unescaped inside text content, and rewriting only the matched cell
elements leaves every other byte of the sheet untouched - the safety property
an XML parse-and-reserialize round trip could not guarantee.
"""

import re


# --------------------------------------------------------------------------
# Function vocabulary
# --------------------------------------------------------------------------

# Functions that POSTDATE dynamic arrays (Excel 365, 2019 onward). A formula
# containing any of these was necessarily authored in dynamic-array-aware
# Excel, so declaring it dynamic-array-aware states a fact and cannot change
# its meaning.
#
# Pre-dynamic-array functions that can also return arrays - INDEX, OFFSET,
# INDIRECT - are DELIBERATELY absent. A legacy formula like
# =INDEX(B2:C5,,F1) relies on implicit intersection to yield one value;
# declaring it dynamic would make it SPILL, silently changing its result.
# The default vocabulary is therefore safe by construction; a caller who
# knows a file's formulas are recipe-authored can extend it per call.
DYNAMIC_ERA_FUNCTIONS = frozenset({
    # Lookup and logic, shipped with or after dynamic arrays
    'XLOOKUP', 'XMATCH', 'LET', 'LAMBDA',

    # The dynamic-array functions themselves
    'FILTER', 'SORT', 'SORTBY', 'UNIQUE', 'SEQUENCE', 'RANDARRAY',

    # Array shaping, Excel 365
    'VSTACK', 'HSTACK', 'TOCOL', 'TOROW',
    'CHOOSECOLS', 'CHOOSEROWS', 'TAKE', 'DROP', 'EXPAND',

    # Text splitting (returns arrays), Excel 365
    'TEXTSPLIT',
})


def build_function_detection_rgx(function_names):
    """
    Compile a pattern matching a call to any named function in stored form.

    Stored formulas may carry the _xlfn. / _xlfn._xlws. prefixes, so the
    pattern accepts an optional prefix chain before the name. The negative
    lookbehind keeps a longer name that merely ends with a shorter one
    (MYSORT) from matching, and the lookahead requires the opening
    parenthesis of an actual call.

    Args:
        function_names: Iterable of bare function names, e.g. {'XLOOKUP'}

    Returns:
        Compiled pattern usable with .search() on stored formula text
    """
    alternation = '|'.join(sorted(re.escape(name) for name in function_names))
    return re.compile(
        r'(?<![A-Za-z0-9_.])(?:_xlfn\.(?:_xlws\.)?)?(' + alternation + r')\s*\('
    )


# --------------------------------------------------------------------------
# Worksheet XML surgery patterns
# --------------------------------------------------------------------------

# One cell element carrying a formula: <c ...><f ...>FORMULA</f>...</c>
# Non-greedy body; safe because '<' cannot appear unescaped in XML content,
# so the first '</c>' genuinely closes this cell.
formula_cell_rgx = re.compile(r'<c\b[^>]*><f\b[^>]*>.*?</c>', re.DOTALL)

# The r="A1" attribute of a cell element
cell_ref_attr_rgx = re.compile(r'\br="([A-Z]{1,3}[0-9]+)"')

# The opening <c ...> tag alone, for attribute insertion
cell_open_tag_rgx = re.compile(r'<c\b[^>]*>')

# The formula element within one cell match: opening tag, text, closing tag
formula_element_rgx = re.compile(r'(<f\b[^>]*>)(.*?)(</f>)', re.DOTALL)

# --------------------------------------------------------------------------
# Package part patterns
# --------------------------------------------------------------------------

# Existing relationship ids in xl/_rels/workbook.xml.rels, to pick a free one
relationship_id_rgx = re.compile(r'\bId="rId([0-9]+)"')

# One <sheet .../> entry in xl/workbook.xml. Attribute order varies between
# producers, so the tag is captured whole and its attributes read separately.
sheet_entry_rgx = re.compile(r'<sheet\b[^>]*/?>')
sheet_name_attr_rgx = re.compile(r'\bname="([^"]*)"')
sheet_rid_attr_rgx = re.compile(r'\br:id="(rId[0-9]+)"')

# One <Relationship .../> entry in a rels part, with its Id and Target
relationship_entry_rgx = re.compile(r'<Relationship\b[^>]*/?>')
relationship_target_attr_rgx = re.compile(r'\bTarget="([^"]*)"')
relationship_id_attr_rgx = re.compile(r'\bId="(rId[0-9]+)"')

# Split a cell reference into column letters and row number
cell_ref_split_rgx = re.compile(r'^([A-Z]{1,3})([0-9]+)$')

# End of file #
