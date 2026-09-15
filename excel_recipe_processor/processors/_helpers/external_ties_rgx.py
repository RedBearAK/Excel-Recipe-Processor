"""
Patterns for the external-ties inventory over xlsx package XML.

excel_recipe_processor/processors/_helpers/external_ties_rgx.py

Shared by audit_external_ties (read-only) and sever_external_ties, so
every consumer finds ties with ONE set of eyes. Only the [N] grammar
and the externalLink plumbing live here; the general package grammar
(cells, formulas, relationships, calcPr, part classifiers) is in
xlsx_package_rgx.py and is imported by name, never copied.

DOCTRINE (2026-09-14):
- Element patterns capture a WHOLE element with its attribute string;
  attribute pulls then run on that string alone. Attribute order is
  NEVER encoded into an element pattern (lesson from the strip drill:
  openpyxl and Excel order Relationship attributes differently, and an
  ordered pattern silently matched nothing).
- The stored grammar marks an external workbook as [N] where N is the
  1-based position in workbook.xml <externalReferences>. Nothing else
  in a stored formula, defined name, CF rule or DV formula uses digits
  in square brackets, so external_index_rgx is the universal tripwire
  and the finer patterns only classify what it caught.
"""

import re


# --- the universal tripwire -----------------------------------------------

external_index_rgx = re.compile(r'\[(\d+)\]')

# A sheet-qualified external reference, quoted or bare, up to the '!':
#   '[1]Some Sheet'!A1    '[1]Sheet1:Sheet3'!A1    [1]Sheet!A1    [1]!Name
# Quoted sheet names escape an apostrophe as ''.
external_sheet_ref_rgx = re.compile(
    r"(?:'\[(?P<qidx>\d+)\](?P<qsheet>(?:[^']|'')*)'"
    r"|\[(?P<idx>\d+)\](?P<sheet>[^!'\s,()+*/^&=<>]*))!"
)

# A workbook-level external name: [N]!Name (no sheet). Handled before the
# sheet pattern so the empty-sheet form never reaches it.
external_name_ref_rgx = re.compile(r'\[(\d+)\]!([A-Za-z_\\][A-Za-z0-9_.]*)')

# --- workbook.xml plumbing ---------------------------------------------------

external_reference_element_rgx = re.compile(r'<externalReference\b[^>]*/?>')
external_references_block_rgx = re.compile(
    r'<externalReferences\b[^>]*>.*?</externalReferences>', re.DOTALL)
external_link_rel_type_rgx = re.compile(r'/relationships/externalLink"')
external_link_override_rgx = re.compile(
    r'<Override\b[^>]*PartName="/xl/externalLinks/[^"]+"[^>]*/>')

# --- worksheet parts ----------------------------------------------------------

# A formula element whose text carries [N]. The enclosing cell is found
# by walking back to the nearest '<c ' (cell_open_rgx), which is cheap
# and exact: <f> only ever lives inside <c>.
formula_with_external_rgx = re.compile(
    r'<f\b(?P<attrs>[^>]*)>(?P<text>[^<]*\[\d+\][^<]*)</f>')

# Enclosing-element classifiers for [N] found OUTSIDE sheetData. Each
# is matched by name so unknown carriers land in 'unclassified' and
# get reported rather than silently ignored.
sheet_feature_open_rgx = re.compile(
    r'<(?P<tag>conditionalFormatting|cfRule|dataValidation|'
    r'x14:conditionalFormatting|x14:cfRule|x14:dataValidation|'
    r'hyperlink|oleObject|control|definedName|tablePart)\b(?P<attrs>[^>]*)>'
)

# --- external link parts ------------------------------------------------------

external_link_part_rgx = re.compile(r'^xl/externalLinks/externalLink\d+\.xml$')
external_link_sheet_name_rgx = re.compile(r'<sheetName\b[^>]*\bval="([^"]*)"')
external_link_kind_rgx = re.compile(r'<(externalBook|oleLink|ddeLink)\b')
external_link_cached_data_rgx = re.compile(r'<sheetDataSet\b')

# End of file #
