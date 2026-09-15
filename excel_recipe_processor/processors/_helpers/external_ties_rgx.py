"""
Patterns for the external-ties inventory over xlsx package XML.

excel_recipe_processor/processors/_helpers/external_ties_rgx.py

Shared by audit_external_ties (read-only) and by the later sever /
repair surgery and the tab-transplant pre-flight, so every consumer
finds ties with ONE set of eyes. Byte-level patterns over package
parts; no XML parser, no openpyxl - the file is never loaded into
anything that could rewrite it.

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

# [N] anywhere in stored formula text.
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

# --- workbook.xml -----------------------------------------------------------

workbook_sheet_element_rgx = re.compile(r'<sheet\b[^>]*/?>')
workbook_defined_name_rgx = re.compile(
    r'<definedName\b(?P<attrs>[^>]*)>(?P<text>[^<]*)</definedName>')
external_reference_element_rgx = re.compile(r'<externalReference\b[^>]*/?>')
calc_pr_rgx = re.compile(r'<calcPr\b[^>]*/?>')

# --- relationships (any .rels part) ----------------------------------------

relationship_element_rgx = re.compile(r'<Relationship\b[^>]*/?>')

# --- attribute pulls, applied to ONE element's attribute string ------------

attr_name_rgx = re.compile(r'\bname="([^"]*)"')
attr_id_rgx = re.compile(r'\bId="([^"]+)"')
attr_rid_rgx = re.compile(r'\br:id="([^"]+)"')
attr_ref_rgx = re.compile(r'\bref="([^"]+)"')
attr_type_rgx = re.compile(r'\bType="([^"]+)"')
attr_sheet_rgx = re.compile(r'\bsheet="([^"]*)"')
attr_sqref_rgx = re.compile(r'\bsqref="([^"]+)"')
attr_hidden_rgx = re.compile(r'\bhidden="(1|true)"')
attr_id_lower_rgx = re.compile(r'\bid="([^"]+)"')
attr_type_lower_rgx = re.compile(r'\btype="([^"]+)"')
attr_target_rgx = re.compile(r'\bTarget="([^"]+)"')
attr_target_mode_rgx = re.compile(r'\bTargetMode="([^"]+)"')
attr_local_sheet_id_rgx = re.compile(r'\blocalSheetId="(\d+)"')
attr_refresh_on_load_rgx = re.compile(r'\brefreshOnLoad="(1|true)"')

# --- worksheet parts ----------------------------------------------------------

# A formula element whose text carries [N]. The enclosing cell is found
# by walking back to the nearest '<c ' (cell_open_rgx), which is cheap
# and exact: <f> only ever lives inside <c>.
formula_with_external_rgx = re.compile(
    r'<f\b(?P<attrs>[^>]*)>(?P<text>[^<]*\[\d+\][^<]*)</f>')
cell_open_rgx = re.compile(r'<c\b(?P<attrs>[^>]*?)>')
cell_ref_attr_rgx = re.compile(r'\br="([A-Z]{1,3}\d+)"')
sheet_data_open_rgx = re.compile(r'<sheetData\b[^>]*>')
sheet_data_close_rgx = re.compile(r'</sheetData>')

# Enclosing-element classifiers for [N] found OUTSIDE sheetData. Each
# is matched by name so unknown carriers land in 'unclassified' and
# get reported rather than silently ignored.
sheet_feature_open_rgx = re.compile(
    r'<(?P<tag>conditionalFormatting|cfRule|dataValidation|'
    r'x14:conditionalFormatting|x14:cfRule|x14:dataValidation|'
    r'hyperlink|oleObject|control|definedName|tablePart)\b(?P<attrs>[^>]*)>'
)

# --- external link parts ------------------------------------------------------

external_link_sheet_name_rgx = re.compile(r'<sheetName\b[^>]*\bval="([^"]*)"')
external_link_kind_rgx = re.compile(r'<(externalBook|oleLink|ddeLink)\b')
external_link_cached_data_rgx = re.compile(r'<sheetDataSet\b')

# --- pivot parts -----------------------------------------------------------

worksheet_source_rgx = re.compile(r'<worksheetSource\b[^>]*/?>')
cache_source_rgx = re.compile(r'<cacheSource\b[^>]*/?>')
pivot_location_rgx = re.compile(r'<location\b[^>]*/?>')
pivot_cache_definition_root_rgx = re.compile(r'<pivotCacheDefinition\b[^>]*>')

# --- chart parts and connections --------------------------------------------

chart_formula_rgx = re.compile(r'<c:f>([^<]*)</c:f>')
connection_element_rgx = re.compile(r'<connection\b[^>]*/?>')

# --- part-name classifiers ---------------------------------------------------

worksheet_part_rgx = re.compile(r'^xl/worksheets/sheet\d+\.xml$')
external_link_part_rgx = re.compile(r'^xl/externalLinks/externalLink\d+\.xml$')
pivot_cache_definition_part_rgx = re.compile(
    r'^xl/pivotCache/pivotCacheDefinition\d+\.xml$')
pivot_table_part_rgx = re.compile(r'^xl/pivotTables/pivotTable\d+\.xml$')
chart_part_rgx = re.compile(r'^xl/charts/chart\d+\.xml$')
query_table_part_rgx = re.compile(r'^xl/queryTables/queryTable\d+\.xml$')
embedding_part_rgx = re.compile(r'^xl/embeddings/')

# Hyperlink targets that are web or mail are not ties to another file.
web_or_mail_target_rgx = re.compile(r'^(?:https?://|mailto:)', re.IGNORECASE)


# --- sever surgery patterns (2026-09-14) -----------------------------------

# One whole <c> element, self-closing or paired, for the freeze pass.
cell_element_rgx = re.compile(
    r'<c\b(?P<attrs>[^>]*?)(?:/>|>(?P<body>.*?)</c>)', re.DOTALL)
formula_element_rgx = re.compile(r'<f\b(?P<attrs>[^>]*?)(?:/>|>(?P<text>[^<]*)</f>)')
value_element_rgx = re.compile(r'<v\b[^>]*(?:/>|>.*?</v>)', re.DOTALL)
inline_string_element_rgx = re.compile(r'<is\b[^>]*(?:/>|>.*?</is>)', re.DOTALL)
formula_type_attr_rgx = re.compile(r'\bt="(array|shared|dataTable)"')
formula_si_attr_rgx = re.compile(r'\bsi="(\d+)"')

# Whole carrier elements for the drop policies.
cf_rule_element_rgx = re.compile(r'<cfRule\b[^>]*>.*?</cfRule>', re.DOTALL)
conditional_formatting_element_rgx = re.compile(
    r'<conditionalFormatting\b[^>]*>(?P<body>.*?)</conditionalFormatting>', re.DOTALL)
data_validation_element_rgx = re.compile(
    r'<dataValidation\b[^>]*>.*?</dataValidation>', re.DOTALL)
data_validations_element_rgx = re.compile(
    r'<dataValidations\b(?P<attrs>[^>]*)>(?P<body>.*?)</dataValidations>', re.DOTALL)
attr_count_rgx = re.compile(r'\bcount="\d+"')

# Link plumbing to remove once every [N] is resolved.
external_references_block_rgx = re.compile(
    r'<externalReferences\b[^>]*>.*?</externalReferences>', re.DOTALL)
external_link_rel_type_rgx = re.compile(r'/relationships/externalLink"')
external_link_override_rgx = re.compile(
    r'<Override\b[^>]*PartName="/xl/externalLinks/[^"]+"[^>]*/>')
calc_chain_override_rgx = re.compile(
    r'<Override\b[^>]*PartName="/xl/calcChain\.xml"[^>]*/>')
calc_chain_rel_type_rgx = re.compile(r'/relationships/calcChain"')
full_calc_on_load_rgx = re.compile(r'\bfullCalcOnLoad="[^"]*"')
calc_completed_rgx = re.compile(r'\bcalcCompleted="[^"]*"')
calc_mode_manual_rgx = re.compile(r'\bcalcMode="manual"')
workbook_close_rgx = re.compile(r'</workbook>')

# End of file #
