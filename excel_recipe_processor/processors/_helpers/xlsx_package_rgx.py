"""
Package-level patterns for xlsx (OOXML) surgery over raw part text.

excel_recipe_processor/processors/_helpers/xlsx_package_rgx.py

Shared by every zip-level processor (external-ties audit and sever,
worksheet transplant) so the grammar of a cell, a formula, a
relationship or a calcPr lives in ONE place. Tie-specific patterns
([N] references, externalLink parts) stay in external_ties_rgx.py.

DOCTRINE:
- Element patterns capture a WHOLE element with its attribute string;
  attribute pulls then run on that string alone. Attribute order is
  NEVER encoded into an element pattern (openpyxl and Excel order
  Relationship attributes differently, and an ordered pattern silently
  matched nothing during the strip drill).
- These are byte-level patterns over package parts; no XML parser, no
  openpyxl - the file is never loaded into anything that could rewrite
  it as a side effect.
"""

import re


# --- workbook.xml --------------------------------------------------------

workbook_sheet_element_rgx = re.compile(r'<sheet\b[^>]*/?>')
workbook_defined_name_rgx = re.compile(
    r'<definedName\b(?P<attrs>[^>]*)>(?P<text>[^<]*)</definedName>')
calc_pr_rgx = re.compile(r'<calcPr\b[^>]*/?>')
workbook_close_rgx = re.compile(r'</workbook>')
full_calc_on_load_rgx = re.compile(r'\bfullCalcOnLoad="[^"]*"')
calc_completed_rgx = re.compile(r'\bcalcCompleted="[^"]*"')
calc_mode_manual_rgx = re.compile(r'\bcalcMode="manual"')

# --- relationships (any .rels part) and content types --------------------

relationship_element_rgx = re.compile(r'<Relationship\b[^>]*/?>')
calc_chain_override_rgx = re.compile(
    r'<Override\b[^>]*PartName="/xl/calcChain\.xml"[^>]*/>')
calc_chain_rel_type_rgx = re.compile(r'/relationships/calcChain"')
web_or_mail_target_rgx = re.compile(r'^(?:https?://|mailto:)', re.IGNORECASE)

# --- attribute pulls, applied to ONE element's attribute string ----------

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
attr_count_rgx = re.compile(r'\bcount="\d+"')

# --- worksheet parts: cells and formulas ---------------------------------

sheet_data_open_rgx = re.compile(r'<sheetData\b[^>]*>')
sheet_data_close_rgx = re.compile(r'</sheetData>')
cell_open_rgx = re.compile(r'<c\b(?P<attrs>[^>]*?)>')
cell_ref_attr_rgx = re.compile(r'\br="([A-Z]{1,3}\d+)"')
cell_element_rgx = re.compile(
    r'<c\b(?P<attrs>[^>]*?)(?:/>|>(?P<body>.*?)</c>)', re.DOTALL)
formula_element_rgx = re.compile(r'<f\b(?P<attrs>[^>]*?)(?:/>|>(?P<text>[^<]*)</f>)')
value_element_rgx = re.compile(r'<v\b[^>]*(?:/>|>.*?</v>)', re.DOTALL)
inline_string_element_rgx = re.compile(r'<is\b[^>]*(?:/>|>.*?</is>)', re.DOTALL)
formula_type_attr_rgx = re.compile(r'\bt="(array|shared|dataTable)"')
formula_si_attr_rgx = re.compile(r'\bsi="(\d+)"')

# --- worksheet parts: conditional formatting and data validation ---------

cf_rule_element_rgx = re.compile(r'<cfRule\b[^>]*>.*?</cfRule>', re.DOTALL)
conditional_formatting_element_rgx = re.compile(
    r'<conditionalFormatting\b[^>]*>(?P<body>.*?)</conditionalFormatting>', re.DOTALL)
data_validation_element_rgx = re.compile(
    r'<dataValidation\b[^>]*>.*?</dataValidation>', re.DOTALL)
data_validations_element_rgx = re.compile(
    r'<dataValidations\b(?P<attrs>[^>]*)>(?P<body>.*?)</dataValidations>', re.DOTALL)

# --- pivot, chart and connection parts -----------------------------------

worksheet_source_rgx = re.compile(r'<worksheetSource\b[^>]*/?>')
cache_source_rgx = re.compile(r'<cacheSource\b[^>]*/?>')
pivot_location_rgx = re.compile(r'<location\b[^>]*/?>')
pivot_cache_definition_root_rgx = re.compile(r'<pivotCacheDefinition\b[^>]*>')
chart_formula_rgx = re.compile(r'<c:f>([^<]*)</c:f>')
connection_element_rgx = re.compile(r'<connection\b[^>]*/?>')

# --- part-name classifiers -----------------------------------------------

worksheet_part_rgx = re.compile(r'^xl/worksheets/sheet\d+\.xml$')
pivot_cache_definition_part_rgx = re.compile(
    r'^xl/pivotCache/pivotCacheDefinition\d+\.xml$')
pivot_table_part_rgx = re.compile(r'^xl/pivotTables/pivotTable\d+\.xml$')
chart_part_rgx = re.compile(r'^xl/charts/chart\d+\.xml$')
query_table_part_rgx = re.compile(r'^xl/queryTables/queryTable\d+\.xml$')
embedding_part_rgx = re.compile(r'^xl/embeddings/')

# End of file #
