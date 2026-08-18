"""
Patterns for the strip_formula_caches processor's zip surgery.

excel_recipe_processor/processors/_helpers/strip_formula_caches_rgx.py

Byte-level patterns over worksheet and workbook XML. The cell pattern
captures a WHOLE <c> element (self-closing or paired) with its
attribute string, so every decision is made with the complete element
in hand - attributes, formula, cached value, inline string - and the
rewrite replaces the entire element atomically. Sub-patterns then
operate WITHIN one captured element only, never across elements.

SAFETY DOCTRINE (2026-08-17, researched before building):
- A cell with <f> and <v> is a formula/result dyad: the <v> is
  disposable, Excel recalculates from the <f>. This is the file state
  every openpyxl-authored workbook ships in.
- EXCEPTION, the one data-loss trap prior art documents: a formula
  referencing an EXTERNAL workbook ([1]Sheet!A1) may have its cached
  value as the ONLY copy of that data when the linked file is absent.
  Such cells are REFUSED - reported, never stripped.
- Dynamic/legacy array spills are NOT dyads: only the anchor carries
  <f t="array" ref="...">; member cells are value-only. Members are
  identified by falling inside an in-scope anchor's ref and are
  removed (or blanked in place when they carry a style attribute).
- A value-only cell OUTSIDE every array ref is literal data and is
  NEVER touched. That is the entire safety boundary.
"""

import re


# One whole <c> element: self-closing, or open..close with body.
# Group 'attrs' is the attribute string; group 'body' the inner XML
# (None for self-closing cells).
cell_element_rgx = re.compile(
    r'<c\b(?P<attrs>[^>]*?)(?:/>|>(?P<body>.*?)</c>)',
    re.DOTALL,
)

# Attribute pulls, applied to ONE element's attribute string only.
cell_ref_attr_rgx = re.compile(r'\br="([A-Z]{1,3})(\d+)"')
cell_style_attr_rgx = re.compile(r'\bs="\d+"')
cell_type_attr_rgx = re.compile(r'\s+t="[^"]*"')
cell_vm_attr_rgx = re.compile(r'\s+[cv]m="\d+"')

# Element pulls, applied WITHIN one cell body only.
formula_element_rgx = re.compile(r'<f\b[^>]*(?:/>|>.*?</f>)', re.DOTALL)
value_element_rgx = re.compile(r'<v\b[^>]*(?:/>|>.*?</v>)', re.DOTALL)
inline_string_rgx = re.compile(r'<is\b', re.DOTALL)

# Array anchors: the stored formula declares its spill extent.
array_formula_ref_rgx = re.compile(
    r'<f\b[^>]*\bt="array"[^>]*\bref="([A-Z]{1,3}\d+)(?::([A-Z]{1,3}\d+))?"'
)

# The external-workbook reference marker inside a stored formula:
# [1]Sheet!A1 / '[2]Other Sheet'!B2. Digits in square brackets are the
# workbook index into externalReferences - nothing else in the stored
# grammar uses this shape.
external_workbook_ref_rgx = re.compile(r'\[\d+\]')

# workbook.xml calculation properties.
calc_pr_rgx = re.compile(r'<calcPr\b[^>]*/?>(?:</calcPr>)?')
sheet_data_close_rgx = re.compile(r'</sheetData>')

# calcChain part references in [Content_Types].xml and workbook rels.
calc_chain_override_rgx = re.compile(
    r'<Override[^>]*PartName="/xl/calcChain\.xml"[^>]*/>')
calc_chain_rel_rgx = re.compile(
    r'<Relationship[^>]*Target="calcChain\.xml"[^>]*/>')

# workbook.xml sheet listing and the rels part, parsed
# attribute-order-INDEPENDENTLY: capture each element, then pull the
# attributes separately. (Lesson from the first real-file drill:
# openpyxl writes Relationship attributes in a different order than
# Excel does, and an ordered pattern silently matched nothing -
# turning the whole surgery into a no-op. Never encode attribute
# order into an element pattern.)
workbook_sheet_element_rgx = re.compile(r'<sheet\b[^>]*/?>')
workbook_rel_element_rgx = re.compile(r'<Relationship\b[^>]*/?>')
attr_name_rgx = re.compile(r'\bname="([^"]+)"')
attr_rid_rgx = re.compile(r'\br:id="(rId\d+)"')
attr_id_rgx = re.compile(r'\bId="(rId\d+)"')
attr_target_rgx = re.compile(r'\bTarget="([^"]+)"')

# End of file #
