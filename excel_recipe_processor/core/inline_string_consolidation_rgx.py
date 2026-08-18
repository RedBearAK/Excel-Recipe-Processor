"""
Patterns for inline-string consolidation at session save.

excel_recipe_processor/core/inline_string_consolidation_rgx.py

openpyxl 3.1+ writes every literal string as an inline string
(t="inlineStr" with an <is><t>...</t></is> body) instead of using the
workbook's shared-string table - a deliberate write-speed trade that
costs file size on repetitive data. On the production VMS sheet the
same customer, species and destination text repeats thousands of
times: measured 35.6 MB of raw sheet XML in openpyxl's dialect against
26.2 MB in Excel's for identical content. These patterns support the
save-time rewrite to Excel's shared-string dialect.

Safety: only cells whose <is> body is a single plain <t> element are
consolidated - anything else (rich-text <r> runs, phonetic data) is
left untouched and counted. The <t> element is reused VERBATIM inside
the new <si>, so escaping and xml:space="preserve" survive
byte-exactly.
"""

import re


# A whole inline-string cell. Group 1/2: attribute text around the
# t="inlineStr" marker; group 3: the full <is>...</is> body.
inline_string_cell_rgx = re.compile(
    r'<c\b([^>]*?)t="inlineStr"([^>]*)>(<is>.*?</is>)</c>',
    re.DOTALL,
)

# The only <is> shape consolidated: exactly one plain <t> element
# (attributes such as xml:space="preserve" allowed), nothing else.
plain_is_body_rgx = re.compile(
    r'^<is>(<t(?:\s[^>]*)?>.*?</t>|<t(?:\s[^>]*)?/>)</is>$',
    re.DOTALL,
)

# Existing shared-string entries, harvested verbatim for the merge.
shared_string_item_rgx = re.compile(r'<si>.*?</si>|<si/>', re.DOTALL)
shared_string_count_rgx = re.compile(r'\bcount="(\d+)"')

# Registration checks.
shared_strings_override_rgx = re.compile(
    r'PartName="/xl/sharedStrings\.xml"')
shared_strings_rel_rgx = re.compile(
    r'Target="sharedStrings\.xml"')
types_close_rgx = re.compile(r'</Types>')
relationships_close_rgx = re.compile(r'</Relationships>')
max_rel_id_rgx = re.compile(r'\bId="rId(\d+)"')

# End of file #
