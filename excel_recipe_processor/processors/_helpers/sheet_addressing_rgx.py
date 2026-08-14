"""
Regex for sheet-index pseudo-name tokens.

excel_recipe_processor/processors/_helpers/sheet_addressing_rgx.py

?sheet_001? addresses a tab by 1-based position. The '?' delimiters come
from Excel's own forbidden-character set (\ * ? : / [ ]), so the token
namespace is STRUCTURALLY disjoint from every real tab name in every
workbook from any source - openpyxl refuses to create such a title.
Case-insensitive; 1-4 digits accepted; three-digit zero-padding is the
documented convention (255-sheet soft limit, up to 9999 supported).
"""

import re


sheet_index_token_rgx = re.compile(r'^\?sheet_(\d{1,4})\?$', re.IGNORECASE)

# End of file #
