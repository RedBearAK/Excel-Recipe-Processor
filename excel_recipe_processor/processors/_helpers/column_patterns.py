"""
Regex patterns for column name processing in processors.

excel_recipe_processor/processors/_helpers/column_patterns.py

Contains compiled regex patterns used to identify and process various column name
patterns during configuration generation, particularly for handling unnamed columns
and date conversions that pandas applies during Excel reading.
"""

import re


# Pattern to detect pandas auto-generated unnamed column names
# Matches: "Unnamed: 0", "Unnamed: 123", etc.
unnamed_column_rgx = re.compile(r'^Unnamed:\s*\d+$')

# Pattern to detect pandas-converted datetime strings in column names
# Matches: "2025-08-04 00:00:00", "2024-12-31 23:59:59", etc.
pandas_datetime_rgx = re.compile(r'^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}$')

# Pattern to detect original date formats that might get converted
# Matches: "8/4/2025", "12/31/24", "1-15-2025", etc.
original_date_rgx = re.compile(r'^\d{1,2}[/-]\d{1,2}[/-]\d{2,4}$')

# Pattern for whitespace-only or empty column names
# Matches: "", "   ", "\t\n", etc.
empty_or_whitespace_rgx = re.compile(r'^\s*$')

# Pattern to detect trailing column markers that indicate end of meaningful data
# Matches variations of: "end", "END", "  end  ", "last_col", etc.
trailing_marker_rgx = re.compile(r'^\s*(end|last|final|stop)\s*(_?(col|column))?\s*$', re.IGNORECASE)


# End of file #
