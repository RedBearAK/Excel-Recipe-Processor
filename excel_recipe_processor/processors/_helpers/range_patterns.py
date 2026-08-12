"""
Regex patterns for Excel range addressing and defined-name validation.

excel_recipe_processor/processors/_helpers/range_patterns.py

Contains compiled regex patterns used to recognize Excel column references,
cell references, and range addresses, and to validate defined names against
both Excel's own rules and this project's stricter house style.

Note that 'excel_column_ref_rgx' was moved here from 'formula_patterns' so that
all range addressing patterns live in one place.
"""

import re


# ============================================================================
# Column, cell, and range references
# ============================================================================

# Bare Excel column reference, uppercase only.
# Matches: "A", "B", "Z", "AA", "AB", "ZZ", "AAA"
# Rejects: "1", "A1", "12A", "a", "Total_Amount", ""
excel_column_ref_rgx = re.compile(r'^[A-Z]{1,3}$')

# Single cell reference with optional absolute markers.
# Matches: "A1", "$A$1", "$A1", "A$1", "XFD1048576"
cell_ref_rgx = re.compile(r'^\$?[A-Z]{1,3}\$?\d{1,7}$')

# Two-cell range address with optional absolute markers.
# Matches: "B4:E856", "$B$4:$E$856", "A1:A100"
range_ref_rgx = re.compile(r'^\$?[A-Z]{1,3}\$?\d{1,7}:\$?[A-Z]{1,3}\$?\d{1,7}$')

# Whole-column range.
# Matches: "C:C", "$C:$C", "B:E", "$B:$E"
full_col_ref_rgx = re.compile(r'^\$?[A-Z]{1,3}:\$?[A-Z]{1,3}$')

# Whole-row range.
# Matches: "4:4", "$4:$4", "1:100"
full_row_ref_rgx = re.compile(r'^\$?\d{1,7}:\$?\d{1,7}$')

# Multi-area reference, which XLOOKUP and VLOOKUP both reject.
# Matches anything containing a comma outside of a function call.
multi_area_ref_rgx = re.compile(r',')


# ============================================================================
# Sheet name quoting
# ============================================================================

# Sheet names need single quotes in a reference unless they consist only of
# letters, digits, underscores and periods, and do not start with a digit.
# Searches rather than matches, so any offending character triggers quoting.
# Example needing quotes: "Region-Carrier", "Q4 Sales", "2026 Data"
sheet_name_needs_quotes_rgx = re.compile(r'[^A-Za-z0-9_.]|^\d')


# ============================================================================
# Defined name validation - Excel's own rules
# ============================================================================

# Excel defined-name character legality.
# First character must be a letter, underscore, or backslash. Remaining
# characters may be letters, digits, periods, underscores, or backslashes.
# No spaces anywhere. Maximum total length is 255 characters.
excel_legal_name_rgx = re.compile(r'^[A-Za-z_\\][A-Za-z0-9_.\\]{0,254}$')

# Names Excel rejects because they resolve as cell references.
# Deliberately case-insensitive and looser than cell_ref_rgx above, since
# Excel rejects "q1" and "TAX24" just as firmly as it rejects "A1".
cellref_like_name_rgx = re.compile(r'^\$?[A-Za-z]{1,3}\$?\d{1,7}$', re.IGNORECASE)

# Bare row/column shorthand that Excel reserves.
# Matches: "R", "r", "C", "c"
bare_rc_name_rgx = re.compile(r'^[RC]$', re.IGNORECASE)

# R1C1-style references that Excel also reserves.
# Matches: "R1C1", "RC", "R12C4", "r1c1"
r1c1_name_rgx = re.compile(r'^R\d*C\d*$', re.IGNORECASE)


# ============================================================================
# Defined name validation - house style
# ============================================================================

# House rule: every digit must be preceded by an underscore, a period, or
# another digit. This guarantees a name can never be read as a cell reference
# even when its leading text falls in the A-XFD column range.
# Flags: "TAX24", "Q1", "rng_PID2026"
# Allows: "TAX_24", "Q_1", "rng_PID_2026", "rng_v1_2" (digits after "_")
unseparated_digit_rgx = re.compile(r'(?<![_.\d])\d')

# House rule: minimum length of three characters, so that a single letter
# followed by a separated digit ("Q_1") is the shortest permitted form.
MINIMUM_NAME_LENGTH = 3

# Project convention prefix. Not enforced by the validator, but supplied here
# so recipes and processors can share one definition.
CONVENTIONAL_NAME_PREFIX = 'rng_'


# End of file #
