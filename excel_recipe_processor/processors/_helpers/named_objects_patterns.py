"""
Regex patterns for named objects processing.

excel_recipe_processor/processors/_helpers/named_objects_patterns.py

Contains regex patterns for parsing Excel named ranges, lambda functions, 
formulas, and table definitions. Separated to avoid artifact corruption
during editing operations.
"""

import re


# Lambda Function Patterns
# ========================

# Detect Excel lambda functions with _xlfn prefix
excel_lambda_detection_rgx = re.compile(
    r'_xlfn\.LAMBDA\s*\(',
    re.IGNORECASE
)

# Extract full Excel lambda function definition (simplified for Python compatibility)
excel_lambda_full_rgx = re.compile(
    r'(_xlfn\.LAMBDA\s*\([^)]*(?:\([^)]*\)[^)]*)*\))',
    re.IGNORECASE
)

# Extract lambda parameters from Excel format
excel_lambda_params_rgx = re.compile(
    r'_xlfn\.LAMBDA\s*\(\s*((?:_xlpm\.\w+\s*,\s*)*_xlpm\.\w+)\s*,',
    re.IGNORECASE
)

# Extract individual parameter names from _xlpm format
excel_param_name_rgx = re.compile(
    r'_xlpm\.(\w+)',
    re.IGNORECASE
)

# Extract lambda body (formula part after parameters)
excel_lambda_body_rgx = re.compile(
    r'_xlfn\.LAMBDA\s*\((?:_xlpm\.\w+\s*,\s*)*(_xlpm\.\w+)\s*,\s*(.+)\)\s*$',
    re.IGNORECASE | re.DOTALL
)

# Detect human-readable lambda functions
human_lambda_detection_rgx = re.compile(
    r'LAMBDA\s*\(',
    re.IGNORECASE
)

# Extract human-readable lambda parameters
human_lambda_params_rgx = re.compile(
    r'LAMBDA\s*\(\s*((?:\w+\s*,\s*)*\w+)\s*,',
    re.IGNORECASE
)

# Extract human-readable lambda body
human_lambda_body_rgx = re.compile(
    r'LAMBDA\s*\((?:\w+\s*,\s*)*(\w+)\s*,\s*(.+)\)\s*$',
    re.IGNORECASE | re.DOTALL
)


# Excel Function Prefix Patterns
# ==============================

# Detect _xlfn function prefixes
xlfn_function_rgx = re.compile(
    r'_xlfn\.([A-Z][A-Z0-9]*)',
    re.IGNORECASE
)

# Detect _xlpm parameter prefixes
xlpm_parameter_rgx = re.compile(
    r'_xlpm\.(\w+)',
    re.IGNORECASE
)

# Remove all Excel prefixes for human-readable conversion
excel_prefix_cleanup_rgx = re.compile(
    r'_xl(?:fn|pm)\.',
    re.IGNORECASE
)


# Named Range and Formula Patterns  
# =================================

# Detect Excel range references (absolute and relative)
excel_range_reference_rgx = re.compile(
    r"(?:'([^']+)'|([A-Za-z_][A-Za-z0-9_]*))!"      # Sheet name (quoted or unquoted)
    r"\$?[A-Z]+\$?\d+(?::\$?[A-Z]+\$?\d+)?",        # Cell range
    re.IGNORECASE
)

# Extract sheet name from range reference
sheet_name_from_range_rgx = re.compile(
    r"^(?:'([^']+)'|([A-Za-z_][A-Za-z0-9_]*))!",
    re.IGNORECASE
)

# Detect table references like Table1[Column] or stg_data[Revenue]
table_reference_rgx = re.compile(
    r'([A-Za-z_][A-Za-z0-9_]*)\[([^\]]+)\]'
)

# Extract table name and column from table reference
table_name_column_rgx = re.compile(
    r'([A-Za-z_][A-Za-z0-9_]*)\[([^\]]+)\]'
)

# Detect structured references like [#Headers], [#Data], [#All]
structured_reference_rgx = re.compile(
    r'\[#(Headers|Data|All|Totals|This Row)\]',
    re.IGNORECASE
)


# Object Name Patterns
# ====================

# Valid Excel name pattern (starts with letter/underscore, contains letters/numbers/underscores)
valid_excel_name_rgx = re.compile(
    r'^[A-Za-z_][A-Za-z0-9_]*$'
)

# Detect reserved Excel names that cannot be used
reserved_excel_names_rgx = re.compile(
    r'^(C|R|RC|[A-Z]{1,3}\d+|\d+[A-Z]{1,3})$',
    re.IGNORECASE
)

# Pattern matching for name filters (supports wildcards)
name_pattern_to_regex_rgx = re.compile(
    r'[*?]'  # Contains wildcards that need conversion
)


# Formula Analysis Patterns
# =========================

# Detect function calls in formulas
function_call_rgx = re.compile(
    r'([A-Z][A-Z0-9]*)\s*\(',
    re.IGNORECASE
)

# Extract string literals from formulas (for dependency analysis)
string_literal_rgx = re.compile(
    r'"([^"]*)"'
)

# Detect numeric constants
numeric_constant_rgx = re.compile(
    r'(?<![A-Za-z0-9_])(\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)(?![A-Za-z0-9_])'
)

# Detect named references in formulas (exclude string literals and functions)
named_reference_in_formula_rgx = re.compile(
    r'(?<![A-Za-z0-9_\'"#])([A-Za-z_][A-Za-z0-9_]*)(?![A-Za-z0-9_\'"#\(])'
)


# Scope and Context Patterns
# ==========================

# Extract local sheet ID from defined name
local_sheet_id_rgx = re.compile(
    r'localSheetId["\']?\s*:\s*["\']?(\d+)["\']?',
    re.IGNORECASE
)

# Validate absolute coordinate format
absolute_coordinate_rgx = re.compile(
    r'^\$[A-Z]+\$\d+(?::\$[A-Z]+\$\d+)?$',
    re.IGNORECASE
)

# Extract coordinate parts for processing
coordinate_parts_rgx = re.compile(
    r'\$?([A-Z]+)\$?(\d+)',
    re.IGNORECASE
)


# Table Definition Patterns
# =========================

# Extract table range definition
table_range_definition_rgx = re.compile(
    r'([A-Za-z_][A-Za-z0-9_]*)!\$([A-Z]+)\$(\d+):\$([A-Z]+)\$(\d+)',
    re.IGNORECASE
)

# Validate table display name
table_display_name_rgx = re.compile(
    r'^[A-Za-z_][A-Za-z0-9_]*$'
)

# Extract table style name
table_style_name_rgx = re.compile(
    r'^TableStyle(Light|Medium|Dark)\d+$|^None$',
    re.IGNORECASE
)


# YAML Format Patterns
# ====================

# Validate YAML object types for import
valid_object_type_rgx = re.compile(
    r'^(range|constant|formula|lambda|table)$',
    re.IGNORECASE
)

# Validate scope values
valid_scope_rgx = re.compile(
    r'^(global|local)$',
    re.IGNORECASE
)

# Extract multiline formula definitions from YAML
yaml_multiline_formula_rgx = re.compile(
    r'^\s*(.+?)\s*$',
    re.MULTILINE | re.DOTALL
)


# Utility Patterns
# ================

# Clean whitespace from formulas
formula_whitespace_cleanup_rgx = re.compile(
    r'\s+',
    re.MULTILINE
)

# Detect potentially problematic characters in names
problematic_name_chars_rgx = re.compile(
    r'[^\w]'
)

# Extract version numbers from metadata
version_number_rgx = re.compile(
    r'(\d+)\.(\d+)(?:\.(\d+))?'
)


# End of file #
