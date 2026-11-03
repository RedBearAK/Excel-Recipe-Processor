"""
Regex patterns for the variable-template syntax.

excel_recipe_processor/core/variable_substitution_rgx.py

Companion to core/variable_substitution.py, following the house
convention of keeping regex patterns in dedicated _rgx modules.

THE DELIMITER (2026-11-02): every placeholder is written with DOUBLED
braces - {{name}}, {{str:name}}, {{list_str:name}}, {{date:YYYYMMDD}},
{{col:Header Name}}. Until this date a single pair was the delimiter,
which collided with everything else that uses braces in a recipe
string: a regex quantifier (\\d{2,4}), an Excel array constant
({1,2,3}), a Python dict in a pandas expression - and there was no
escape. Double braces never occur in a regex, an Excel formula or
Python source, so the collision is gone and nothing needs escaping. A
single brace is now ordinary text.

VERBATIM NAMES: the text after the colon in {{col:...}} is taken
exactly as written, up to the closing }} - no trimming, no quoting, so
a header with leading or trailing whitespace is referenced by typing
it (the same rule as Excel's own Table1[ Header ]). Padding inside the
delimiters ({{ col:X }}) is therefore a typo, and is refused.

RETIRED FORM: the single-brace token is detected and refused with the
doubled form named, so a recipe (or a file the sweep missed) fails at
--validate instead of passing '{col:X}' through as literal text.
"""

import re


# ---- the live grammar -----------------------------------------------------

# {{type:name}} - a typed variable; the whole string, or inside a string
TYPED_VARIABLE_RGX = re.compile(r'\{\{(\w+):(\w+)\}\}')

# {{name}} - an untyped (string) variable
SIMPLE_VARIABLE_RGX = re.compile(r'\{\{([a-zA-Z_]\w*)\}\}')

# {{date:FMT}} / {{time:FMT}} - and the general {{word:anything}} shape the
# formatted pass walks; the value runs to the closing }} and may not
# contain a brace
FORMATTED_VARIABLE_RGX = re.compile(r'\{\{(\w+):([^{}]+)\}\}')

# {{col:Header}} - the column reference consumed downstream by processors;
# the name is VERBATIM (group 1 is not stripped here)
COLUMN_PLACEHOLDER_RGX = re.compile(r'\{\{col:([^{}]+)\}\}')

TOKEN_OPEN = '{{'
TOKEN_CLOSE = '}}'


# ---- typo and retired-form detection -------------------------------------

# A single-brace token of any of the old shapes: {name}, {type:name},
# {date:FMT}, {col:Header}. The lookarounds exclude the inner brace of a
# doubled token. A regex quantifier {2,4} or {3} and an Excel array
# {1,2,3} start with a digit, a dict literal {'a': 1} with a quote, so
# none of them match: the shape is identifier-first.
RETIRED_SINGLE_BRACE_RGX = re.compile(r'(?<!\{)\{(?!\{)([a-zA-Z_]\w*)(:[^{}]*)?\}(?!\})')

# Opened doubled, closed single - {{name} - or the reverse - {name}}
MISMATCHED_OPEN_RGX = re.compile(r'\{\{(?!\{)[^{}]*\}(?!\})')
MISMATCHED_CLOSE_RGX = re.compile(r'(?<!\{)\{(?!\{)[^{}]*\}\}')

# Whitespace just inside the delimiters: {{ name }} is not a token and
# would otherwise pass through as literal text
PADDED_TOKEN_RGX = re.compile(r'\{\{\s|\s\}\}')

# The bracketed member-typing syntax {{list[int]:name}} was CONSIDERED
# and rejected (2026-08-17) in favor of the flat underscore family
# ({{list_int:name}}, {{list_float:name}}, {{list_str:name}},
# {{list_any:name}}): unknown underscore variants fail loud through the
# existing typed-reference path, while unknown bracket variants would
# pass through as literal text. The syntax is intuitive enough that
# authors will reach for it accidentally, so it is caught as a typo.
#   complete:  {{word[anything]:  - a bracketed type followed by a colon
#   dangling:  {{word[...        - an opened bracket never closed
#                                   before the token ends
# Either brace count: the single-brace spelling of this rejected syntax is
# refused too, rather than becoming ordinary text under the new delimiter.
BRACKETED_TYPE_COMPLETE_RGX = re.compile(r'\{\{?\s*\w+\s*\[[^\]{}]*\]\s*:')
BRACKETED_TYPE_DANGLING_RGX = re.compile(r'\{\{?\s*\w+\s*\[[^\]{}]*[:}]')


def retired_form_message(match) -> str:
    """The guided error for one RETIRED_SINGLE_BRACE_RGX match."""
    old = match.group(0)
    return (f"Retired placeholder syntax {old!r}: since 2026-11-02 placeholders use "
            f"doubled braces - write {'{' + old + '}'!r}. (A single brace is now ordinary "
            f"text, so this would otherwise pass through unsubstituted.)")


# End of file #
