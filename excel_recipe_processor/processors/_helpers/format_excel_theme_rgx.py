"""
Regex patterns for Office theme XML manipulation.

excel_recipe_processor/processors/_helpers/format_excel_theme_rgx.py

Patterns live in their own module so editing the surrounding logic can never
corrupt them, and so each one can be read and reasoned about on its own.

Theme XML nests its accent colours like this:

    <a:accent1><a:srgbClr val="4F81BD"/></a:accent1>

The accent pattern captures the opening tag, the colour value, and the tail,
so a substitution can replace only the six hex digits.
"""

import re


# One accent slot: group 1 is everything up to the colour, group 2 the six
# hex digits, group 3 the remainder. The tag number is captured separately
# so a substitution can look up the right replacement colour.
accent_slot_rgx = re.compile(
    r'(<a:accent([1-6])>\s*<a:srgbClr\s+val=")([0-9A-Fa-f]{6})("\s*/>)'
)

# Theme part inside any OOXML package: xl/theme/theme1.xml (workbooks),
# ppt/theme/theme1.xml (presentations), word/theme/theme1.xml (documents).
# A .thmx package carries theme/theme1.xml at its root.
theme_part_rgx = re.compile(
    r'^(?:xl|ppt|word)?/?theme/theme[0-9]*\.xml$'
)

# The theme element's name attribute, for reporting which theme was adopted.
theme_name_rgx = re.compile(
    r'<a:theme[^>]*\bname="([^"]*)"'
)

# A six-hex-digit colour, with or without a leading hash, for validation.
hex_color_rgx = re.compile(
    r'^#?([0-9A-Fa-f]{6})$'
)

# End of file #
