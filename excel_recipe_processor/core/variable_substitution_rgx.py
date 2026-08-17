"""
Regex patterns for variable-template syntax detection.

excel_recipe_processor/core/variable_substitution_rgx.py

Companion to core/variable_substitution.py, following the house
convention of keeping regex patterns in dedicated _rgx modules.
"""

import re


# The bracketed member-typing syntax {list[int]:name} was CONSIDERED
# and rejected (2026-08-17) in favor of the flat underscore family
# ({list_int:name}, {list_float:name}, {list_str:name}, {list_any:name}):
# unknown underscore variants fail loud through the existing typed-
# reference path, while unknown bracket variants would pass through as
# literal text. The syntax is intuitive enough that authors will reach
# for it accidentally, so it is caught as a typo instead of ignored.
#
# Two shapes are detected:
#   complete:  {word[anything]:  - a bracketed type followed by a colon
#   dangling:  {word[...         - an opened bracket never closed
#              before the brace ends (covers {list[int:x} and friends)
BRACKETED_TYPE_COMPLETE_RGX = re.compile(r'\{\s*\w+\s*\[[^\]{}]*\]\s*:')
BRACKETED_TYPE_DANGLING_RGX = re.compile(r'\{\s*\w+\s*\[[^\]{}]*[:}]')


# End of file #
