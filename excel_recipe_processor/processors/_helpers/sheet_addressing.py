"""
One recognizer for sheet addressing, shared by every processor.

excel_recipe_processor/processors/_helpers/sheet_addressing.py

Doctrine (vetted 2026-08-14, see dev_notes survey): sheets are addressed
by NAME everywhere. The one escape for untrusted foreign files is the
?sheet_001? pseudo-name, resolved here against the live sheet list. Plain
numbers are treated as NAMES with a warning naming the token form, so an
intended index never silently grabs the wrong tab and a tab literally
named "1" stays reachable. Creation contexts (export) must reject tokens:
they address tabs that already exist.
"""

import logging

from excel_recipe_processor.processors._helpers.sheet_addressing_rgx import sheet_index_token_rgx


logger = logging.getLogger(__name__)


def resolve_sheet_ref(value, sheet_names_in_workbook, context: str) -> str:
    """
    Turn a sheet_name config value into the actual tab name it addresses.

    Args:
        value: The configured value - a real name, a ?sheet_NNN? token, or
               a number (treated as a NAME, with a warning)
        sheet_names_in_workbook: The workbook's current tab names, in order
        context: For messages, e.g. "step 'Format the output'"

    Returns:
        The addressed tab's actual name

    Raises:
        ValueError: Token out of range, or the named tab does not exist -
                    loud by design; the step's on_error decides consequence
    """
    if isinstance(value, (int, float)):
        logger.warning(
            f"⚠️ {context}: numeric sheet_name {value!r} is treated as the tab "
            f"NAMED '{int(value)}'. If you meant the tab at position {int(value)}, "
            f"use the index pseudo-name '?sheet_{int(value):03d}?'."
        )
        value = str(int(value))

    text = str(value)

    token_match = sheet_index_token_rgx.match(text.strip())
    if token_match:
        index = int(token_match.group(1))
        count = len(sheet_names_in_workbook)
        if not 1 <= index <= count:
            raise ValueError(
                f"{context}: {text.strip()} is out of range - the workbook has "
                f"{count} sheet(s)"
            )
        resolved = sheet_names_in_workbook[index - 1]
        logger.debug(f"{context}: {text.strip()} resolved to tab '{resolved}'")
        return resolved

    if text not in sheet_names_in_workbook:
        raise ValueError(
            f"{context}: sheet '{text}' not found. "
            f"Available: {sheet_names_in_workbook}"
        )
    return text


def is_sheet_index_token(value) -> bool:
    """True when the value is a ?sheet_NNN? pseudo-name."""
    return isinstance(value, str) and bool(sheet_index_token_rgx.match(value.strip()))


def reject_token_for_creation(value, context: str) -> None:
    """
    Creation contexts address tabs that DO NOT exist yet; a positional
    pseudo-name is meaningless there (and its characters are illegal in a
    real title anyway). Fail loud with the reason.
    """
    if is_sheet_index_token(value):
        raise ValueError(
            f"{context}: {str(value).strip()} is an index pseudo-name, which "
            f"addresses an EXISTING tab - a sheet being created needs a real "
            f"name"
        )

# End of file #
