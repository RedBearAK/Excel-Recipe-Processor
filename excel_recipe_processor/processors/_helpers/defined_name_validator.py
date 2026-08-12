"""
Validation of Excel defined names against Excel's rules and house style.

excel_recipe_processor/processors/_helpers/defined_name_validator.py

Excel silently rejects some defined names and refuses to open workbooks
containing others, so names must be checked before they are written rather
than after. House style adds one rule on top of Excel's: every digit must be
separated from preceding letters, which makes it impossible for a name to be
read as a cell reference no matter what letters precede the digit.
"""

import logging

from excel_recipe_processor.processors._helpers.range_patterns import (
    bare_rc_name_rgx,
    r1c1_name_rgx,
    excel_legal_name_rgx,
    cellref_like_name_rgx,
    unseparated_digit_rgx,
    MINIMUM_NAME_LENGTH,
)


logger = logging.getLogger(__name__)


RESERVED_NAMES = frozenset({
    'Print_Area',
    'Print_Titles',
    'Criteria',
    'Database',
    'Extract',
    'Consolidate_Area',
    'Sheet_Title',
})


class DefinedNameError(Exception):
    """Raised when a defined name violates Excel rules or house style."""
    pass


def check_defined_name(name, enforce_house_style: bool = True) -> list:
    """
    Check a defined name and return every problem found.

    Collecting all problems rather than stopping at the first means a recipe
    author sees the whole picture in one pass.

    Args:
        name:                   Candidate defined name
        enforce_house_style:    Apply the digit separation and length rules

    Returns:
        List of problem descriptions, empty when the name is acceptable
    """
    problems = []

    if not isinstance(name, str):
        return [f"Name must be a string, got {type(name).__name__}"]

    if not name:
        return ["Name cannot be empty"]

    if len(name) > 255:
        problems.append(f"Name is {len(name)} characters; Excel allows at most 255")

    if not excel_legal_name_rgx.match(name):
        problems.append(
            "Name must start with a letter, underscore, or backslash and contain "
            "only letters, digits, periods, underscores, and backslashes "
            "(no spaces)"
        )

    if cellref_like_name_rgx.match(name):
        problems.append(
            f"Excel reads '{name}' as a cell reference, so it cannot be a name"
        )

    if bare_rc_name_rgx.match(name):
        problems.append(f"'{name}' is reserved by Excel as row/column shorthand")

    if r1c1_name_rgx.match(name):
        problems.append(f"Excel reads '{name}' as an R1C1 reference")

    if name in RESERVED_NAMES:
        problems.append(f"'{name}' is reserved by Excel for built-in use")

    if enforce_house_style:
        if len(name) < MINIMUM_NAME_LENGTH:
            problems.append(
                f"House style requires at least {MINIMUM_NAME_LENGTH} characters; "
                f"'{name}' has {len(name)}"
            )

        match = unseparated_digit_rgx.search(name)
        if match:
            problems.append(
                f"House style requires a separator before digits: '{name}' has a "
                f"digit at position {match.start() + 1} that directly follows a "
                f"letter. Use an underscore, as in "
                f"'{name[:match.start()]}_{name[match.start():]}'."
            )

    return problems


def validate_defined_name(name, enforce_house_style: bool = True) -> None:
    """
    Raise if a defined name is unacceptable.

    Args:
        name:                   Candidate defined name
        enforce_house_style:    Apply the digit separation and length rules

    Raises:
        DefinedNameError: If any problem is found
    """
    problems = check_defined_name(name, enforce_house_style)

    if not problems:
        return

    detail = '; '.join(problems)
    raise DefinedNameError(f"Invalid defined name '{name}': {detail}")


def is_valid_defined_name(name, enforce_house_style: bool = True) -> bool:
    """
    Report whether a defined name is acceptable.

    Args:
        name:                   Candidate defined name
        enforce_house_style:    Apply the digit separation and length rules

    Returns:
        True when the name passes every check
    """
    return len(check_defined_name(name, enforce_house_style)) == 0


# End of file #
