"""
Column-addressed formatting for format_excel.

excel_recipe_processor/processors/_helpers/format_excel_column_formats.py

Number formats, column hiding and per-column alignment all need the same thing:
turn a column NAME into a column letter, then act on that whole column below the
header. Resolution is delegated to excel_range_resolver, which already does this
for named-range creation, so a column name means the same thing everywhere.

Excel number format codes are cryptic and easy to get subtly wrong, so the
common ones are available under readable aliases. Any literal Excel format code
is still accepted.
"""

import logging

from openpyxl.styles import Alignment
from openpyxl.utils import get_column_letter, column_index_from_string

from excel_recipe_processor.processors._helpers.excel_range_resolver import (
    resolve_column_letters, find_last_data_row, ExcelRangeResolverError
)


logger = logging.getLogger(__name__)


# Readable names for the format codes this project actually uses. Anything not
# listed here is passed to Excel verbatim, so custom codes still work.
NUMBER_FORMAT_ALIASES = {
    # Whole numbers with thousands separators - Cases, Packages, Units, weights
    'thousands': '#,##0',
    'thousands_2dp': '#,##0.00',

    # Accounting: currency symbol pinned left, negatives in parentheses, zero as
    # a dash. This is what Excel's own "Accounting" button produces.
    'accounting': '_($* #,##0.00_);_($* (#,##0.00);_($* "-"??_);_(@_)',
    'accounting_0dp': '_($* #,##0_);_($* (#,##0);_($* "-"_);_(@_)',

    'currency': '$#,##0.00',
    'currency_0dp': '$#,##0',

    'percent': '0%',
    'percent_2dp': '0.00%',

    'date': 'mm/dd/yyyy',
    'date_iso': 'yyyy-mm-dd',
    'datetime': 'mm/dd/yyyy hh:mm',

    'integer': '0',
    'text': '@',
}

VALID_HORIZONTAL = (
    'left', 'center', 'right', 'justify', 'distributed', 'centerContinuous',
    'fill', 'general'
)

VALID_VERTICAL = ('top', 'center', 'bottom', 'justify', 'distributed')


class ColumnFormatError(Exception):
    """Raised when a column formatting rule cannot be applied."""
    pass


def resolve_number_format(spec: str) -> str:
    """
    Turn a format alias into an Excel format code.

    Args:
        spec: An alias from NUMBER_FORMAT_ALIASES, or a literal Excel code

    Returns:
        Excel number format code
    """
    if not isinstance(spec, str) or not spec.strip():
        raise ColumnFormatError("number_format must be a non-empty string")

    key = spec.strip()

    if key in NUMBER_FORMAT_ALIASES:
        return NUMBER_FORMAT_ALIASES[key]

    # Not an alias, so treat it as a literal Excel format code
    return key


def apply_column_formats(worksheet, rules: list, header_row: int = 1,
                         on_missing: str = 'warn') -> list:
    """
    Apply number formats and alignment to whole columns, addressed by name.

    Args:
        worksheet:      openpyxl worksheet object
        rules:          List of rule dictionaries
        header_row:     Row holding the headers
        on_missing:     'error', 'warn', or 'skip' for unresolvable columns

    Returns:
        List of human-readable descriptions of what was applied
    """
    if not isinstance(rules, list):
        raise ColumnFormatError("column_formats must be a list of rules")

    applied = []

    # One extent for the whole sheet, measured from the widest column. Measuring
    # per column would truncate a rule applied to a sparsely populated column.
    try:
        anchor_letters = [get_column_letter(i) for i in range(1, worksheet.max_column + 1)]
        last_row = find_last_data_row(worksheet, anchor_letters, header_row)
    except ExcelRangeResolverError as error:
        raise ColumnFormatError(f"Could not determine data extent: {error}")

    if last_row <= header_row:
        logger.warning(f"[{worksheet.title}] No data rows below header, skipping column formats")
        return applied

    for index, rule in enumerate(rules):
        if not isinstance(rule, dict):
            raise ColumnFormatError(f"column_formats rule {index + 1} must be a dictionary")

        columns = rule.get('columns')

        if not isinstance(columns, list) or len(columns) == 0:
            raise ColumnFormatError(
                f"column_formats rule {index + 1} requires a non-empty 'columns' list"
            )

        number_format = rule.get('number_format')
        horizontal = rule.get('alignment_horizontal')
        vertical = rule.get('alignment_vertical')
        wrap_text = rule.get('wrap_text')

        if number_format is None and horizontal is None and vertical is None and wrap_text is None:
            raise ColumnFormatError(
                f"column_formats rule {index + 1} does nothing: supply number_format, "
                f"alignment_horizontal, alignment_vertical, or wrap_text"
            )

        if horizontal is not None and horizontal not in VALID_HORIZONTAL:
            valid = ', '.join(VALID_HORIZONTAL)
            raise ColumnFormatError(
                f"column_formats rule {index + 1} invalid alignment_horizontal "
                f"'{horizontal}'. Valid: {valid}"
            )

        if vertical is not None and vertical not in VALID_VERTICAL:
            valid = ', '.join(VALID_VERTICAL)
            raise ColumnFormatError(
                f"column_formats rule {index + 1} invalid alignment_vertical "
                f"'{vertical}'. Valid: {valid}"
            )

        try:
            letters = resolve_column_letters(
                worksheet, columns, header_row,
                force_column_names=rule.get('force_column_names', False),
                on_missing=on_missing
            )
        except ExcelRangeResolverError as error:
            if on_missing == 'error':
                raise ColumnFormatError(f"column_formats rule {index + 1}: {error}")
            logger.warning(f"column_formats rule {index + 1}: {error}")
            continue

        format_code = resolve_number_format(number_format) if number_format else None

        for letter in letters:
            col_index = column_index_from_string(letter)

            for row_num in range(header_row + 1, last_row + 1):
                cell = worksheet.cell(row=row_num, column=col_index)

                if format_code:
                    cell.number_format = format_code

                if horizontal is not None or vertical is not None or wrap_text is not None:
                    existing = cell.alignment
                    cell.alignment = Alignment(
                        horizontal=horizontal if horizontal is not None else existing.horizontal,
                        vertical=vertical if vertical is not None else existing.vertical,
                        wrap_text=wrap_text if wrap_text is not None else existing.wrap_text
                    )

        parts = []
        if format_code:
            label = number_format if number_format in NUMBER_FORMAT_ALIASES else 'custom'
            parts.append(f"number format {label}")
        if horizontal is not None:
            parts.append(f"h-align {horizontal}")
        if vertical is not None:
            parts.append(f"v-align {vertical}")
        if wrap_text is not None:
            parts.append(f"wrap {wrap_text}")

        description = f"{', '.join(parts)} on {len(letters)} column(s): {', '.join(columns[:4])}"
        if len(columns) > 4:
            description += ' ...'

        applied.append(description)
        logger.info(f"🔢 [{worksheet.title}] {description}")

    return applied


def apply_hidden_columns(worksheet, columns: list, header_row: int = 1,
                         on_missing: str = 'warn') -> list:
    """
    Hide whole columns, addressed by name.

    Hiding sets the column dimension's hidden flag, which is what Excel's own
    Hide command does. The data stays in the file and stays available to
    formulas; it simply is not displayed.

    Args:
        worksheet:      openpyxl worksheet object
        columns:        Column names or letters to hide
        header_row:     Row holding the headers
        on_missing:     'error', 'warn', or 'skip' for unresolvable columns

    Returns:
        List of the column letters hidden
    """
    if not isinstance(columns, list) or len(columns) == 0:
        raise ColumnFormatError("hidden_columns must be a non-empty list")

    try:
        letters = resolve_column_letters(worksheet, columns, header_row, False, on_missing)
    except ExcelRangeResolverError as error:
        if on_missing == 'error':
            raise ColumnFormatError(f"hidden_columns: {error}")
        logger.warning(f"hidden_columns: {error}")
        return []

    for letter in letters:
        worksheet.column_dimensions[letter].hidden = True

    logger.info(
        f"🙈 [{worksheet.title}] Hid {len(letters)} column(s): "
        f"{', '.join(f'{c} ({l})' for c, l in zip(columns, letters))}"
    )

    return letters


# End of file #
