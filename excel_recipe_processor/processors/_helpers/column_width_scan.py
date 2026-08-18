"""
Shared column-width measurement math.

excel_recipe_processor/processors/_helpers/column_width_scan.py

ONE clamp, TWO measurers. The clamp (fitted_width) is imported by both
format_excel's workbook-side auto-fit and profile_sheets' stage-side
scanner, so the two can never drift on the arithmetic that turns a
content length into a column width. The measurers necessarily differ
by source:

- Stage-side (scan_frame_column_widths): pandas values - no fonts, no
  formulas, a straight max of header length and cell text lengths.
- Workbook-side (format_excel._auto_fit_columns): applies font-size
  and bold factors and SKIPS formula cells (a formula cell holds its
  source text, not its displayed result).

Parity is therefore EXACT for plain unstyled data and approximate
under styling - the profile_sheets docstring states the same.
"""

# House defaults, shared by both consumers. tpl-level overrides win.
BASE_PADDING = 4
DEFAULT_MIN_WIDTH = 8
DEFAULT_MAX_WIDTH = 100


def fitted_width(content_length, min_width=DEFAULT_MIN_WIDTH,
                 max_width=DEFAULT_MAX_WIDTH, padding=BASE_PADDING):
    """The one clamp: content length -> final column width."""
    return max(min_width, min(int(content_length) + padding, max_width))


def scan_frame_column_widths(frame, min_width=DEFAULT_MIN_WIDTH,
                             max_width=DEFAULT_MAX_WIDTH,
                             padding=BASE_PADDING, scan_rows=None):
    """
    Auto-fit widths for a DataFrame, header always measured.

    Args:
        frame:      pandas DataFrame (the stage)
        min_width:  Floor after padding
        max_width:  Ceiling after padding
        padding:    Extra characters beyond the longest content
        scan_rows:  Measure only the first N data rows (None = all).
                    The header row is always measured regardless.

    Returns:
        List of (column_name, width, max_content_length) tuples in
        frame column order.
    """
    results = []
    data = frame if scan_rows is None else frame.head(scan_rows)
    for column_name in frame.columns:
        max_length = len(str(column_name))
        series = data[column_name].dropna()
        if len(series) > 0:
            longest = series.astype(str).str.len().max()
            if longest > max_length:
                max_length = int(longest)
        results.append((
            column_name,
            fitted_width(max_length, min_width, max_width, padding),
            max_length,
        ))
    return results

# End of file #
