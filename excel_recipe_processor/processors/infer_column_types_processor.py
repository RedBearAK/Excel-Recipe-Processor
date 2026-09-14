"""
Give text columns the type their content proves, and only then.

excel_recipe_processor/processors/infer_column_types_processor.py

For a file whose columns are not known in advance - a Parquet or csv
from a system export, every column text - decide per column, from its
content, whether it can safely become a number or a date so that Excel
sorts, filters and sums it correctly the moment the file opens.

The rule is conservative by content and generous by name, because the
failure that matters is not a column left as text - it is a column made
numeric that should not have been: a package number shown as 1.00E+06,
a lot number that loses its leading zeros, a sequence number like
2633K074 next to one like 01234 that does not survive. So:

- DATES: every non-empty value parses in ONE of the given date formats
  (tried in order; the first format that parses every value wins). The
  column becomes datetime. A value with a time component keeps it.
- DECIMALS: every non-empty value is numeric AND at least one carries
  a decimal point (or exponent). The column becomes float. Weights,
  prices, percentages.
- INTEGERS stay text unless the column is named in `integer_columns`,
  because integer-looking columns are where identifiers live. Named
  columns become Int64 (nullable) when every non-empty value parses.
- `text_columns` are never touched, whatever their content.
- A column with no non-empty values stays text.
- Coercion is ALL OR NOTHING per column: one value that does not parse
  leaves the whole column text. No value is ever turned into NaN by
  this processor. `strict: true` turns "named but would not parse" into
  an error instead of a warning.

Every decision is reported: which columns became what and why, and
which stayed text. The report is the audit trail for a best guess.
"""

import re
import logging

import pandas as pd

from excel_recipe_processor.core.base_processor import StepProcessorError, TransformBaseProcessor
from excel_recipe_processor.core.config_schema import Key, Schema, name_list
from excel_recipe_processor.processors._helpers.infer_column_types_rgx import (
    decimal_marker_rgx,
    numeric_text_rgx,
)


logger = logging.getLogger(__name__)

DEFAULT_DATE_FORMATS = ['%Y-%m-%d', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M', '%m/%d/%Y', '%m/%d/%y',
                        '%m/%d/%Y %H:%M', '%m/%d/%y %H:%M', '%m/%d/%y %H:%M:%S', '%Y%m%d']


class InferColumnTypesProcessor(TransformBaseProcessor):
    """Text columns become dates or decimals when their content proves it; integers only by name."""

    @classmethod
    def config_schema(cls) -> Schema:
        """Declared keys (2026-09-14); see the module docstring for the rule."""
        return Schema([
            name_list('integer_columns', description='Columns allowed to become integers when every value parses (identifiers are NOT listed)'),
            name_list('text_columns', description='Columns never coerced, whatever their content'),
            Key('date_formats', 'list', item_kind='str',
                description='strptime formats tried in order; the first that parses every value wins. Default covers ISO and US m/d/y with optional times'),
            Key('decimals', 'bool', default=True, description='Turn all-numeric columns with a decimal point into floats'),
            Key('dates', 'bool', default=True, description='Turn columns that parse in one date format into datetimes'),
            Key('strict', 'bool', default=False, description='A named integer column that would not parse is an error, not a warning'),
        ])

    @classmethod
    def get_minimal_config(cls) -> dict:
        return {'source_stage': 'stg_text', 'save_to_stage': 'stg_typed'}

    def execute(self, data: pd.DataFrame) -> pd.DataFrame:
        if not isinstance(data, pd.DataFrame):
            raise StepProcessorError(f"Infer column types step '{self.step_name}' requires a pandas DataFrame")
        integer_columns = set(self.get_config_value('integer_columns', []) or [])
        text_columns = set(self.get_config_value('text_columns', []) or [])
        date_formats = list(self.get_config_value('date_formats', None) or DEFAULT_DATE_FORMATS)
        want_decimals = bool(self.get_config_value('decimals', True))
        want_dates = bool(self.get_config_value('dates', True))
        strict = bool(self.get_config_value('strict', False))

        missing = sorted((integer_columns | text_columns) - set(data.columns))
        if missing:
            raise StepProcessorError(f"Infer column types step '{self.step_name}': named columns not in the data: {missing}")

        result = data.copy()
        decisions = []
        for column in result.columns:
            series = result[column]
            if column in text_columns:
                decisions.append((column, 'text', 'named in text_columns'))
                continue
            if not (series.dtype == object or str(series.dtype) in ('string', 'str')):
                decisions.append((column, str(series.dtype), 'already typed'))
                continue
            values = series.dropna().astype(str).str.strip()
            values = values[values != '']
            if values.empty:
                decisions.append((column, 'text', 'no values'))
                continue

            if want_dates:
                date_format = first_format_parsing_all(values, date_formats)
                if date_format:
                    result[column] = pd.to_datetime(series.where(series.notna() & (series.astype(str).str.strip() != ''), None),
                                                    format=date_format, errors='coerce')
                    decisions.append((column, 'datetime', f"every value parses as {date_format}"))
                    continue

            all_numeric = values.map(lambda value: bool(numeric_text_rgx.fullmatch(value))).all()
            if all_numeric and column in integer_columns and not values.map(lambda value: bool(decimal_marker_rgx.search(value))).any():
                result[column] = pd.to_numeric(series.where(series.notna() & (series.astype(str).str.strip() != ''), None),
                                               errors='coerce').astype('Int64')
                decisions.append((column, 'integer', 'named in integer_columns and every value is a whole number'))
                continue
            if column in integer_columns and not all_numeric:
                message = f"'{column}' is named in integer_columns but not every value is numeric; left as text"
                if strict:
                    raise StepProcessorError(f"Infer column types step '{self.step_name}': {message}")
                logger.warning(f"⚠️  {message}")
                decisions.append((column, 'text', 'named integer, but a value would not parse'))
                continue
            if want_decimals and all_numeric and values.map(lambda value: bool(decimal_marker_rgx.search(value))).any():
                result[column] = pd.to_numeric(series.where(series.notna() & (series.astype(str).str.strip() != ''), None),
                                               errors='coerce').astype(float)
                decisions.append((column, 'float', 'every value is numeric and at least one has a decimal point'))
                continue
            if all_numeric:
                decisions.append((column, 'text', 'every value is a whole number: an identifier unless named in integer_columns'))
                continue
            decisions.append((column, 'text', 'not all values are numeric or dates'))

        self.report(decisions)
        return result

    @staticmethod
    def report(decisions: list):
        changed = [(column, kind, why) for column, kind, why in decisions if kind not in ('text',) and why != 'already typed']
        for column, kind, why in changed:
            logger.info(f"typed      {column!r} -> {kind}: {why}")
        kept = [column for column, kind, why in decisions if kind == 'text' and why not in ('no values',)]
        logger.info(f"{len(changed)} column(s) typed, {len(kept)} left as text")

    def get_capabilities(self) -> dict:
        return {
            'description': 'Text columns become dates or decimals when every value proves it; integers only by name',
            'rule': ['dates: every value parses in one of date_formats', 'decimals: every value numeric and one has a point',
                     'integers: only columns named in integer_columns (identifiers look numeric and are not)',
                     'all or nothing per column: one unparseable value keeps the column text; no value becomes NaN'],
            'reporting': 'every decision logged with its reason',
        }


def first_format_parsing_all(values: pd.Series, date_formats: list):
    """The first format that parses EVERY value, or None. A sample of the
    values is tried first so an obviously non-date column fails fast."""
    sample = values.head(50)
    for date_format in date_formats:
        if pd.to_datetime(sample, format=date_format, errors='coerce').isna().any():
            continue
        if not pd.to_datetime(values, format=date_format, errors='coerce').isna().any():
            return date_format
    return None


# End of file #
