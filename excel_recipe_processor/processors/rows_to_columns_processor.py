"""
Rows-to-columns reshaping (re-crosstabbing) for long data.

excel_recipe_processor/processors/rows_to_columns_processor.py

The inverse of columns_to_rows: promotes one column's VALUES into the
header row, spreading another column's values into the new columns - one
output row per distinct id, one new column per distinct label. A pivot
WITHOUT aggregation, and that distinction is enforced rather than assumed:
each (id, label) pair must map to at most one value, and duplicates halt
the run naming the offending pairs. pivot_table would silently aggregate
them; this processor exists precisely to refuse that.

New columns appear in FIRST-APPEARANCE order of the labels in the data
(months stay in the order they arrived, not alphabetized). Combinations
absent from the data become blank cells unless fill_missing_with says
otherwise.
"""

import logging
import pandas as pd

from excel_recipe_processor.core.stage_manager import StageManager, StageError
from excel_recipe_processor.core.base_processor import StepProcessorError, TransformBaseProcessor
from excel_recipe_processor.core.config_schema import Key, Schema, name_list


logger = logging.getLogger(__name__)


class RowsToColumnsProcessor(TransformBaseProcessor):
    """Promote a column's values into headers (long to wide), verified lossless."""

    @classmethod
    def config_schema(cls) -> Schema:
        """Declared keys (2026-09-03); see core/config_schema.py."""
        return Schema([
            name_list('id_columns', description='Omit to imply every column not named by labels_from / values_from'),
            Key('labels_from', 'str', required=True), Key('values_from', 'str', required=True),
            Key('fill_missing_with', 'any'),
        ])

    @classmethod
    def get_minimal_config(cls) -> dict:
        """Smallest configuration that constructs and validates."""
        return {
            'source_stage': 'stg_long_data',
            'save_to_stage': 'stg_wide_data',
            'labels_from': 'Month',
            'values_from': 'Amount',
        }

    def execute(self, data) -> pd.DataFrame:
        """
        Execute the rows-to-columns reshape.

        Args:
            data: Input DataFrame, or None when loading from source_stage

        Returns:
            Wide-form DataFrame: id columns, then one column per label
        """
        self.log_step_start()

        source_stage = self.get_config_value('source_stage')
        if source_stage:
            try:
                data = StageManager.load_stage(source_stage)
            except StageError as error:
                raise StepProcessorError(
                    f"Error loading source stage '{source_stage}': {error}"
                )

        if not isinstance(data, pd.DataFrame):
            raise StepProcessorError(
                f"Rows-to-columns step '{self.step_name}' requires a pandas DataFrame"
            )
        self.validate_data_not_empty(data)
        self.validate_required_fields(['labels_from', 'values_from'])

        labels_from = self.get_config_value('labels_from')
        values_from = self.get_config_value('values_from')

        all_columns = list(data.columns)
        for option_name, column_name in (('labels_from', labels_from),
                                         ('values_from', values_from)):
            if column_name not in all_columns:
                raise StepProcessorError(
                    f"Rows-to-columns step '{self.step_name}': {option_name} "
                    f"'{column_name}' not found. Available: {all_columns}"
                )
        if labels_from == values_from:
            raise StepProcessorError(
                f"Rows-to-columns step '{self.step_name}': labels_from and "
                f"values_from must differ, both are '{labels_from}'"
            )

        id_columns = self._resolve_id_columns(all_columns, labels_from, values_from)

        labels = data[labels_from]
        blank_mask = labels.isna() | (labels.astype(str).str.strip() == '')
        if blank_mask.any():
            blank_rows = [index + 2 for index in range(len(labels)) if blank_mask.iloc[index]]
            raise StepProcessorError(
                f"Rows-to-columns step '{self.step_name}': '{labels_from}' has "
                f"blank value(s) at data row(s) {blank_rows[:10]} - every label "
                f"becomes a column name, and a blank one would be unaddressable"
            )

        label_strings = labels.astype(str)
        label_order = list(label_strings.drop_duplicates())

        collisions = sorted(set(label_order) & set(id_columns))
        if collisions:
            raise StepProcessorError(
                f"Rows-to-columns step '{self.step_name}': label value(s) "
                f"{collisions} collide with id column names of the same spelling"
            )

        # THE losslessness check: at most one value per (id, label) pair.
        # pivot_table would silently aggregate duplicates; halting and
        # naming them is this processor's whole reason to exist.
        key_frame = data[id_columns].astype(str).copy()
        key_frame['__label__'] = label_strings
        duplicate_mask = key_frame.duplicated(keep=False)
        if duplicate_mask.any():
            offenders = (key_frame[duplicate_mask]
                         .drop_duplicates()
                         .head(8)
                         .to_dict('records'))
            raise StepProcessorError(
                f"Rows-to-columns step '{self.step_name}': "
                f"{int(duplicate_mask.sum())} row(s) share an (id, label) pair - "
                f"the reshape would need to aggregate, which this processor "
                f"refuses to do silently. First offending pair(s): {offenders}. "
                f"Deduplicate first, or aggregate deliberately with "
                f"aggregate_data / pivot_table."
            )

        working = data[id_columns].copy()
        working['__label__'] = label_strings
        working['__value__'] = data[values_from]

        wide = working.pivot(index=id_columns, columns='__label__', values='__value__')
        wide = wide.reindex(columns=label_order)
        wide.columns.name = None
        wide = wide.reset_index()

        fill_value = self.get_config_value('fill_missing_with', None)
        if fill_value is not None:
            wide[label_order] = wide[label_order].fillna(fill_value)

        rows_in, columns_in = data.shape
        rows_out, columns_out = wide.shape
        self.log_step_complete(
            f"reshaped {rows_in}×{columns_in} long → {rows_out}×{columns_out} wide "
            f"({len(label_order)} label(s) promoted to columns, verified one "
            f"value per pair)"
        )
        return wide

    def _resolve_id_columns(self, all_columns, labels_from, values_from) -> list:
        """
        Settle the row identity.

        Default: every column that is neither the labels nor the values
        column. An explicit id_columns list may narrow that - but any
        column then left unclaimed would silently vanish from the output,
        so leftovers halt instead.
        """
        id_columns = self.get_config_value('id_columns', None)

        implied = [name for name in all_columns
                   if name not in (labels_from, values_from)]

        if id_columns is None:
            if not implied:
                raise StepProcessorError(
                    f"Rows-to-columns step '{self.step_name}': no id columns "
                    f"remain - the data has only '{labels_from}' and "
                    f"'{values_from}'"
                )
            return implied

        if not isinstance(id_columns, list) or not id_columns:
            raise StepProcessorError(
                f"Rows-to-columns step '{self.step_name}': id_columns must be "
                f"a non-empty list when given"
            )
        missing = [name for name in id_columns if name not in all_columns]
        if missing:
            raise StepProcessorError(
                f"Rows-to-columns step '{self.step_name}': id_columns names "
                f"missing column(s) {missing}. Available: {all_columns}"
            )
        claimed_specials = sorted(set(id_columns) & {labels_from, values_from})
        if claimed_specials:
            raise StepProcessorError(
                f"Rows-to-columns step '{self.step_name}': id_columns may not "
                f"include labels_from/values_from: {claimed_specials}"
            )

        unclaimed = [name for name in implied if name not in id_columns]
        if unclaimed:
            raise StepProcessorError(
                f"Rows-to-columns step '{self.step_name}': column(s) "
                f"{unclaimed} are neither ids nor the labels/values columns "
                f"and would silently vanish. Add them to id_columns, or remove "
                f"them first with select_columns."
            )

        return id_columns

    def get_usage_examples(self) -> dict:
        """Get usage examples from the external YAML file."""
        from excel_recipe_processor.utils.processor_examples_loader import load_processor_examples
        return load_processor_examples('rows_to_columns')

    def get_capabilities(self) -> dict:
        """
        Get processor capabilities information.

        Returns:
            Dictionary with processor capabilities
        """
        return {
            'description': "Promote a column's values into headers: long to wide, silent aggregation refused",
            'losslessness': 'each (id, label) pair must map to at most one value; '
                            'duplicates halt naming the offending pairs, because '
                            'pivot_table would silently aggregate them and this '
                            'processor exists to refuse that',
            'inverse': 'columns_to_rows produces this processor\'s input shape; '
                       'the round trip restores the original table',
            'column_order': 'new columns appear in first-appearance order of the '
                            'labels (months stay in arrival order, not '
                            'alphabetized)',
            'options': ['id_columns (default: all other columns; explicit lists '
                        'must claim everything or the step halts)',
                        'fill_missing_with (default: blank cells for absent '
                        'combinations)'],
        }

# End of file #
