"""
Columns-to-rows reshaping (de-crosstabbing) for wide data.

excel_recipe_processor/processors/columns_to_rows_processor.py

Takes data that arrived WIDE - one dimension hiding in the header row,
like a customer per row with Jan..Dec as twelve amount columns - and
demotes that dimension into the data: one row per (id, label) pair, with
the old header names in a labels column and the cell contents in a values
column. Nothing is summarized or unsummarized; every cell simply moves.
The id columns repeat down the stacked rows, which is what makes the
result filterable, joinable, and re-pivotable on the demoted dimension.

NOT a transpose: transpose (slice_data) rotates the whole grid and keeps
one dimension in the headers; this eliminates the header dimension
entirely. The inverse operation is rows_to_columns.

pandas.melt silently DROPS columns named in neither list; this processor
refuses instead - a column falling off a reshape without being mentioned
is exactly the silent data loss the framework exists to prevent.
"""

import logging
import pandas as pd

from excel_recipe_processor.core.stage_manager import StageManager, StageError
from excel_recipe_processor.core.base_processor import BaseStepProcessor, StepProcessorError


logger = logging.getLogger(__name__)


class ColumnsToRowsProcessor(BaseStepProcessor):
    """Demote header columns into label/value rows (wide to long)."""

    @classmethod
    def get_minimal_config(cls) -> dict:
        """Smallest configuration that constructs and validates."""
        return {
            'source_stage': 'stg_wide_data',
            'save_to_stage': 'stg_long_data',
            'id_columns': ['Customer'],
        }

    def execute(self, data) -> pd.DataFrame:
        """
        Execute the columns-to-rows reshape.

        Args:
            data: Input DataFrame, or None when loading from source_stage

        Returns:
            Long-form DataFrame: id columns, labels column, values column
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
                f"Columns-to-rows step '{self.step_name}' requires a pandas DataFrame"
            )
        self.validate_data_not_empty(data)

        id_columns, value_columns = self._resolve_column_split(data)

        labels_to = self.get_config_value('labels_to', 'Field')
        values_to = self.get_config_value('values_to', 'Value')

        for new_name, option in ((labels_to, 'labels_to'), (values_to, 'values_to')):
            if new_name in id_columns:
                raise StepProcessorError(
                    f"Columns-to-rows step '{self.step_name}': {option} "
                    f"'{new_name}' collides with an id column of the same name"
                )
        if labels_to == values_to:
            raise StepProcessorError(
                f"Columns-to-rows step '{self.step_name}': labels_to and "
                f"values_to must differ, both are '{labels_to}'"
            )

        result = pd.melt(
            data,
            id_vars=id_columns,
            value_vars=value_columns,
            var_name=labels_to,
            value_name=values_to,
        )

        if self.get_config_value('drop_empty_values', False):
            before = len(result)
            values = result[values_to]
            keep = ~(values.isna() | (values.astype(str).str.strip() == ''))
            result = result[keep].reset_index(drop=True)
            logger.debug(
                f"drop_empty_values removed {before - len(result)} blank-value row(s)"
            )

        rows_in, columns_in = data.shape
        rows_out, columns_out = result.shape
        self.log_step_complete(
            f"reshaped {rows_in}×{columns_in} wide → {rows_out}×{columns_out} long "
            f"({len(value_columns)} column(s) demoted into '{labels_to}')"
        )
        return result

    def _resolve_column_split(self, data) -> tuple:
        """
        Settle which columns stay (ids) and which stack (values).

        Either list may be given; the other becomes the complement. Given
        BOTH, every column must appear in exactly one - a column in
        neither would be silently dropped by pandas.melt, and this
        framework does not lose data silently.
        """
        id_columns = self.get_config_value('id_columns', None)
        value_columns = self.get_config_value('value_columns', None)

        if id_columns is None and value_columns is None:
            raise StepProcessorError(
                f"Columns-to-rows step '{self.step_name}' needs id_columns, "
                f"value_columns, or both"
            )

        all_columns = list(data.columns)

        for option_name, listed in (('id_columns', id_columns),
                                    ('value_columns', value_columns)):
            if listed is None:
                continue
            if not isinstance(listed, list) or not listed:
                raise StepProcessorError(
                    f"Columns-to-rows step '{self.step_name}': {option_name} "
                    f"must be a non-empty list"
                )
            missing = [name for name in listed if name not in all_columns]
            if missing:
                raise StepProcessorError(
                    f"Columns-to-rows step '{self.step_name}': {option_name} "
                    f"names missing column(s) {missing}. Available: {all_columns}"
                )
            duplicated = sorted({name for name in listed if listed.count(name) > 1})
            if duplicated:
                raise StepProcessorError(
                    f"Columns-to-rows step '{self.step_name}': {option_name} "
                    f"lists {duplicated} more than once"
                )

        if id_columns is None:
            id_columns = [name for name in all_columns if name not in value_columns]
        elif value_columns is None:
            value_columns = [name for name in all_columns if name not in id_columns]
        else:
            overlap = sorted(set(id_columns) & set(value_columns))
            if overlap:
                raise StepProcessorError(
                    f"Columns-to-rows step '{self.step_name}': column(s) "
                    f"{overlap} appear in both id_columns and value_columns"
                )
            unclaimed = [name for name in all_columns
                         if name not in id_columns and name not in value_columns]
            if unclaimed:
                raise StepProcessorError(
                    f"Columns-to-rows step '{self.step_name}': column(s) "
                    f"{unclaimed} are in neither list and would be silently "
                    f"dropped. Add them to id_columns or value_columns, or "
                    f"remove them first with select_columns."
                )

        if not value_columns:
            raise StepProcessorError(
                f"Columns-to-rows step '{self.step_name}': no value columns "
                f"remain to demote - id_columns claims every column"
            )

        return id_columns, value_columns

    def get_usage_examples(self) -> dict:
        """Get usage examples from the external YAML file."""
        from excel_recipe_processor.utils.processor_examples_loader import load_processor_examples
        return load_processor_examples('columns_to_rows')

    def get_capabilities(self) -> dict:
        """
        Get processor capabilities information.

        Returns:
            Dictionary with processor capabilities
        """
        return {
            'description': 'Demote header columns into label/value rows - wide to '
                           'long, nothing lost',
            'not_a_transpose': 'transpose (slice_data) rotates the grid keeping one '
                               'dimension in the headers; this ELIMINATES the '
                               'header dimension by turning header names into data',
            'inverse': 'rows_to_columns restores the wide layout (losslessly, '
                       'because that processor verifies uniqueness)',
            'column_split': 'give id_columns, value_columns, or both; the missing '
                            'one is the complement, and with both given every '
                            'column must be claimed - pandas.melt silently drops '
                            'unclaimed columns and this processor refuses instead',
            'options': ['labels_to (default Field)', 'values_to (default Value)',
                        'drop_empty_values (default false)'],
        }

# End of file #
