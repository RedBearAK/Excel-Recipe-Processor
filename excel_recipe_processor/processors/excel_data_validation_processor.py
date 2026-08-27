r"""
Native Excel data-validation rules (dropdowns, bounds, custom formulas).

excel_recipe_processor/processors/excel_data_validation_processor.py

Writes in-Excel data validation - the live data-entry guardrails Excel
enforces as the human types: in-cell dropdown lists, numeric/date/time/
text-length bounds, and custom-formula gates, each optionally carrying an
input prompt and a styled error alert. A file operation on the session
workbook, same architecture as conditional_format: run it after export,
before flush.

Vocabulary (settled 2026-08-14, see dev_notes/NOTES_excel_data_validation):
each entry in 'validations' names a sheet_name (token-capable via the
shared recognizer), an 'apply_to_ranges' LIST (always a list - one rule
legitimately covers several areas, and one-element lists are cheap), a
'validation_type', and per-type keys. List sources are three mutually
exclusive flat keys: values_list / list_from_named_range /
list_from_spill_ref. Bounded types spell intent, not storage slots:
operator + minimum/maximum for intervals, operator + compare_to for
comparisons - formula1/formula2 are an implementation detail this module
translates to.

SHARP EDGES - do not simplify:
- OOXML's showDropDown attribute is INVERTED: true means SUPPRESS the
  in-cell arrow (openpyxl even aliases it 'hide_drop_down'). Config
  show_dropdown: true therefore maps to attribute-ABSENT, and
  show_dropdown: false maps to showDropDown=True.
- Leading '=' is stripped from stored formulas (named ranges, spill refs,
  bounds, custom) because Excel's own files omit it in <formula1>.
- ISO date bounds ("2026-01-01") and clock-time bounds ("17:30") convert
  to DATE()/TIME() formulas; Excel does not evaluate raw ISO strings.
- RESOLVED (2026-08-14, harvested from real Excel output): a stored
  literal '#' spill reference in formula1 is INVALID - Excel's repair
  strips the validation. list_from_spill_ref therefore stores the
  _xlfn.ANCHORARRAY(...) form while recipes keep writing "$Z$1#".
"""

import logging

from openpyxl.worksheet.datavalidation import DataValidation

from excel_recipe_processor.core.base_processor import FileOpsBaseProcessor, StepProcessorError
from excel_recipe_processor.core.workbook_session import WorkbookSession
from excel_recipe_processor.processors._helpers.range_patterns import (
    cell_ref_rgx,
    range_ref_rgx,
    spill_anchor_ref_rgx,
)
from excel_recipe_processor.processors._helpers.sheet_addressing import resolve_sheet_ref
from excel_recipe_processor.processors._helpers.excel_data_validation_rgx import (
    iso_date_rgx,
    clock_time_rgx,
)


logger = logging.getLogger(__name__)


# Canonical validation type -> openpyxl/OOXML storage type.
VALIDATION_TYPES = {
    'list':         'list',
    'whole_number': 'whole',
    'decimal':      'decimal',
    'date':         'date',
    'time':         'time',
    'text_length':  'textLength',
    'custom':       'custom',
}

# Canonical operator -> openpyxl/OOXML storage operator.
OPERATORS = {
    'between':               'between',
    'not_between':           'notBetween',
    'equal':                 'equal',
    'not_equal':             'notEqual',
    'greater_than':          'greaterThan',
    'less_than':             'lessThan',
    'greater_than_or_equal': 'greaterThanOrEqual',
    'less_than_or_equal':    'lessThanOrEqual',
}

# Interval operators take minimum + maximum; the rest take compare_to.
INTERVAL_OPERATORS = {'between', 'not_between'}

# Types that carry operator + bounds (everything except list and custom).
BOUNDED_TYPES = {'whole_number', 'decimal', 'date', 'time', 'text_length'}

# The three mutually exclusive list sources.
LIST_SOURCE_KEYS = ('values_list', 'list_from_named_range', 'list_from_spill_ref')

# Bound keys, for cross-type misuse guards.
BOUND_KEYS = ('operator', 'minimum', 'maximum', 'compare_to')

ERROR_ALERT_STYLES = ('stop', 'warning', 'information')

# Excel refuses data-validation formulas longer than this, including the
# quotes around an inline list.
EXCEL_DV_FORMULA_LIMIT = 255


class ExcelDataValidationProcessor(FileOpsBaseProcessor):
    """Write native Excel data-validation rules in canonical ERP vocabulary."""

    @classmethod
    def get_minimal_config(cls) -> dict:
        """Smallest configuration that constructs and validates."""
        return {
            'target_file': 'output.xlsx',
            'validations': [
                {'sheet_name': 'Data',
                 'apply_to_ranges': ['B2'],
                 'validation_type': 'list',
                 'values_list': ['Open', 'Closed']},
            ],
        }

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def _validate_file_operation_config(self):
        """Validate structure and every entry up front, before any file I/O."""
        if not self.get_config_value('target_file'):
            raise StepProcessorError(
                f"Excel data validation step '{self.step_name}' requires 'target_file'"
            )

        validations = self.get_config_value('validations', [])
        if not isinstance(validations, list) or not validations:
            raise StepProcessorError(
                f"Excel data validation step '{self.step_name}' requires a "
                f"non-empty 'validations' list"
            )

        for entry_index, entry in enumerate(validations, start=1):
            self._validate_entry(entry, entry_index)

    def _validate_entry(self, entry, entry_index: int) -> None:
        """One validation entry: addressing, type, per-type keys, behaviors."""
        context = f"validation {entry_index} in step '{self.step_name}'"

        if not isinstance(entry, dict):
            raise StepProcessorError(f"{context}: each validation must be a mapping")

        self._reject_retired_keys(entry, context)

        if not entry.get('sheet_name'):
            raise StepProcessorError(
                f"{context}: requires 'sheet_name' (a tab name, or "
                f"'?sheet_001?' by position)"
            )

        self._validate_ranges(entry, context)

        validation_type = entry.get('validation_type')
        if validation_type not in VALIDATION_TYPES:
            raise StepProcessorError(
                f"{context}: unknown validation_type {validation_type!r}. "
                f"Canonical types: {sorted(VALIDATION_TYPES)}"
            )

        if validation_type == 'list':
            self._validate_list_entry(entry, context)
        elif validation_type in BOUNDED_TYPES:
            self._validate_bounded_entry(entry, validation_type, context)
        else:  # custom
            self._validate_custom_entry(entry, context)

        self._validate_behaviors(entry, validation_type, context)

    @staticmethod
    def _reject_retired_keys(entry: dict, context: str) -> None:
        """Loudly refuse vocabulary this processor never spoke."""
        if 'sheet' in entry:
            raise StepProcessorError(
                f"{context}: 'sheet' is not a key here - use 'sheet_name' "
                f"(2026-08-14 sheet-addressing doctrine)"
            )
        for wrong_key in ('range', 'ranges'):
            if wrong_key in entry:
                raise StepProcessorError(
                    f"{context}: '{wrong_key}' is not a key here - use "
                    f"'apply_to_ranges' (always a list, even for one range)"
                )

    @staticmethod
    def _validate_ranges(entry: dict, context: str) -> None:
        """apply_to_ranges: non-empty list of A1-shaped cell/range strings."""
        ranges = entry.get('apply_to_ranges')
        if not isinstance(ranges, list) or not ranges:
            raise StepProcessorError(
                f"{context}: requires 'apply_to_ranges' as a non-empty list "
                f"of A1-style cells or ranges, e.g. [\"B2\"] or "
                f"[\"A2:A500\", \"C2:C500\"]"
            )
        for range_text in ranges:
            if not isinstance(range_text, str):
                raise StepProcessorError(
                    f"{context}: apply_to_ranges entries must be strings, "
                    f"got {range_text!r}"
                )
            candidate = range_text.strip()
            if not (cell_ref_rgx.match(candidate) or range_ref_rgx.match(candidate)):
                raise StepProcessorError(
                    f"{context}: {range_text!r} is not an A1-style cell or "
                    f"range (like \"B2\", \"$B$2\", or \"A2:A500\")"
                )

    def _validate_list_entry(self, entry: dict, context: str) -> None:
        """List type: exactly one source; no bound or custom keys."""
        sources_given = [key for key in LIST_SOURCE_KEYS if key in entry]
        if len(sources_given) != 1:
            raise StepProcessorError(
                f"{context}: a list validation needs exactly one of "
                f"{list(LIST_SOURCE_KEYS)}, got {sources_given or 'none'}"
            )

        self._reject_keys(entry, BOUND_KEYS, context,
                          "only apply to bounded types (whole_number, decimal, "
                          "date, time, text_length)")
        self._reject_keys(entry, ('formula', 'excel_formula'), context,
                          "only applies to validation_type: custom")

        source_key = sources_given[0]
        if source_key == 'values_list':
            self._validate_values_list(entry['values_list'], context)
        elif source_key == 'list_from_named_range':
            named_range = entry['list_from_named_range']
            if not isinstance(named_range, str) or not named_range.strip():
                raise StepProcessorError(
                    f"{context}: list_from_named_range must be a defined "
                    f"name, e.g. \"rng_customers\""
                )
        else:  # list_from_spill_ref
            spill_ref = str(entry['list_from_spill_ref']).strip().lstrip('=')
            if not spill_anchor_ref_rgx.match(spill_ref):
                raise StepProcessorError(
                    f"{context}: {entry['list_from_spill_ref']!r} is not a "
                    f"spill anchor reference (like \"$Z$2#\" or "
                    f"\"Lookups!$Z$2#\")"
                )

    @staticmethod
    def _validate_values_list(values, context: str) -> None:
        """Inline list: no commas or quotes in items; joined form fits Excel."""
        if not isinstance(values, list) or not values:
            raise StepProcessorError(
                f"{context}: values_list must be a non-empty list of values"
            )
        items = [str(value) for value in values]
        for item in items:
            if ',' in item:
                raise StepProcessorError(
                    f"{context}: inline item {item!r} contains a comma, which "
                    f"Excel's inline list form cannot represent - put the "
                    f"values on a lookup tab and use list_from_named_range"
                )
            if '"' in item:
                raise StepProcessorError(
                    f"{context}: inline item {item!r} contains a double "
                    f"quote, which Excel's inline list form cannot represent "
                    f"- use list_from_named_range instead"
                )
        joined = '"' + ','.join(items) + '"'
        if len(joined) > EXCEL_DV_FORMULA_LIMIT:
            raise StepProcessorError(
                f"{context}: inline list is {len(joined)} characters; Excel "
                f"refuses data-validation formulas over "
                f"{EXCEL_DV_FORMULA_LIMIT} - put the values on a lookup tab "
                f"and use list_from_named_range"
            )

    def _validate_bounded_entry(self, entry: dict, validation_type: str,
                                context: str) -> None:
        """Bounded types: operator decides which bound keys are required."""
        self._reject_keys(entry, LIST_SOURCE_KEYS, context,
                          "only apply to validation_type: list")
        self._reject_keys(entry, ('formula', 'excel_formula'), context,
                          "only applies to validation_type: custom")

        operator = entry.get('operator')
        if operator not in OPERATORS:
            raise StepProcessorError(
                f"{context}: validation_type '{validation_type}' needs an "
                f"'operator'. Canonical operators: {sorted(OPERATORS)}"
            )

        if operator in INTERVAL_OPERATORS:
            missing = [key for key in ('minimum', 'maximum') if key not in entry]
            if missing:
                raise StepProcessorError(
                    f"{context}: operator '{operator}' needs 'minimum' and "
                    f"'maximum', missing {missing}"
                )
            if 'compare_to' in entry:
                raise StepProcessorError(
                    f"{context}: operator '{operator}' takes minimum/maximum, "
                    f"not 'compare_to'"
                )
        else:
            if 'compare_to' not in entry:
                raise StepProcessorError(
                    f"{context}: operator '{operator}' needs 'compare_to'"
                )
            present = [key for key in ('minimum', 'maximum') if key in entry]
            if present:
                raise StepProcessorError(
                    f"{context}: operator '{operator}' takes 'compare_to', "
                    f"not {present}"
                )

    def _validate_custom_entry(self, entry: dict, context: str) -> None:
        """Custom type: one formula; nothing borrowed from other types."""
        self._reject_keys(entry, LIST_SOURCE_KEYS, context,
                          "only apply to validation_type: list")
        self._reject_keys(entry, BOUND_KEYS, context,
                          "only apply to bounded types (whole_number, decimal, "
                          "date, time, text_length)")

        if 'formula' in entry:
            raise StepProcessorError(
                f"{context}: 'formula' was renamed 'excel_formula' "
                f"(2026-08-26) - custom validation rules are EXCEL "
                f"formulas, and the key now says so. Rename the key; the "
                f"value is unchanged."
            )
        formula = entry.get('excel_formula')
        if not isinstance(formula, str) or not formula.strip():
            raise StepProcessorError(
                f"{context}: validation_type 'custom' needs an "
                f"'excel_formula' that evaluates TRUE for acceptable entries"
            )

    @staticmethod
    def _reject_keys(entry: dict, keys, context: str, reason: str) -> None:
        """Guided error for keys that belong to a different validation type."""
        present = [key for key in keys if key in entry]
        if present:
            raise StepProcessorError(f"{context}: {present} {reason}")

    @staticmethod
    def _validate_behaviors(entry: dict, validation_type: str,
                            context: str) -> None:
        """Cross-cutting behaviors: dropdown, blank, prompt, error alert."""
        if 'show_dropdown' in entry:
            if validation_type != 'list':
                raise StepProcessorError(
                    f"{context}: 'show_dropdown' only applies to "
                    f"validation_type: list"
                )
            if not isinstance(entry['show_dropdown'], bool):
                raise StepProcessorError(
                    f"{context}: 'show_dropdown' must be true or false"
                )

        if 'allow_blank' in entry and not isinstance(entry['allow_blank'], bool):
            raise StepProcessorError(
                f"{context}: 'allow_blank' must be true or false"
            )

        for block_key in ('input_prompt', 'error_alert'):
            if block_key not in entry:
                continue
            block = entry[block_key]
            if not isinstance(block, dict):
                raise StepProcessorError(
                    f"{context}: '{block_key}' must be a mapping with "
                    f"title/message"
                )
            allowed = {'title', 'message'}
            if block_key == 'error_alert':
                allowed.add('style')
            unknown = sorted(set(block) - allowed)
            if unknown:
                raise StepProcessorError(
                    f"{context}: unknown {block_key} keys {unknown}; "
                    f"allowed: {sorted(allowed)}"
                )
            if not ('title' in block or 'message' in block):
                raise StepProcessorError(
                    f"{context}: '{block_key}' needs a title and/or message"
                )

        style = entry.get('error_alert', {}).get('style', 'stop') \
            if isinstance(entry.get('error_alert'), dict) else 'stop'
        if style not in ERROR_ALERT_STYLES:
            raise StepProcessorError(
                f"{context}: error_alert style {style!r} is not one of "
                f"{list(ERROR_ALERT_STYLES)}"
            )

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    def perform_file_operation(self):
        """Apply every entry to its sheet via the workbook session."""
        target_file = self.get_config_value('target_file')
        validations = self.get_config_value('validations')

        resolved_file = self._resolve_path(target_file)
        workbook = WorkbookSession.get_workbook(resolved_file)

        rules_written = 0
        sheets_touched = set()

        for entry_index, entry in enumerate(validations, start=1):
            context = f"validation {entry_index} in step '{self.step_name}'"

            try:
                sheet_name = resolve_sheet_ref(
                    entry['sheet_name'], workbook.sheetnames, context
                )
            except ValueError as error:
                raise StepProcessorError(str(error))

            worksheet = workbook[sheet_name]
            data_validation = self._build_data_validation(entry)

            for range_text in entry['apply_to_ranges']:
                # openpyxl normalizes '$' away, but strip anyway so the
                # stored sqref never depends on library behavior.
                data_validation.add(range_text.strip().replace('$', ''))

            worksheet.add_data_validation(data_validation)
            rules_written += 1
            sheets_touched.add(sheet_name)

        WorkbookSession.mark_dirty(resolved_file)

        return (
            f"wrote {rules_written} data-validation rule(s) on "
            f"{sorted(sheets_touched)}"
        )

    def _build_data_validation(self, entry: dict) -> DataValidation:
        """Translate one validated entry into an openpyxl DataValidation."""
        validation_type = entry['validation_type']

        data_validation = DataValidation(
            type=VALIDATION_TYPES[validation_type],
            allow_blank=bool(entry.get('allow_blank', True)),
        )

        if validation_type == 'list':
            data_validation.formula1 = self._list_formula(entry)
            # INVERTED ATTRIBUTE (see module docstring): OOXML showDropDown
            # true means SUPPRESS the in-cell arrow. Config show_dropdown
            # defaults true, mapping to attribute-ABSENT (None) - openpyxl's
            # constructor default of False would serialize an explicit
            # showDropDown="0", which Excel itself never writes.
            if not entry.get('show_dropdown', True):
                data_validation.showDropDown = True
            else:
                data_validation.showDropDown = None
        elif validation_type in BOUNDED_TYPES:
            operator = entry['operator']
            data_validation.operator = OPERATORS[operator]
            if operator in INTERVAL_OPERATORS:
                data_validation.formula1 = self._bound_as_formula(
                    entry['minimum'], validation_type)
                data_validation.formula2 = self._bound_as_formula(
                    entry['maximum'], validation_type)
            else:
                data_validation.formula1 = self._bound_as_formula(
                    entry['compare_to'], validation_type)
        else:  # custom
            data_validation.formula1 = str(entry['excel_formula']).strip().lstrip('=')

        input_prompt = entry.get('input_prompt')
        if isinstance(input_prompt, dict):
            data_validation.showInputMessage = True
            data_validation.promptTitle = input_prompt.get('title')
            data_validation.prompt = input_prompt.get('message')

        error_alert = entry.get('error_alert')
        if isinstance(error_alert, dict):
            data_validation.showErrorMessage = True
            data_validation.errorStyle = error_alert.get('style', 'stop')
            data_validation.errorTitle = error_alert.get('title')
            data_validation.error = error_alert.get('message')

        return data_validation

    @staticmethod
    def _list_formula(entry: dict) -> str:
        """The one configured list source, in Excel's stored formula1 form."""
        if 'values_list' in entry:
            items = [str(value) for value in entry['values_list']]
            return '"' + ','.join(items) + '"'
        if 'list_from_named_range' in entry:
            return str(entry['list_from_named_range']).strip().lstrip('=')
        # Recipes write the natural spill form ("$Z$1#"); Excel STORES the
        # ANCHORARRAY form, and a stored literal '#' triggers the repair
        # dialog, which strips the whole validation. Harvested verbatim
        # 2026-08-14: <formula1>_xlfn.ANCHORARRAY($D$1)</formula1>.
        spill_ref = str(entry['list_from_spill_ref']).strip().lstrip('=')
        return f"_xlfn.ANCHORARRAY({spill_ref.rstrip('#')})"

    @staticmethod
    def _bound_as_formula(value, validation_type: str) -> str:
        """A bound value in the form Excel evaluates.

        Numbers pass through as text. Strings starting with '=' are
        formulas (cell refs, named ranges) and lose the '=' per Excel's
        own storage convention. ISO dates and clock times become
        DATE()/TIME() calls for their matching types, because Excel does
        not evaluate the raw strings. Everything else passes verbatim.
        """
        if isinstance(value, bool):
            return 'TRUE' if value else 'FALSE'
        if isinstance(value, (int, float)):
            return str(value)

        text = str(value).strip()
        if text.startswith('='):
            return text.lstrip('=')

        if validation_type == 'date':
            date_match = iso_date_rgx.match(text)
            if date_match:
                year, month, day = (int(part) for part in date_match.groups())
                return f"DATE({year},{month},{day})"

        if validation_type == 'time':
            time_match = clock_time_rgx.match(text)
            if time_match:
                hour = int(time_match.group(1))
                minute = int(time_match.group(2))
                second = int(time_match.group(3) or 0)
                return f"TIME({hour},{minute},{second})"

        return text

    def _resolve_path(self, filename: str) -> str:
        """Apply recipe variable substitution to a configured path."""
        if hasattr(self, 'variable_substitution') and self.variable_substitution:
            return self.variable_substitution.substitute(filename)
        return filename

    # ------------------------------------------------------------------
    # Self-description
    # ------------------------------------------------------------------

    def get_usage_examples(self) -> dict:
        """Get usage examples from the external YAML file."""
        from excel_recipe_processor.utils.processor_examples_loader import load_processor_examples
        return load_processor_examples('excel_data_validation')

    def get_capabilities(self) -> dict:
        """
        Get processor capabilities information.

        Returns:
            Dictionary with processor capabilities
        """
        return {
            'description': 'Native Excel data-validation rules: dropdowns, bounds, formula gates, prompts',
            'validation_types': sorted(VALIDATION_TYPES),
            'list_sources': 'exactly one of values_list (inline), '
                            'list_from_named_range (pairs with lookup tabs '
                            'and manage_named_objects), or '
                            'list_from_spill_ref ("$Z$2#" anchors)',
            'bounds_vocabulary': 'operator + minimum/maximum for '
                                 'between/not_between; operator + compare_to '
                                 'for the six comparisons; bounds accept '
                                 'numbers, "=formulas", ISO dates, and clock '
                                 'times (auto-converted to DATE()/TIME())',
            'targeting': "apply_to_ranges is ALWAYS a list of A1 cells/ranges; "
                         "one rule may cover several areas (multi-area sqref)",
            'behaviors': 'allow_blank (default true), show_dropdown (list '
                         'only, default true), input_prompt {title, message}, '
                         'error_alert {style: stop/warning/information, '
                         'title, message}',
            'enforcement_scope': 'fires on MANUAL entry only - paste, '
                                 'fill-down and formulas bypass Excel data '
                                 'validation; use verify_data for pipeline '
                                 'integrity',
            'session': 'file operation on the workbook session, like '
                       'conditional_format - run after export, before flush',
        }

# End of file #
