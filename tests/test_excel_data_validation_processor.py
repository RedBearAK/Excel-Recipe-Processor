"""
Tests for the excel_data_validation processor.

tests/test_excel_data_validation_processor.py

Creates real Excel files, applies validations, then reopens the saved
files with openpyxl and inspects the stored dataValidation records:
types, formula1/formula2, operators, sqref (including multi-area), the
inverted showDropDown attribute, and error alert fields. Guided errors
are exercised against the config validator. Runnable directly or with
pytest.
"""

import os
import sys
import tempfile

import pandas as pd

from openpyxl import load_workbook

from excel_recipe_processor.core.base_processor import StepProcessorError
from excel_recipe_processor.core.workbook_session import WorkbookSession
from excel_recipe_processor.processors.excel_data_validation_processor import (
    ExcelDataValidationProcessor,
)


def make_workbook(path: str) -> None:
    """A small real workbook with a Data_Entry sheet."""
    frame = pd.DataFrame({
        'Ref':    ['R1', 'R2', 'R3'],
        'Status': ['Open', 'Closed', 'Open'],
        'Qty':    [5, 12, 9],
    })
    frame.to_excel(path, index=False, sheet_name='Data_Entry')


def stored_validations(path: str, sheet_name: str = 'Data_Entry') -> list:
    """Reload the saved file and return its dataValidation records."""
    workbook = load_workbook(path)
    return list(workbook[sheet_name].data_validations.dataValidation)


def run_step(target_file: str, validations: list) -> None:
    """Construct and execute one processor step against a target file."""
    step_config = {
        'processor_type': 'excel_data_validation',
        'step_description': 'Test validations',
        'target_file': target_file,
        'validations': validations,
    }
    processor = ExcelDataValidationProcessor(step_config)
    processor.execute()
    WorkbookSession.reset()


def test_list_sources_and_sqref():
    """All three list sources store correctly; multi-area sqref works."""
    print("\nTesting list sources and sqref storage...")

    with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as handle:
        target_file = handle.name

    try:
        make_workbook(target_file)
        run_step(target_file, [
            {'sheet_name': 'Data_Entry',
             'apply_to_ranges': ['B2:B100', 'D2:D100'],
             'validation_type': 'list',
             'values_list': ['Open', 'Closed', 'Pending']},
            {'sheet_name': 'Data_Entry',
             'apply_to_ranges': ['E2'],
             'validation_type': 'list',
             'list_from_named_range': '=rng_customers'},
            {'sheet_name': 'Data_Entry',
             'apply_to_ranges': ['F2'],
             'validation_type': 'list',
             'list_from_spill_ref': 'Lookups!$Z$2#'},
        ])

        records = stored_validations(target_file)
        if len(records) != 3:
            print(f"✗ Expected 3 stored rules, got {len(records)}")
            return False
        print(f"✓ Stored {len(records)} rules")

        by_formula = {record.formula1: record for record in records}

        inline = by_formula.get('"Open,Closed,Pending"')
        if inline is None:
            print(f"✗ Inline list formula1 wrong: {sorted(by_formula)}")
            return False
        if str(inline.sqref) != 'B2:B100 D2:D100':
            print(f"✗ Multi-area sqref wrong: {inline.sqref}")
            return False
        print("✓ Inline list quoted correctly with multi-area sqref")

        if 'rng_customers' not in by_formula:
            print("✗ Named range '=' not stripped or missing")
            return False
        print("✓ Named range stored without leading '='")

        # Harvested from real Excel output (2026-08-14): a stored literal
        # '#' is invalid and repair strips the validation; Excel stores
        # the ANCHORARRAY form.
        if '_xlfn.ANCHORARRAY(Lookups!$Z$2)' not in by_formula:
            print(f"✗ Spill ref not in ANCHORARRAY form: {sorted(by_formula)}")
            return False
        print("✓ Spill ref stored as _xlfn.ANCHORARRAY(...)")

        # openpyxl's loader refills the constructor default (False) even
        # when the attribute is absent, so the reloaded object cannot
        # answer this - only the stored XML can.
        import zipfile
        with zipfile.ZipFile(target_file) as archive:
            sheet_xml = archive.read('xl/worksheets/sheet1.xml').decode()
        if 'showDropDown' in sheet_xml:
            print("✗ Default show_dropdown wrote a showDropDown attribute; "
                  "Excel parity is attribute-absent")
            return False
        print("✓ Default show_dropdown stored attribute-absent (Excel parity)")
        return True

    finally:
        WorkbookSession.reset()
        if os.path.exists(target_file):
            os.unlink(target_file)


def test_bounds_and_date_conversion():
    """Interval and comparison bounds; ISO date becomes DATE()."""
    print("\nTesting bounded types and date conversion...")

    with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as handle:
        target_file = handle.name

    try:
        make_workbook(target_file)
        run_step(target_file, [
            {'sheet_name': 'Data_Entry',
             'apply_to_ranges': ['C2:C100'],
             'validation_type': 'whole_number',
             'operator': 'between',
             'minimum': 1, 'maximum': 100},
            {'sheet_name': 'Data_Entry',
             'apply_to_ranges': ['G2:G100'],
             'validation_type': 'date',
             'operator': 'greater_than_or_equal',
             'compare_to': '2026-01-01'},
            {'sheet_name': 'Data_Entry',
             'apply_to_ranges': ['H2:H100'],
             'validation_type': 'date',
             'operator': 'greater_than_or_equal',
             'compare_to': '=$B$1'},
        ])

        records = stored_validations(target_file)
        by_type_op = {(record.type, record.formula1): record
                      for record in records}

        interval = by_type_op.get(('whole', '1'))
        if interval is None or interval.formula2 != '100' \
                or interval.operator != 'between':
            print(f"✗ Interval bounds wrong: {[(r.type, r.operator, r.formula1, r.formula2) for r in records]}")
            return False
        print("✓ whole_number between stored as formula1/formula2")

        if ('date', 'DATE(2026,1,1)') not in by_type_op:
            print("✗ ISO date not converted to DATE()")
            return False
        print("✓ ISO date converted to DATE(2026,1,1)")

        cell_bound = by_type_op.get(('date', '$B$1'))
        if cell_bound is None \
                or cell_bound.operator != 'greaterThanOrEqual':
            print("✗ Cell-reference bound or operator mapping wrong")
            return False
        print("✓ '=$B$1' bound stripped to $B$1, operator mapped")
        return True

    finally:
        WorkbookSession.reset()
        if os.path.exists(target_file):
            os.unlink(target_file)


def test_behaviors_and_alerts():
    """show_dropdown inversion, allow_blank, prompts, error styles."""
    print("\nTesting behavior flags and alerts...")

    with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as handle:
        target_file = handle.name

    try:
        make_workbook(target_file)
        run_step(target_file, [
            {'sheet_name': 'Data_Entry',
             'apply_to_ranges': ['B2:B100'],
             'validation_type': 'list',
             'values_list': ['A', 'B'],
             'show_dropdown': False,
             'allow_blank': False,
             'input_prompt': {'title': 'Pick', 'message': 'A or B.'},
             'error_alert': {'style': 'warning', 'title': 'Bad',
                             'message': 'Not A or B.'}},
        ])

        record = stored_validations(target_file)[0]

        # OOXML inversion: config show_dropdown False -> attribute True.
        if record.showDropDown is not True:
            print(f"✗ showDropDown inversion wrong: {record.showDropDown}")
            return False
        print("✓ show_dropdown: false stored as inverted showDropDown=True")

        if record.allowBlank:
            print("✗ allow_blank: false not stored")
            return False
        print("✓ allow_blank: false stored")

        if not (record.showInputMessage and record.promptTitle == 'Pick'
                and record.prompt == 'A or B.'):
            print("✗ Input prompt fields wrong")
            return False
        print("✓ Input prompt stored")

        if not (record.showErrorMessage and record.errorStyle == 'warning'
                and record.errorTitle == 'Bad'):
            print("✗ Error alert fields wrong")
            return False
        print("✓ Error alert with warning style stored")
        return True

    finally:
        WorkbookSession.reset()
        if os.path.exists(target_file):
            os.unlink(target_file)


def test_guided_errors():
    """Config validator refuses misuse loudly, with guidance."""
    print("\nTesting guided errors...")

    base_entry = {'sheet_name': 'Data_Entry',
                  'apply_to_ranges': ['B2'],
                  'validation_type': 'list',
                  'values_list': ['A']}

    cases = [
        ("two list sources",
         dict(base_entry, list_from_named_range='rng_x'),
         "exactly one of"),
        ("show_dropdown on bounded type",
         {'sheet_name': 'Data_Entry', 'apply_to_ranges': ['B2'],
          'validation_type': 'whole_number', 'operator': 'equal',
          'compare_to': 1, 'show_dropdown': True},
         "only applies to"),
        ("comma inside inline item",
         dict(base_entry, values_list=['Good', 'Bad,Worse']),
         "list_from_named_range"),
        ("bad range shape",
         dict(base_entry, apply_to_ranges=['B2:B']),
         "A1-style"),
        ("retired singular 'range' key",
         dict(base_entry, range='B2'),
         "apply_to_ranges"),
        ("interval missing maximum",
         {'sheet_name': 'Data_Entry', 'apply_to_ranges': ['B2'],
          'validation_type': 'decimal', 'operator': 'between',
          'minimum': 1},
         "'minimum' and 'maximum'"),
        ("bad error style",
         dict(base_entry, error_alert={'style': 'severe', 'message': 'x'}),
         "not one of"),
    ]

    for label, entry, expected_fragment in cases:
        try:
            ExcelDataValidationProcessor({
                'processor_type': 'excel_data_validation',
                'target_file': 'unused.xlsx',
                'validations': [entry],
            })
            print(f"✗ {label}: should have raised")
            return False
        except StepProcessorError as error:
            if expected_fragment not in str(error):
                print(f"✗ {label}: error lacks guidance: {error}")
                return False
            print(f"✓ {label}: refused with guidance")

    return True


def main():
    """Run all tests and report results."""
    print("excel_data_validation processor tests")
    print("=" * 50)

    tests = [
        test_list_sources_and_sqref,
        test_bounds_and_date_conversion,
        test_behaviors_and_alerts,
        test_guided_errors,
    ]

    passed = 0
    for test in tests:
        try:
            if test():
                passed += 1
            else:
                print(f"FAILED: {test.__name__}")
        except Exception as error:
            print(f"FAILED with exception: {test.__name__}: {error}")

    print("=" * 50)
    print(f"Final score: {passed}/{len(tests)} tests passed")
    return passed == len(tests)


if __name__ == '__main__':
    sys.exit(0 if main() else 1)

# End of file #
