"""
Tests for the conditional_format processor.

tests/test_conditional_format_processor.py

Runnable with pytest, but written to run standalone and report a score.
Builds real xlsx files and asserts on the stored conditional-formatting XML
- the bytes Excel will actually evaluate.
"""

import re
import logging
import zipfile
import tempfile

import openpyxl
import pandas as pd

from pathlib import Path

from excel_recipe_processor.core.workbook_session import WorkbookSession
from excel_recipe_processor.core.base_processor import StepProcessorError
from excel_recipe_processor.processors.conditional_format_processor import ConditionalFormatProcessor


def build_target(work_dir):
    """A small sheet with the columns the rules reference."""
    file_path = str(Path(work_dir) / 'target.xlsx')
    pd.DataFrame({
        'Contracts': ['C1', '', 'C3'],
        'Price':     [None, 5, None],
        'Test Dest': [0, 2, 1],
        'Booking':   ['B1', 'B1', 'B2'],
    }).to_excel(file_path, index=False, sheet_name='Data')
    return file_path


def run_rules(file_path, rules):
    """Run one conditional_format step against the file, flush, return sheet XML."""
    WorkbookSession.reset()
    ConditionalFormatProcessor({
        'processor_type': 'conditional_format',
        'step_description': 'cf test',
        'target_file': file_path,
        'sheet_name': 'Data',
        'rules': rules,
    }).perform_file_operation()
    WorkbookSession.flush_all()

    with zipfile.ZipFile(file_path) as archive:
        return archive.read('xl/worksheets/sheet1.xml').decode('utf-8')


def test_formula_rule_locked_and_prefixed():
    """when_formula: {col:} resolves $-locked, _xlfn applied, entire_row range built."""
    print("\nTesting formula rule locking and prefixing...")

    passed = True

    with tempfile.TemporaryDirectory() as work_dir:
        xml = run_rules(build_target(work_dir), [
            {'when_formula': '=AND({col:Contracts}2<>"", IFS({col:Price}2="",TRUE,TRUE,FALSE))',
             'apply_to': 'entire_row',
             'style': {'fill': 'FFC7CE'}, 'stop_if_true': True},
        ])

        if 'AND($A2&lt;&gt;"", _xlfn.IFS($B2="",TRUE,TRUE,FALSE))' in xml:
            print("  ✓ Placeholders $-locked and IFS carries the _xlfn storage prefix")
        else:
            print(f"  ✗ Formula stored as: {re.search(r'<formula>.*?</formula>', xml).group(0)[:120]}")
            passed = False

        if '<conditionalFormatting sqref="A2:D4">' in xml:
            print("  ✓ entire_row range spans the full data body")
        else:
            print("  ✗ entire_row range wrong")
            passed = False

        if 'stopIfTrue="1"' in xml:
            print("  ✓ stop_if_true stored")
        else:
            print("  ✗ stop_if_true missing")
            passed = False

    return passed


def test_canonical_conditions_map_to_excel_rules():
    """Each canonical condition family lands as the correct Excel rule type."""
    print("\nTesting canonical condition mapping...")

    passed = True

    with tempfile.TemporaryDirectory() as work_dir:
        xml = run_rules(build_target(work_dir), [
            {'when_cell': {'column_names': ['Test Dest'], 'condition': 'greater_than', 'value': 1},
             'style': {'fill': 'FFEB9C'}},
            {'when_cell': {'column_names': ['Contracts'], 'condition': 'equals', 'value': 'C1'},
             'style': {'fill': 'EEEEEE'}},
            {'when_cell': {'column_names': ['Price'], 'condition': 'is_empty'},
             'style': {'fill': 'FFC7CE'}},
            {'when_cell': {'column_names': ['Booking'], 'condition': 'duplicates'},
             'style': {'fill': 'FFC7CE'}},
            {'when_cell': {'column_names': ['Contracts'], 'condition': 'contains', 'value': 'C'},
             'style': {'bold': True}},
        ])

        expectations = [
            ('greater_than -> cellIs greaterThan with bare number',
             'operator="greaterThan"' in xml and '<formula>1</formula>' in xml),
            ('equals string operand quoted for the formula grammar',
             '<formula>"C1"</formula>' in xml),
            ('is_empty -> containsBlanks with LEN(TRIM())',
             'containsBlanks' in xml and 'LEN(TRIM(B2))=0' in xml),
            ('duplicates -> duplicateValues', 'duplicateValues' in xml),
            ('contains -> containsText anchored at its own column',
             'NOT(ISERROR(SEARCH("C",A2)))' in xml),
        ]
        for label, ok in expectations:
            if ok:
                print(f"  ✓ {label}")
            else:
                print(f"  ✗ {label}")
                passed = False

    return passed


def test_alias_warns_once_and_normalizes():
    """Excel-native spelling works, warns exactly once, stores the same rule."""
    print("\nTesting alias acceptance with a single warning...")

    passed = True

    records = []
    handler = logging.Handler()
    handler.emit = lambda record: records.append(record.getMessage())
    cf_logger = logging.getLogger('excel_recipe_processor.processors.conditional_format_processor')
    cf_logger.addHandler(handler)

    try:
        with tempfile.TemporaryDirectory() as work_dir:
            xml = run_rules(build_target(work_dir), [
                {'when_cell': {'column_names': ['Test Dest'], 'condition': 'greaterThan', 'value': 1},
                 'style': {'fill': 'FFEB9C'}},
            ])
    finally:
        cf_logger.removeHandler(handler)

    warnings = [message for message in records if 'canonical ERP condition' in message]

    if len(warnings) == 1 and "'greater_than'" in warnings[0]:
        print("  ✓ Exactly one warning, naming the canonical form")
    else:
        print(f"  ✗ Warnings: {warnings}")
        passed = False

    if 'operator="greaterThan"' in xml:
        print("  ✓ Rule stored identically to the canonical spelling")
    else:
        print("  ✗ Aliased rule not stored")
        passed = False

    return passed


def test_validation_fails_loud():
    """Unknown conditions, missing pieces, and bad colors raise clearly."""
    print("\nTesting validation failures...")

    passed = True

    cases = [
        ('unknown condition',
         [{'when_cell': {'column_names': ['Price'], 'condition': 'sorta_biggish', 'value': 1}}],
         'unknown condition'),
        ('two rule kinds at once',
         [{'when_formula': '=1', 'when_cell': {'column_names': ['Price'], 'condition': 'is_empty'},
           'range': 'A2:A3'}],
         'exactly one'),
        ('between without a pair',
         [{'when_cell': {'column_names': ['Price'], 'condition': 'between', 'value': 5}}],
         'low, high'),
        ('bad color',
         [{'when_cell': {'column_names': ['Price'], 'condition': 'is_empty'},
           'style': {'fill': 'nope!'}}],
         'invalid fill'),
        ('formula without a target',
         [{'when_formula': '=TRUE()'}],
         'exactly one target'),
    ]

    for label, rules, expected_fragment in cases:
        try:
            ConditionalFormatProcessor({
                'processor_type': 'conditional_format', 'step_description': 'v',
                'target_file': 'x.xlsx', 'sheet_name': 'Data', 'rules': rules,
            })
            print(f"  ✗ {label}: accepted silently")
            passed = False
        except StepProcessorError as error:
            if expected_fragment.lower() in str(error).lower():
                print(f"  ✓ {label}: raised, message names the problem")
            else:
                print(f"  ✗ {label}: raised, but unhelpfully: {error}")
                passed = False

    return passed


def test_rules_survive_session_round_trip():
    """Rules written through the session survive a later openpyxl load/save."""
    print("\nTesting session round-trip survival...")

    with tempfile.TemporaryDirectory() as work_dir:
        file_path = build_target(work_dir)
        xml = run_rules(file_path, [
            {'when_cell': {'column_names': ['Price'], 'condition': 'is_empty'},
             'style': {'fill': 'FFC7CE'}},
            {'color_scale': {'column_names': ['Test Dest'],
                             'min_color': 'FFFFFF', 'max_color': '63BE7B'}},
        ])
        rules_before = xml.count('<cfRule')

        workbook = openpyxl.load_workbook(file_path)
        workbook.active['Z1'] = 'later edit'
        workbook.save(file_path)
        workbook.close()

        with zipfile.ZipFile(file_path) as archive:
            xml_after = archive.read('xl/worksheets/sheet1.xml').decode('utf-8')

        if xml_after.count('<cfRule') == rules_before and 'colorScale' in xml_after:
            print(f"  ✓ All {rules_before} rules (incl. color scale) survive load/save")
            return True

        print(f"  ✗ Rules before: {rules_before}, after: {xml_after.count('<cfRule')}")
        return False


def main():
    tests = [
        test_formula_rule_locked_and_prefixed,
        test_canonical_conditions_map_to_excel_rules,
        test_alias_warns_once_and_normalizes,
        test_validation_fails_loud,
        test_rules_survive_session_round_trip,
    ]

    passed = 0

    for test_func in tests:
        try:
            if test_func():
                passed += 1
        except Exception as error:
            print(f"  ✗ {test_func.__name__} crashed: {error}")

    print(f"\n=== Results: {passed}/{len(tests)} tests passed ===")

    if passed == len(tests):
        print("✅ All conditional_format tests passed!")
        return 1

    print("❌ Some conditional_format tests failed!")
    return 0


if __name__ == '__main__':
    exit(0 if main() else 1)


# End of file #
