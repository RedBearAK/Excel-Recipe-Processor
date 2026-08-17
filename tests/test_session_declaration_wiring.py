"""
Wiring test: the SESSION save path applies the dynamic-array declaration.

tests/test_session_declaration_wiring.py

The declaration machinery itself is unit-tested to the byte in
test_dynamic_array_metadata; this module pins the WIRING above it. The
guarantee users rely on lives in WorkbookSession._save_workbook: with
declaration enabled, the workbook serializes to memory, the declaration
is applied to the bytes, and only the corrected package touches disk -
"the file never exists on disk in the form that draws the
implicit-intersection @". If a refactor dropped that branch, or the
declare_dynamic flag stopped reaching the save, every byte-level unit
test would stay green while production output regressed to @-wearing
files. These tests fail in that world.

The flag is OPT-IN: recipe settings key 'declare_dynamic_formulas'
routes through RecipePipeline to WorkbookSession.set_declare_dynamic.

Runnable standalone or under pytest; the exit code carries the verdict.
"""

import os
import sys
import zipfile
import tempfile

import openpyxl

from pathlib import Path

from excel_recipe_processor.core.workbook_session import WorkbookSession
from excel_recipe_processor.core.excel_storage_audit import audit_legacy_cse


# XLOOKUP postdates dynamic arrays, so the declaration pass marks it
DYNAMIC_FORMULA = '=_xlfn.XLOOKUP(A1,D1:D3,E1:E3)'


def make_plain_workbook(path):
    """A workbook with data but no formulas, saved undeclared."""
    workbook = openpyxl.Workbook()
    sheet = workbook.active
    sheet['A1'] = 'key2'
    for row, (key, value) in enumerate(
            [('key1', 10), ('key2', 20), ('key3', 30)], start=1):
        sheet[f'D{row}'] = key
        sheet[f'E{row}'] = value
    workbook.save(path)
    workbook.close()


def read_worksheet_xml(path):
    """Raw sheet1 xml text from the saved package."""
    with zipfile.ZipFile(path) as archive:
        return archive.read('xl/worksheets/sheet1.xml').decode('utf-8')


def flush_with_dynamic_formula(path, declare):
    """Open via the session, plant a dynamic formula, flush to disk."""
    WorkbookSession.reset()
    WorkbookSession.set_deferred(True)
    WorkbookSession.set_declare_dynamic(declare)

    workbook = WorkbookSession.get_workbook(path)
    workbook.active['B1'] = DYNAMIC_FORMULA
    WorkbookSession.mark_dirty(path)
    WorkbookSession.flush_all()
    WorkbookSession.reset()


def test_enabled_session_save_lands_declared():
    """Flag on: the flushed file carries cm and the metadata part."""
    print("Testing declaration-enabled session flush...")

    with tempfile.TemporaryDirectory() as temp_dir:
        path = str(Path(temp_dir) / 'declared.xlsx')
        make_plain_workbook(path)

        flush_with_dynamic_formula(path, declare=True)

        passed = True
        sheet_xml = read_worksheet_xml(path)

        if 'cm="1"' in sheet_xml and '<f t="array" ref="B1">' in sheet_xml:
            print('  ✓ B1 stored with cm="1" and t="array" ref')
        else:
            marker = sheet_xml[sheet_xml.find('<c r="B1"'):][:120]
            print(f'  ✗ B1 stored undeclared: {marker}')
            passed = False

        with zipfile.ZipFile(path) as archive:
            has_part = 'xl/metadata.xml' in archive.namelist()
        if has_part:
            print('  ✓ xl/metadata.xml part present')
        else:
            print('  ✗ xl/metadata.xml part missing')
            passed = False

        violations = audit_legacy_cse(path)
        if not violations:
            print('  ✓ audit_legacy_cse clean - no @-drawing bytes on disk')
        else:
            print(f'  ✗ legacy CSE cells on disk: {violations}')
            passed = False

        return passed


def test_disabled_session_save_stays_plain():
    """Flag off: the toggle actually toggles - no declaration appears."""
    print("\nTesting declaration-disabled session flush...")

    with tempfile.TemporaryDirectory() as temp_dir:
        path = str(Path(temp_dir) / 'plain.xlsx')
        make_plain_workbook(path)

        flush_with_dynamic_formula(path, declare=False)

        passed = True
        sheet_xml = read_worksheet_xml(path)

        if 'cm="1"' not in sheet_xml:
            print('  ✓ no cm marker without the flag')
        else:
            print('  ✗ cm marker appeared with declaration disabled')
            passed = False

        with zipfile.ZipFile(path) as archive:
            has_part = 'xl/metadata.xml' in archive.namelist()
        if not has_part:
            print('  ✓ no metadata part without the flag')
        else:
            print('  ✗ metadata part appeared with declaration disabled')
            passed = False

        return passed


def test_pipeline_settings_key_reaches_the_session_flag():
    """The recipe settings key is wired through to set_declare_dynamic."""
    print("\nTesting the settings-key wiring in the pipeline source...")

    # A full recipe run is exercised elsewhere; this pins the WIRING
    # fact cheaply and loudly - the pipeline must read the settings key
    # and pass it to the session. If either half is renamed or removed,
    # this names the break.
    source_path = Path('excel_recipe_processor/core/recipe_pipeline.py')
    source = source_path.read_text()

    reads_key = "'declare_dynamic_formulas'" in source
    sets_flag = 'WorkbookSession.set_declare_dynamic(' in source

    passed = True
    if reads_key:
        print("  ✓ pipeline reads settings 'declare_dynamic_formulas'")
    else:
        print("  ✗ pipeline no longer reads the settings key")
        passed = False
    if sets_flag:
        print('  ✓ pipeline passes it to WorkbookSession.set_declare_dynamic')
    else:
        print('  ✗ pipeline no longer sets the session flag')
        passed = False
    return passed


def main():
    """Run every test and report a final score."""
    print("=== session declaration wiring tests ===")

    tests = [
        test_enabled_session_save_lands_declared,
        test_disabled_session_save_stays_plain,
        test_pipeline_settings_key_reaches_the_session_flag,
    ]

    passed = 0
    for test_func in tests:
        try:
            if test_func():
                passed += 1
        except Exception as error:
            print(f"✗ {test_func.__name__} crashed: {error}")

    print(f"\n=== Results: {passed}/{len(tests)} tests passed ===")
    return passed == len(tests)


if __name__ == '__main__':
    sys.exit(0 if main() else 1)

# End of file #
