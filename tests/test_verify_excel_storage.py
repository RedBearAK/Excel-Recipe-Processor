"""
Tests for the verify_excel_storage processor.

tests/test_verify_excel_storage.py

The audits themselves are pinned elsewhere (the grammar corpus, the
declaration tests); these tests pin the PROCESSOR behaviors around
them: a clean file passes, planted violations halt with every one
listed, warn mode continues, and - the honest part - a file in the
workbook SESSION audits its will-be-written bytes, catching an
in-memory violation the stale disk copy does not carry. Runnable
directly or with pytest; direct runs are the authoritative score.
"""

import os
import sys
import tempfile

from openpyxl import Workbook
from openpyxl.workbook.defined_name import DefinedName
from openpyxl.worksheet.formula import ArrayFormula

from excel_recipe_processor.core.base_processor import StepProcessorError
from excel_recipe_processor.core.workbook_session import WorkbookSession
from excel_recipe_processor.processors.verify_excel_storage_processor import (
    VerifyExcelStorageProcessor,
)


def run_processor(files, on_violation='halt'):
    config = {'processor_type': 'verify_excel_storage',
              'files': files, 'on_violation': on_violation}
    return VerifyExcelStorageProcessor(config).perform_file_operation()


def save_clean_workbook(path):
    workbook = Workbook()
    sheet = workbook.active
    sheet['A1'] = 'Header'
    sheet['A2'] = 42
    workbook.defined_names['rng_ok'] = DefinedName(
        'rng_ok', attr_text='Sheet!$A$2')
    workbook.save(path)


def save_dirty_workbook(path):
    """Two violation classes planted: bad grammar and legacy CSE."""
    workbook = Workbook()
    sheet = workbook.active
    sheet['A1'] = 'Header'
    # Legacy CSE: t="array" saved by openpyxl with NO cm declaration
    sheet['A2'] = ArrayFormula('A2', '=SUM(A1:A1)')
    # Grammar: a LAMBDA whose declaration slot lacks _xlpm.
    workbook.defined_names['fn_bad'] = DefinedName(
        'fn_bad', attr_text='_xlfn.LAMBDA(v,v)')
    workbook.save(path)


def test_clean_and_dirty():
    """Clean passes; planted violations halt with each one named."""
    print("\nTesting clean pass and violation halt...")
    WorkbookSession.close_all() if hasattr(WorkbookSession, 'close_all') else None

    with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as good, \
         tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as bad:
        good_path, bad_path = good.name, bad.name
    try:
        save_clean_workbook(good_path)
        save_dirty_workbook(bad_path)

        result = run_processor([good_path])
        if 'clean' not in result:
            print(f"✗ clean file did not report clean: {result}")
            return False
        print("✓ clean workbook passes")

        try:
            run_processor([bad_path])
            print("✗ dirty workbook should have halted")
            return False
        except StepProcessorError as error:
            message = str(error)
            if 'lacks _xlpm.' not in message or 'legacy CSE' not in message:
                print(f"✗ halt message incomplete: {message}")
                return False
            print("✓ both violation classes named in the halt")

        result = run_processor([bad_path], on_violation='warn')
        if 'warn mode' not in result:
            print(f"✗ warn mode did not continue: {result}")
            return False
        print("✓ warn mode logs and continues")
        return True
    finally:
        os.unlink(good_path)
        os.unlink(bad_path)


def test_session_audits_pending_bytes():
    """An in-memory violation is caught though the DISK copy is clean."""
    print("\nTesting the session-aware path...")

    with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as handle:
        path = handle.name
    try:
        save_clean_workbook(path)

        # Pull the file into the session and plant a violation ONLY in
        # the live object - the disk bytes remain clean
        workbook = WorkbookSession.get_workbook(path)
        workbook.defined_names['fn_pending_bad'] = DefinedName(
            'fn_pending_bad', attr_text='_xlfn.LAMBDA(x,x)')

        try:
            run_processor([path])
            print("✗ pending in-memory violation not caught")
            return False
        except StepProcessorError as error:
            if 'fn_pending_bad' not in str(error) \
                    or 'will-be-written' not in str(error):
                print(f"✗ wrong provenance or target: {error}")
                return False
            print("✓ session workbook audited as will-be-written bytes;")
            print("  the stale disk copy could not have caught this")
        return True
    finally:
        WorkbookSession._open_workbooks.pop(
            WorkbookSession._key(path), None)
        os.unlink(path)


def main():
    """Run all tests and report results."""
    print("verify_excel_storage tests")
    print("=" * 50)

    tests = [
        test_clean_and_dirty,
        test_session_audits_pending_bytes,
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
