"""
Antagonistic tests for write_mode in_place and the shared
write-and-verify path.

tests/test_sever_external_ties_in_place.py

Covers: the in-place happy path (backup taken, source replaced,
verified, no temp left behind); a lock file refusing before anything
happens; an existing backup refusing; a refusal leaving the source and
producing no backup; a deliberately tampered part (changed without
being claimed) failing verification with the source untouched; a
claimed-removed part that is still present failing; new-file-only keys
refused with in_place; and verify_with_openpyxl. Runnable directly or
with pytest.
"""

import os
import sys
import tempfile

from excel_recipe_processor.core.base_processor import StepProcessorError
from excel_recipe_processor.processors._helpers.external_ties_sever import SeverPlan
from excel_recipe_processor.processors._helpers.xlsx_package_write import (
    lock_file_for,
    PackageWriteError,
)
from excel_recipe_processor.processors._helpers.external_ties_inventory import (
    inventory_external_ties,
)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from test_audit_external_ties import build_hostile_workbook  # noqa: E402
from test_sever_external_ties import (  # noqa: E402
    sha,
    check_all,
    only_output,
    make_processor,
    build_linked_workbook,
)

DEFAULT_POLICIES = {'formula': 'freeze', 'defined_name': 'drop',
                    'conditional_formatting': 'drop', 'data_validation': 'drop'}


def leftovers(workdir: str) -> list:
    return sorted(name for name in os.listdir(workdir) if name.startswith('.'))


def test_in_place_happy_path():
    """Source replaced by the verified result; backup is the original."""
    print("\nTesting in-place severing...")
    with tempfile.TemporaryDirectory() as workdir:
        path = os.path.join(workdir, 'copied.xlsx')
        build_linked_workbook(path, local_sheet_for_refs=True)
        before = sha(path)
        result = make_processor({'files': [path],
                                 'write_mode': 'in_place'}).perform_file_operation()
        backup = path + '.severbak'
        after = inventory_external_ties(path)
        return check_all([
            ('in place' in result and 'copied.xlsx' in result, f"result: {result}"),
            (os.path.exists(backup) and sha(backup) == before, 'backup is the untouched original'),
            (sha(path) != before, 'source now holds the result'),
            (not after['summary']['has_ties'], 'source re-inventories clean'),
            (only_output(workdir, 'copied') == '', 'no timestamped copy written'),
            (leftovers(workdir) == [], 'no temp file left behind'),
        ])


def test_lock_file_and_existing_backup_refuse():
    """Excel's ~$ lock refuses before any work; a stale backup refuses too."""
    print("\nTesting lock-file and existing-backup refusals...")
    with tempfile.TemporaryDirectory() as workdir:
        path = os.path.join(workdir, 'copied.xlsx')
        build_linked_workbook(path, local_sheet_for_refs=True)
        before = sha(path)
        lock = lock_file_for(path)
        open(lock, 'w').close()
        try:
            make_processor({'files': [path], 'write_mode': 'in_place'}).perform_file_operation()
            lock_raised = ''
        except StepProcessorError as error:
            lock_raised = str(error)
        os.remove(lock)

        open(path + '.severbak', 'w').close()
        try:
            make_processor({'files': [path], 'write_mode': 'in_place'}).perform_file_operation()
            backup_raised = ''
        except StepProcessorError as error:
            backup_raised = str(error)
        return check_all([
            ('open in Excel' in lock_raised, f"lock: {lock_raised[:60]}"),
            ('backup already exists' in backup_raised, f"backup: {backup_raised[:60]}"),
            (sha(path) == before, 'source untouched by both refusals'),
            (os.path.getsize(path + '.severbak') == 0, 'stale backup not overwritten'),
            (leftovers(workdir) == [], 'no temp file left behind'),
        ])


def test_refusal_in_place_leaves_no_backup():
    """A surgery refusal in in-place mode writes nothing, not even a backup."""
    print("\nTesting in-place refusal leaves no trace...")
    with tempfile.TemporaryDirectory() as workdir:
        path = os.path.join(workdir, 'hostile.xlsx')
        build_hostile_workbook(path)
        before = sha(path)
        try:
            make_processor({'files': [path], 'write_mode': 'in_place'}).perform_file_operation()
            raised = ''
        except StepProcessorError as error:
            raised = str(error)
        return check_all([
            ('no output written' in raised, 'refused'),
            (sha(path) == before, 'source untouched'),
            (not os.path.exists(path + '.severbak'), 'no backup taken'),
            (leftovers(workdir) == [], 'no temp file left behind'),
        ])


def test_tampered_part_fails_verification():
    """An unclaimed change to any part is caught; nothing is replaced."""
    print("\nTesting the untouched-parts proof against a tampered plan...")
    with tempfile.TemporaryDirectory() as workdir:
        path = os.path.join(workdir, 'copied.xlsx')
        build_linked_workbook(path, local_sheet_for_refs=True)
        before = sha(path)
        plan = SeverPlan(path, DEFAULT_POLICIES, retarget_if_possible=True)
        plan.run()
        # Corrupt a part the plan never claimed, bypassing _set_text.
        plan.parts['xl/worksheets/sheet2.xml'] = plan.parts['xl/worksheets/sheet2.xml'] + b' '
        try:
            plan.write(os.path.join(workdir, 'out.xlsx'), in_place=False,
                       backup_suffix='.severbak')
            new_raised = ''
        except PackageWriteError as error:
            new_raised = str(error)
        try:
            plan.write(path, in_place=True, backup_suffix='.severbak')
            in_place_raised = ''
        except PackageWriteError as error:
            in_place_raised = str(error)

        # A part claimed removed but still present.
        plan2 = SeverPlan(path, DEFAULT_POLICIES, retarget_if_possible=True)
        plan2.run()
        plan2.removed_parts.add('xl/worksheets/sheet2.xml')
        try:
            plan2.write(os.path.join(workdir, 'out2.xlsx'), in_place=False,
                        backup_suffix='.severbak')
            still_raised = ''
        except PackageWriteError as error:
            still_raised = str(error)
        return check_all([
            ('did not claim' in new_raised and 'sheet2.xml' in new_raised,
             f"new-file: {new_raised[:70]}"),
            ('did not claim' in in_place_raised, 'in-place: same proof'),
            ('claimed removed are still present' in still_raised, f"removed: {still_raised[:60]}"),
            (sha(path) == before and not os.path.exists(path + '.severbak'),
             'source untouched, no backup'),
            (not os.path.exists(os.path.join(workdir, 'out.xlsx')), 'no output named'),
            (leftovers(workdir) == [], 'temp files removed'),
        ])


def test_new_file_keys_refused_with_in_place():
    """output_dir / output_suffix / timestamp_format cannot combine with in_place."""
    print("\nTesting new-file-only keys refused with in_place...")
    raised = {}
    for key, value in (('output_dir', '.'), ('output_suffix', '_x'),
                       ('timestamp_format', '%Y')):
        try:
            make_processor({'write_mode': 'in_place', key: value})
            raised[key] = ''
        except StepProcessorError as error:
            raised[key] = str(error)
    try:
        make_processor({'write_mode': 'sideways'})
        bad_mode = ''
    except StepProcessorError as error:
        bad_mode = str(error)
    return check_all([
        (all('only apply to write_mode new_file' in text for text in raised.values()),
         f"refused: {sorted(k for k, v in raised.items() if v)}"),
        ('is not one of' in bad_mode, 'unknown mode refused by the schema'),
    ])


def test_verify_with_openpyxl():
    """The optional second-parser check passes on a good result."""
    print("\nTesting verify_with_openpyxl...")
    with tempfile.TemporaryDirectory() as workdir:
        path = os.path.join(workdir, 'gone.xlsx')
        build_linked_workbook(path, local_sheet_for_refs=False)
        report = os.path.join(workdir, 'r.json')
        result = make_processor({'files': [path], 'verify_with_openpyxl': True,
                                 'report_file': report}).perform_file_operation()
        output = only_output(workdir, 'gone')
        return check_all([
            ('severed 1 of 1' in result, f"result: {result}"),
            (output != '', 'output written after the openpyxl load passed'),
        ])


def main():
    """Run every test and report a final score."""
    print("=== sever_external_ties in-place tests ===")

    tests = [
        test_in_place_happy_path,
        test_lock_file_and_existing_backup_refuse,
        test_refusal_in_place_leaves_no_backup,
        test_tampered_part_fails_verification,
        test_new_file_keys_refused_with_in_place,
        test_verify_with_openpyxl,
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
