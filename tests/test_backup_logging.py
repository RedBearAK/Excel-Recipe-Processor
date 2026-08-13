"""
Tests for timestamped backups, trimming, and backup logging.

tests/test_backup_logging.py

Backups are named report_erpbkup_YYMMDD_HHMMSS.xlsx - extension preserved so
the file still opens in its default application, and marked with a token
that essentially cannot arise from anything but this tool, because the
trimmer DELETES everything the pattern matches.

The deletion tests matter most: a trimmer that reaches outside its own
pattern would quietly destroy a neighbouring file's backups or a hand-named
file, so those cases are asserted explicitly.

Runnable with pytest, but written to run standalone and report a score.
"""

import time
import logging
import tempfile

from pathlib import Path

import openpyxl

from excel_recipe_processor.core.file_writer import FileWriter


class LogCapture(logging.Handler):
    """Collect log messages for assertion."""

    def __init__(self):
        super().__init__()
        self.messages = []

    def emit(self, record):
        self.messages.append(record.getMessage())


def make_file(path):
    wb = openpyxl.Workbook()
    wb.active['A1'] = 'value'
    wb.save(path)
    wb.close()


def capture_backup(path):
    """Run one backup, returning (backup_path, captured messages)."""
    capture = LogCapture()
    root = logging.getLogger()
    previous_level = root.level
    root.addHandler(capture)
    root.setLevel(logging.INFO)

    try:
        backup_path = FileWriter.create_backup(path)
    finally:
        root.removeHandler(capture)
        root.setLevel(previous_level)

    return backup_path, capture.messages


def test_one_backup_logs_exactly_one_line():
    """One copy, one log line - not two."""
    print("\nTesting backup log emission...")

    with tempfile.TemporaryDirectory() as temp_dir:
        path = str(Path(temp_dir) / 'data.xlsx')
        make_file(path)

        backup_path, messages = capture_backup(path)
        backup_lines = [m for m in messages if 'Created backup' in m]

        passed = True

        if len(backup_lines) == 1:
            print("  ✓ Exactly one 'Created backup' line")
        else:
            print(f"  ✗ {len(backup_lines)} lines: {backup_lines}")
            passed = False

        created = backups_in(temp_dir)
        if len(created) == 1:
            print(f"  ✓ Exactly one backup file on disk ({created[0]})")
        else:
            print(f"  ✗ Files: {created}")
            passed = False

        if Path(backup_path).exists():
            print("  ✓ Returned path exists")
        else:
            print(f"  ✗ Returned path missing: {backup_path}")
            passed = False

        return passed


def backups_in(folder):
    """Backup files present, newest last."""
    return sorted(p.name for p in Path(folder).iterdir() if '_erpbkup_' in p.name)


def test_backup_keeps_the_extension():
    """The marker goes BEFORE the extension so the file still opens."""
    print("\nTesting backup naming...")

    with tempfile.TemporaryDirectory() as temp_dir:
        path = str(Path(temp_dir) / 'data.xlsx')
        make_file(path)

        backup_path = Path(FileWriter.create_backup(path))

        passed = True

        if backup_path.suffix == '.xlsx':
            print(f"  ✓ Extension preserved: {backup_path.name}")
        else:
            print(f"  ✗ Extension is {backup_path.suffix}")
            passed = False

        if '_erpbkup_' in backup_path.stem:
            print("  ✓ Carries the _erpbkup_ marker")
        else:
            print(f"  ✗ Stem is {backup_path.stem}")
            passed = False

        return passed


def test_trim_keeps_the_newest_and_deletes_the_rest():
    """delete_backups_beyond: N keeps the N newest and deletes the rest."""
    print("\nTesting delete_backups_beyond...")

    with tempfile.TemporaryDirectory() as temp_dir:
        path = str(Path(temp_dir) / 'data.xlsx')
        make_file(path)

        made = []
        for _ in range(5):
            made.append(Path(FileWriter.create_backup(path, delete_backups_beyond=2)).name)
            time.sleep(1.05)   # distinct second, so timestamps differ

        remaining = backups_in(temp_dir)

        passed = True

        if len(remaining) == 2:
            print("  ✓ Two newest kept across 5 runs, older deleted")
        else:
            print(f"  ✗ {len(remaining)} remain: {remaining}")
            passed = False

        if remaining == made[-2:]:
            print("  ✓ The two survivors are the newest two")
        else:
            print(f"  ✗ survivors {remaining}, expected {made[-2:]}")
            passed = False

        return passed


def test_trim_never_reaches_outside_its_pattern():
    """Neighbouring backups, lookalikes and legacy files must survive."""
    print("\nTesting deletion safety...")

    with tempfile.TemporaryDirectory() as temp_dir:
        folder = Path(temp_dir)
        path = str(folder / 'data.xlsx')
        make_file(path)

        untouchable = [
            'other.xlsx',                          # a neighbouring file
            'other_erpbkup_260101_000000.xlsx',    # ITS backup
            'data_bkup_260101_000000.xlsx',        # human shorthand, not ours
            'data_erpbkup_notatimestamp.xlsx',     # marker but malformed
        ]
        for name in untouchable:
            make_file(str(folder / name))

        legacy = folder / 'data.xlsx.backup'
        legacy.write_text('legacy')

        for _ in range(3):
            FileWriter.create_backup(path, delete_backups_beyond=1)
            time.sleep(1.05)

        survivors = {p.name for p in folder.iterdir()}

        passed = True

        missing = [name for name in untouchable if name not in survivors]
        if not missing:
            print(f"  ✓ All {len(untouchable)} out-of-pattern files survived")
        else:
            print(f"  ✗ Deleted: {missing}")
            passed = False

        if legacy.name in survivors:
            print("  ✓ Legacy .backup file left alone")
        else:
            print("  ✗ Legacy .backup file was deleted")
            passed = False

        # Count only WELL-FORMED backups of this file: the malformed
        # lookalike planted above also contains the marker, and it is
        # supposed to survive, so it must not be counted as one of ours.
        import re as _re
        well_formed = [name for name in survivors
                       if _re.match(r'^data_erpbkup_\d{6}_\d{6}(?:_\d+)?\.xlsx$', name)]

        if len(well_formed) == 1:
            print(f"  ✓ Exactly one well-formed backup kept: {well_formed[0]}")
        else:
            print(f"  ✗ {well_formed}")
            passed = False

        return passed


def test_same_second_collision_gets_a_distinct_name():
    """Two backups inside one second must not overwrite each other."""
    print("\nTesting same-second collisions...")

    with tempfile.TemporaryDirectory() as temp_dir:
        path = str(Path(temp_dir) / 'data.xlsx')
        make_file(path)

        first = Path(FileWriter.create_backup(path, delete_backups_beyond=5)).name
        second = Path(FileWriter.create_backup(path, delete_backups_beyond=5)).name

        if first != second and len(backups_in(temp_dir)) == 2:
            print(f"  ✓ Distinct names within one second: {first}, {second}")
            return True
        print(f"  ✗ {first} vs {second}")
        return False


def test_zero_and_negative_values():
    """Keeping zero means making none; a negative count is an error."""
    print("\nTesting boundary values...")

    with tempfile.TemporaryDirectory() as temp_dir:
        path = str(Path(temp_dir) / 'data.xlsx')
        make_file(path)

        passed = True

        result = FileWriter.create_backup(path, delete_backups_beyond=0)
        if result is None and not backups_in(temp_dir):
            print("  ✓ Keeping zero makes no backup")
        else:
            print(f"  ✗ returned {result}, files {backups_in(temp_dir)}")
            passed = False

        try:
            FileWriter.create_backup(path, delete_backups_beyond=-1)
            print("  ✗ Negative count accepted")
            passed = False
        except Exception:
            print("  ✓ Negative count rejected")

        return passed


def test_backup_content_matches_the_source():
    """The backup is a real copy, not an empty placeholder."""
    print("\nTesting backup content...")

    with tempfile.TemporaryDirectory() as temp_dir:
        path = str(Path(temp_dir) / 'data.xlsx')
        make_file(path)

        backup_path = FileWriter.create_backup(path)

        # The backup keeps a .backup suffix, which openpyxl refuses to open
        # by extension, so compare the bytes instead - a copy is a copy.
        source_bytes = Path(path).read_bytes()
        backup_bytes = Path(backup_path).read_bytes()

        if source_bytes == backup_bytes and len(backup_bytes) > 0:
            print(f"  ✓ Backup is a byte-for-byte copy ({len(backup_bytes)} bytes)")
            return True
        print(f"  ✗ source {len(source_bytes)} bytes vs backup {len(backup_bytes)}")
        return False


def main():
    """Run every test and report a final score."""
    print("=== backup logging tests ===")

    tests = [
        test_one_backup_logs_exactly_one_line,
        test_backup_keeps_the_extension,
        test_trim_keeps_the_newest_and_deletes_the_rest,
        test_trim_never_reaches_outside_its_pattern,
        test_same_second_collision_gets_a_distinct_name,
        test_zero_and_negative_values,
        test_backup_content_matches_the_source,
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
        print("✅ All backup logging tests passed!")
        return 1

    print("❌ Some backup logging tests failed!")
    return 0


if __name__ == '__main__':
    exit(0 if main() else 1)


# End of file #
