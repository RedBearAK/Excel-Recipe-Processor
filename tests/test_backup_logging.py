"""
Tests for backup creation and its logging.

tests/test_backup_logging.py

FileWriter.create_backup delegates to ExcelWriter.create_backup, which logs
the backup at the point the copy happens. The wrapper used to log the same
line again, which read in the run log as two backups of one file.

Runnable with pytest, but written to run standalone and report a score.
"""

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

        created = sorted(p.name for p in Path(temp_dir).iterdir() if '.backup' in p.name)
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


def test_backups_accumulate_with_numbered_names():
    """Repeat runs keep every earlier backup - documented behaviour."""
    print("\nTesting repeated backups...")

    with tempfile.TemporaryDirectory() as temp_dir:
        path = str(Path(temp_dir) / 'data.xlsx')
        make_file(path)

        for _ in range(3):
            FileWriter.create_backup(path)

        created = sorted(p.name for p in Path(temp_dir).iterdir() if '.backup' in p.name)

        if created == ['data.xlsx.backup', 'data.xlsx.backup1', 'data.xlsx.backup2']:
            print(f"  ✓ Three distinct backups kept: {created}")
            return True
        print(f"  ✗ {created}")
        return False


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
        test_backups_accumulate_with_numbered_names,
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
