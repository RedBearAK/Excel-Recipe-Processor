"""
Tests for the profile_files processor.

tests/test_profile_files.py

Runnable with pytest, but written to run standalone and report a score.
"""

import os
import time
import tempfile

from pathlib import Path
from datetime import datetime

from excel_recipe_processor.processors.profile_files_processor import ProfileFilesProcessor


def build(files, **overrides):
    config = {'processor_type': 'profile_files', 'files': files,
              'save_to_stage': 'stg_test_meta'}
    config.update(overrides)
    return ProfileFilesProcessor(config)


def test_metadata_matches_the_filesystem():
    """Modified and size come from the actual file, to the second."""
    print("\nTesting metadata matches the filesystem...")

    with tempfile.TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / 'sample.xlsx'
        path.write_bytes(b'x' * 2048)

        frame = build([str(path)]).load_data()
        row = frame.iloc[0]

        expected_mtime = datetime.fromtimestamp(path.stat().st_mtime).replace(microsecond=0)

        passed = True

        if row['File'] == 'sample.xlsx':
            print("  ✓ File is the basename")
        else:
            print(f"  ✗ File is {row['File']!r}")
            passed = False

        if row['Modified'] == expected_mtime:
            print(f"  ✓ Modified matches stat: {row['Modified']}")
        else:
            print(f"  ✗ Modified {row['Modified']} != {expected_mtime}")
            passed = False

        if row['Size (KB)'] == 2.0:
            print("  ✓ Size is 2.0 KB")
        else:
            print(f"  ✗ Size is {row['Size (KB)']}")
            passed = False

        return passed


def test_rows_keep_listed_order():
    """Output order is the config order, not alphabetical."""
    print("\nTesting listed order is preserved...")

    with tempfile.TemporaryDirectory() as temp_dir:
        names = ['zebra.xlsx', 'apple.xlsx', 'mango.xlsx']
        for name in names:
            (Path(temp_dir) / name).write_bytes(b'x')

        frame = build([str(Path(temp_dir) / n) for n in names]).load_data()

        if list(frame['File']) == names:
            print(f"  ✓ Order preserved: {list(frame['File'])}")
            return True

        print(f"  ✗ Order is {list(frame['File'])}")
        return False


def test_missing_file_halts_by_default():
    """A provenance sheet must not silently omit a missing input."""
    print("\nTesting a missing file halts...")

    try:
        build(['/nonexistent/nowhere.xlsx']).load_data()
    except Exception as error:
        if 'not found' in str(error):
            print("  ✓ Halted, naming the file")
            return True
        print(f"  ✗ Wrong error: {error}")
        return False

    print("  ✗ Did not halt")
    return False


def test_on_missing_note_and_skip():
    """note keeps a MISSING row; skip drops it."""
    print("\nTesting on_missing note and skip...")

    with tempfile.TemporaryDirectory() as temp_dir:
        real = Path(temp_dir) / 'real.xlsx'
        real.write_bytes(b'x')
        listed = [str(real), str(Path(temp_dir) / 'ghost.xlsx')]

        noted = build(listed, on_missing='note').load_data()
        skipped = build(listed, on_missing='skip').load_data()

        passed = True

        if len(noted) == 2 and noted['Modified'].iloc[1] == 'MISSING':
            print("  ✓ note: 2 rows, second marked MISSING")
        else:
            print(f"  ✗ note gave {len(noted)} rows: {list(noted['Modified'])}")
            passed = False

        if len(skipped) == 1 and skipped['File'].iloc[0] == 'real.xlsx':
            print("  ✓ skip: only the real file remains")
        else:
            print(f"  ✗ skip gave {len(skipped)} rows")
            passed = False

        return passed


def test_invalid_config_rejected():
    """Bad on_missing and missing files list fail at construction."""
    print("\nTesting invalid configuration is rejected...")

    passed = True

    try:
        build(['x.xlsx'], on_missing='banana')
        print("  ✗ on_missing 'banana' accepted")
        passed = False
    except Exception:
        print("  ✓ bad on_missing rejected")

    try:
        ProfileFilesProcessor({'processor_type': 'profile_files',
                               'save_to_stage': 'stg_x'})
        print("  ✗ absent files list accepted")
        passed = False
    except Exception:
        print("  ✓ absent files list rejected")

    return passed


def main():
    """Run every test and report a final score."""
    print("=== profile_files tests ===")

    tests = [
        test_metadata_matches_the_filesystem,
        test_rows_keep_listed_order,
        test_missing_file_halts_by_default,
        test_on_missing_note_and_skip,
        test_invalid_config_rejected,
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
        print("✅ All profile_files tests passed!")
        return 1

    print("❌ Some profile_files tests failed!")
    return 0


if __name__ == '__main__':
    exit(0 if main() else 1)


# End of file #
