"""
Tests for the log-file mirroring feature.

tests/test_log_file_output.py

The stream carries no ANSI codes, so a FileHandler with explicit UTF-8
captures the terminal content byte-identically - Unicode symbols and
all. Two behaviors pinned: the file receives emoji intact, and a
recipe-settings attachment defers to an already-active --log-file.
Runnable directly or with pytest; direct runs are the authoritative
score.
"""

import os
import sys
import logging
import tempfile

from excel_recipe_processor.core import main as erp_main


def reset_state():
    """Fresh handler/registry state for each test."""
    root = logging.getLogger()
    # A bare test process has no basicConfig, leaving root at WARNING -
    # INFO records would be dropped before any handler sees them
    root.setLevel(logging.INFO)
    for handler in list(root.handlers):
        if isinstance(handler, logging.FileHandler):
            root.removeHandler(handler)
            handler.close()
    erp_main._attached_log_files = {'cli': None, 'recipe': None}


def test_file_receives_unicode():
    """Attached file carries the emoji-laden lines byte-identically."""
    print("\nTesting UTF-8 log mirroring...")
    reset_state()
    with tempfile.NamedTemporaryFile(suffix='.txt', delete=False) as handle:
        path = handle.name
    try:
        attached = erp_main.attach_log_file(path, source='cli')
        message = "🧬 Dynamic-array declaration: 5 cell(s) marked 📏 ✅"
        logging.getLogger('test').info(message)
        logging.getLogger().handlers  # flush via close below
        reset_state()  # closes the handler, flushing it

        content = open(path, encoding='utf-8').read()
        if not attached or message not in content:
            print(f"✗ file content wrong: {content!r}")
            return False
        if '🪵 Logging to file' not in content:
            print("✗ the attach announcement did not reach the file")
            return False
        print("✓ emoji and announcement byte-identical in the file")
        return True
    finally:
        os.unlink(path)


def test_cli_outranks_recipe():
    """A recipe-settings attachment is skipped when the CLI one is live."""
    print("\nTesting CLI-over-recipe precedence...")
    reset_state()
    with tempfile.NamedTemporaryFile(suffix='.txt', delete=False) as handle:
        cli_path = handle.name
    with tempfile.NamedTemporaryFile(suffix='.txt', delete=False) as handle:
        recipe_path = handle.name
    try:
        first = erp_main.attach_log_file(cli_path, source='cli')
        second = erp_main.attach_log_file(recipe_path, source='recipe')
        reset_state()

        if not first or second:
            print(f"✗ precedence wrong: cli={first}, recipe={second}")
            return False
        if os.path.getsize(recipe_path) != 0:
            print("✗ the skipped recipe file was written to")
            return False
        print("✓ recipe attachment skipped, its file untouched")
        return True
    finally:
        os.unlink(cli_path)
        os.unlink(recipe_path)


def main():
    """Run all tests and report results."""
    print("Log-file output tests")
    print("=" * 50)

    tests = [
        test_file_receives_unicode,
        test_cli_outranks_recipe,
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
