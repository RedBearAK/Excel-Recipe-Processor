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
    erp_main.discard_early_log_buffer()
    erp_main._attached_log_streams.clear()
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


def test_early_buffer_leads_the_file():
    """Startup lines buffered before attach open the file, in order."""
    print("\nTesting the early-record buffer...")
    reset_state()
    with tempfile.NamedTemporaryFile(suffix='.txt', delete=False) as handle:
        path = handle.name
    try:
        erp_main.install_early_log_buffer()
        logging.getLogger('test').info("📖 Loading recipe: early line one")
        logging.getLogger('test').info("✓ Resolved variables: early line two")
        erp_main.attach_log_file(path, source='recipe')
        logging.getLogger('test').info("⚡ live line after attach")
        reset_state()

        lines = open(path, encoding='utf-8').read().splitlines()
        expected_order = ('early line one', 'early line two',
                          '🪵 Logging to file', 'live line after attach')
        positions = []
        for fragment in expected_order:
            hits = [i for i, line in enumerate(lines) if fragment in line]
            if not hits:
                print(f"✗ missing from file: {fragment}")
                return False
            positions.append(hits[0])
        if positions != sorted(positions):
            print(f"✗ order wrong: {positions} for {expected_order}")
            return False
        print("✓ buffered lines open the file, announcement and live lines follow")
        return True
    finally:
        os.unlink(path)


def test_declined_decision_discards():
    """No directive at the seam: buffer dropped, no residue, no file."""
    print("\nTesting the discard path...")
    reset_state()
    erp_main.install_early_log_buffer()
    logging.getLogger('test').info("early line nobody wants")
    erp_main.discard_early_log_buffer()

    root_buffers = [h for h in logging.getLogger().handlers
                    if isinstance(h, erp_main._EarlyLogBuffer)]
    if root_buffers or erp_main._early_buffer is not None:
        print("✗ buffer still installed after discard")
        return False
    print("✓ buffer removed and cleared at the decision point")
    return True


def test_mirror_print_reaches_file_unprefixed():
    """Print-based summary lines land in the file, byte-matching terminal."""
    print("\nTesting mirror_print summary parity...")
    reset_state()
    with tempfile.NamedTemporaryFile(suffix='.txt', delete=False) as handle:
        path = handle.name
    try:
        erp_main.attach_log_file(path, source='recipe')
        erp_main.mirror_print()
        erp_main.mirror_print("✓ Recipe completed successfully in 21.7s")
        erp_main.mirror_print("  Steps executed: 73")
        reset_state()

        content = open(path, encoding='utf-8').read()
        if "✓ Recipe completed successfully in 21.7s" not in content \
                or "  Steps executed: 73" not in content:
            print(f"✗ summary missing from file: {content!r}")
            return False
        if "INFO: ✓ Recipe completed" in content:
            print("✗ summary was prefixed - file no longer matches terminal")
            return False
        print("✓ summary lines in the file, unprefixed, terminal-identical")
        return True
    finally:
        os.unlink(path)


def main():
    """Run all tests and report results."""
    print("Log-file output tests")
    print("=" * 50)

    tests = [
        test_file_receives_unicode,
        test_cli_outranks_recipe,
        test_early_buffer_leads_the_file,
        test_declined_decision_discards,
        test_mirror_print_reaches_file_unprefixed,
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
