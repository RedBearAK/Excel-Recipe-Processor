"""
Tests for the default log location and the log_file setting resolver.

tests/test_default_log_path.py

Runnable with pytest, but written to run standalone and report a score.
"""

import os
import sys

from pathlib import Path
from datetime import datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from excel_recipe_processor.core.default_log_path import (
    default_log_dir, default_log_path, resolve_log_file_setting,
)


def test_platform_folders():
    """Each platform maps to its standard per-user log folder."""
    print("\nTesting platform folders...")

    home = Path.home()
    cases = [
        ('darwin', {}, home / 'Library' / 'Logs' / 'excel_recipe_processor'),
        ('win32', {'LOCALAPPDATA': '/lad'}, Path('/lad') / 'excel_recipe_processor' / 'logs'),
        ('win32', {}, home / 'AppData' / 'Local' / 'excel_recipe_processor' / 'logs'),
        ('linux', {'XDG_STATE_HOME': '/xdg'}, Path('/xdg') / 'excel_recipe_processor' / 'logs'),
        ('linux', {}, home / '.local' / 'state' / 'excel_recipe_processor' / 'logs'),
    ]

    passed = True
    for platform, environ, expected in cases:
        actual = default_log_dir(platform, environ)
        if actual == expected:
            print(f"  ✓ {platform} {sorted(environ) or ''} -> {actual}")
        else:
            print(f"  ✗ {platform}: expected {expected}, got {actual}")
            passed = False

    return passed


def test_env_override_wins():
    """ERP_LOG_DIR beats the platform choice on every platform."""
    print("\nTesting ERP_LOG_DIR override...")

    for platform in ('darwin', 'win32', 'linux'):
        actual = default_log_dir(platform, {'ERP_LOG_DIR': '/custom/logs', 'XDG_STATE_HOME': '/xdg'})
        if actual != Path('/custom/logs'):
            print(f"  ✗ {platform}: override ignored, got {actual}")
            return False

    print("  ✓ Override honored on all three platforms")
    return True


def test_default_file_name():
    """<recipe stem>_<YYMMDD>_<HHMMSS>_log.txt inside the folder."""
    print("\nTesting default file name...")

    when = datetime(2026, 9, 4, 13, 5, 9)
    actual = default_log_path('/x/recipe_files/vms_merge_downloads.yaml', when,
                              'linux', {'ERP_LOG_DIR': '/logs'})
    expected = Path('/logs/vms_merge_downloads_260904_130509_log.txt')

    if actual == expected:
        print(f"  ✓ {actual}")
        return True

    print(f"  ✗ expected {expected}, got {actual}")
    return False


def test_setting_resolution():
    """absent/true -> default; false -> none; template -> substituted; junk -> error."""
    print("\nTesting settings.log_file resolution...")

    os.environ['ERP_LOG_DIR'] = '/logs'
    try:
        substitute = lambda text: text.replace('{output_dir}', '/out')
        passed = True

        for setting in (None, True):
            resolved = resolve_log_file_setting(setting, 'r.yaml', substitute)
            if resolved is not None and resolved.parent == Path('/logs') and resolved.name.startswith('r_'):
                print(f"  ✓ {setting!r} -> platform default {resolved.name}")
            else:
                print(f"  ✗ {setting!r} -> {resolved}")
                passed = False

        if resolve_log_file_setting(False, 'r.yaml', substitute) is None:
            print("  ✓ false -> no file")
        else:
            print("  ✗ false did not opt out")
            passed = False

        resolved = resolve_log_file_setting('{output_dir}/run_log.txt', 'r.yaml', substitute)
        if resolved == Path('/out/run_log.txt'):
            print("  ✓ template substituted")
        else:
            print(f"  ✗ template gave {resolved}")
            passed = False

        for junk in (42, '', '   '):
            try:
                resolve_log_file_setting(junk, 'r.yaml', substitute)
                print(f"  ✗ {junk!r} accepted")
                passed = False
            except ValueError:
                print(f"  ✓ {junk!r} rejected")

        return passed
    finally:
        del os.environ['ERP_LOG_DIR']


def main():
    """Run every test and report a final score."""
    print("=== default_log_path tests ===")

    tests = [
        test_platform_folders,
        test_env_override_wins,
        test_default_file_name,
        test_setting_resolution,
    ]

    passed = 0
    for test_func in tests:
        try:
            if test_func():
                passed += 1
        except Exception as error:
            print(f"  ✗ {test_func.__name__} crashed: {error}")

    print(f"\n=== Results: {passed}/{len(tests)} tests passed ===")
    return 0 if passed == len(tests) else 1


if __name__ == '__main__':
    exit(main())


# End of file #
