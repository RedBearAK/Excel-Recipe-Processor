"""
Basic package tests: import and version.

File: excel_recipe_processor/tests/test_basic.py

The project scaffold's first test, kept as the cheapest possible smoke
check and rewritten in house style (2026-09-04): no pytest import, runs
on its own, prints what it checked, exits 0/1.

Run: PYTHONPATH=. python3 tests/test_basic.py
"""

import sys


def test_package_imports() -> bool:
    print('\nTesting package import...')
    import excel_recipe_processor
    good = excel_recipe_processor is not None
    print(f"  package imported -> {'OK' if good else 'FAIL'}")
    return good


def test_version_is_dotted_string() -> bool:
    print('\nTesting version string...')
    from excel_recipe_processor import __version__
    good = isinstance(__version__, str) and len(__version__.split('.')) >= 2
    print(f"  __version__ = {__version__!r} -> {'OK' if good else 'FAIL'}")
    return good


def main() -> int:
    tests = [test_package_imports, test_version_is_dotted_string]
    passed = 0
    for test in tests:
        try:
            if test():
                passed += 1
        except Exception as error:
            print(f'  EXCEPTION in {test.__name__}: {error}')
    print(f'\n{passed}/{len(tests)} tests passed')
    return 0 if passed == len(tests) else 1


if __name__ == '__main__':
    sys.exit(main())


# End of file #
