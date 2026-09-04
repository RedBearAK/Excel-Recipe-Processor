"""
Test lookup_data behaviour when a lookup column shares a main column's name.

File: excel_recipe_processor/tests/test_lookup_name_collisions.py

Before 2026-09-03 the merge ran first and a same-named payload column then
OVERWROTE the main column: unmatched rows lost their own values and the
prefix landed on the survivor. Three behaviours now hold:

- a same-named lookup column with no prefix/suffix is an error, not an
  overwrite;
- with a prefix the main column is untouched and the lookup copy lands
  under the prefixed name, blank where unmatched;
- the lookup key may itself be listed in lookup_columns (an echo beside
  the main key) without tripping pandas on a doubled label.

Run: PYTHONPATH=. python3 tests/test_lookup_name_collisions.py
"""

import sys

import pandas as pd

from excel_recipe_processor.core.stage_manager import StageManager
from excel_recipe_processor.core.base_processor import StepProcessorError
from excel_recipe_processor.processors.lookup_data_processor import LookupDataProcessor


def build_frames() -> tuple:
    """Main frame with its own 'Carrier'; lookup with a same-named 'Carrier'."""
    main = pd.DataFrame({
        'Seq': ['A1', 'A2', 'A3'],
        'Carrier': ['AML', 'AKAIR', 'AML'],
    })
    lookup = pd.DataFrame({
        'Van Seq': ['A1', 'A3'],
        'Carrier': ['Lynden', 'Lynden'],
        'Pro': ['1001', '1003'],
    })
    return main, lookup


def run_lookup(config_extra: dict) -> pd.DataFrame:
    main, lookup = build_frames()
    StageManager.save_stage('stg_lookup', lookup, 'lookup')
    config = {
        'processor_type': 'lookup_data',
        'step_description': 'collision test',
        'lookup_stage': 'stg_lookup',
        'match_col_in_main_data': 'Seq',
        'match_col_in_lookup_data': 'Van Seq',
        'lookup_columns': ['Carrier', 'Pro'],
    }
    config.update(config_extra)
    return LookupDataProcessor(config).execute(main)


def test_collision_without_prefix_is_an_error() -> bool:
    print('\nTesting that a same-named lookup column with no prefix is refused...')
    StageManager.initialize_stages()
    try:
        run_lookup({})
        print('  accepted -> FAIL (main column would have been overwritten)')
        return False
    except StepProcessorError as error:
        good = 'Carrier' in str(error) and 'prefix' in str(error)
        print(f"  refused: {str(error)[:90]}... -> {'OK' if good else 'FAIL'}")
        return good
    finally:
        StageManager.cleanup_stages()


def test_prefix_keeps_main_column_intact() -> bool:
    print('\nTesting that a prefix lands the lookup copy beside the main column...')
    StageManager.initialize_stages()
    try:
        result = run_lookup({'prefix': 'VMS '})
        columns = list(result.columns)
        main_kept = result['Carrier'].tolist() == ['AML', 'AKAIR', 'AML']
        copy = result['VMS Carrier'].tolist()
        copy_good = copy[0] == 'Lynden' and pd.isna(copy[1]) and copy[2] == 'Lynden'
        order_good = columns == ['Seq', 'Carrier', 'VMS Carrier', 'VMS Pro']
        print(f'  columns={columns}')
        print(f'  main Carrier={result["Carrier"].tolist()} lookup copy={copy}')
        good = main_kept and copy_good and order_good
        print(f"  -> {'OK' if good else 'FAIL'}")
        return good
    finally:
        StageManager.cleanup_stages()


def test_key_echo_in_lookup_columns() -> bool:
    print('\nTesting that the lookup key can be echoed as a payload column...')
    StageManager.initialize_stages()
    try:
        result = run_lookup({'prefix': 'VMS ', 'lookup_columns': ['Van Seq', 'Pro']})
        echo = result['VMS Van Seq'].tolist()
        good = echo[0] == 'A1' and pd.isna(echo[1]) and echo[2] == 'A3' and len(result) == 3
        print(f"  echo={echo} rows={len(result)} -> {'OK' if good else 'FAIL'}")
        return good
    finally:
        StageManager.cleanup_stages()


def main() -> int:
    tests = [
        test_collision_without_prefix_is_an_error,
        test_prefix_keeps_main_column_intact,
        test_key_echo_in_lookup_columns,
    ]
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
