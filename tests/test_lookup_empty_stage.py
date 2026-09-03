"""
Test lookup_data against an EMPTY lookup stage.

File: excel_recipe_processor/tests/test_lookup_empty_stage.py

The import_file create_empty fallback hands downstream an empty frame
that still carries the declared columns. A join against it must come
back with every row unmatched, not halt (2026-09-02) - otherwise the
fail-safe import can never reach the step it exists to protect. An
empty stage that lacks the declared columns is still an error.

Run: PYTHONPATH=. python3 tests/test_lookup_empty_stage.py
"""

import sys

import pandas as pd

from excel_recipe_processor.core.base_processor import StepProcessorError
from excel_recipe_processor.core.stage_manager import StageManager
from excel_recipe_processor.processors.lookup_data_processor import LookupDataProcessor


def make_processor(stage_name: str, default_values=None) -> LookupDataProcessor:
    config = {
        'processor_type': 'lookup_data',
        'step_description': 'join against empty lookup',
        'lookup_stage': stage_name,
        'match_col_in_main_data': 'Order Number',
        'match_col_in_lookup_data': 'Order Number',
        'lookup_columns': ['Status', 'Location'],
        'low_match_warning': False,
        'save_to_stage': f'stg_result_{stage_name}',
    }
    if default_values is not None:
        config['default_values'] = default_values
    return LookupDataProcessor(config)


def main_frame() -> pd.DataFrame:
    return pd.DataFrame({'Order Number': ['1000001', '1000002', None], 'Quantity': [1, 2, 3]})


def test_empty_stage_with_columns_survives() -> bool:
    print('\nTesting an empty lookup stage that carries the declared columns...')
    empty = pd.DataFrame(columns=['Order Number', 'Status', 'Location'])
    StageManager.save_stage('stg_empty_with_columns', empty, overwrite=True)
    result = make_processor('stg_empty_with_columns').execute(main_frame())
    good = (
        len(result) == 3
        and 'Status' in result.columns and 'Location' in result.columns
        and result['Status'].isna().all() and result['Location'].isna().all()
        and result['Quantity'].tolist() == [1, 2, 3]
    )
    print(f'  rows={len(result)} columns={list(result.columns)} all blank='
          f'{bool(result["Status"].isna().all())} -> {"OK" if good else "FAIL"}')
    return good


def test_empty_stage_defaults_apply() -> bool:
    print('\nTesting that default_values still apply on the empty path...')
    empty = pd.DataFrame(columns=['Order Number', 'Status', 'Location'])
    StageManager.save_stage('stg_empty_defaults', empty, overwrite=True)
    result = make_processor('stg_empty_defaults',
                            default_values={'Status': 'NO MATCH'}).execute(main_frame())
    good = result['Status'].tolist() == ['NO MATCH'] * 3
    print(f'  Status={result["Status"].tolist()} -> {"OK" if good else "FAIL"}')
    return good


def test_empty_stage_without_columns_errors() -> bool:
    print('\nTesting that an empty stage WITHOUT the columns still errors...')
    empty = pd.DataFrame(columns=['Order Number'])
    StageManager.save_stage('stg_empty_no_columns', empty, overwrite=True)
    try:
        make_processor('stg_empty_no_columns').execute(main_frame())
        print('  accepted -> FAIL')
        return False
    except StepProcessorError as error:
        print(f'  rejected ({str(error)[:70]}...) -> OK')
        return True


def main() -> int:
    tests = [
        test_empty_stage_with_columns_survives,
        test_empty_stage_defaults_apply,
        test_empty_stage_without_columns_errors,
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
