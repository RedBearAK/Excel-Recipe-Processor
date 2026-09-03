"""
Test spill_columns and the first_match calculation type.

File: excel_recipe_processor/tests/test_add_calculated_column_spill_first_match.py

A calculation may fill more than one column (a horizontal spill, declared
in spill_columns) and a first_match rule table fills every declared
column from the winning rule's row, so paired outputs cannot drift.
Shape checks refuse undeclared spills, missing spills, and results that
are not one value per row.

Run: PYTHONPATH=. python3 tests/test_add_calculated_column_spill_first_match.py
"""

import sys

import pandas as pd

from excel_recipe_processor.core.base_processor import StepProcessorError
from excel_recipe_processor.processors.add_calculated_column_processor import AddCalculatedColumnProcessor


def orders() -> pd.DataFrame:
    return pd.DataFrame({
        'Kind': ['Fresh', 'Fresh', 'Frozen', 'Frozen', 'Other'],
        'Shipped': pd.to_datetime(['2026-07-01', None, '2026-07-03', None, '2026-07-05']),
        'Terms': [30, 30, 45, 45, 10],
        'Price': [10, 10, 0, 10, 10],
    })


def run(config: dict, frame: pd.DataFrame) -> pd.DataFrame:
    base = {'processor_type': 'add_calculated_column', 'step_description': 'test', 'save_to_stage': 'stg_t'}
    return AddCalculatedColumnProcessor({**base, **config}).execute(frame)


def test_expression_spills_two_columns() -> bool:
    print('\nTesting expression spill into a declared column...')
    result = run({
        'new_column': 'Due', 'spill_columns': ['Why'],
        'calculation': {'pandas_formula':
            "pd.DataFrame({'d': {col:Shipped} + pd.to_timedelta({col:Terms}, unit='D'), "
            "'w': {col:Kind} + ' terms'})"},
    }, orders())
    good = (list(result.columns)[-2:] == ['Due', 'Why']
            and result['Due'].iloc[0] == pd.Timestamp('2026-07-31')
            and result['Why'].iloc[2] == 'Frozen terms')
    print(f"  Due[0]={result['Due'].iloc[0].date()} Why[2]={result['Why'].iloc[2]!r} -> {'OK' if good else 'FAIL'}")
    return good


def test_shape_checks() -> bool:
    print('\nTesting spill shape checks...')
    cases = [
        ('undeclared spill', {'new_column': 'Due',
            'calculation': {'pandas_formula': "pd.DataFrame({'a': {col:Terms}, 'b': {col:Terms}})"}}),
        ('missing spill', {'new_column': 'Due', 'spill_columns': ['Why'],
            'calculation': {'pandas_formula': "{col:Terms} * 2"}}),
        ('vertical spill', {'new_column': 'Due',
            'calculation': {'pandas_formula': "{col:Terms}.unique()"}}),
        ('spill on declarative type', {'new_column': 'Due', 'spill_columns': ['Why'],
            'calculation_type': 'concat', 'calculation': {'columns': ['Kind', 'Terms']}}),
        ('duplicate names', {'new_column': 'Due', 'spill_columns': ['Due'],
            'calculation': {'pandas_formula': "{col:Terms}"}}),
    ]
    ok = True
    for label, config in cases:
        try:
            run(config, orders())
            print(f'  {label}: accepted -> FAIL'); ok = False
        except StepProcessorError as error:
            print(f'  {label}: rejected ({str(error)[:64]}...) -> OK')
    return ok


def first_match_config() -> dict:
    return {
        'new_column': 'Due', 'spill_columns': ['Why'], 'calculation_type': 'first_match',
        'calculation': {
            'pandas_rules': [
                {'when': "~({col:Price} > 0)", 'then': ['', "'No Price'"]},
                {'when': "({col:Kind} == 'Fresh') & {col:Shipped}.notna()",
                 'then': ["{col:Shipped} + pd.to_timedelta({col:Terms} + 7, unit='D')", "'Fr-Ship'"]},
                {'when': "{col:Kind} == 'Fresh'", 'then': ['', "'Fr-NoShip'"]},
                {'when': "{col:Kind} == 'Never'", 'then': ['', "'Unreachable'"]},
                {'when': "({col:Kind} == 'Frozen') & {col:Shipped}.notna()",
                 'then': ["{col:Shipped} + pd.to_timedelta({col:Terms}, unit='D')", "'Fz-Ship'"]},
            ],
            'pandas_default': ['', "'Unknown'"],
        },
    }


def test_first_match_pairs_outputs() -> bool:
    print('\nTesting first_match: winning rule fills both columns...')
    result = run(first_match_config(), orders())
    why = result['Why'].tolist()
    due = [None if pd.isna(v) else pd.Timestamp(v).date().isoformat() for v in result['Due']]
    good = (why == ['Fr-Ship', 'Fr-NoShip', 'No Price', 'Unknown', 'Unknown']
            and due == ['2026-08-07', None, None, None, None]
            and pd.api.types.is_datetime64_any_dtype(result['Due']))
    print(f'  Why={why}\n  Due={due} dtype={result["Due"].dtype} -> {"OK" if good else "FAIL"}')
    return good


def test_first_match_all_blank_output_keeps_dtype() -> bool:
    print('\nTesting first_match: an output no rule fills still lands typed...')
    config = first_match_config()
    frame = orders(); frame['Kind'] = 'Other'
    result = run(config, frame)
    good = result['Due'].isna().all() and result['Why'].tolist() == ['No Price' if p == 0 else 'Unknown' for p in frame['Price']]
    print(f"  Due all blank={bool(result['Due'].isna().all())} Why={result['Why'].tolist()} -> {'OK' if good else 'FAIL'}")
    return good


def test_first_match_validation() -> bool:
    print('\nTesting first_match validation before evaluation...')
    ok = True
    def variant(mutate, label):
        nonlocal ok
        config = first_match_config(); mutate(config['calculation'])
        try:
            run(config, orders()); print(f'  {label}: accepted -> FAIL'); ok = False
        except StepProcessorError as error:
            print(f'  {label}: rejected ({str(error)[:64]}...) -> OK')
    variant(lambda c: c['pandas_rules'][1]['then'].pop(), 'short then')
    variant(lambda c: c['pandas_rules'][1]['then'].append("'x'"), 'long then')
    variant(lambda c: c.pop('pandas_default'), 'no default')
    variant(lambda c: c.__setitem__('pandas_default', ["'only one'"]), 'short default')
    variant(lambda c: c['pandas_rules'][2].__setitem__('when', "{col:Nope} == 1"), 'unknown column by rule')
    variant(lambda c: c['pandas_rules'][2]['then'].__setitem__(1, 'Fr-NoShip'), 'unquoted literal')
    variant(lambda c: c['pandas_rules'][0].__setitem__('else', 1), 'unknown rule key')
    variant(lambda c: c.__setitem__('rules', c.pop('pandas_rules')), 'bare rules key')
    return ok


def main() -> int:
    tests = [
        test_expression_spills_two_columns,
        test_shape_checks,
        test_first_match_pairs_outputs,
        test_first_match_all_blank_output_keeps_dtype,
        test_first_match_validation,
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
