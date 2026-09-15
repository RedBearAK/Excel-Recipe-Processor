"""
infer_column_types: the rule, and the near misses that must stay text.

File: excel_recipe_processor/tests/test_infer_column_types.py

Run: PYTHONPATH=. python3 tests/test_infer_column_types.py
"""

import sys

import pandas as pd

from excel_recipe_processor.processors.infer_column_types_processor import InferColumnTypesProcessor
from excel_recipe_processor.core.base_processor import StepProcessorError


def run(frame: pd.DataFrame, **options) -> pd.DataFrame:
    config = {'step_description': 't', 'processor_type': 'infer_column_types', **options}
    return InferColumnTypesProcessor(config).execute(frame)


def test_identifiers_stay_text() -> bool:
    print('\nInteger-looking columns stay text unless named: package numbers, lot numbers with leading zeros...')
    frame = pd.DataFrame({'Package Number': ['25FP31593024', '25PG00051213'], 'Lot Number': ['01234', '00567'],
                          'Plain Ints': ['1', '2'], 'Seq': ['2529D272', '2633K074']})
    out = run(frame)
    good = (out['Lot Number'].tolist() == ['01234', '00567'] and out['Plain Ints'].tolist() == ['1', '2']
            and all(str(out[c].dtype) in ('object', 'str', 'string') for c in out.columns))
    print(f"  dtypes={ {c: str(out[c].dtype) for c in out.columns} } leading zeros kept={out['Lot Number'].tolist() == ['01234', '00567']} -> {'OK' if good else 'FAIL'}")
    return good


def test_decimals_and_named_integers() -> bool:
    print('\nAll-numeric with a point becomes float (including .000000); a named count becomes Int64 with nulls kept...')
    frame = pd.DataFrame({'Net Weight': ['1285.000000', '.000000', '53.1'], 'Packages': ['1', '2', None],
                          'Sci': ['1e3', '2.5E-2', '7']})
    out = run(frame, integer_columns=['Packages'])
    good = (str(out['Net Weight'].dtype) == 'float64' and out['Net Weight'].tolist() == [1285.0, 0.0, 53.1]
            and str(out['Packages'].dtype) == 'Int64' and out['Packages'].isna().tolist() == [False, False, True]
            and str(out['Sci'].dtype) == 'float64')
    print(f"  weight={out['Net Weight'].tolist()} packages dtype={out['Packages'].dtype} sci dtype={out['Sci'].dtype} -> {'OK' if good else 'FAIL'}")
    return good


def test_one_bad_value_keeps_the_column_text() -> bool:
    print('\nAll or nothing: one non-numeric value keeps the whole column text; nothing becomes NaN...')
    frame = pd.DataFrame({'Weight': ['12.5', 'n/a', '3.0'], 'Count': ['1', '2', 'x']})
    out = run(frame, integer_columns=['Count'])
    good = out['Weight'].tolist() == ['12.5', 'n/a', '3.0'] and out['Count'].tolist() == ['1', '2', 'x']
    print(f"  weight={out['Weight'].tolist()} count={out['Count'].tolist()} -> {'OK' if good else 'FAIL'}")
    return good


def test_strict_named_integer_that_will_not_parse() -> bool:
    print('\nstrict: a named integer column with a bad value is an error, not a warning...')
    frame = pd.DataFrame({'Count': ['1', 'x']})
    try:
        run(frame, integer_columns=['Count'], strict=True)
        print('  FAIL: no error')
        return False
    except StepProcessorError as error:
        good = 'Count' in str(error) and 'not every value is numeric' in str(error)
        print(f"  {str(error)[:100]!r} -> {'OK' if good else 'FAIL'}")
        return good


def test_dates_in_one_format() -> bool:
    print('\nA column that parses entirely in one date format becomes datetime; a mixed one stays text...')
    frame = pd.DataFrame({'Pack Date': ['08/20/25', '08/04/25', None], 'Stamp': ['08/20/25 05:32', '08/04/25 16:22'][:3] + [None],
                          'Iso': ['2026-09-14', '2026-09-13', '2026-09-12'], 'Mixed': ['08/20/25', '2026-09-13', 'soon']})
    out = run(frame)
    good = (str(out['Pack Date'].dtype).startswith('datetime64') and out['Pack Date'].isna().tolist() == [False, False, True]
            and str(out['Stamp'].dtype).startswith('datetime64') and out['Stamp'].iloc[0].hour == 5
            and str(out['Iso'].dtype).startswith('datetime64') and out['Mixed'].tolist() == ['08/20/25', '2026-09-13', 'soon'])
    print(f"  pack date={out['Pack Date'].dtype} stamp hour={out['Stamp'].iloc[0].hour} iso={out['Iso'].dtype} mixed kept text={out['Mixed'].tolist()[2] == 'soon'} -> {'OK' if good else 'FAIL'}")
    return good


def test_text_columns_and_empties_untouched() -> bool:
    print('\ntext_columns are never coerced; an all-empty column stays text; a named column that is missing is an error...')
    frame = pd.DataFrame({'Weight': ['1.5', '2.5'], 'Empty': [None, ''], 'Dt': ['2026-01-01', '2026-01-02']})
    out = run(frame, text_columns=['Weight', 'Dt'])
    kept = out['Weight'].tolist() == ['1.5', '2.5'] and out['Dt'].tolist() == ['2026-01-01', '2026-01-02']
    empty_text = str(out['Empty'].dtype) in ('object', 'str', 'string')
    try:
        run(frame, integer_columns=['Nope'])
        missing_error = False
    except StepProcessorError as error:
        missing_error = 'Nope' in str(error)
    good = kept and empty_text and missing_error
    print(f"  text kept={kept} empty stays text={empty_text} missing named column errors={missing_error} -> {'OK' if good else 'FAIL'}")
    return good


def main() -> int:
    tests = [test_identifiers_stay_text, test_decimals_and_named_integers, test_one_bad_value_keeps_the_column_text,
             test_strict_named_integer_that_will_not_parse, test_dates_in_one_format, test_text_columns_and_empties_untouched]
    passed = sum(1 for test in tests if test())
    print(f"\n{passed}/{len(tests)} tests passed")
    return 0 if passed == len(tests) else 1


if __name__ == '__main__':
    sys.exit(main())


# End of file #
