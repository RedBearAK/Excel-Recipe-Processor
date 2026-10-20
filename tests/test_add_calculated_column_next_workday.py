"""
Test the next_workday calculation type.

File: excel_recipe_processor/tests/test_add_calculated_column_next_workday.py

next_workday is Excel's WORKDAY(date - 1, 1, holidays) done statically:
the first working day on or after each date, honoring a weekmask and a
holiday column read from another stage. The holiday table is a whole-
column parameter, not a row-wise join, which is why it is its own
calculation type rather than a pandas_formula. Blank in, blank out; a
non-date anywhere, a missing stage or column, or a malformed weekmask
stops the run.

Run: PYTHONPATH=. python3 tests/test_add_calculated_column_next_workday.py
"""

import sys

import pandas as pd

from excel_recipe_processor.core.stage_manager import StageManager
from excel_recipe_processor.core.base_processor import StepProcessorError
from excel_recipe_processor.processors.add_calculated_column_processor import AddCalculatedColumnProcessor


HOLIDAYS_STAGE = 'stg_test_holidays'


def holidays() -> pd.DataFrame:
    # Friday 2026-07-03 (observed Independence Day) and Monday 2026-09-07
    # (Labor Day), plus a blank row the loader must skip.
    return pd.DataFrame({
        'Date': pd.to_datetime(['2026-07-03', '2026-09-07', None]),
        'Scope': ['US', 'US', ''],
    })


def dated_rows() -> pd.DataFrame:
    return pd.DataFrame({
        'Case': [
            'weekday stays',
            'saturday -> monday',
            'sunday -> monday',
            'holiday friday -> monday',
            'thursday before holiday stays',
            'holiday monday -> tuesday',
            'blank stays blank',
        ],
        'Proj Due Date': pd.to_datetime([
            '2026-07-01',    # Wednesday
            '2026-07-11',    # Saturday
            '2026-07-12',    # Sunday
            '2026-07-03',    # Friday, holiday
            '2026-07-02',    # Thursday
            '2026-09-07',    # Monday, holiday
            None,
        ]),
    })


def run(config: dict, frame: pd.DataFrame) -> pd.DataFrame:
    base = {'processor_type': 'add_calculated_column', 'step_description': 'test', 'save_to_stage': 'stg_t'}
    return AddCalculatedColumnProcessor({**base, **config}).execute(frame)


def with_holidays_stage(frame_builder):
    """Decorator-free helper: save the holiday stage, run the test body, clean up."""
    StageManager.initialize_stages(max_stages=10)
    StageManager.save_stage(HOLIDAYS_STAGE, holidays(), 'Test holidays')
    try:
        return frame_builder()
    finally:
        StageManager.cleanup_stages()


def test_workday_semantics() -> bool:
    print('\nTesting WORKDAY(date - 1, 1, holidays) semantics row by row...')

    def body() -> bool:
        result = run({
            'new_column': 'DUE DATE',
            'calculation_type': 'next_workday',
            'calculation': {
                'date_column': 'Proj Due Date',
                'holidays_stage': HOLIDAYS_STAGE,
                'holidays_column': 'Date',
            },
        }, dated_rows())
        expected = pd.to_datetime([
            '2026-07-01', '2026-07-13', '2026-07-13', '2026-07-06',
            '2026-07-02', '2026-09-08', None,
        ])
        good = True
        for case, got, want in zip(result['Case'], result['DUE DATE'], expected):
            match = (pd.isna(got) and pd.isna(want)) or got == want
            good = good and match
            got_text = 'blank' if pd.isna(got) else got.date()
            want_text = 'blank' if pd.isna(want) else want.date()
            print(f"  {case:32s} got {got_text}  want {want_text}  {'OK' if match else 'FAIL'}")
        return good

    return with_holidays_stage(body)


def test_weekends_only_without_holidays() -> bool:
    print('\nTesting weekends-only when no holiday stage is given...')
    result = run({
        'new_column': 'DUE DATE',
        'calculation_type': 'next_workday',
        'calculation': {'date_column': 'Proj Due Date'},
    }, dated_rows())
    # The holiday Friday is now an ordinary working day and stays put.
    holiday_row = result.loc[result['Case'] == 'holiday friday -> monday', 'DUE DATE'].iloc[0]
    saturday_row = result.loc[result['Case'] == 'saturday -> monday', 'DUE DATE'].iloc[0]
    good = holiday_row == pd.Timestamp('2026-07-03') and saturday_row == pd.Timestamp('2026-07-13')
    print(f"  holiday Friday stays {holiday_row.date()}, Saturday -> {saturday_row.date()} -> {'OK' if good else 'FAIL'}")
    return good


def test_custom_weekmask() -> bool:
    print('\nTesting a six-day week (Saturday working)...')
    result = run({
        'new_column': 'DUE DATE',
        'calculation_type': 'next_workday',
        'calculation': {'date_column': 'Proj Due Date', 'weekmask': '1111110'},
    }, dated_rows())
    saturday_row = result.loc[result['Case'] == 'saturday -> monday', 'DUE DATE'].iloc[0]
    sunday_row = result.loc[result['Case'] == 'sunday -> monday', 'DUE DATE'].iloc[0]
    good = saturday_row == pd.Timestamp('2026-07-11') and sunday_row == pd.Timestamp('2026-07-13')
    print(f"  Saturday stays {saturday_row.date()}, Sunday -> {sunday_row.date()} -> {'OK' if good else 'FAIL'}")
    return good


def test_text_blanks_and_result_dtype() -> bool:
    print('\nTesting text-typed dates with empty-string blanks, and the datetime result dtype...')
    frame = pd.DataFrame({'When': ['2026-07-11', '', '2026-07-01']})
    result = run({
        'new_column': 'Moved',
        'calculation_type': 'next_workday',
        'calculation': {'date_column': 'When'},
    }, frame)
    good = (str(result['Moved'].dtype).startswith('datetime64')
            and result['Moved'].iloc[0] == pd.Timestamp('2026-07-13')
            and pd.isna(result['Moved'].iloc[1])
            and result['Moved'].iloc[2] == pd.Timestamp('2026-07-01'))
    print(f"  dtype {result['Moved'].dtype}, values {[str(v)[:10] for v in result['Moved']]} -> {'OK' if good else 'FAIL'}")
    return good


def test_fail_loud() -> bool:
    print('\nTesting the fail-loud paths...')
    cases = [
        ('non-date in date column', {'date_column': 'When'},
         pd.DataFrame({'When': ['2026-07-01', 'soon']})),
        ('stage without column', {'date_column': 'When', 'holidays_stage': HOLIDAYS_STAGE},
         pd.DataFrame({'When': ['2026-07-01']})),
        ('missing stage', {'date_column': 'When', 'holidays_stage': 'stg_nope', 'holidays_column': 'Date'},
         pd.DataFrame({'When': ['2026-07-01']})),
        ('missing holiday column', {'date_column': 'When', 'holidays_stage': HOLIDAYS_STAGE, 'holidays_column': 'Nope'},
         pd.DataFrame({'When': ['2026-07-01']})),
        ('bad weekmask', {'date_column': 'When', 'weekmask': '11111'},
         pd.DataFrame({'When': ['2026-07-01']})),
        ('no working day', {'date_column': 'When', 'weekmask': '0000000'},
         pd.DataFrame({'When': ['2026-07-01']})),
        ('missing date column', {'date_column': 'Nope'},
         pd.DataFrame({'When': ['2026-07-01']})),
    ]

    def body() -> bool:
        passed = 0
        for label, calculation, frame in cases:
            try:
                run({'new_column': 'X', 'calculation_type': 'next_workday', 'calculation': calculation}, frame)
                print(f"  {label:26s} did not raise -> FAIL")
            except StepProcessorError as error:
                passed += 1
                print(f"  {label:26s} raised: {str(error)[:70]}")
        return passed == len(cases)

    return with_holidays_stage(body)


def main() -> int:
    tests = [
        test_workday_semantics,
        test_weekends_only_without_holidays,
        test_custom_weekmask,
        test_text_blanks_and_result_dtype,
        test_fail_loud,
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
