"""
Tests for the columns_to_rows / rows_to_columns reshape pair.

tests/test_reshape_processors.py

Runnable with pytest, but written to run standalone and report a score.
The centerpiece is the round trip: wide -> long -> wide restores the
original table, which is what "complementary and lossless" means in code.
"""

import pandas as pd
import numpy as np

from excel_recipe_processor.core.stage_manager import StageManager
from excel_recipe_processor.core.base_processor import StepProcessorError
from excel_recipe_processor.processors.columns_to_rows_processor import ColumnsToRowsProcessor
from excel_recipe_processor.processors.rows_to_columns_processor import RowsToColumnsProcessor


def run_processor(processor_class, frame, **config_extra):
    """Stage the frame and run one reshape step on it."""
    StageManager.initialize_stages(max_stages=5)
    StageManager.save_stage('stg_reshape_in', frame, description='test input')

    config = {
        'processor_type': 'reshape_test',
        'step_description': 'reshape test',
        'source_stage': 'stg_reshape_in',
        'save_to_stage': 'stg_reshape_out',
    }
    config.update(config_extra)
    return processor_class(config).execute(frame)


WIDE = pd.DataFrame({
    'Customer': ['Acme', 'Bolt'],
    'Region':   ['West', 'East'],
    'Jan':      [10, 20],
    'Feb':      [11, 21],
    'Mar':      [12, 22],
})


def test_columns_to_rows_demotes_headers():
    """Wide 2x5 becomes long 6x4; ids repeat; headers become data."""
    print("\nTesting columns_to_rows demotes headers into data...")

    passed = True

    result = run_processor(ColumnsToRowsProcessor, WIDE,
                           id_columns=['Customer', 'Region'],
                           labels_to='Month', values_to='Amount')

    if result.shape == (6, 4) and list(result.columns) == ['Customer', 'Region', 'Month', 'Amount']:
        print("  ✓ 2×5 wide became 6×4 long with the declared column names")
    else:
        print(f"  ✗ Shape/columns: {result.shape} {list(result.columns)}")
        passed = False

    acme = result[result['Customer'] == 'Acme']
    if list(acme['Month']) == ['Jan', 'Feb', 'Mar'] and list(acme['Amount']) == [10, 11, 12]:
        print("  ✓ Header names now data, values beside them, ids repeated")
    else:
        print(f"  ✗ Acme rows: {acme.to_dict('list')}")
        passed = False

    if list(acme['Region']) == ['West'] * 3:
        print("  ✓ Second id column repeats down the stack too")
    else:
        print(f"  ✗ Region repetition wrong: {list(acme['Region'])}")
        passed = False

    return passed


def test_unclaimed_columns_refuse_to_vanish():
    """Both lists given but a column in neither: halt, never silently drop."""
    print("\nTesting unclaimed columns halt instead of vanishing...")

    try:
        run_processor(ColumnsToRowsProcessor, WIDE,
                      id_columns=['Customer'], value_columns=['Jan', 'Feb'])
        print("  ✗ Region and Mar were silently dropped")
        return False
    except StepProcessorError as error:
        if 'Region' in str(error) and 'Mar' in str(error) and 'silently' in str(error):
            print("  ✓ Halted, naming Region and Mar as unclaimed")
            return True
        print(f"  ✗ Halted with an unhelpful message: {error}")
        return False


def test_rows_to_columns_promotes_labels():
    """Long back to wide, labels in first-appearance order, blanks for gaps."""
    print("\nTesting rows_to_columns promotes labels into headers...")

    passed = True

    long_frame = pd.DataFrame({
        'Customer': ['Acme', 'Acme', 'Bolt'],
        'Month':    ['Feb', 'Jan', 'Feb'],   # Feb appears first on purpose
        'Amount':   [11, 10, 21],
    })

    result = run_processor(RowsToColumnsProcessor, long_frame,
                           labels_from='Month', values_from='Amount')

    if list(result.columns) == ['Customer', 'Feb', 'Jan']:
        print("  ✓ Labels became columns in first-appearance order, not alphabetical")
    else:
        print(f"  ✗ Columns: {list(result.columns)}")
        passed = False

    bolt = result[result['Customer'] == 'Bolt']
    if bolt['Feb'].iloc[0] == 21 and pd.isna(bolt['Jan'].iloc[0]):
        print("  ✓ Values landed correctly; the absent combination is blank")
    else:
        print(f"  ✗ Bolt row: {bolt.to_dict('list')}")
        passed = False

    return passed


def test_duplicate_pairs_halt_loudly():
    """Two values for one (id, label) pair: refuse, naming the pair."""
    print("\nTesting duplicate (id, label) pairs halt...")

    long_frame = pd.DataFrame({
        'Customer': ['Acme', 'Acme'],
        'Month':    ['Jan', 'Jan'],
        'Amount':   [10, 99],
    })

    try:
        run_processor(RowsToColumnsProcessor, long_frame,
                      labels_from='Month', values_from='Amount')
        print("  ✗ Duplicate pair silently accepted")
        return False
    except StepProcessorError as error:
        message = str(error)
        if 'Acme' in message and 'aggregate' in message:
            print("  ✓ Halted, naming the pair and pointing at deliberate aggregation")
            return True
        print(f"  ✗ Halted with an unhelpful message: {message[:140]}")
        return False


def test_round_trip_restores_original():
    """wide -> columns_to_rows -> rows_to_columns == wide, exactly."""
    print("\nTesting the round trip restores the original table...")

    long_form = run_processor(ColumnsToRowsProcessor, WIDE,
                              id_columns=['Customer', 'Region'],
                              labels_to='Month', values_to='Amount')
    wide_again = run_processor(RowsToColumnsProcessor, long_form,
                               labels_from='Month', values_from='Amount')

    same_columns = list(wide_again.columns) == list(WIDE.columns)
    same_values = same_columns and all(
        list(wide_again[column]) == list(WIDE[column]) for column in WIDE.columns
    )

    if same_columns and same_values:
        print("  ✓ Columns, order, and every value restored")
        return True

    print(f"  ✗ Round trip differs:\n{wide_again}")
    return False


def main():
    tests = [
        test_columns_to_rows_demotes_headers,
        test_unclaimed_columns_refuse_to_vanish,
        test_rows_to_columns_promotes_labels,
        test_duplicate_pairs_halt_loudly,
        test_round_trip_restores_original,
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
        print("✅ All reshape processor tests passed!")
        return 1

    print("❌ Some reshape processor tests failed!")
    return 0


if __name__ == '__main__':
    exit(0 if main() else 1)


# End of file #
