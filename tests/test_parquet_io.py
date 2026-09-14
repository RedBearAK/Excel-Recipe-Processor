"""
Test Parquet in import_file / export_file, types preserved or discarded.

File: excel_recipe_processor/tests/test_parquet_io.py

Parquet keeps column types across the file boundary, which csv cannot;
that is the reason to support it, and the default. `parquet_types: text`
on either side discards them on purpose. The tests run real recipes:
a typed stage out to Parquet and back (types intact), the same with
text on export and on import, a csv-to-parquet conversion, and the
option refused where it is not a valid value.

Run: PYTHONPATH=. python3 tests/test_parquet_io.py
"""

import os
import sys
import shutil
import tempfile

import pandas as pd

from excel_recipe_processor.core.file_reader import FileReader
from excel_recipe_processor.core.file_writer import FileWriter
from excel_recipe_processor.core.recipe_pipeline import RecipePipeline


WORK = tempfile.mkdtemp(prefix='erp_parquet_')


def typed_frame() -> pd.DataFrame:
    return pd.DataFrame({
        'Van Seq #': ['2633K074', '2633K075', '2633K076'],
        'Units': [48, 24, 58],
        'Net Weight': [1200.5, 600.25, None],
        'Ship Date': pd.to_datetime(['2026-08-20', '2026-08-21', None]),
        'Paid': [True, False, True],
    })


def test_round_trip_preserves_types() -> bool:
    print('\nA typed frame written to Parquet reads back with every dtype intact...')
    path = os.path.join(WORK, 'typed.parquet')
    original = typed_frame()
    FileWriter.write_file(original, path, create_backup=False)
    back = FileReader.read_file(path)
    same_dtypes = all(str(back[column].dtype) == str(original[column].dtype) for column in original.columns)
    same_values = back.equals(original)
    good = same_dtypes and same_values
    print(f"  dtypes back={dict((c, str(back[c].dtype)) for c in back.columns)} intact={same_dtypes} values equal={same_values} -> {'OK' if good else 'FAIL'}")
    return good


def test_text_on_import() -> bool:
    print("\nparquet_types='text' on import: every column text, nulls still missing...")
    path = os.path.join(WORK, 'typed2.parquet')
    FileWriter.write_file(typed_frame(), path, create_backup=False)
    back = FileReader.read_file(path, parquet_types='text')
    all_text = all(back[column].dropna().map(type).eq(str).all() for column in back.columns)
    units_text = back['Units'].tolist() == ['48', '24', '58']
    null_kept = back['Net Weight'].isna().tolist() == [False, False, True]
    good = all_text and units_text and null_kept
    print(f"  all text={all_text} Units={back['Units'].tolist()} null kept={null_kept} -> {'OK' if good else 'FAIL'}")
    return good


def test_text_on_export() -> bool:
    print("\nparquet_types='text' on export: the FILE's schema is all strings, nulls null...")
    import pyarrow.parquet as pq
    path = os.path.join(WORK, 'text_out.parquet')
    FileWriter.write_file(typed_frame(), path, create_backup=False, parquet_types='text')
    schema = pq.read_schema(path)
    all_string = all(str(field.type) in ('string', 'large_string') for field in schema)
    back = pd.read_parquet(path)
    null_kept = back['Ship Date'].isna().tolist() == [False, False, True]
    dates_iso = str(back['Ship Date'].iloc[0]).startswith('2026-08-20')
    good = all_string and null_kept and dates_iso
    print(f"  file schema={[(f.name, str(f.type)) for f in schema]} all string={all_string} null kept={null_kept} -> {'OK' if good else 'FAIL'}")
    return good


def test_recipe_csv_to_parquet_and_back() -> bool:
    print('\nA recipe: csv in, Parquet out (typed), Parquet in, xlsx out - the types survive the middle...')
    csv_path = os.path.join(WORK, 'in.csv')
    typed_frame().to_csv(csv_path, index=False)
    recipe = f'''
settings:
  description: "csv -> parquet -> xlsx"
  stages:
    - stage_name: "stg_in"
      description: "from csv"
      protected: false
    - stage_name: "stg_back"
      description: "from parquet"
      protected: false
recipe:
  - step_description: "csv in"
    processor_type: "import_file"
    input_file: "{csv_path}"
    save_to_stage: "stg_in"
  - step_description: "parquet out, typed"
    processor_type: "export_file"
    source_stage: "stg_in"
    output_file: "{WORK}/mid.parquet"
  - step_description: "parquet back in"
    processor_type: "import_file"
    input_file: "{WORK}/mid.parquet"
    save_to_stage: "stg_back"
  - step_description: "xlsx out"
    processor_type: "export_file"
    source_stage: "stg_back"
    output_file: "{WORK}/out.xlsx"
    sheet_name: "Data"
'''
    recipe_path = os.path.join(WORK, 'convert.yaml')
    with open(recipe_path, 'w', encoding='utf-8') as handle:
        handle.write(recipe)
    try:
        RecipePipeline().run_complete_recipe(recipe_path)
    except Exception as error:
        print(f"  FAIL recipe raised {type(error).__name__}: {str(error)[:160]}")
        return False
    mid = pd.read_parquet(os.path.join(WORK, 'mid.parquet'))
    out = pd.read_excel(os.path.join(WORK, 'out.xlsx'))
    numeric_kept = str(mid['Units'].dtype).startswith('int') and str(mid['Net Weight'].dtype).startswith('float')
    rows_kept = len(out) == 3 and out['Van Seq #'].tolist() == ['2633K074', '2633K075', '2633K076']
    good = numeric_kept and rows_kept
    print(f"  parquet dtypes={dict((c, str(mid[c].dtype)) for c in mid.columns)} numeric kept={numeric_kept} xlsx rows={rows_kept} -> {'OK' if good else 'FAIL'}")
    return good


def test_bad_option_is_refused_at_validation() -> bool:
    print("\nparquet_types must be preserve or text; anything else fails validation before a step runs...")
    from excel_recipe_processor.config.recipe_loader import RecipeLoader
    recipe = f'''
settings:
  description: "bad option"
  stages:
    - stage_name: "stg_x"
      description: "x"
      protected: false
recipe:
  - step_description: "bad"
    processor_type: "import_file"
    input_file: "{WORK}/typed.parquet"
    parquet_types: "strings"
    save_to_stage: "stg_x"
  - step_description: "use it"
    processor_type: "export_file"
    source_stage: "stg_x"
    output_file: "{WORK}/never.csv"
'''
    recipe_path = os.path.join(WORK, 'bad.yaml')
    with open(recipe_path, 'w', encoding='utf-8') as handle:
        handle.write(recipe)
    # the validator logs each error and raises a summary: read the log
    import logging

    class Catch(logging.Handler):
        def __init__(self):
            super().__init__()
            self.lines = []

        def emit(self, record):
            self.lines.append(record.getMessage())

    catcher = Catch()
    logging.getLogger().addHandler(catcher)
    try:
        RecipePipeline().run_complete_recipe(recipe_path)
        print('  FAIL: ran')
        return False
    except Exception:
        pass
    finally:
        logging.getLogger().removeHandler(catcher)
    detail = [line for line in catcher.lines if 'parquet_types' in line]
    good = bool(detail) and 'strings' in detail[0]
    print(f"  refused: {detail[0][:120] if detail else 'no parquet_types line in the log'!r} -> {'OK' if good else 'FAIL'}")
    return good


def main() -> int:
    tests = [test_round_trip_preserves_types, test_text_on_import, test_text_on_export,
             test_recipe_csv_to_parquet_and_back, test_bad_option_is_refused_at_validation]
    passed = 0
    try:
        for test in tests:
            if test():
                passed += 1
    finally:
        shutil.rmtree(WORK, ignore_errors=True)
    print(f"\n{passed}/{len(tests)} tests passed")
    return 0 if passed == len(tests) else 1


if __name__ == '__main__':
    sys.exit(main())


# End of file #
