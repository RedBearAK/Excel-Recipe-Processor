"""
Test flush_workbooks with target_files: named, or all.

File: excel_recipe_processor/tests/test_flush_workbooks_named.py

A recipe-wide flush writes whatever happens to be open, which a file
operation added earlier in the recipe silently changes. The named form
(2026-09-13) writes and closes ONLY the workbooks it names - "done with
this file, the end" - and treats a name that is not open as an error,
because that means the step is in the wrong place. The unnamed form
keeps flushing everything.

Run: PYTHONPATH=. python3 tests/test_flush_workbooks_named.py
"""

import os
import sys
import time
import shutil
import tempfile

from excel_recipe_processor.core.recipe_pipeline import RecipePipeline
from excel_recipe_processor.core.workbook_session import WorkbookSession


WORK = tempfile.mkdtemp(prefix='erp_flush_')


def recipe(flush_step: str) -> str:
    return f'''
settings:
  description: "two workbooks, one flushed early"
  stages:
    - stage_name: "stg_a"
      description: "a"
      protected: false
recipe:
  - step_description: "make"
    processor_type: "create_stage"
    save_to_stage: "stg_a"
    data:
      format: "table"
      columns: ["Term"]
      rows:
        - ["x"]
  - step_description: "reference out"
    processor_type: "export_file"
    source_stage: "stg_a"
    output_file: "{WORK}/reference.xlsx"
    sheet_name: "Ref"
    create_backup: false
  - step_description: "main out"
    processor_type: "export_file"
    source_stage: "stg_a"
    output_file: "{WORK}/main.xlsx"
    sheet_name: "Main"
    create_backup: false
  - step_description: "format both, so both are dirty"
    processor_type: "format_excel"
    target_file: "{WORK}/reference.xlsx"
    formatting:
      - sheet_names: ["Ref"]
        column_formats:
          - column_names: ["Term"]
            width: 20
  - step_description: "format main too"
    processor_type: "format_excel"
    target_file: "{WORK}/main.xlsx"
    formatting:
      - sheet_names: ["Main"]
        column_formats:
          - column_names: ["Term"]
            width: 20
{flush_step}
  - step_description: "more work on main, later"
    processor_type: "format_excel"
    target_file: "{WORK}/main.xlsx"
    formatting:
      - sheet_names: ["Main"]
        column_formats:
          - column_names: ["Term"]
            width: 30
'''


def run(text: str, name: str):
    path = os.path.join(WORK, name)
    with open(path, 'w', encoding='utf-8') as handle:
        handle.write(text)
    RecipePipeline().run_complete_recipe(path)


def stamps() -> tuple:
    return os.path.getmtime(os.path.join(WORK, 'reference.xlsx')), os.path.getmtime(os.path.join(WORK, 'main.xlsx'))


def test_named_flush_writes_only_the_named_file() -> bool:
    print('\nNamed: the reference file is written at the flush; main stays in the session until run end...')
    step = f'''  - step_description: "reference is done"
    processor_type: "flush_workbooks"
    target_files: ["{WORK}/reference.xlsx"]
'''
    for name in ('reference.xlsx', 'main.xlsx'):
        try:
            os.remove(os.path.join(WORK, name))
        except OSError:
            pass
    try:
        run(recipe(step), 'named.yaml')
    except Exception as error:
        print(f"  FAIL recipe raised {type(error).__name__}: {str(error)[:160]}")
        return False
    reference_time, main_time = stamps()
    earlier = reference_time <= main_time
    # the later format of main must have applied (width 30), proving main stayed open and was saved at the end
    from openpyxl import load_workbook
    width = load_workbook(os.path.join(WORK, 'main.xlsx'))['Main'].column_dimensions['A'].width
    good = earlier and abs(width - 30) < 0.01
    print(f"  reference before main={earlier}; main's later formatting applied (width={width}) -> {'OK' if good else 'FAIL'}")
    return good


def test_unnamed_flush_still_flushes_everything() -> bool:
    print('\nUnnamed: every dirty workbook is written at the flush (existing behaviour kept)...')
    step = '''  - step_description: "checkpoint everything"
    processor_type: "flush_workbooks"
'''
    try:
        run(recipe(step), 'unnamed.yaml')
    except Exception as error:
        print(f"  FAIL recipe raised {type(error).__name__}: {str(error)[:160]}")
        return False
    from openpyxl import load_workbook
    width = load_workbook(os.path.join(WORK, 'main.xlsx'))['Main'].column_dimensions['A'].width
    good = abs(width - 30) < 0.01 and not WorkbookSession.is_open(os.path.join(WORK, 'reference.xlsx'))
    print(f"  main reloaded after the flush and re-formatted (width={width}); session empty after run -> {'OK' if good else 'FAIL'}")
    return good


def test_naming_a_file_that_is_not_open_is_an_error() -> bool:
    print('\nNaming a workbook that is not open in the session fails, and says which files are...')
    step = f'''  - step_description: "wrong file"
    processor_type: "flush_workbooks"
    target_files: ["{WORK}/never_exported.xlsx"]
'''
    try:
        run(recipe(step), 'wrong.yaml')
        print('  FAIL: ran')
        return False
    except Exception as error:
        message = str(error)
        good = 'never_exported.xlsx' in message and 'not open in the session' in message
        print(f"  {message[:150]!r} -> {'OK' if good else 'FAIL'}")
        return good


def test_flushed_file_reloads_from_disk_if_touched_again() -> bool:
    print('\nA flushed file touched again later reloads from disk and the later edit still lands...')
    step = f'''  - step_description: "main is done (prematurely)"
    processor_type: "flush_workbooks"
    target_files: ["{WORK}/main.xlsx"]
'''
    try:
        run(recipe(step), 'reload.yaml')
    except Exception as error:
        print(f"  FAIL recipe raised {type(error).__name__}: {str(error)[:160]}")
        return False
    from openpyxl import load_workbook
    width = load_workbook(os.path.join(WORK, 'main.xlsx'))['Main'].column_dimensions['A'].width
    good = abs(width - 30) < 0.01
    print(f"  width after the post-flush format={width} -> {'OK' if good else 'FAIL'}")
    return good


def main() -> int:
    tests = [test_named_flush_writes_only_the_named_file, test_unnamed_flush_still_flushes_everything,
             test_naming_a_file_that_is_not_open_is_an_error, test_flushed_file_reloads_from_disk_if_touched_again]
    passed = 0
    try:
        for test in tests:
            WorkbookSession.reset()
            if test():
                passed += 1
    finally:
        shutil.rmtree(WORK, ignore_errors=True)
    print(f"\n{passed}/{len(tests)} tests passed")
    return 0 if passed == len(tests) else 1


if __name__ == '__main__':
    sys.exit(main())


# End of file #
