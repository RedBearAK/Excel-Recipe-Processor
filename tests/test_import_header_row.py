"""
Test import_file header_row and the regex_replace null mask.

File: excel_recipe_processor/tests/test_import_header_row.py

Two small behaviours added 2026-09-02 for report exports that lead with
title lines (headers on row 3 under a title and a subtitle):

- import_file accepts header_row (1-based); rows above it are discarded
  and the frame arrives headed, for xlsx and csv alike.
- clean_data regex_replace leaves blanks blank instead of stamping the
  literal text "nan" into them, while still stringifying populated
  non-text values such as datetimes in a mixed export column.

Run: PYTHONPATH=. python3 tests/test_import_header_row.py
"""

import sys
import tempfile
import datetime

from pathlib import Path

import pandas as pd

from excel_recipe_processor.core.base_processor import StepProcessorError
from excel_recipe_processor.processors.clean_data_processor import CleanDataProcessor
from excel_recipe_processor.processors.import_file_processor import ImportFileProcessor


def write_report_shaped_files(folder: Path) -> tuple:
    """Write an xlsx and a csv whose headers sit on row 3 under two title rows."""
    header = ['Order Number', 'Status', 'Status Date']
    body = [['1000001', 'Delivered', '07/13/2026'],
            ['1000002', 'In Transit', '']]
    xlsx_path = folder / 'status_report.xlsx'
    with pd.ExcelWriter(xlsx_path) as writer:
        frame = pd.DataFrame([['Status Report', None, None],
                              ['Covering: all records to date', None, None],
                              header] + body)
        frame.to_excel(writer, index=False, header=False, sheet_name='Status Report')
    csv_path = folder / 'status_report.csv'
    lines = ['Status Report,,', 'Covering: all records to date,,', ','.join(header)]
    lines += [','.join(row) for row in body]
    csv_path.write_text('\n'.join(lines) + '\n')
    return xlsx_path, csv_path


def test_header_row_xlsx_and_csv() -> bool:
    print('\nTesting header_row on xlsx and csv...')
    with tempfile.TemporaryDirectory() as temp_dir:
        xlsx_path, csv_path = write_report_shaped_files(Path(temp_dir))
        ok = True
        for path in (xlsx_path, csv_path):
            processor = ImportFileProcessor({
                'processor_type': 'import_file',
                'step_description': 'header row three',
                'input_file': str(path),
                'header_row': 3,
                'save_to_stage': f'stg_test_{path.suffix[1:]}',
            })
            result = processor.execute(pd.DataFrame())
            columns = list(result.columns)
            good = columns == ['Order Number', 'Status', 'Status Date'] and len(result) == 2
            print(f"  {path.suffix}: columns={columns} rows={len(result)} -> {'OK' if good else 'FAIL'}")
            ok = ok and good
        return ok


def test_header_row_default_unchanged() -> bool:
    print('\nTesting that the default still reads row 1 as the header...')
    with tempfile.TemporaryDirectory() as temp_dir:
        xlsx_path, _ = write_report_shaped_files(Path(temp_dir))
        processor = ImportFileProcessor({
            'processor_type': 'import_file',
            'step_description': 'default header',
            'input_file': str(xlsx_path),
            'save_to_stage': 'stg_test_default',
        })
        result = processor.execute(pd.DataFrame())
        good = list(result.columns)[0] == 'Status Report'
        print(f"  first column={list(result.columns)[0]!r} -> {'OK' if good else 'FAIL'}")
        return good


def test_header_row_rejects_bad_values() -> bool:
    print('\nTesting header_row validation...')
    ok = True
    for bad in (0, -2, '3', True, 2.5):
        try:
            ImportFileProcessor({
                'processor_type': 'import_file',
                'step_description': 'bad header row',
                'input_file': 'anything.xlsx',
                'header_row': bad,
                'save_to_stage': 'stg_test',
            }).execute(pd.DataFrame())
            print(f'  {bad!r}: accepted -> FAIL')
            ok = False
        except StepProcessorError as error:
            print(f'  {bad!r}: rejected ({str(error)[:60]}...) -> OK')
    return ok


def test_regex_replace_keeps_blanks_blank() -> bool:
    print('\nTesting regex_replace null mask on a mixed column...')
    frame = pd.DataFrame({
        'Status Date': [datetime.datetime(2026, 1, 9), '01/09/2026\n01/09/2026', None, ''],
        'Location': ['Depot A\nDepot A', None, 'Delivered', ''],
    })
    processor = CleanDataProcessor({
        'processor_type': 'clean_data',
        'step_description': 'first line only',
        'rules': [{'columns': ['Status Date', 'Location'],
                   'action': 'regex_replace',
                   'pattern': r'\n[\s\S]*$',
                   'replacement': ''}],
        'save_to_stage': 'stg_test',
    })
    result = processor.execute(frame)
    status = result['Status Date'].tolist()
    location = result['Location'].tolist()
    good = (
        status[0] == '2026-01-09 00:00:00'
        and status[1] == '01/09/2026'
        and pd.isna(status[2])
        and location[0] == 'Depot A'
        and pd.isna(location[1])
        and location[2] == 'Delivered'
        and 'nan' not in [str(v) for v in status + location if not pd.isna(v)]
    )
    print(f'  Status Date={status}')
    print(f'  Location={location} -> {"OK" if good else "FAIL"}')
    return good


def main() -> int:
    tests = [
        test_header_row_xlsx_and_csv,
        test_header_row_default_unchanged,
        test_header_row_rejects_bad_values,
        test_regex_replace_keeps_blanks_blank,
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
