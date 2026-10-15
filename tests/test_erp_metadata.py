"""
Test the erp_metadata frontmatter block.

File: excel_recipe_processor/tests/test_erp_metadata.py

erp_metadata declares what an ERP YAML file is (erp_file_kind) and which
ERP version last validated it. Absent is reported at both ends of the run
and never fatal; present, it is a closed mapping (unknown key named with
a suggestion, kind from a fixed list, version in YYYYMMDD.N shape); the
wrong kind for the door it came through is fatal; the version comparison
against the running ERP is always reported, with the runner-OLDER case a
warning repeated at the end.

Run: PYTHONPATH=. python3 tests/test_erp_metadata.py
"""

import sys

from excel_recipe_processor._version import __version__
from excel_recipe_processor.core.erp_metadata import (
    ErpMetadataError, inspect_erp_metadata, parse_erp_version, build_named_objects_metadata,
)


def bump(version: str, days: int) -> str:
    date, n = version.split('.')
    return f"{int(date) + days}.{n}"


def test_absent_block_is_reported_not_fatal() -> bool:
    print('\nTesting an absent block...')
    report = inspect_erp_metadata({'settings': {}, 'recipe': []}, 'recipe', 'x.yaml')
    good = (not report.present and len(report.start_lines) == 1
            and report.start_lines[0][0] == 'warning' and report.end_line is not None)
    print(f"  present={report.present} start={report.start_lines[0][0]} end_line={'yes' if report.end_line else 'no'} -> {'OK' if good else 'FAIL'}")
    return good


def test_version_comparison() -> bool:
    print('\nTesting the version comparison against the running ERP...')
    cases = [
        (__version__, 'same', 'info', False),
        (bump(__version__, -1), 'runner_newer', 'info', False),
        (bump(__version__, 1), 'runner_older', 'warning', True),
    ]
    passed = 0
    for tagged, expect_cmp, expect_level, expect_end in cases:
        doc = {'erp_metadata': {'erp_file_kind': 'recipe', 'last_validated_via_erp_version': tagged},
               'settings': {}, 'recipe': []}
        report = inspect_erp_metadata(doc, 'recipe', 'x.yaml')
        level = report.start_lines[-1][0]
        ok = (report.comparison == expect_cmp and level == expect_level
              and (report.end_line is not None) == expect_end)
        passed += ok
        print(f"  tagged {tagged} vs running {__version__}: {report.comparison} ({level}, end={'yes' if report.end_line else 'no'}) -> {'OK' if ok else 'FAIL'}")
    return passed == len(cases)


def test_rejections() -> bool:
    print('\nTesting rejected blocks...')
    cases = [
        ('unknown key', {'erp_file_kind': 'recipe', 'erp_file_kinds': 'recipe'}, 'recipe'),
        ('bad kind', {'erp_file_kind': 'cookbook'}, 'recipe'),
        ('missing kind', {'last_validated_via_erp_version': __version__}, 'recipe'),
        ('bad version shape', {'erp_file_kind': 'recipe', 'last_validated_via_erp_version': '2026.10.14'}, 'recipe'),
        ('unquoted version', {'erp_file_kind': 'recipe', 'last_validated_via_erp_version': 20261014.2}, 'recipe'),
        ('wrong door: library as recipe', {'erp_file_kind': 'named_objects'}, 'recipe'),
        ('wrong door: recipe as library', {'erp_file_kind': 'recipe'}, 'named_objects'),
        ('provenance key on a recipe', {'erp_file_kind': 'recipe', 'named_object_count': 3}, 'recipe'),
    ]
    passed = 0
    for label, block, expected in cases:
        try:
            inspect_erp_metadata({'erp_metadata': block, 'x': 1}, expected, 'x.yaml')
            print(f"  {label:34s} did not raise -> FAIL")
        except ErpMetadataError as error:
            passed += 1
            print(f"  {label:34s} raised: {str(error)[:72]}")
    return passed == len(cases)


def test_named_objects_block_round_trip() -> bool:
    print('\nTesting the named_objects block an export builds...')
    block = build_named_objects_metadata(14, exported_from_workbook='book.xlsx',
                                         exported_on='2026-10-14T20:00:00', openpyxl_version='3.1.5')
    doc = {'erp_metadata': block, 'named_ranges': [], 'lambda_functions': []}
    report = inspect_erp_metadata(doc, 'named_objects', 'lib.yaml')
    good = (report.present and report.file_kind == 'named_objects'
            and report.comparison == 'same' and block['named_object_count'] == 14
            and parse_erp_version(block['last_validated_via_erp_version']) == parse_erp_version(__version__))
    print(f"  kind={report.file_kind} comparison={report.comparison} count={block['named_object_count']} -> {'OK' if good else 'FAIL'}")
    return good


def test_not_first_is_only_a_note() -> bool:
    print('\nTesting a block that is not the first top-level key...')
    doc = {'settings': {}, 'erp_metadata': {'erp_file_kind': 'recipe'}, 'recipe': []}
    report = inspect_erp_metadata(doc, 'recipe', 'x.yaml')
    good = report.present and all(level == 'info' for level, _ in report.start_lines) and report.end_line is None
    print(f"  levels={[l for l, _ in report.start_lines]} -> {'OK' if good else 'FAIL'}")
    return good


def main() -> int:
    tests = [
        test_absent_block_is_reported_not_fatal,
        test_version_comparison,
        test_rejections,
        test_named_objects_block_round_trip,
        test_not_first_is_only_a_note,
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
