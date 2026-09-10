"""
Test {col:Header} in the RANGE fields of three processors.

File: excel_recipe_processor/tests/test_column_placeholders_in_ranges.py

conditional_format's `range:`, format_excel's `cells:`, and
excel_data_validation's `apply_to_ranges` accept
"{col:Header}2:{col:Other}5" and resolve the letters from the sheet's
header row at apply time (2026-09-10). The point of the feature is that a
directive follows its columns when columns are inserted to their left,
so the decisive test builds the same workbook twice - once with the
filter block at N:P and once at P:R - runs the SAME recipe against both,
and checks each directive landed on the right cells both times. Literal
ranges keep working; a missing header is a clear error.

Run: PYTHONPATH=. python3 tests/test_column_placeholders_in_ranges.py
"""

import os
import sys
import shutil
import tempfile

from openpyxl import Workbook, load_workbook
from openpyxl.utils import get_column_letter, column_index_from_string

from excel_recipe_processor.core.recipe_pipeline import RecipePipeline


WORK = tempfile.mkdtemp(prefix='erp_colph_')

FILTERS = ['Filter: SALE TYPE1', 'Filter: Fishery Group', 'Filter: Process Year']


def build_workbook(path: str, leading_columns: int):
    """A Van_List-shaped sheet: N leading columns, a spacer, the three
    filter columns, four pick rows under them."""
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = 'Van_List'
    headers = [f"Col {index + 1}" for index in range(leading_columns)] + [' '] + FILTERS
    sheet.append(headers)
    for row in range(2, 6):
        sheet.append([''] * leading_columns + [''] + ['' if row > 2 else 'x'] * 3)
    workbook.save(path)


RECIPE = '''
settings:
  description: "Directives addressed by header, not by letter"
  yaml_anchors:
    - &pick_block "{col:Filter: SALE TYPE1}2:{col:Filter: Process Year}5"

recipe:
  - step_description: "Gray when empty, bold when picked"
    processor_type: "conditional_format"
    target_file: "__FILE__"
    sheet_name: "Van_List"
    rules:
      - when_formula: '=LEN({col:Filter: SALE TYPE1}2)=0'
        range: *pick_block
        style:
          fill: "EEEEEE"
      - when_formula: '=LEN({col:Filter: SALE TYPE1}2)>0'
        range: *pick_block
        style:
          bold: true

  - step_description: "Border the pick block"
    processor_type: "format_excel"
    target_file: "__FILE__"
    formatting:
      - sheet_names: ["Van_List"]
        cell_formats:
          - cells: ["{col:Filter: SALE TYPE1}2:{col:Filter: Process Year}5"]
            font_italic: true

  - step_description: "Dropdowns on each pick block"
    processor_type: "excel_data_validation"
    target_file: "__FILE__"
    validations:
      - sheet_name: "Van_List"
        apply_to_ranges: ["{col:Filter: SALE TYPE1}2:{col:Filter: SALE TYPE1}5"]
        validation_type: "list"
        values_list: ["Fresh", "Frozen"]
      - sheet_name: "Van_List"
        apply_to_ranges: ["{col:Filter: Process Year}2:{col:Filter: Process Year}5"]
        validation_type: "list"
        values_list: ["2025", "2026"]
'''


def run_recipe(text: str, target: str, name: str):
    path = os.path.join(WORK, name)
    with open(path, 'w', encoding='utf-8') as handle:
        handle.write(text.replace('__FILE__', target))
    RecipePipeline().run_complete_recipe(path)


def expected_block(leading_columns: int) -> str:
    first = get_column_letter(leading_columns + 2)
    last = get_column_letter(leading_columns + 4)
    return f"{first}2:{last}5"


def landed(path: str, leading_columns: int) -> dict:
    """Where each directive actually landed, read back from the file."""
    workbook = load_workbook(path)
    sheet = workbook['Van_List']
    block = expected_block(leading_columns)
    first_letter = get_column_letter(leading_columns + 2)
    last_letter = get_column_letter(leading_columns + 4)
    # openpyxl keys conditional formatting by range: two rules on one
    # range are one entry holding two rules
    cf_ranges = sorted(str(cf.sqref) for cf in sheet.conditional_formatting)
    cf_rule_count = sum(len(cf.rules) for cf in sheet.conditional_formatting)
    italic_block = all(sheet[f"{letter}{row}"].font.italic
                       for letter in (first_letter, last_letter) for row in (2, 5))
    outside_plain = not sheet[f"{get_column_letter(leading_columns + 1)}2"].font.italic
    dv_ranges = sorted(str(dv.sqref) for dv in sheet.data_validations.dataValidation)
    workbook.close()
    return {'cf': cf_ranges, 'cf_rules': cf_rule_count, 'cf_expected': block, 'italic_block': italic_block, 'outside_plain': outside_plain,
            'dv': dv_ranges, 'dv_expected': sorted([f"{first_letter}2:{first_letter}5", f"{last_letter}2:{last_letter}5"])}


def test_directives_follow_their_columns() -> bool:
    print('\nThe same recipe lands on the right cells with the block at N:P and again after two columns are inserted (P:R)...')
    good = True
    for leading in (12, 14):
        target = os.path.join(WORK, f"van_list_{leading}.xlsx")
        build_workbook(target, leading)
        try:
            run_recipe(RECIPE, target, f"recipe_{leading}.yaml")
        except Exception as error:
            print(f"  FAIL leading={leading}: recipe raised {type(error).__name__}: {str(error)[:140]}")
            good = False
            continue
        result = landed(target, leading)
        cf_ok = result['cf'] == [result['cf_expected']] and result['cf_rules'] == 2
        dv_ok = result['dv'] == result['dv_expected']
        ok = cf_ok and result['italic_block'] and result['outside_plain'] and dv_ok
        print(f"  leading columns={leading}: block={result['cf_expected']} conditional={result['cf']} "
              f"cells italic in block={result['italic_block']} outside untouched={result['outside_plain']} "
              f"validations={result['dv']} -> {'OK' if ok else 'FAIL'}")
        good = good and ok
    return good


def test_literal_ranges_still_work() -> bool:
    print('\nA literal A1 range is untouched by the resolver...')
    target = os.path.join(WORK, 'literal.xlsx')
    build_workbook(target, 12)
    literal = RECIPE.replace('*pick_block', '"N2:P5"') \
                    .replace('cells: ["{col:Filter: SALE TYPE1}2:{col:Filter: Process Year}5"]', 'cells: ["N2:P5"]') \
                    .replace('apply_to_ranges: ["{col:Filter: SALE TYPE1}2:{col:Filter: SALE TYPE1}5"]', 'apply_to_ranges: ["N2:N5"]') \
                    .replace('apply_to_ranges: ["{col:Filter: Process Year}2:{col:Filter: Process Year}5"]', 'apply_to_ranges: ["P2:P5"]')
    try:
        run_recipe(literal, target, 'literal.yaml')
    except Exception as error:
        print(f"  FAIL recipe raised {type(error).__name__}: {str(error)[:140]}")
        return False
    result = landed(target, 12)
    good = result['cf'] == ['N2:P5'] and result['cf_rules'] == 2 and result['italic_block'] and result['dv'] == ['N2:N5', 'P2:P5']
    print(f"  conditional={result['cf']} validations={result['dv']} -> {'OK' if good else 'FAIL'}")
    return good


def test_unknown_header_is_a_clear_error() -> bool:
    print('\nA header that is not on the sheet fails with a message naming it and the sheet...')
    target = os.path.join(WORK, 'unknown.xlsx')
    build_workbook(target, 12)
    broken = RECIPE.replace('{col:Filter: Process Year}5"', '{col:Filter: Nope}5"', 1)
    try:
        run_recipe(broken, target, 'unknown.yaml')
        print('  FAIL: ran without complaint')
        return False
    except Exception as error:
        message = str(error)
        good = 'Filter: Nope' in message and 'Van_List' in message
        print(f"  {message[:150]!r} -> {'OK' if good else 'FAIL'}")
        return good


def main() -> int:
    tests = [test_directives_follow_their_columns, test_literal_ranges_still_work, test_unknown_header_is_a_clear_error]
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
