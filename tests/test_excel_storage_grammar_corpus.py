"""
Storage-grammar oracle tests: our output vs Excel-verified references.

tests/test_excel_storage_grammar_corpus.py

The 2026-08-14 repair incidents established that storage grammar cannot
be inferred - only compared against ground truth. This module pins our
generated forms to TWO independent oracles:

ORACLE A - xlsxwriter's serializer, generated fresh at test time. Its
output is validated upstream against files created in real Excel (the
XlsxWriter regression corpus), and its documented LAMBDA handling
matches our transformer's grammar. Requires the xlsxwriter package
(dev dependency); the test FAILS LOUD with a pip hint if missing,
because silently skipping an oracle is how grammar drift gets back in.

ORACLE B - a workbook hand-built in real Excel 365 and saved by Excel
(tests/fixtures/harvest_2026-08-14_lambda_eta_spill.xlsx): eta-reduced
GROUPBY aggregation, full-lambda GROUPBY with _xlpm parameters, spill
reference as ANCHORARRAY in both a cell formula and a data-validation
formula1. Our live pipeline must reproduce each stored form byte-wise
(whitespace-normalized outside strings).

Runnable directly or with pytest. Direct runs are the authoritative
score (return-value tests do not fail pytest).
"""

import os
import re
import sys
import zipfile
import tempfile

from excel_recipe_processor.processors._helpers.xlpm_name_storage import (
    transform_xlpm_names,
)
from excel_recipe_processor.processors._helpers.inject_formulas_functions import (
    prefix_future_functions,
    transform_storage_forms,
)


FIXTURE = os.path.join(os.path.dirname(__file__), 'fixtures',
                       'harvest_2026-08-14_lambda_eta_spill.xlsx')


def live_pipeline(formula: str) -> str:
    """The injector's full live storage pipeline."""
    return transform_storage_forms(
        prefix_future_functions(transform_xlpm_names(formula)))


def squeeze(text: str) -> str:
    """Collapse whitespace OUTSIDE string literals for byte comparison."""
    pieces = []
    last = 0
    for match in re.finditer(r'"(?:[^"]|"")*"', text):
        pieces.append(re.sub(r'\s+', '', text[last:match.start()]))
        pieces.append(match.group(0))
        last = match.end()
    pieces.append(re.sub(r'\s+', '', text[last:]))
    return ''.join(pieces)


def test_oracle_a_xlsxwriter():
    """Named-lambda definedName and lambda cell match the serializer."""
    print("\nOracle A: xlsxwriter serializer (Excel-verified upstream)...")

    try:
        import xlsxwriter
    except ImportError:
        print("✗ xlsxwriter missing - the grammar oracle is a dev "
              "dependency: pip install xlsxwriter --break-system-packages")
        return False

    with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as handle:
        oracle_path = handle.name
    try:
        workbook = xlsxwriter.Workbook(oracle_path)
        sheet = workbook.add_worksheet()
        sheet.write('A1', '=LAMBDA(_xlpm.temp,(5/9)*(_xlpm.temp-32))(32)')
        workbook.define_name(
            'ToCelsius', '=_xlfn.LAMBDA(_xlpm.temp,(5/9)*(_xlpm.temp-32))')
        workbook.close()

        with zipfile.ZipFile(oracle_path) as archive:
            workbook_xml = archive.read('xl/workbook.xml').decode()
            sheet_xml = archive.read('xl/worksheets/sheet1.xml').decode()

        oracle_name = re.search(
            r'<definedName name="ToCelsius"[^>]*>(.*?)</definedName>',
            workbook_xml, re.S).group(1)
        oracle_cell = re.search(
            r'<c r="A1"[^>]*><f[^>]*>(.*?)</f>', sheet_xml, re.S).group(1)

        # Our forms from HUMAN syntax (no storage prefixes in the input)
        ours_name = live_pipeline('LAMBDA(temp,(5/9)*(temp-32))')
        ours_cell = live_pipeline('LAMBDA(temp,(5/9)*(temp-32))(32)')

        if squeeze(ours_name) != squeeze(oracle_name):
            print(f"✗ definedName drift:\n  ours   {ours_name}\n"
                  f"  oracle {oracle_name}")
            return False
        print(f"✓ definedName grammar matches: {oracle_name}")

        # Leading '=' must NOT appear in stored content, either place
        if oracle_name.startswith('=') or ours_name.startswith('='):
            print("✗ A stored leading '=' slipped through")
            return False
        print("✓ No leading '=' in stored definedName content")

        if squeeze(ours_cell) != squeeze(oracle_cell):
            print(f"✗ Lambda cell drift:\n  ours   {ours_cell}\n"
                  f"  oracle {oracle_cell}")
            return False
        print(f"✓ Lambda cell grammar matches")
        return True
    finally:
        if os.path.exists(oracle_path):
            os.unlink(oracle_path)


def test_oracle_b_harvest_fixture():
    """Our pipeline reproduces every form in the Excel-saved harvest."""
    print("\nOracle B: harvested real-Excel workbook...")

    if not os.path.exists(FIXTURE):
        print(f"✗ Harvest fixture missing: {FIXTURE}")
        return False

    with zipfile.ZipFile(FIXTURE) as archive:
        sheet_xml = archive.read('xl/worksheets/sheet1.xml').decode()

    def stored_formula(cell_ref):
        match = re.search(
            rf'<c r="{cell_ref}"[^>]*><f[^>]*>(.*?)</f>', sheet_xml, re.S)
        return match.group(1).replace('&quot;', '"') if match else None

    cases = [
        # (harvest cell, our recipe-syntax input)
        ('G1', 'GROUPBY(A1:A6,B1:B6,SUM)'),
        ('H8', 'GROUPBY(A1:A6,B1:B6,LAMBDA(x,SUM(x)))'),
        ('K1', 'SUM(D1#)'),
        ('D1', 'SORT(UNIQUE(A1:A6))'),
    ]
    for cell_ref, recipe_form in cases:
        oracle = stored_formula(cell_ref)
        ours = live_pipeline(recipe_form)
        if oracle is None:
            print(f"✗ {cell_ref} missing from harvest fixture")
            return False
        if squeeze(ours) != squeeze(oracle):
            print(f"✗ {cell_ref} drift:\n  ours   {ours}\n  oracle {oracle}")
            return False
        print(f"✓ {cell_ref}: {oracle}")

    dv_formula = re.search(r'<formula1>(.*?)</formula1>', sheet_xml, re.S)
    if dv_formula is None or dv_formula.group(1) != '_xlfn.ANCHORARRAY($D$1)':
        print(f"✗ Harvest DV formula1 unexpected: {dv_formula}")
        return False
    print(f"✓ DV formula1 pinned: {dv_formula.group(1)}")
    return True


def main():
    """Run all tests and report results."""
    print("Excel storage-grammar oracle tests")
    print("=" * 50)

    tests = [
        test_oracle_a_xlsxwriter,
        test_oracle_b_harvest_fixture,
    ]

    passed = 0
    for test in tests:
        try:
            if test():
                passed += 1
            else:
                print(f"FAILED: {test.__name__}")
        except Exception as error:
            print(f"FAILED with exception: {test.__name__}: {error}")

    print("=" * 50)
    print(f"Final score: {passed}/{len(tests)} tests passed")
    return passed == len(tests)


if __name__ == '__main__':
    sys.exit(0 if main() else 1)

# End of file #
