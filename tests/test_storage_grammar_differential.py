"""
Differential and whole-file antagonism for stored formula grammar.

tests/test_storage_grammar_differential.py

Two further angles of attack beyond the adversarial unit suite:

1. MAP DIFFERENTIAL - every entry in FUTURE_FUNCTION_PREFIXES is
   written through xlsxwriter with use_future_functions enabled, the
   stored bytes are read back, and OUR prefix for that name must match
   THEIRS exactly (including the _xlfn._xlws. duo). xlsxwriter's table
   is validated upstream against real Excel output, so a divergence
   means one side is wrong - and the burden of proof is on ours.
   Divergences fail loud with the full listing; a name xlsxwriter does
   not know is reported (their table can lag new functions) but only
   fails if THEY prefix and WE don't, or the prefixes differ.

2. WHOLE-FILE AUDIT - audit_stored_grammar() sweeps every stored
   formula surface in a produced workbook (cell <f> elements,
   dataValidation <formula1>/<formula2>, definedName content) for the
   violation classes this project has actually shipped and repaired:
   leading '=' in definedName or DV formulas, literal '#' outside
   strings, _xlfn on unmapped names, bare LAMBDA/LET declaration
   slots, chained prefixes. Run here against the current demo output
   and the harvest fixture; importable by future tooling (a
   verify_excel_storage processor is the natural promotion).

Runnable directly or with pytest; direct runs are authoritative.
"""

import os
import re
import sys
import zipfile
import tempfile

from excel_recipe_processor.processors._helpers.inject_formulas_functions import (
    FUTURE_FUNCTION_PREFIXES,
)


STRING_RGX = re.compile(r'"(?:[^"]|"")*"')
XLFN_NAME_RGX = re.compile(r'_xlfn\.(?:_xlws\.)?([A-Za-z][A-Za-z0-9.]*)')
BAD_PREFIX_CHAIN_RGX = re.compile(r'_xl(?:pm|eta|ws)\._xl|_xlfn\._xl(?!ws\.)')
CONSTRUCT_RGX = re.compile(r'(?<![A-Za-z0-9_.\\])(?:_xlfn\.)?(LAMBDA|LET)\s*\(')

FIXTURE = os.path.join(os.path.dirname(__file__), 'fixtures',
                       'harvest_2026-08-14_lambda_eta_spill.xlsx')
DEMO_OUTPUT = '/tmp/demo/interactive_test.xlsx'


def audit_stored_grammar(xlsx_path: str) -> list:
    """Violation strings for every stored-formula surface in a workbook.

    Empty list means the file carries none of the violation classes this
    project has shipped, repaired, and pinned. Candidate for promotion
    into a verify_excel_storage processor.
    """
    violations = []

    with zipfile.ZipFile(xlsx_path) as archive:
        surfaces = []

        workbook_xml = archive.read('xl/workbook.xml').decode()
        for match in re.finditer(
                r'<definedName name="([^"]+)"[^>]*>(.*?)</definedName>',
                workbook_xml, re.S):
            surfaces.append((f"definedName {match.group(1)}",
                             match.group(2), True))

        for name in archive.namelist():
            if not re.match(r'xl/worksheets/sheet\d+\.xml$', name):
                continue
            sheet_xml = archive.read(name).decode()
            for match in re.finditer(r'<c r="([A-Z]+\d+)"[^>]*><f[^>]*>(.*?)</f>',
                                     sheet_xml, re.S):
                surfaces.append((f"{name}!{match.group(1)}",
                                 match.group(2), False))
            for match in re.finditer(r'<formula([12])>(.*?)</formula\1>',
                                     sheet_xml, re.S):
                surfaces.append((f"{name} DV formula{match.group(1)}",
                                 match.group(2), True))

    for where, raw, equals_forbidden in surfaces:
        stored = raw.replace('&quot;', '"').replace('&lt;', '<') \
                    .replace('&gt;', '>').replace('&amp;', '&')
        bare = STRING_RGX.sub('""', stored)

        if equals_forbidden and stored.lstrip().startswith('='):
            violations.append(f"{where}: stored leading '='")
        if '#' in bare:
            violations.append(f"{where}: literal '#' outside strings")
        if BAD_PREFIX_CHAIN_RGX.search(bare):
            violations.append(f"{where}: chained storage prefixes")
        for name_match in XLFN_NAME_RGX.finditer(bare):
            if name_match.group(1).upper() not in FUTURE_FUNCTION_PREFIXES:
                violations.append(
                    f"{where}: _xlfn on unmapped {name_match.group(1)!r}")
        for construct in CONSTRUCT_RGX.finditer(bare):
            tail = bare[construct.end():construct.end() + 60]
            first_slot = tail.split(',', 1)[0].strip()
            if first_slot and not first_slot.startswith('_xlpm.'):
                violations.append(
                    f"{where}: {construct.group(1)} declaration "
                    f"{first_slot[:20]!r} lacks _xlpm.")

    return violations


def test_prefix_map_differential():
    """Our prefix map vs xlsxwriter's Excel-validated table, name by name."""
    print("\nDifferential: FUTURE_FUNCTION_PREFIXES vs xlsxwriter...")

    try:
        import xlsxwriter
    except ImportError:
        print("✗ xlsxwriter missing - dev dependency: "
              "pip install xlsxwriter --break-system-packages")
        return False

    # LAMBDA/LET are construct keywords (the _xlpm transformer's turf,
    # arg shapes matter); ANCHORARRAY is display-form-rewritten by real
    # Excel. Exercise everything else as a plain call.
    skip = {'LAMBDA', 'LET', 'ANCHORARRAY'}
    names = sorted(name for name in FUTURE_FUNCTION_PREFIXES
                   if name not in skip)

    with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as handle:
        path = handle.name
    try:
        workbook = xlsxwriter.Workbook(path, {'use_future_functions': True})
        sheet = workbook.add_worksheet()
        for row, name in enumerate(names):
            sheet.write_formula(row, 0, f'={name}(A1)')
        workbook.close()

        with zipfile.ZipFile(path) as archive:
            sheet_xml = archive.read('xl/worksheets/sheet1.xml').decode()

        stored = {}
        for match in re.finditer(r'<c r="A(\d+)"[^>]*><f[^>]*>(.*?)</f>',
                                 sheet_xml, re.S):
            stored[names[int(match.group(1)) - 1]] = match.group(2)

        disagreements = []
        oracle_gaps = []
        for name in names:
            ours = FUTURE_FUNCTION_PREFIXES[name] + name
            theirs = stored.get(name, '')
            if theirs.startswith(ours + '('):
                continue
            if theirs.startswith(name + '('):
                oracle_gaps.append(name)   # xlsxwriter table lags; ours adds
                continue
            disagreements.append(f"{name}: ours {ours!r} vs stored {theirs[:50]!r}")

        if disagreements:
            print("✗ Prefix disagreements (burden of proof is on OUR map):")
            for line in disagreements:
                print("   ", line)
            return False

        print(f"✓ {len(names) - len(oracle_gaps)} names agree exactly with "
              f"the xlsxwriter table")
        if oracle_gaps:
            print(f"  note: xlsxwriter leaves {sorted(oracle_gaps)} bare - "
                  f"their table may lag; harvest before trusting ours blindly")
        return True
    finally:
        if os.path.exists(path):
            os.unlink(path)


def test_whole_file_audits():
    """The demo output and the harvest fixture audit clean."""
    print("\nWhole-file stored-grammar audits...")

    targets = [(FIXTURE, "harvest fixture")]
    if os.path.exists(DEMO_OUTPUT):
        targets.insert(0, (DEMO_OUTPUT, "demo output"))
    else:
        print("  (demo output absent in this environment; fixture only)")

    for path, label in targets:
        if not os.path.exists(path):
            print(f"✗ {label} missing: {path}")
            return False
        violations = audit_stored_grammar(path)
        if violations:
            print(f"✗ {label} has stored-grammar violations:")
            for line in violations:
                print("   ", line)
            return False
        print(f"✓ {label} audits clean")
    return True


def main():
    """Run all tests and report results."""
    print("Storage-grammar differential and audit tests")
    print("=" * 50)

    tests = [
        test_prefix_map_differential,
        test_whole_file_audits,
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
