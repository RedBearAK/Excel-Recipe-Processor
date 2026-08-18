"""
Workbook storage audits: stored-formula grammar and declaration state.

excel_recipe_processor/core/excel_storage_audit.py

Promoted 2026-08-16 from the test suite (test_storage_grammar_differential's
audit_stored_grammar and test_declaration_lambda_and_registry's
assert_no_legacy_cse), where both had proven themselves as whole-file
sweeps for the violation classes this project has shipped, repaired,
and pinned. One home, imported by the tests AND by the
verify_excel_storage processor, so recipes can hold their own output
to the same standard the test suite enforces.

Both auditors take BYTES SOURCES (a path or a file-like object), so
the processor can audit the in-flight session workbook serialized to
memory - the bytes as they WILL be written - without touching disk.
"""

import re
import zipfile

from excel_recipe_processor.processors._helpers.inject_formulas_functions import (
    FUTURE_FUNCTION_PREFIXES,
)


STRING_RGX = re.compile(r'"(?:[^"]|"")*"')
XLFN_NAME_RGX = re.compile(r'_xlfn\.(?:_xlws\.)?([A-Za-z][A-Za-z0-9.]*)')
BAD_PREFIX_CHAIN_RGX = re.compile(r'_xl(?:pm|eta|ws)\._xl|_xlfn\._xl(?!ws\.)')
CONSTRUCT_RGX = re.compile(r'(?<![A-Za-z0-9_.])(?:_xlfn\.)?(LAMBDA|LET)\s*\(')


def audit_stored_grammar(xlsx_source) -> list:
    """Violation strings for every stored-formula surface in a workbook.

    Surfaces: workbook definedNames, worksheet cell formulas, and
    data-validation formulas. Empty list = clean. Checks: forbidden
    leading '=' where storage forbids it, literal '#' outside strings,
    chained storage prefixes, _xlfn on names outside the validated
    future-function map, and LAMBDA/LET declaration slots lacking the
    _xlpm. prefix.
    """
    violations = []

    with zipfile.ZipFile(xlsx_source) as archive:
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


def audit_legacy_cse(xlsx_source) -> list:
    """(sheet part, cell ref) for every t="array" cell missing cm.

    A bare t="array" is a legacy Ctrl+Shift+Enter formula: Excel shows
    {braces} and a spill collapses to one value (the Dom_View braces
    incident, 2026-08-16). Empty list = every array formula carries its
    dynamic declaration.
    """
    violations = []
    with zipfile.ZipFile(xlsx_source) as archive:
        for member in archive.namelist():
            if not member.startswith('xl/worksheets/'):
                continue
            xml = archive.read(member).decode('utf-8')
            for cell in re.finditer(r'<c [^>]*>.*?</c>', xml, re.S):
                cell_text = cell.group(0)
                if 't="array"' in cell_text and 'cm="' not in \
                        cell_text.split('>', 1)[0]:
                    ref = re.search(r'r="([A-Z]+\d+)"', cell_text)
                    violations.append((member, ref.group(1) if ref else '?'))
    return violations

# End of file #
