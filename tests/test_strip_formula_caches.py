"""
Antagonistic tests for strip_formula_caches: the golden safety path.

tests/test_strip_formula_caches.py

The one rule that must survive every attack: LITERAL DATA IS NEVER
TOUCHED. Around it: dyads stripped, spills removed (styles blanked in
place), external-reference and inline-string cells REFUSED by name,
scope respected, calc flags set, calcChain gone, and the whole file
still openable by openpyxl afterward with every literal intact.

Runnable standalone or under pytest; the exit code carries the verdict.
"""

import os
import sys
import shutil
import logging
import zipfile
import tempfile

import openpyxl

from excel_recipe_processor.core.pipeline import registry
from excel_recipe_processor.core.base_processor import StepProcessorError


def build_hostile_workbook(path):
    """Every cell species, adversarially arranged.

    Data!A1:B4  literals (str/int/float/bool) - MUST survive untouched
    Data!C1:C3  ordinary formulas WITH cached values
    Data!C4     formula with cached ERROR value
    Data!D1:D3  shared formula (master + slaves) with caches
    Data!E1     formula referencing an EXTERNAL workbook - REFUSED
    Data!F1     legacy array formula anchor with ref F1:F3 + cached
                member cells F2 (bare) and F3 (styled)
    Data!G1     value-only cell that LOOKS like it could be a spill
                member but is outside every ref - literal, untouched
    Other!A1    literal + A2 cached formula (scope tests)
    """
    xml_data = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/'
        'spreadsheetml/2006/main"><sheetData>'
        '<row r="1">'
        '<c r="A1" t="str"><v>hello</v></c>'
        '<c r="B1"><v>42</v></c>'
        '<c r="C1"><f>B1*2</f><v>84</v></c>'
        '<c r="D1"><f t="shared" ref="D1:D3" si="0">B1+1</f><v>43</v></c>'
        '<c r="E1"><f>[1]Ext!A1*2</f><v>999</v></c>'
        '<c r="F1"><f t="array" ref="F1:F3">SEQUENCE(3)</f><v>1</v></c>'
        '<c r="G1"><v>777</v></c>'
        '</row>'
        '<row r="2">'
        '<c r="A2"><v>3.14</v></c>'
        '<c r="B2" t="b"><v>1</v></c>'
        '<c r="C2"><f>B1*3</f><v>126</v></c>'
        '<c r="D2"><f t="shared" si="0"/><v>44</v></c>'
        '<c r="F2"><v>2</v></c>'
        '</row>'
        '<row r="3">'
        '<c r="A3" t="str"><v>literal keep</v></c>'
        '<c r="C3"><f>B1*4</f><v>168</v></c>'
        '<c r="D3"><f t="shared" si="0"/><v>45</v></c>'
        '<c r="F3" s="7"><v>3</v></c>'
        '</row>'
        '<row r="4">'
        '<c r="A4"><v>100</v></c>'
        '<c r="C4" t="e"><f>1/0</f><v>#DIV/0!</v></c>'
        '</row>'
        '</sheetData></worksheet>'
    )
    xml_other = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/'
        'spreadsheetml/2006/main"><sheetData>'
        '<row r="1"><c r="A1" t="str"><v>keepme</v></c></row>'
        '<row r="2"><c r="A2"><f>1+1</f><v>2</v></c></row>'
        '</sheetData></worksheet>'
    )
    xml_workbook = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<workbook xmlns="http://schemas.openxmlformats.org/'
        'spreadsheetml/2006/main" xmlns:r="http://schemas.'
        'openxmlformats.org/officeDocument/2006/relationships">'
        '<sheets>'
        '<sheet name="Data" sheetId="1" r:id="rId1"/>'
        '<sheet name="Other" sheetId="2" r:id="rId2"/>'
        '</sheets>'
        '<calcPr calcId="1" calcMode="manual"/>'
        '</workbook>'
    )
    xml_rels = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/'
        'package/2006/relationships">'
        '<Relationship Id="rId1" Type="http://schemas.openxmlformats.'
        'org/officeDocument/2006/relationships/worksheet" '
        'Target="worksheets/sheet1.xml"/>'
        '<Relationship Id="rId2" Type="http://schemas.openxmlformats.'
        'org/officeDocument/2006/relationships/worksheet" '
        'Target="worksheets/sheet2.xml"/>'
        '<Relationship Id="rId3" Type="http://schemas.openxmlformats.'
        'org/officeDocument/2006/relationships/styles" '
        'Target="styles.xml"/>'
        '</Relationships>'
    )
    xml_content_types = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/'
        'content-types">'
        '<Default Extension="rels" ContentType="application/vnd.'
        'openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
        '<Override PartName="/xl/workbook.xml" ContentType="application'
        '/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main'
        '+xml"/>'
        '<Override PartName="/xl/worksheets/sheet1.xml" ContentType='
        '"application/vnd.openxmlformats-officedocument.spreadsheetml.'
        'worksheet+xml"/>'
        '<Override PartName="/xl/worksheets/sheet2.xml" ContentType='
        '"application/vnd.openxmlformats-officedocument.spreadsheetml.'
        'worksheet+xml"/>'
        '<Override PartName="/xl/styles.xml" ContentType='
        '"application/vnd.openxmlformats-officedocument.spreadsheetml.'
        'styles+xml"/>'
        '<Override PartName="/xl/calcChain.xml" ContentType='
        '"application/vnd.openxmlformats-officedocument.spreadsheetml.'
        'calcChain+xml"/>'
        '</Types>'
    )
    xml_root_rels = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/'
        'package/2006/relationships">'
        '<Relationship Id="rId1" Type="http://schemas.openxmlformats.'
        'org/officeDocument/2006/relationships/officeDocument" '
        'Target="xl/workbook.xml"/>'
        '</Relationships>'
    )
    xf = '<xf numFmtId="0" fontId="0" fillId="0" borderId="0"/>'
    xml_styles = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<styleSheet xmlns="http://schemas.openxmlformats.org/'
        'spreadsheetml/2006/main">'
        '<fonts count="1"><font><name val="Calibri"/></font></fonts>'
        '<fills count="1"><fill><patternFill patternType="none"/>'
        '</fill></fills>'
        '<borders count="1"><border/></borders>'
        '<cellStyleXfs count="1">' + xf + '</cellStyleXfs>'
        '<cellXfs count="8">' + xf * 8 + '</cellXfs>'
        '</styleSheet>'
    )
    with zipfile.ZipFile(path, 'w', zipfile.ZIP_DEFLATED) as archive:
        archive.writestr('xl/styles.xml', xml_styles)
        archive.writestr('[Content_Types].xml', xml_content_types)
        archive.writestr('_rels/.rels', xml_root_rels)
        archive.writestr('xl/workbook.xml', xml_workbook)
        archive.writestr('xl/_rels/workbook.xml.rels', xml_rels)
        archive.writestr('xl/worksheets/sheet1.xml', xml_data)
        archive.writestr('xl/worksheets/sheet2.xml', xml_other)
        archive.writestr('xl/calcChain.xml',
                         '<calcChain><c r="C1" i="1"/></calcChain>')


def run_strip(config_extra=None, workdir=None):
    """Build the hostile workbook and strip it; return its path."""
    path = os.path.join(workdir, 'hostile.xlsx')
    build_hostile_workbook(path)
    config = {'processor_type': 'strip_formula_caches',
              'step_description': 'antagonist',
              'files': [path]}
    config.update(config_extra or {})
    processor = registry._processors['strip_formula_caches'](config)
    processor.perform_file_operation()
    return path


def sheet_xml(path, part='xl/worksheets/sheet1.xml'):
    with zipfile.ZipFile(path) as archive:
        return archive.read(part).decode('utf-8')


def test_literals_are_never_touched():
    """Every literal cell survives byte-identical in meaning."""
    print("Testing the golden rule: literals untouched...")

    with tempfile.TemporaryDirectory() as workdir:
        path = run_strip(workdir=workdir)
        xml = sheet_xml(path)
        survivors = ['<c r="A1" t="str"><v>hello</v></c>',
                     '<c r="B1"><v>42</v></c>',
                     '<c r="A2"><v>3.14</v></c>',
                     '<c r="B2" t="b"><v>1</v></c>',
                     '<c r="A3" t="str"><v>literal keep</v></c>',
                     '<c r="A4"><v>100</v></c>',
                     '<c r="G1"><v>777</v></c>']
        passed = True
        for cell in survivors:
            if cell in xml:
                print(f"  ✓ {cell[:34]}... intact")
            else:
                print(f"  ✗ MISSING literal: {cell}")
                passed = False
        return passed


def test_dyads_stripped_formulas_kept():
    """Ordinary, shared and error-cached formulas lose <v>, keep <f>."""
    print("\nTesting dyad stripping...")

    with tempfile.TemporaryDirectory() as workdir:
        path = run_strip(workdir=workdir)
        xml = sheet_xml(path)
        checks = [
            ('<f>B1*2</f>', True), ('<v>84</v>', False),
            ('<f>B1*3</f>', True), ('<v>126</v>', False),
            ('<f>B1*4</f>', True), ('<v>168</v>', False),
            ('si="0">B1+1</f>', True), ('<v>43</v>', False),
            ('<v>44</v>', False), ('<v>45</v>', False),
            ('<f>1/0</f>', True), ('<v>#DIV/0!</v>', False),
            ('t="e"', False),
        ]
        passed = True
        for needle, wanted in checks:
            present = needle in xml
            ok = present == wanted
            print(f"  {'✓' if ok else '✗'} {needle!r} "
                  f"{'present' if present else 'absent'}")
            passed &= ok
        return passed


def test_spill_members_removed_styles_blanked():
    """Anchor keeps <f>, bare member removed, styled member blanked."""
    print("\nTesting array spill handling...")

    with tempfile.TemporaryDirectory() as workdir:
        path = run_strip(workdir=workdir)
        xml = sheet_xml(path)
        checks = [
            ('ref="F1:F3">SEQUENCE(3)</f>', True),
            ('SEQUENCE(3)</f><v>1</v>', False),
            ('<c r="F2">', False),
            ('<c r="F3" s="7"/>', True),
            ('<v>3</v>', False),
        ]
        passed = True
        for needle, wanted in checks:
            ok = (needle in xml) == wanted
            print(f"  {'✓' if ok else '✗'} {needle!r} "
                  f"{'as expected' if ok else 'WRONG'}")
            passed &= ok
        return passed


def test_external_reference_refused_and_named():
    """The external-workbook cell keeps its cache; the report names it."""
    print("\nTesting the external-reference refusal...")

    with tempfile.TemporaryDirectory() as workdir:
        records = []
        handler = logging.Handler()
        handler.emit = lambda record: records.append(record.getMessage())
        handler.setLevel(logging.INFO)
        strip_logger = logging.getLogger(
            'excel_recipe_processor.processors.strip_formula_caches_processor')
        strip_logger.setLevel(logging.INFO)
        strip_logger.addHandler(handler)
        try:
            path = run_strip(workdir=workdir)
        finally:
            strip_logger.removeHandler(handler)
        xml = sheet_xml(path)
        kept = '<f>[1]Ext!A1*2</f><v>999</v>' in xml
        named = any('REFUSED' in message and 'Data!E1' in message
                    for message in records)
        print(f"  {'✓' if kept else '✗'} cached value 999 kept")
        print(f"  {'✓' if named else '✗'} refusal names Data!E1 in the report")
        return kept and named


def test_scope_limits_the_surgery():
    """Scoped to Other, the Data sheet is untouched entirely."""
    print("\nTesting scope enforcement...")

    with tempfile.TemporaryDirectory() as workdir:
        path = run_strip({'scope': [{'sheet_names': ['Other']}]},
                         workdir=workdir)
        data_xml = sheet_xml(path)
        other_xml = sheet_xml(path, 'xl/worksheets/sheet2.xml')
        data_untouched = '<v>84</v>' in data_xml and '<c r="F2"><v>2</v></c>' in data_xml
        other_stripped = '<f>1+1</f>' in other_xml and '<v>2</v>' not in other_xml
        other_literal = '<v>keepme</v>' in other_xml
        for ok, label in ((data_untouched, 'out-of-scope sheet untouched'),
                          (other_stripped, 'in-scope dyad stripped'),
                          (other_literal, 'in-scope literal kept')):
            print(f"  {'✓' if ok else '✗'} {label}")
        return data_untouched and other_stripped and other_literal


def test_scope_vocabulary_fails_loud():
    """Two restrictions in one entry, and scalar sheet_names, refuse."""
    print("\nTesting scope vocabulary guards...")

    passed = True
    for bad_scope, expect in (
            ([{'sheet_names': ['Data'], 'columns': ['C'], 'rows': [1]}],
             'at most ONE'),
            ([{'sheet_names': 'Data'}], 'LIST'),
            ([{'sheet_names': ['Data'], 'cellz': ['A1']}], 'unknown')):
        try:
            registry._processors['strip_formula_caches']({
                'processor_type': 'strip_formula_caches',
                'step_description': 'guard drill',
                'files': ['x.xlsx'], 'scope': bad_scope})
            print(f"  ✗ accepted: {bad_scope}")
            passed = False
        except StepProcessorError as error:
            ok = expect in str(error)
            print(f"  {'✓' if ok else '✗'} refused with {expect!r}")
            passed &= ok
    return passed


def test_calc_flags_and_chain():
    """calcChain gone from zip+content types; recalc flags forced."""
    print("\nTesting calc flags and chain removal...")

    with tempfile.TemporaryDirectory() as workdir:
        path = run_strip(workdir=workdir)
        with zipfile.ZipFile(path) as archive:
            names = archive.namelist()
            workbook = archive.read('xl/workbook.xml').decode()
            content_types = archive.read('[Content_Types].xml').decode()
        checks = [
            ('xl/calcChain.xml' not in names, 'calcChain part removed'),
            ('calcChain' not in content_types, 'content-type override removed'),
            ('fullCalcOnLoad="1"' in workbook, 'fullCalcOnLoad set'),
            ('calcCompleted="0"' in workbook, 'calcCompleted cleared'),
            ('calcMode="manual"' not in workbook,
             'manual mode normalized (Excel ignores the flag under manual)'),
        ]
        passed = True
        for ok, label in checks:
            print(f"  {'✓' if ok else '✗'} {label}")
            passed &= ok
        return passed


def test_backup_and_openpyxl_roundtrip():
    """A backup lands beside the file; openpyxl reads the result."""
    print("\nTesting backup and round-trip readability...")

    with tempfile.TemporaryDirectory() as workdir:
        path = run_strip(workdir=workdir)
        backup_ok = os.path.isfile(path + '.stripbak')
        workbook = openpyxl.load_workbook(path)
        literals_ok = (workbook['Data']['A1'].value == 'hello'
                       and workbook['Data']['G1'].value == 777
                       and workbook['Other']['A1'].value == 'keepme')
        formulas_ok = workbook['Data']['C1'].value == '=B1*2'
        for ok, label in ((backup_ok, '.stripbak backup created'),
                          (literals_ok, 'literals readable via openpyxl'),
                          (formulas_ok, 'formulas readable via openpyxl')):
            print(f"  {'✓' if ok else '✗'} {label}")
        return backup_ok and literals_ok and formulas_ok


def main():
    """Run every test and report a final score."""
    print("=== strip_formula_caches antagonistic tests ===")

    tests = [
        test_literals_are_never_touched,
        test_dyads_stripped_formulas_kept,
        test_spill_members_removed_styles_blanked,
        test_external_reference_refused_and_named,
        test_scope_limits_the_surgery,
        test_scope_vocabulary_fails_loud,
        test_calc_flags_and_chain,
        test_backup_and_openpyxl_roundtrip,
    ]

    passed = 0
    for test_func in tests:
        try:
            if test_func():
                passed += 1
        except Exception as error:
            print(f"✗ {test_func.__name__} crashed: {error}")

    print(f"\n=== Results: {passed}/{len(tests)} tests passed ===")
    return passed == len(tests)


if __name__ == '__main__':
    sys.exit(0 if main() else 1)

# End of file #
