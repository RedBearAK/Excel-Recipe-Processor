"""
Antagonistic tests for audit_external_ties and its shared inventory.

tests/test_audit_external_ties.py

A hand-built package carries one tie of every kind the 2026-09-14
problem grid names: a real external link with cached data, cell
formulas (quoted sheet, bare sheet, shared master, external name),
a defined name, CF and DV rules, a pivot chained sheet -> table ->
cache -> local source, a file hyperlink, an orphan [3], a phantom
[2] and a dangling externalLink9.xml. Runnable directly or with
pytest.
"""

import os
import sys
import json
import hashlib
import logging
import tempfile
import zipfile

import openpyxl

from excel_recipe_processor.core.base_processor import StepProcessorError
from excel_recipe_processor.processors.audit_external_ties_processor import (
    AuditExternalTiesProcessor,
)
from excel_recipe_processor.processors._helpers.external_ties_inventory import (
    summarize,
    parse_external_sheet_refs,
    inventory_external_ties,
)


NS_MAIN = 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'
NS_R = 'http://schemas.openxmlformats.org/officeDocument/2006/relationships'
NS_PKG = 'http://schemas.openxmlformats.org/package/2006/relationships'
REL = 'http://schemas.openxmlformats.org/officeDocument/2006/relationships/'


def build_hostile_workbook(path: str) -> None:
    """Every tie kind in one package, written part by part."""
    xml_head = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'

    xml_workbook = (
        f'{xml_head}<workbook xmlns="{NS_MAIN}" xmlns:r="{NS_R}">'
        '<sheets>'
        '<sheet name="Data" sheetId="1" r:id="rId1"/>'
        '<sheet name="Ext" sheetId="2" r:id="rId2"/>'
        '<sheet name="Pivots" sheetId="3" r:id="rId3"/>'
        '</sheets>'
        '<externalReferences>'
        '<externalReference r:id="rId4"/>'
        '<externalReference r:id="rId5"/>'
        '</externalReferences>'
        '<definedNames>'
        '<definedName name="LocalName">Data!$A$1</definedName>'
        '<definedName name="ExtName" hidden="1">[1]Ext!$B$2</definedName>'
        '<definedName name="ScopedBroken" localSheetId="1">#REF!</definedName>'
        '</definedNames>'
        '<calcPr calcId="1" fullCalcOnLoad="1"/>'
        '</workbook>'
    )
    # Relationship attributes in Excel order here and openpyxl order in
    # the link rels below - the parser must not care.
    xml_workbook_rels = (
        f'{xml_head}<Relationships xmlns="{NS_PKG}">'
        f'<Relationship Id="rId1" Type="{REL}worksheet" Target="worksheets/sheet1.xml"/>'
        f'<Relationship Id="rId2" Type="{REL}worksheet" Target="worksheets/sheet2.xml"/>'
        f'<Relationship Id="rId3" Type="{REL}worksheet" Target="worksheets/sheet3.xml"/>'
        f'<Relationship Target="externalLinks/externalLink1.xml" Type="{REL}externalLink" Id="rId4"/>'
        f'<Relationship Id="rId5" Type="{REL}externalLink" Target="externalLinks/externalLink2.xml"/>'
        f'<Relationship Id="rId6" Type="{REL}pivotCacheDefinition" Target="pivotCache/pivotCacheDefinition1.xml"/>'
        '</Relationships>'
    )
    xml_data = (
        f'{xml_head}<worksheet xmlns="{NS_MAIN}" xmlns:r="{NS_R}">'
        '<sheetData>'
        '<row r="1">'
        '<c r="A1" t="s"><v>0</v></c>'
        '<c r="B1"><f>A1*2</f><v>4</v></c>'
        '<c r="C1"><f>[1]Ext!A1*2</f><v>999</v></c>'
        "<c r=\"D1\"><f>'[1]Other Sheet'!B2</f></c>"
        '<c r="E1"><f>[1]!ExtOnlyName</f><v>5</v></c>'
        '<c r="F1"><f t="shared" ref="F1:F3" si="0">[1]Ext!C1+[3]Gone!A1</f><v>1</v></c>'
        '<c r="G1"><f>SUM(Data!A1:A9)</f><v>1</v></c>'
        '</row>'
        '<row r="2"><c r="F2"><f t="shared" si="0"/><v>2</v></c></row>'
        '<row r="3"><c r="F3"><f t="shared" si="0"/><v>3</v></c></row>'
        '</sheetData>'
        '<conditionalFormatting sqref="A1:A9">'
        '<cfRule type="expression" priority="1"><formula>A1&gt;[1]Ext!A1</formula></cfRule>'
        '</conditionalFormatting>'
        '<dataValidations count="1">'
        '<dataValidation type="list" sqref="B2:B9"><formula1>[1]Ext!$A$1:$A$5</formula1></dataValidation>'
        '</dataValidations>'
        '<hyperlinks><hyperlink ref="G2" r:id="rId1"/></hyperlinks>'
        '</worksheet>'
    )
    xml_data_rels = (
        f'{xml_head}<Relationships xmlns="{NS_PKG}">'
        f'<Relationship Id="rId1" Type="{REL}hyperlink" '
        'Target="../Other%20Book.xlsx" TargetMode="External"/>'
        f'<Relationship Id="rId2" Type="{REL}hyperlink" '
        'Target="https://example.com" TargetMode="External"/>'
        '</Relationships>'
    )
    xml_ext = (
        f'{xml_head}<worksheet xmlns="{NS_MAIN}"><sheetData>'
        '<row r="1"><c r="A1"><v>7</v></c></row>'
        '</sheetData></worksheet>'
    )
    xml_pivots = (
        f'{xml_head}<worksheet xmlns="{NS_MAIN}"><sheetData/></worksheet>'
    )
    xml_pivots_rels = (
        f'{xml_head}<Relationships xmlns="{NS_PKG}">'
        f'<Relationship Id="rId1" Type="{REL}pivotTable" Target="../pivotTables/pivotTable1.xml"/>'
        '</Relationships>'
    )
    xml_pivot_table = (
        f'{xml_head}<pivotTableDefinition xmlns="{NS_MAIN}" name="VanSummary" cacheId="0">'
        '<location ref="A3:C20" firstHeaderRow="1" firstDataRow="2" firstDataCol="1"/>'
        '</pivotTableDefinition>'
    )
    xml_pivot_table_rels = (
        f'{xml_head}<Relationships xmlns="{NS_PKG}">'
        f'<Relationship Id="rId1" Type="{REL}pivotCacheDefinition" '
        'Target="../pivotCache/pivotCacheDefinition1.xml"/>'
        '</Relationships>'
    )
    xml_cache_def = (
        f'{xml_head}<pivotCacheDefinition xmlns="{NS_MAIN}" xmlns:r="{NS_R}" '
        'r:id="rId1" refreshOnLoad="1">'
        '<cacheSource type="worksheet"><worksheetSource ref="A1:K500" sheet="Data"/></cacheSource>'
        '</pivotCacheDefinition>'
    )
    xml_link1 = (
        f'{xml_head}<externalLink xmlns="{NS_MAIN}" xmlns:r="{NS_R}">'
        '<externalBook r:id="rId1"><sheetNames><sheetName val="Ext"/>'
        '<sheetName val="Other Sheet"/></sheetNames>'
        '<sheetDataSet><sheetData sheetId="0"/></sheetDataSet></externalBook>'
        '</externalLink>'
    )
    xml_link1_rels = (
        f'{xml_head}<Relationships xmlns="{NS_PKG}">'
        f'<Relationship Target="file:///Users/mmf/Source%20Book.xlsx" '
        f'TargetMode="External" Type="{REL}externalLinkPath" Id="rId1"/>'
        '</Relationships>'
    )
    xml_link2 = (
        f'{xml_head}<externalLink xmlns="{NS_MAIN}" xmlns:r="{NS_R}">'
        '<externalBook r:id="rId1"><sheetNames><sheetName val="Unused"/></sheetNames>'
        '</externalBook></externalLink>'
    )
    xml_link2_rels = (
        f'{xml_head}<Relationships xmlns="{NS_PKG}">'
        f'<Relationship Id="rId1" Type="{REL}externalLinkPath" '
        'Target="Phantom.xlsx" TargetMode="External"/>'
        '</Relationships>'
    )
    xml_sst = (
        f'{xml_head}<sst xmlns="{NS_MAIN}" count="1" uniqueCount="1">'
        '<si><t>hello</t></si></sst>'
    )
    xml_content_types = (
        f'{xml_head}<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
        '</Types>'
    )
    xml_root_rels = (
        f'{xml_head}<Relationships xmlns="{NS_PKG}">'
        f'<Relationship Id="rId1" Type="{REL}officeDocument" Target="xl/workbook.xml"/>'
        '</Relationships>'
    )
    parts = {
        '[Content_Types].xml': xml_content_types,
        '_rels/.rels': xml_root_rels,
        'xl/workbook.xml': xml_workbook,
        'xl/_rels/workbook.xml.rels': xml_workbook_rels,
        'xl/sharedStrings.xml': xml_sst,
        'xl/worksheets/sheet1.xml': xml_data,
        'xl/worksheets/_rels/sheet1.xml.rels': xml_data_rels,
        'xl/worksheets/sheet2.xml': xml_ext,
        'xl/worksheets/sheet3.xml': xml_pivots,
        'xl/worksheets/_rels/sheet3.xml.rels': xml_pivots_rels,
        'xl/pivotTables/pivotTable1.xml': xml_pivot_table,
        'xl/pivotTables/_rels/pivotTable1.xml.rels': xml_pivot_table_rels,
        'xl/pivotCache/pivotCacheDefinition1.xml': xml_cache_def,
        'xl/externalLinks/externalLink1.xml': xml_link1,
        'xl/externalLinks/_rels/externalLink1.xml.rels': xml_link1_rels,
        'xl/externalLinks/externalLink2.xml': xml_link2,
        'xl/externalLinks/_rels/externalLink2.xml.rels': xml_link2_rels,
        'xl/externalLinks/externalLink9.xml': xml_link2,
    }
    with zipfile.ZipFile(path, 'w', zipfile.ZIP_DEFLATED) as archive:
        for name, xml in parts.items():
            archive.writestr(name, xml)


def hostile_inventory(workdir: str) -> dict:
    path = os.path.join(workdir, 'hostile.xlsx')
    build_hostile_workbook(path)
    return inventory_external_ties(path)


def make_processor(config_extra=None) -> AuditExternalTiesProcessor:
    config = {'processor_type': 'audit_external_ties',
              'step_description': 'antagonist',
              'files': ['unused.xlsx']}
    config.update(config_extra or {})
    return AuditExternalTiesProcessor(config)


def check_all(checks: list) -> bool:
    passed = True
    for ok, label in checks:
        print(f"  {'✓' if ok else '✗'} {label}")
        passed &= bool(ok)
    return passed


def test_external_link_parts():
    """Both declared links parsed regardless of attribute order."""
    print("\nTesting external link part parsing...")
    with tempfile.TemporaryDirectory() as workdir:
        inv = hostile_inventory(workdir)
    links = {link['index']: link for link in inv['external_links']}
    return check_all([
        (set(links) == {1, 2}, 'indexes 1 and 2 found'),
        (links[1]['target'] == 'file:///Users/mmf/Source%20Book.xlsx',
         'link 1 target read despite Excel attribute order'),
        (links[1]['sheet_names'] == ['Ext', 'Other Sheet'], 'link 1 sheet list'),
        (links[1]['has_cached_values'] and not links[2]['has_cached_values'],
         'cached-data flag per link'),
        (links[1]['kind'] == 'workbook', 'kind classified as workbook'),
    ])


def test_formula_refs():
    """Quoted, bare, external-name and shared-master forms all found."""
    print("\nTesting formula reference discovery...")
    with tempfile.TemporaryDirectory() as workdir:
        inv = hostile_inventory(workdir)
    refs = {ref['cell']: ref for ref in inv['formula_refs']}
    return check_all([
        (set(refs) == {'C1', 'D1', 'E1', 'F1'}, f"cells {sorted(refs)}"),
        (refs['C1']['ref_sheets'] == ['Ext'] and refs['C1']['has_cached_value'],
         'bare sheet ref with cached value'),
        (refs['D1']['ref_sheets'] == ['Other Sheet'] and not refs['D1']['has_cached_value'],
         'quoted sheet ref without cached value'),
        (refs['E1']['ref_sheets'] == [''], 'external workbook-level name'),
        (refs['F1']['shared_ref'] == 'F1:F3' and refs['F1']['indexes'] == [1, 3],
         'shared master carries its ref range and both indexes'),
        (all(ref['sheet'] == 'Data' for ref in refs.values()), 'sheet attributed'),
        ('B1' not in refs and 'G1' not in refs, 'local formulas ignored'),
    ])


def test_names_and_sheet_features():
    """Defined names, CF and DV classified with scope and sqref."""
    print("\nTesting defined names and sheet features...")
    with tempfile.TemporaryDirectory() as workdir:
        inv = hostile_inventory(workdir)
    names = {name['name']: name for name in inv['defined_names']}
    features = {feature['kind']: feature for feature in inv['sheet_features']}
    return check_all([
        (set(names) == {'ExtName', 'ScopedBroken'}, f"names {sorted(names)}"),
        (names['ExtName']['hidden'] and names['ExtName']['indexes'] == [1],
         'hidden external name'),
        (names['ScopedBroken']['scope'] == 'Ext' and names['ScopedBroken']['has_ref_error'],
         'localSheetId resolved to sheet name, #REF! flagged'),
        (set(features) == {'conditional_formatting', 'data_validation'},
         f"feature kinds {sorted(features)}"),
        (features['conditional_formatting']['sqref'] == 'A1:A9',
         'cfRule sqref pulled from enclosing conditionalFormatting'),
        (features['data_validation']['sqref'] == 'B2:B9', 'DV sqref'),
    ])


def test_pivot_chain_and_hyperlinks():
    """Pivot chained to host sheet and local source; file link found."""
    print("\nTesting pivot chain and file hyperlinks...")
    with tempfile.TemporaryDirectory() as workdir:
        inv = hostile_inventory(workdir)
    pivots = inv['pivots']
    pivot = pivots[0] if pivots else {}
    links = inv['file_hyperlinks']
    return check_all([
        (len(pivots) == 1, 'one pivot table'),
        (pivot.get('on_sheet') == 'Pivots' and pivot.get('location') == 'A3:C20',
         'host sheet and location'),
        (pivot.get('source_sheet') == 'Data' and pivot.get('source_ref') == 'A1:K500',
         'local source sheet and fixed range'),
        (pivot.get('refresh_on_load') is True, 'refreshOnLoad read'),
        (pivot.get('name') == 'VanSummary', 'pivot name'),
        (not pivot.get('external_rid'), 'not counted as external'),
        (len(links) == 1 and links[0]['target'] == '../Other%20Book.xlsx',
         'file hyperlink found, web link excluded'),
    ])


def test_orphans_and_summary():
    """[3] unresolved, [2] phantom, externalLink9 dangling; verdict set."""
    print("\nTesting orphans and summary verdict...")
    with tempfile.TemporaryDirectory() as workdir:
        inv = hostile_inventory(workdir)
    orphans = inv['orphans']
    summary = inv['summary']
    return check_all([
        (orphans['unresolved_indexes'] == [3], 'orphan index 3'),
        (orphans['unused_indexes'] == [2], 'phantom index 2'),
        (orphans['dangling_parts'] == ['xl/externalLinks/externalLink9.xml'],
         'dangling part'),
        (summary['has_ties'] is True, 'has_ties'),
        (summary['formula_refs'] == 4 and summary['defined_names'] == 2,
         'counts match sections'),
        (summary == summarize(inv), 'summary is a pure function of the inventory'),
    ])


def test_processor_report_and_gate():
    """Report JSON round-trips; fail_on_ties raises after the report."""
    print("\nTesting processor report file and gate...")
    with tempfile.TemporaryDirectory() as workdir:
        path = os.path.join(workdir, 'hostile.xlsx')
        build_hostile_workbook(path)
        report = os.path.join(workdir, 'ties.json')
        processor = make_processor({'files': [path], 'report_file': report})
        result = processor.perform_file_operation()
        with open(report, encoding='utf-8') as handle:
            loaded = json.load(handle)
        gated = make_processor({'files': [path], 'fail_on_ties': True})
        raised = ''
        try:
            gated.perform_file_operation()
        except StepProcessorError as error:
            raised = str(error)
        try:
            make_processor({'files': [path], 'name_cap': 0})
            cap_refused = False
        except StepProcessorError:
            cap_refused = True
    return check_all([
        ('1 with external ties' in result, f"result: {result}"),
        (loaded['summary']['has_ties'] is True, 'JSON report parses with verdict'),
        (len(loaded['formula_refs']) == 4, 'JSON carries the full formula list'),
        ('fail_on_ties' in raised and 'hostile.xlsx' in raised,
         'gate names the file'),
        (cap_refused, 'name_cap 0 refused'),
    ])


def test_clean_workbook_untouched():
    """An openpyxl-authored file has no ties and is not modified."""
    print("\nTesting clean workbook and read-only guarantee...")
    with tempfile.TemporaryDirectory() as workdir:
        path = os.path.join(workdir, 'clean.xlsx')
        workbook = openpyxl.Workbook()
        sheet = workbook.active
        sheet.title = 'Data'
        sheet['A1'] = 'hello'
        sheet['B1'] = '=A1'
        sheet['C1'].hyperlink = 'https://example.com'
        workbook.save(path)
        before = hashlib.sha256(open(path, 'rb').read()).hexdigest()
        records = []
        handler = logging.Handler()
        handler.emit = lambda record: records.append(record.getMessage())
        audit_logger = logging.getLogger(
            'excel_recipe_processor.processors.audit_external_ties_processor')
        audit_logger.setLevel(logging.INFO)
        audit_logger.addHandler(handler)
        try:
            processor = make_processor({'files': [path]})
            result = processor.perform_file_operation()
        finally:
            audit_logger.removeHandler(handler)
        after = hashlib.sha256(open(path, 'rb').read()).hexdigest()
    return check_all([
        (before == after, 'bytes identical before and after'),
        ('0 with external ties' in result, f"result: {result}"),
        (any('no external ties' in message for message in records),
         'clean verdict logged'),
    ])


def test_ref_parser_edge_cases():
    """Escaped apostrophes, 3D ranges and bare names parse as intended."""
    print("\nTesting reference parser edge cases...")
    cases = [
        ("'[1]Bob''s Sheet'!A1", [(1, "Bob's Sheet")]),
        ("'[2]Sheet1:Sheet3'!A1", [(2, 'Sheet1:Sheet3')]),
        ("[1]Ext!A1+[1]Ext!B1", [(1, 'Ext'), (1, 'Ext')]),
        ("[4]!SomeName", [(4, '')]),
        ("Data!A1*2", []),
    ]
    return check_all([
        (parse_external_sheet_refs(text) == expected, f"{text} -> {expected}")
        for text, expected in cases
    ])


def main():
    """Run every test and report a final score."""
    print("=== audit_external_ties antagonistic tests ===")

    tests = [
        test_external_link_parts,
        test_formula_refs,
        test_names_and_sheet_features,
        test_pivot_chain_and_hyperlinks,
        test_orphans_and_summary,
        test_processor_report_and_gate,
        test_clean_workbook_untouched,
        test_ref_parser_edge_cases,
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
