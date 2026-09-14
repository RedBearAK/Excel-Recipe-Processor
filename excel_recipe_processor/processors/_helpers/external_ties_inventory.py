"""
Read-only inventory of every tie an xlsx package has to other files.

excel_recipe_processor/processors/_helpers/external_ties_inventory.py

One scan, one dict, three consumers: the audit_external_ties processor
(reports it), the later sever/repair surgery (acts on it), and the
tab-transplant pre-flight (checks pivot sources against an incoming
tab). The package is opened with zipfile in read mode and NOTHING is
written - the inventory cannot alter a file by construction.

WHAT IS FOUND (2026-09-14 problem grid):
  external_links   externalLinkN.xml parts: index, target, sheet list,
                   cached data present, kind (workbook / ole / dde)
  formula_refs     every <f> carrying [N], with sheet, cell, N, the
                   referenced sheet name(s), cached-value flag, and
                   the shared-formula ref range when it is a master
  defined_names    workbook.xml names carrying [N] or #REF!
  sheet_features   [N] outside sheetData: conditional formatting, data
                   validation, or UNCLASSIFIED (any carrier the grid
                   does not know is reported, never ignored)
  chart_refs       <c:f> series formulas carrying a bracket reference
  pivots           every pivot table: host sheet, location, cache
                   source (sheet/ref/name or external r:id),
                   refreshOnLoad - needed by the transplant pre-flight
                   even when the source is local
  file_hyperlinks  hyperlink rels to files (web and mailto excluded)
  connections      xl/connections.xml entries
  query_tables     queryTable parts
  ole_objects      oleObject rels and embeddings
  orphans          [N] with no externalReference behind it, and
                   externalLink parts no reference points at

Every list is sorted deterministically so two scans of the same bytes
yield the same inventory, and a JSON dump diffs cleanly across runs.
"""

import json
import zipfile
import posixpath

from excel_recipe_processor.processors._helpers.external_ties_rgx import (
    calc_pr_rgx,
    attr_id_rgx,
    attr_ref_rgx,
    attr_rid_rgx,
    attr_name_rgx,
    attr_type_rgx,
    cell_open_rgx,
    attr_sheet_rgx,
    attr_sqref_rgx,
    attr_hidden_rgx,
    attr_id_lower_rgx,
    attr_target_rgx,
    chart_part_rgx,
    cache_source_rgx,
    attr_type_lower_rgx,
    cell_ref_attr_rgx,
    chart_formula_rgx,
    worksheet_part_rgx,
    external_index_rgx,
    embedding_part_rgx,
    pivot_location_rgx,
    pivot_table_part_rgx,
    sheet_data_open_rgx,
    query_table_part_rgx,
    sheet_data_close_rgx,
    worksheet_source_rgx,
    attr_target_mode_rgx,
    external_link_kind_rgx,
    external_sheet_ref_rgx,
    sheet_feature_open_rgx,
    external_link_part_rgx,
    connection_element_rgx,
    attr_local_sheet_id_rgx,
    attr_refresh_on_load_rgx,
    web_or_mail_target_rgx,
    relationship_element_rgx,
    formula_with_external_rgx,
    workbook_sheet_element_rgx,
    workbook_defined_name_rgx,
    external_link_sheet_name_rgx,
    external_link_cached_data_rgx,
    external_reference_element_rgx,
    pivot_cache_definition_part_rgx,
    pivot_cache_definition_root_rgx,
)


FORMULA_SNIPPET_CAP = 120

# Inventory sections whose non-emptiness means the workbook has a tie
# to something outside itself. 'pivots' is deliberately NOT here: a
# pivot on a local sheet is a fact the transplant needs, not a tie.
TIE_SECTIONS = (
    'external_links', 'formula_refs', 'defined_names', 'sheet_features',
    'chart_refs', 'file_hyperlinks', 'connections', 'query_tables',
    'ole_objects',
)


class ExternalTiesInventoryError(Exception):
    """The package is not a shape the inventory claims to understand."""
    pass


def _first(rgx, text: str, default: str = '') -> str:
    match = rgx.search(text)
    return match.group(1) if match else default


def _read_text(archive: zipfile.ZipFile, part: str) -> str:
    return archive.read(part).decode('utf-8')


def _rels_part_for(part: str) -> str:
    """xl/worksheets/sheet1.xml -> xl/worksheets/_rels/sheet1.xml.rels"""
    directory, name = posixpath.split(part)
    return posixpath.join(directory, '_rels', name + '.rels')


def _resolve_target(from_part: str, target: str) -> str:
    """Resolve a relationship Target against the part that holds it."""
    if target.startswith('/'):
        return target.lstrip('/')
    return posixpath.normpath(posixpath.join(posixpath.dirname(from_part), target))


def _snippet(text: str) -> str:
    if len(text) <= FORMULA_SNIPPET_CAP:
        return text
    return text[:FORMULA_SNIPPET_CAP] + '...'


def read_relationships(archive: zipfile.ZipFile, part: str) -> list:
    """All relationships of a part as dicts; empty when it has no rels."""
    rels_part = _rels_part_for(part)
    if rels_part not in archive.namelist():
        return []
    xml = _read_text(archive, rels_part)
    relationships = []
    for element in relationship_element_rgx.finditer(xml):
        attrs = element.group(0)
        relationships.append({
            'id': _first(attr_id_rgx, attrs),
            'type': _first(attr_type_rgx, attrs).rsplit('/', 1)[-1],
            'target': _first(attr_target_rgx, attrs),
            'target_mode': _first(attr_target_mode_rgx, attrs, 'Internal'),
        })
    return relationships


def sheet_part_map(archive: zipfile.ZipFile) -> list:
    """Ordered [(sheet_name, part)] from workbook.xml and its rels."""
    workbook_xml = _read_text(archive, 'xl/workbook.xml')
    rels = {rel['id']: rel for rel in read_relationships(archive, 'xl/workbook.xml')}
    sheets = []
    for element in workbook_sheet_element_rgx.finditer(workbook_xml):
        attrs = element.group(0)
        name = _first(attr_name_rgx, attrs)
        rid = _first(attr_rid_rgx, attrs)
        rel = rels.get(rid)
        if rel is None:
            raise ExternalTiesInventoryError(
                f"workbook.xml sheet {name!r} points at {rid!r}, which the "
                f"workbook rels do not define")
        sheets.append((name, _resolve_target('xl/workbook.xml', rel['target'])))
    return sheets


def parse_external_sheet_refs(formula_text: str) -> list:
    """(index, sheet_name) pairs from a stored formula; sheet may be ''."""
    pairs = []
    for match in external_sheet_ref_rgx.finditer(formula_text):
        if match.group('qidx') is not None:
            pairs.append((int(match.group('qidx')),
                          match.group('qsheet').replace("''", "'")))
        else:
            pairs.append((int(match.group('idx')), match.group('sheet')))
    return pairs


# --- section scanners --------------------------------------------------------

def _scan_external_links(archive: zipfile.ZipFile, names: list) -> tuple:
    """(links, dangling_parts) - links carry their 1-based [N] index."""
    workbook_xml = _read_text(archive, 'xl/workbook.xml')
    rels = {rel['id']: rel for rel in read_relationships(archive, 'xl/workbook.xml')}
    links = []
    referenced_parts = set()
    for index, element in enumerate(
            external_reference_element_rgx.finditer(workbook_xml), start=1):
        rid = _first(attr_rid_rgx, element.group(0))
        rel = rels.get(rid)
        part = _resolve_target('xl/workbook.xml', rel['target']) if rel else ''
        entry = {
            'index': index, 'rid': rid, 'part': part, 'target': '',
            'target_mode': '', 'kind': 'missing', 'sheet_names': [],
            'has_cached_values': False,
        }
        if part and part in names:
            referenced_parts.add(part)
            xml = _read_text(archive, part)
            entry['kind'] = _first(external_link_kind_rgx, xml, 'unknown')
            if entry['kind'] == 'externalBook':
                entry['kind'] = 'workbook'
            entry['sheet_names'] = external_link_sheet_name_rgx.findall(xml)
            entry['has_cached_values'] = bool(
                external_link_cached_data_rgx.search(xml))
            for link_rel in read_relationships(archive, part):
                if link_rel['type'] in ('externalLinkPath', 'oleObject'):
                    entry['target'] = link_rel['target']
                    entry['target_mode'] = link_rel['target_mode']
        links.append(entry)
    dangling = sorted(part for part in names
                      if external_link_part_rgx.match(part)
                      and part not in referenced_parts)
    return links, dangling


def _scan_worksheet(archive: zipfile.ZipFile, sheet_name: str,
                    part: str) -> tuple:
    """(formula_refs, sheet_features) for one worksheet part."""
    xml = _read_text(archive, part)
    formula_refs = []
    for match in formula_with_external_rgx.finditer(xml):
        cell_start = xml.rfind('<c ', 0, match.start())
        cell_open = cell_open_rgx.match(xml, cell_start) if cell_start >= 0 else None
        cell_attrs = cell_open.group('attrs') if cell_open else ''
        cell_close = xml.find('</c>', match.end())
        body_after = xml[match.end():cell_close] if cell_close >= 0 else ''
        pairs = parse_external_sheet_refs(match.group('text'))
        indexes = sorted({int(n) for n in external_index_rgx.findall(match.group('text'))})
        formula_refs.append({
            'sheet': sheet_name,
            'cell': _first(cell_ref_attr_rgx, cell_attrs, '?'),
            'indexes': indexes,
            'ref_sheets': sorted({sheet for _, sheet in pairs}),
            'has_cached_value': '<v' in body_after,
            'shared_ref': _first(attr_ref_rgx, match.group('attrs')),
            'formula': _snippet(match.group('text')),
        })

    # Everything outside sheetData: CF, DV, extLst, hyperlinks...
    open_match = sheet_data_open_rgx.search(xml)
    close_match = sheet_data_close_rgx.search(xml)
    if open_match and close_match:
        outside = xml[:open_match.start()] + xml[close_match.end():]
    else:
        outside = xml
    features = []
    for match in external_index_rgx.finditer(outside):
        enclosing = None
        for candidate in sheet_feature_open_rgx.finditer(outside, 0, match.start()):
            enclosing = candidate
        tag = enclosing.group('tag') if enclosing else 'unclassified'
        sqref = ''
        if enclosing:
            sqref = _first(attr_sqref_rgx, enclosing.group('attrs'))
            if not sqref:
                # cfRule sits inside <conditionalFormatting sqref=...>
                for outer in sheet_feature_open_rgx.finditer(outside, 0, enclosing.start()):
                    candidate_sqref = _first(attr_sqref_rgx, outer.group('attrs'))
                    if candidate_sqref:
                        sqref = candidate_sqref
        text_start = outside.rfind('>', 0, match.start()) + 1
        text_end = outside.find('<', match.end())
        features.append({
            'sheet': sheet_name,
            'kind': _classify_feature_tag(tag),
            'element': tag,
            'sqref': sqref,
            'index': int(match.group(1)),
            'snippet': _snippet(outside[text_start:text_end]),
        })
    return formula_refs, features


def _classify_feature_tag(tag: str) -> str:
    if tag.endswith('cfRule') or tag.endswith('conditionalFormatting'):
        return 'conditional_formatting'
    if tag.endswith('dataValidation'):
        return 'data_validation'
    if tag == 'unclassified':
        return 'unclassified'
    return 'other:' + tag


def _scan_defined_names(archive: zipfile.ZipFile, sheets: list) -> list:
    workbook_xml = _read_text(archive, 'xl/workbook.xml')
    entries = []
    for match in workbook_defined_name_rgx.finditer(workbook_xml):
        attrs, text = match.group('attrs'), match.group('text')
        indexes = sorted({int(n) for n in external_index_rgx.findall(text)})
        has_ref_error = '#REF!' in text
        if not indexes and not has_ref_error:
            continue
        local_id = _first(attr_local_sheet_id_rgx, attrs)
        scope = ''
        if local_id:
            position = int(local_id)
            scope = sheets[position][0] if position < len(sheets) else f'?{local_id}'
        entries.append({
            'name': _first(attr_name_rgx, attrs),
            'scope': scope,
            'hidden': bool(attr_hidden_rgx.search(attrs)),
            'indexes': indexes,
            'has_ref_error': has_ref_error,
            'text': _snippet(text),
        })
    return entries


def _scan_charts(archive: zipfile.ZipFile, names: list) -> list:
    entries = []
    for part in sorted(names):
        if not chart_part_rgx.match(part):
            continue
        xml = _read_text(archive, part)
        for match in chart_formula_rgx.finditer(xml):
            formula = match.group(1)
            if '[' not in formula:
                continue
            indexes = sorted({int(n) for n in external_index_rgx.findall(formula)})
            entries.append({'part': part, 'indexes': indexes,
                            'formula': _snippet(formula)})
    return entries


def _scan_pivots(archive: zipfile.ZipFile, names: list, sheets: list) -> list:
    """Chain sheet -> pivotTable -> pivotCacheDefinition -> source."""
    cache_sources = {}
    for part in names:
        if not pivot_cache_definition_part_rgx.match(part):
            continue
        xml = _read_text(archive, part)
        root = pivot_cache_definition_root_rgx.search(xml)
        cache_source = cache_source_rgx.search(xml)
        source_type = _first(attr_type_lower_rgx,
                             cache_source.group(0) if cache_source else '')
        source_attrs = ''
        source = worksheet_source_rgx.search(xml)
        if source:
            source_attrs = source.group(0)
        external_rid = _first(attr_rid_rgx, source_attrs)
        external_part = ''
        if external_rid:
            for rel in read_relationships(archive, part):
                if rel['id'] == external_rid:
                    external_part = _resolve_target(part, rel['target'])
        cache_sources[part] = {
            'source_type': source_type,
            'source_sheet': _first(attr_sheet_rgx, source_attrs),
            'source_ref': _first(attr_ref_rgx, source_attrs),
            'source_name': _first(attr_name_rgx, source_attrs),
            'external_rid': external_rid,
            'external_part': external_part,
            'refresh_on_load': bool(root and attr_refresh_on_load_rgx.search(root.group(0))),
        }

    table_to_cache = {}
    for part in names:
        if not pivot_table_part_rgx.match(part):
            continue
        for rel in read_relationships(archive, part):
            if rel['type'] == 'pivotCacheDefinition':
                table_to_cache[part] = _resolve_target(part, rel['target'])

    entries = []
    for sheet_name, sheet_part in sheets:
        for rel in read_relationships(archive, sheet_part):
            if rel['type'] != 'pivotTable':
                continue
            table_part = _resolve_target(sheet_part, rel['target'])
            table_xml = _read_text(archive, table_part) if table_part in names else ''
            location_match = pivot_location_rgx.search(table_xml)
            location = _first(attr_ref_rgx,
                              location_match.group(0) if location_match else '')
            cache_part = table_to_cache.get(table_part, '')
            entry = {
                'pivot_table_part': table_part,
                'on_sheet': sheet_name,
                'location': location,
                'name': _first(attr_name_rgx, table_xml[:400]),
                'cache_part': cache_part,
            }
            entry.update(cache_sources.get(cache_part, {
                'source_type': 'missing', 'source_sheet': '', 'source_ref': '',
                'source_name': '', 'external_rid': '', 'external_part': '',
                'refresh_on_load': False,
            }))
            entries.append(entry)
    entries.sort(key=lambda entry: (entry['on_sheet'], entry['pivot_table_part']))
    return entries


def _scan_file_hyperlinks(archive: zipfile.ZipFile, sheets: list) -> list:
    entries = []
    for sheet_name, sheet_part in sheets:
        for rel in read_relationships(archive, sheet_part):
            if rel['type'] != 'hyperlink' or rel['target_mode'] != 'External':
                continue
            if web_or_mail_target_rgx.match(rel['target']):
                continue
            entries.append({'sheet': sheet_name, 'rid': rel['id'],
                            'target': rel['target']})
    return entries


def _scan_connections(archive: zipfile.ZipFile, names: list) -> list:
    if 'xl/connections.xml' not in names:
        return []
    xml = _read_text(archive, 'xl/connections.xml')
    return [{'name': _first(attr_name_rgx, element.group(0)),
             'id': _first(attr_id_lower_rgx, element.group(0))}
            for element in connection_element_rgx.finditer(xml)]


def _scan_ole_objects(archive: zipfile.ZipFile, names: list, sheets: list) -> list:
    entries = []
    for sheet_name, sheet_part in sheets:
        for rel in read_relationships(archive, sheet_part):
            if rel['type'] in ('oleObject', 'package'):
                entries.append({'sheet': sheet_name, 'kind': rel['type'],
                                'target': rel['target'],
                                'target_mode': rel['target_mode']})
    for part in sorted(names):
        if embedding_part_rgx.match(part):
            entries.append({'sheet': '', 'kind': 'embedding', 'target': part,
                            'target_mode': 'Internal'})
    return entries


# --- entry point ---------------------------------------------------------------

def inventory_external_ties(path: str, heartbeat=None) -> dict:
    """
    Scan one xlsx package and return the inventory dict.

    heartbeat: optional callable(sheet_position, sheet_count, sheet_name)
    invoked once per worksheet so a long scan stays audible.
    """
    with zipfile.ZipFile(path, 'r') as archive:
        names = archive.namelist()
        if 'xl/workbook.xml' not in names:
            raise ExternalTiesInventoryError(
                f"{path}: no xl/workbook.xml - not an xlsx package")
        sheets = sheet_part_map(archive)

        links, dangling = _scan_external_links(archive, names)
        formula_refs, sheet_features = [], []
        for position, (sheet_name, part) in enumerate(sheets, start=1):
            if heartbeat is not None:
                heartbeat(position, len(sheets), sheet_name)
            if part not in names or not worksheet_part_rgx.match(part):
                continue
            refs, features = _scan_worksheet(archive, sheet_name, part)
            formula_refs.extend(refs)
            sheet_features.extend(features)

        workbook_xml = _read_text(archive, 'xl/workbook.xml')
        calc_pr = calc_pr_rgx.search(workbook_xml)

        inventory = {
            'file': path,
            'sheets': [name for name, _ in sheets],
            'external_links': links,
            'formula_refs': formula_refs,
            'defined_names': _scan_defined_names(archive, sheets),
            'sheet_features': sheet_features,
            'chart_refs': _scan_charts(archive, names),
            'pivots': _scan_pivots(archive, names, sheets),
            'file_hyperlinks': _scan_file_hyperlinks(archive, sheets),
            'connections': _scan_connections(archive, names),
            'query_tables': sorted(p for p in names if query_table_part_rgx.match(p)),
            'ole_objects': _scan_ole_objects(archive, names, sheets),
            'calc_pr': calc_pr.group(0) if calc_pr else '',
        }

    known_indexes = {link['index'] for link in inventory['external_links']}
    used_indexes = set()
    for ref in inventory['formula_refs']:
        used_indexes.update(ref['indexes'])
    for name in inventory['defined_names']:
        used_indexes.update(name['indexes'])
    for feature in inventory['sheet_features']:
        used_indexes.add(feature['index'])
    for chart in inventory['chart_refs']:
        used_indexes.update(chart['indexes'])
    inventory['orphans'] = {
        'unresolved_indexes': sorted(used_indexes - known_indexes),
        'unused_indexes': sorted(known_indexes - used_indexes),
        'dangling_parts': dangling,
    }
    inventory['summary'] = summarize(inventory)
    return inventory


def summarize(inventory: dict) -> dict:
    """Counts per section plus the one verdict bit."""
    counts = {section: len(inventory[section]) for section in TIE_SECTIONS}
    counts['pivots'] = len(inventory['pivots'])
    counts['pivots_external'] = sum(
        1 for pivot in inventory['pivots'] if pivot['external_rid'])
    counts['unclassified_features'] = sum(
        1 for feature in inventory['sheet_features']
        if feature['kind'] == 'unclassified')
    counts['unresolved_indexes'] = len(inventory['orphans']['unresolved_indexes'])
    counts['dangling_parts'] = len(inventory['orphans']['dangling_parts'])
    counts['has_ties'] = any(counts[section] for section in TIE_SECTIONS) \
        or bool(counts['pivots_external']) or bool(counts['dangling_parts'])
    return counts


def inventory_to_json(inventory: dict) -> str:
    return json.dumps(inventory, indent=2, sort_keys=True, ensure_ascii=False)

# End of file #
