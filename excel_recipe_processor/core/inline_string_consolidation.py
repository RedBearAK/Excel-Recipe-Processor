"""
Consolidate inline strings into the shared-string table at save time.

excel_recipe_processor/core/inline_string_consolidation.py

openpyxl 3.1+ writes every literal string INLINE (t="inlineStr"), a
write-speed trade that costs file size heavily on repetitive data:
identical content measured 3.85 MB in openpyxl's dialect vs 3.46 MB in
Excel's, with 9.4 MB of raw repeated text on the production VMS sheet
alone. This module rewrites the will-be-written bytes to Excel's
dialect - each unique string stored ONCE in xl/sharedStrings.xml,
cells reduced to t="s" index references - so ERP-authored files match
Excel's own storage efficiency from birth. Runs intrinsically at
every session save (2026-08-17 ruling), alongside the dynamic-array
declaration in the same serialize-to-memory pipeline.

Safety doctrine:
- Only cells whose <is> body is a single plain <t> element are
  consolidated; rich-text runs or anything unrecognized stay inline
  and are counted, never guessed at.
- The <t> element is reused VERBATIM in the new <si>: escaping and
  xml:space="preserve" survive byte-exactly, so a reload reads the
  identical value.
- An existing sharedStrings table (a session working on an
  Excel-lineage file) is merged, existing entries and their indices
  preserved untouched.
- The pass is idempotent: a package with no inline strings returns
  its original bytes unchanged.
"""

import io
import logging
import zipfile

from excel_recipe_processor.core.inline_string_consolidation_rgx import (
    max_rel_id_rgx,
    types_close_rgx,
    plain_is_body_rgx,
    shared_strings_rel_rgx,
    relationships_close_rgx,
    inline_string_cell_rgx,
    shared_string_item_rgx,
    shared_string_count_rgx,
    shared_strings_override_rgx,
)

logger = logging.getLogger(__name__)

SHARED_STRINGS_CONTENT_TYPE = (
    'application/vnd.openxmlformats-officedocument.spreadsheetml'
    '.sharedStrings+xml')
SHARED_STRINGS_REL_TYPE = (
    'http://schemas.openxmlformats.org/officeDocument/2006/'
    'relationships/sharedStrings')


def consolidate_inline_strings(package_bytes: bytes):
    """Rewrite inline strings to shared-string references.

    Args:
        package_bytes: A complete xlsx package as bytes.

    Returns:
        (new_package_bytes, stats) where stats carries
        cells_consolidated, unique_strings, cells_skipped, and
        bytes_before/bytes_after. When nothing needed consolidating
        the ORIGINAL bytes come back unchanged (idempotence).
    """
    with zipfile.ZipFile(io.BytesIO(package_bytes)) as archive:
        members = {name: archive.read(name) for name in archive.namelist()}

    # Existing table (Excel-lineage files): entries kept verbatim, in
    # order, so every pre-existing t="s" index stays valid.
    table_entries = []
    table_index = {}
    existing_refs = 0
    existing_sst = members.get('xl/sharedStrings.xml')
    if existing_sst is not None:
        sst_text = existing_sst.decode('utf-8')
        count_match = shared_string_count_rgx.search(sst_text)
        existing_refs = int(count_match.group(1)) if count_match else 0
        for item in shared_string_item_rgx.findall(sst_text):
            table_index.setdefault(item, len(table_entries))
            table_entries.append(item)

    stats = {'cells_consolidated': 0, 'unique_strings': 0,
             'cells_skipped': 0, 'bytes_before': len(package_bytes),
             'bytes_after': len(package_bytes)}
    starting_entries = len(table_entries)

    def rewrite_cell(match):
        attrs_before, attrs_after, is_body = match.groups()
        if not plain_is_body_rgx.match(is_body):
            stats['cells_skipped'] += 1
            return match.group(0)
        t_element = is_body[len('<is>'):-len('</is>')]
        item = f'<si>{t_element}</si>'
        index = table_index.get(item)
        if index is None:
            index = len(table_entries)
            table_index[item] = index
            table_entries.append(item)
        stats['cells_consolidated'] += 1
        return f'<c{attrs_before}t="s"{attrs_after}><v>{index}</v></c>'

    changed = False
    for name in list(members):
        if not (name.startswith('xl/worksheets/') and name.endswith('.xml')):
            continue
        xml = members[name].decode('utf-8')
        if 't="inlineStr"' not in xml:
            continue
        new_xml = inline_string_cell_rgx.sub(rewrite_cell, xml)
        if new_xml != xml:
            members[name] = new_xml.encode('utf-8')
            changed = True

    if not changed:
        return package_bytes, stats

    stats['unique_strings'] = len(table_entries) - starting_entries
    total_refs = existing_refs + stats['cells_consolidated']
    sst = ('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
           f'<sst xmlns="http://schemas.openxmlformats.org/'
           f'spreadsheetml/2006/main" count="{total_refs}" '
           f'uniqueCount="{len(table_entries)}">'
           + ''.join(table_entries) + '</sst>')
    members['xl/sharedStrings.xml'] = sst.encode('utf-8')

    content_types = members['[Content_Types].xml'].decode('utf-8')
    if not shared_strings_override_rgx.search(content_types):
        members['[Content_Types].xml'] = types_close_rgx.sub(
            f'<Override PartName="/xl/sharedStrings.xml" '
            f'ContentType="{SHARED_STRINGS_CONTENT_TYPE}"/></Types>',
            content_types, count=1).encode('utf-8')

    rels_name = 'xl/_rels/workbook.xml.rels'
    if rels_name in members:
        rels = members[rels_name].decode('utf-8')
        if not shared_strings_rel_rgx.search(rels):
            next_id = max((int(n) for n in max_rel_id_rgx.findall(rels)),
                          default=0) + 1
            members[rels_name] = relationships_close_rgx.sub(
                f'<Relationship Id="rId{next_id}" '
                f'Type="{SHARED_STRINGS_REL_TYPE}" '
                f'Target="sharedStrings.xml"/></Relationships>',
                rels, count=1).encode('utf-8')

    out = io.BytesIO()
    # compresslevel 9: the save-side twin of the strip processor's
    # packing - authored files are archives from birth.
    with zipfile.ZipFile(out, 'w', zipfile.ZIP_DEFLATED,
                         compresslevel=9) as archive:
        for name, data in members.items():
            archive.writestr(name, data)
    result = out.getvalue()
    stats['bytes_after'] = len(result)
    return result, stats


def log_consolidation(stats, context: str) -> None:
    """One INFO line naming what the pass did, or silence for no-ops."""
    if not stats['cells_consolidated'] and not stats['cells_skipped']:
        return
    saved = stats['bytes_before'] - stats['bytes_after']
    message = (f"Consolidated {stats['cells_consolidated']:,} inline "
               f"string cell(s) into {stats['unique_strings']:,} shared "
               f"entr(ies) ({context}): {stats['bytes_before']:,} -> "
               f"{stats['bytes_after']:,} bytes ({saved:+,})")
    if stats['cells_skipped']:
        message += (f"; {stats['cells_skipped']} non-plain inline "
                    f"cell(s) left untouched")
    logger.info(message)

# End of file #
