"""
Dynamic-array metadata declaration for saved workbooks.

excel_recipe_processor/core/dynamic_array_metadata.py

WHY THIS EXISTS

Excel 365 displays a leading @ (the implicit-intersection operator) on any
stored formula that could return an array - one containing XLOOKUP, say -
unless the file declares the formula dynamic-array-aware. openpyxl cannot
write that declaration, so every formula this framework injects or seeds
comes up wearing an @ the user never typed.

The declaration Excel itself writes has three parts, captured verbatim from
an Excel-authored workbook (and byte-identical to what xlsxwriter produces):

    1. The cell carries cm="1" and its formula element becomes
       <f t="array" ref="AV2">            (in xl/worksheets/sheetN.xml)
    2. An xl/metadata.xml part holds the XLDAPR record declaring
       fDynamic="1" - "this array formula is the DYNAMIC kind, not
       legacy CSE"; without it, t="array" alone renders as {braces}
    3. [Content_Types].xml and xl/_rels/workbook.xml.rels register the part

All three travel together. This module applies them to a finished xlsx -
either a file on disk or in-memory bytes on their way to disk - by targeted
surgery on the zip members, leaving every untouched member byte-identical.

WHAT GETS MARKED - AND THE SAFETY LINE

Only formulas containing a function that POSTDATES dynamic arrays (XLOOKUP,
FILTER, LET, ...). Such a formula was necessarily authored in
dynamic-array-aware Excel, so the declaration states a fact and cannot
change results. Pre-dynamic-array array-capable functions (INDEX, OFFSET)
are deliberately NOT marked: a legacy formula depending on implicit
intersection would start SPILLING if declared dynamic - a silent change of
computed values. Callers who know a file's formulas are recipe-authored may
extend the vocabulary per call.

COMPATIBILITY NOTE

Pre-2019 Excel shows marked cells as legacy {CSE} formulas - Microsoft's
intended graceful degradation. Results are unchanged either way.
"""

import io
import shutil
import logging
import zipfile

from pathlib import Path

from excel_recipe_processor.core.dynamic_array_metadata_rgx import (
    DYNAMIC_ERA_FUNCTIONS,
    build_function_detection_rgx,
    formula_cell_rgx,
    cell_ref_attr_rgx,
    cell_open_tag_rgx,
    formula_element_rgx,
    relationship_id_rgx,
)


logger = logging.getLogger(__name__)


class DynamicArrayMetadataError(Exception):
    """Raised when a workbook cannot safely receive the declaration."""
    pass


# --------------------------------------------------------------------------
# Reference bytes, captured verbatim from an Excel-authored workbook
# (export_destinations.xlsx, authored in Excel 365). xlsxwriter emits the
# identical structure. Do not reformat.
# --------------------------------------------------------------------------

EXCEL_METADATA_XML = (
    b'<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\r\n'
    b'<metadata xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
    b'xmlns:xda="http://schemas.microsoft.com/office/spreadsheetml/2017/dynamicarray">'
    b'<metadataTypes count="1">'
    b'<metadataType name="XLDAPR" minSupportedVersion="120000" copy="1" '
    b'pasteAll="1" pasteValues="1" merge="1" splitFirst="1" rowColShift="1" '
    b'clearFormats="1" clearComments="1" assign="1" coerce="1" cellMeta="1"/>'
    b'</metadataTypes>'
    b'<futureMetadata name="XLDAPR" count="1">'
    b'<bk><extLst><ext uri="{bdbb8cdc-fa1e-496e-a857-3c3f30c029c3}">'
    b'<xda:dynamicArrayProperties fDynamic="1" fCollapsed="0"/>'
    b'</ext></extLst></bk>'
    b'</futureMetadata>'
    b'<cellMetadata count="1"><bk><rc t="1" v="0"/></bk></cellMetadata>'
    b'</metadata>'
)

METADATA_PART_NAME = 'xl/metadata.xml'

CONTENT_TYPES_OVERRIDE = (
    '<Override PartName="/xl/metadata.xml" ContentType="application/'
    'vnd.openxmlformats-officedocument.spreadsheetml.sheetMetadata+xml"/>'
)

SHEET_METADATA_REL_TYPE = (
    'http://schemas.openxmlformats.org/officeDocument/2006/relationships/sheetMetadata'
)


# --------------------------------------------------------------------------
# Public entry points
# --------------------------------------------------------------------------

def declare_dynamic_formulas_in_zip(source, destination_path, extra_functions=None) -> dict:
    """
    Apply the dynamic-array declaration to a finished xlsx.

    Reads the whole package from `source`, marks qualifying formula cells,
    adds the metadata part and its registrations if any cell was marked,
    and writes the result to `destination_path`. Source and destination may
    be the same path; the source is read fully before anything is written.

    Args:
        source:           Path to an xlsx, or a binary file-like object
                          positioned at 0 (e.g. BytesIO from workbook.save)
        destination_path: Path to write the resulting xlsx
        extra_functions:  Optional iterable of additional bare function
                          names to treat as dynamic-era (e.g. ['IFS'] for a
                          file whose formulas are known recipe-authored)

    Returns:
        Report dict: cells_scanned, cells_marked, cells_completed (already
        t="array", cm added), cells_already_declared, cells_shared_skipped,
        metadata_part_added, per-sheet breakdown under 'sheets'

    Raises:
        DynamicArrayMetadataError: On an unrecognized existing metadata
        part, or malformed package structure
    """
    vocabulary = set(DYNAMIC_ERA_FUNCTIONS)
    if extra_functions:
        for name in extra_functions:
            if not str(name).replace('.', '').replace('_', '').isalnum():
                raise DynamicArrayMetadataError(
                    f"extra_functions entry does not look like a function name: {name!r}"
                )
            vocabulary.add(str(name).upper())

    detection_rgx = build_function_detection_rgx(vocabulary)

    members = _read_all_members(source)

    report = {
        'cells_scanned': 0,
        'cells_marked': 0,
        'cells_completed': 0,
        'cells_already_declared': 0,
        'cells_shared_skipped': 0,
        'metadata_part_added': False,
        'sheets': {},
    }

    for name in sorted(members):
        if name.startswith('xl/worksheets/') and name.endswith('.xml'):
            new_bytes, sheet_report = _mark_sheet_cells(members[name], detection_rgx)
            members[name] = new_bytes
            report['sheets'][name] = sheet_report
            for key in ('cells_scanned', 'cells_marked', 'cells_completed',
                        'cells_already_declared', 'cells_shared_skipped'):
                report[key] += sheet_report[key]

    declaration_needed = (
        report['cells_marked'] > 0
        or report['cells_completed'] > 0
        or report['cells_already_declared'] > 0
    )

    if declaration_needed:
        report['metadata_part_added'] = _ensure_metadata_part(members)
        _ensure_content_types_override(members)
        _ensure_workbook_relationship(members)

    _write_all_members(members, destination_path)

    logger.info(
        f"🧬 Dynamic-array declaration: {report['cells_marked']} cell(s) marked, "
        f"{report['cells_completed']} completed, "
        f"{report['cells_already_declared']} already declared, "
        f"{report['cells_shared_skipped']} shared-formula cell(s) skipped "
        f"({Path(destination_path).name})"
    )

    return report


def save_workbook_with_declaration(workbook, file_path, extra_functions=None) -> dict:
    """
    Save an openpyxl workbook with the declaration applied in memory.

    The workbook serializes to a buffer, the declaration is applied to the
    bytes, and only the corrected package touches disk - the file is never
    on disk in the @-prone form.

    Args:
        workbook:        Live openpyxl Workbook
        file_path:       Destination path
        extra_functions: Passed through to declare_dynamic_formulas_in_zip

    Returns:
        The declaration report dict
    """
    buffer = io.BytesIO()
    workbook.save(buffer)
    buffer.seek(0)

    return declare_dynamic_formulas_in_zip(buffer, file_path, extra_functions)


# --------------------------------------------------------------------------
# Worksheet surgery
# --------------------------------------------------------------------------

def _mark_sheet_cells(sheet_bytes: bytes, detection_rgx) -> tuple:
    """
    Mark qualifying formula cells in one worksheet's XML.

    Returns:
        (new_bytes, sheet_report_dict)
    """
    text = sheet_bytes.decode('utf-8')

    sheet_report = {
        'cells_scanned': 0,
        'cells_marked': 0,
        'cells_completed': 0,
        'cells_already_declared': 0,
        'cells_shared_skipped': 0,
    }

    def rewrite_cell(match):
        cell_text = match.group(0)
        sheet_report['cells_scanned'] += 1

        open_tag_match = cell_open_tag_rgx.match(cell_text)
        if open_tag_match is None:
            return cell_text
        open_tag = open_tag_match.group(0)

        if 'cm="' in open_tag:
            sheet_report['cells_already_declared'] += 1
            return cell_text

        formula_match = formula_element_rgx.search(cell_text)
        if formula_match is None:
            # Self-closing <f/> (a shared-formula child) has no text to test
            sheet_report['cells_shared_skipped'] += 1
            return cell_text

        f_open, formula_text, f_close = formula_match.groups()

        if 't="shared"' in f_open:
            # Marking a shared formula would require unsharing the group;
            # out of scope, and none are produced by this framework.
            sheet_report['cells_shared_skipped'] += 1
            return cell_text

        if detection_rgx.search(formula_text) is None:
            return cell_text

        ref_match = cell_ref_attr_rgx.search(open_tag)
        if ref_match is None:
            return cell_text
        cell_ref = ref_match.group(1)

        new_open_tag = open_tag[:-1] + ' cm="1">'

        if 't="array"' in f_open:
            # Already an array formula (legacy CSE, showing {braces});
            # adding cm completes it into a dynamic-array formula.
            new_f_open = f_open
            sheet_report['cells_completed'] += 1
        else:
            new_f_open = f_open[:-1] + f' t="array" ref="{cell_ref}">'
            sheet_report['cells_marked'] += 1

        new_cell_text = cell_text.replace(open_tag, new_open_tag, 1)
        new_cell_text = new_cell_text.replace(
            f_open + formula_text + f_close,
            new_f_open + formula_text + f_close,
            1,
        )
        return new_cell_text

    new_text = formula_cell_rgx.sub(rewrite_cell, text)

    return new_text.encode('utf-8'), sheet_report


# --------------------------------------------------------------------------
# Package part registration
# --------------------------------------------------------------------------

def _ensure_metadata_part(members: dict) -> bool:
    """
    Add xl/metadata.xml if absent; verify it if present.

    Returns:
        True if the part was added, False if a recognized one already existed

    Raises:
        DynamicArrayMetadataError: If an existing part is not the XLDAPR
        form this module knows how to coexist with (merging is out of scope)
    """
    if METADATA_PART_NAME in members:
        existing = members[METADATA_PART_NAME]
        if b'XLDAPR' in existing and b'dynamicArrayProperties' in existing:
            return False
        raise DynamicArrayMetadataError(
            f"{METADATA_PART_NAME} exists but is not the XLDAPR dynamic-array "
            f"form; merging metadata types is not supported. Leave this file "
            f"alone or investigate its metadata part first."
        )

    members[METADATA_PART_NAME] = EXCEL_METADATA_XML
    return True


def _ensure_content_types_override(members: dict) -> None:
    """Register the metadata part's content type if not already present."""
    part = '[Content_Types].xml'
    if part not in members:
        raise DynamicArrayMetadataError("Package has no [Content_Types].xml")

    text = members[part].decode('utf-8')

    if 'sheetMetadata+xml' in text:
        return

    if '</Types>' not in text:
        raise DynamicArrayMetadataError("[Content_Types].xml has no closing </Types> tag")

    members[part] = text.replace(
        '</Types>', CONTENT_TYPES_OVERRIDE + '</Types>', 1
    ).encode('utf-8')


def _ensure_workbook_relationship(members: dict) -> None:
    """Point the workbook at the metadata part if not already related."""
    part = 'xl/_rels/workbook.xml.rels'
    if part not in members:
        raise DynamicArrayMetadataError("Package has no xl/_rels/workbook.xml.rels")

    text = members[part].decode('utf-8')

    if 'sheetMetadata' in text:
        return

    if '</Relationships>' not in text:
        raise DynamicArrayMetadataError(
            "xl/_rels/workbook.xml.rels has no closing </Relationships> tag"
        )

    used_ids = [int(number) for number in relationship_id_rgx.findall(text)]
    next_id = (max(used_ids) + 1) if used_ids else 1

    relationship = (
        f'<Relationship Id="rId{next_id}" Type="{SHEET_METADATA_REL_TYPE}" '
        f'Target="metadata.xml"/>'
    )
    members[part] = text.replace(
        '</Relationships>', relationship + '</Relationships>', 1
    ).encode('utf-8')


# --------------------------------------------------------------------------
# Zip plumbing
# --------------------------------------------------------------------------

def _read_all_members(source) -> dict:
    """
    Read every member of the package into memory.

    Args:
        source: Path or binary file-like object

    Returns:
        dict of member name -> bytes, in original order (py3.7+ dicts)
    """
    if isinstance(source, (str, Path)):
        source_path = Path(source)
        if not source_path.is_file():
            raise DynamicArrayMetadataError(f"Source file does not exist: {source_path}")
        opened = zipfile.ZipFile(source_path, 'r')
    else:
        opened = zipfile.ZipFile(source, 'r')

    with opened as archive:
        bad_member = archive.testzip()
        if bad_member is not None:
            raise DynamicArrayMetadataError(f"Corrupt zip member: {bad_member}")
        return {name: archive.read(name) for name in archive.namelist()}


def _write_all_members(members: dict, destination_path) -> None:
    """
    Write the package to disk via a sibling temp file, then move it over.

    The temp-then-replace keeps a crash mid-write from leaving a truncated
    file at the destination path.
    """
    destination = Path(destination_path)
    if not destination.parent.is_dir():
        raise DynamicArrayMetadataError(
            f"Destination directory does not exist: {destination.parent}"
        )

    temp_path = destination.with_name(destination.name + '.da_tmp')

    with zipfile.ZipFile(temp_path, 'w', compression=zipfile.ZIP_DEFLATED) as archive:
        for name, content in members.items():
            archive.writestr(name, content)

    shutil.move(str(temp_path), str(destination))

# End of file #
