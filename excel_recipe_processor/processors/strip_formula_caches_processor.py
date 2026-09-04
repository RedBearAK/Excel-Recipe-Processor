"""
Strip cached formula results from saved Excel files, keeping formulas.

excel_recipe_processor/processors/strip_formula_caches_processor.py

Excel unconditionally writes the cached value of every calculated cell
on save - there is no setting to refuse it - so a freshly authored
workbook balloons on its first user save cycle (measured in
production: 3.9 MB authored -> 7.7 MB after open + save with NO
changes). This processor reverses that: it removes cached RESULTS
while keeping every FORMULA, returning the file to the
formulas-without-caches state openpyxl authors natively, and sets
fullCalcOnLoad so Excel recalculates on the next open. No mainstream
tool does this (researched 2026-08-17: compressors target images,
pivot caches and styles, and where they touch formulas they do the
OPPOSITE - formulas to values).

SAFETY DOCTRINE - what is touched and what is refused:
- Formula/result dyads (<c> with <f> and <v>): the <v> is stripped,
  the <f> kept. Ordinary, shared-master and shared-slave formulas.
- Array spills (dynamic or legacy CSE): the anchor's cached <v> is
  stripped; MEMBER cells (value-only cells inside the anchor's
  declared ref) are removed entirely, or blanked in place when they
  carry a style attribute so per-cell formatting survives.
- REFUSED, per the one data-loss trap prior art documents: any
  formula referencing an EXTERNAL workbook ([1]Sheet!A1) - its cached
  value may be the only copy of that data when the linked file is
  absent. Skipped and NAMED in the report.
- REFUSED: a cell carrying both <f> and an inline string (<is>) -
  not a shape this surgery claims to understand. Skipped and named.
- NEVER touched: value-only cells outside every array ref - that is
  literal data, and it is the entire safety boundary.
- calcChain.xml is deleted (a rebuild-on-demand index) along with its
  content-type override and relationship; calcPr gains
  fullCalcOnLoad="1" calcCompleted="0", and calcMode="manual" is
  normalized to "auto" because Excel IGNORES fullCalcOnLoad under
  manual mode (researched: XlsxWriter #91).

Scoping: without 'scope', the whole workbook. With it, a list of
entries, each naming sheet_names (a LIST) and at most ONE of cells /
columns / rows. An array anchor inside scope strips its whole spill;
an anchor outside scope leaves the spill untouched.

The completion report names exactly what was found and removed, per
sheet, including every refused cell (capped per class).
"""

import os
import re
import time
import shutil
import zipfile
import logging
import tempfile

from excel_recipe_processor.core.base_processor import (
    FileOpsBaseProcessor,
    StepProcessorError,
)
from excel_recipe_processor.core.config_schema import Key, Schema, name_list
from excel_recipe_processor.processors._helpers.strip_formula_caches_rgx import (
    calc_pr_rgx,
    cell_ref_attr_rgx,
    cell_vm_attr_rgx,
    attr_id_rgx,
    attr_rid_rgx,
    attr_name_rgx,
    attr_target_rgx,
    cell_element_rgx,
    inline_string_rgx,
    cell_type_attr_rgx,
    calc_chain_rel_rgx,
    cell_style_attr_rgx,
    value_element_rgx,
    formula_element_rgx,
    workbook_rel_element_rgx,
    workbook_sheet_element_rgx,
    array_formula_ref_rgx,
    calc_chain_override_rgx,
    external_workbook_ref_rgx,
)

from excel_recipe_processor.core.log_format import q

logger = logging.getLogger(__name__)

REPORT_NAME_CAP = 10
HEARTBEAT_SECONDS = 5.0


def column_letters_to_number(letters: str) -> int:
    """A -> 1, Z -> 26, AA -> 27 - Excel column arithmetic."""
    number = 0
    for character in letters:
        number = number * 26 + (ord(character.upper()) - ord('A') + 1)
    return number


class ScopeMatcher:
    """Decides whether a sheet/cell falls inside the configured scope.

    Without scope entries everything matches. Each entry names
    sheet_names (a LIST, per the one-spelling doctrine) plus at most
    ONE of cells / columns / rows; an entry with only sheet_names
    covers that whole sheet.
    """

    def __init__(self, scope_entries, step_name: str):
        self.match_all = scope_entries is None
        self.entries = []
        if self.match_all:
            return
        if not isinstance(scope_entries, list) or not scope_entries:
            raise StepProcessorError(
                f"Step '{step_name}': 'scope' must be a non-empty list "
                f"of entries, each with sheet_names and at most one of "
                f"cells / columns / rows"
            )
        for position, entry in enumerate(scope_entries, start=1):
            if not isinstance(entry, dict):
                raise StepProcessorError(
                    f"Step '{step_name}': scope entry {position} must "
                    f"be a mapping, got {type(entry).__name__}"
                )
            sheet_names = entry.get('sheet_names')
            if isinstance(sheet_names, str) or not sheet_names:
                raise StepProcessorError(
                    f"Step '{step_name}': scope entry {position} needs "
                    f"'sheet_names' as a non-empty LIST, even for one "
                    f"sheet"
                )
            restrictions = [key for key in ('cells', 'columns', 'rows')
                            if key in entry]
            if len(restrictions) > 1:
                raise StepProcessorError(
                    f"Step '{step_name}': scope entry {position} gives "
                    f"{restrictions} together - each entry takes at "
                    f"most ONE of cells / columns / rows (use separate "
                    f"entries to combine)"
                )
            unknown = set(entry) - {'sheet_names', 'cells', 'columns', 'rows'}
            if unknown:
                raise StepProcessorError(
                    f"Step '{step_name}': scope entry {position} has "
                    f"unknown key(s): {sorted(unknown)}"
                )
            self.entries.append(self._compile_entry(entry, position, step_name))

    def _compile_entry(self, entry, position, step_name):
        compiled = {'sheets': set(entry['sheet_names'])}
        if 'cells' in entry:
            compiled['cells'] = [self._parse_range(spec, position, step_name)
                                 for spec in entry['cells']]
        if 'columns' in entry:
            spans = []
            for spec in entry['columns']:
                parts = str(spec).split(':')
                first = column_letters_to_number(parts[0])
                last = column_letters_to_number(parts[-1])
                spans.append((min(first, last), max(first, last)))
            compiled['columns'] = spans
        if 'rows' in entry:
            spans = []
            for spec in entry['rows']:
                parts = str(spec).split(':')
                first, last = int(parts[0]), int(parts[-1])
                spans.append((min(first, last), max(first, last)))
            compiled['rows'] = spans
        return compiled

    @staticmethod
    def _parse_range(spec, position, step_name):
        parts = str(spec).replace('$', '').split(':')
        cells = []
        for part in parts:
            match = cell_ref_attr_rgx.match(f'r="{part}"')
            if not match:
                raise StepProcessorError(
                    f"Step '{step_name}': scope entry {position} cell "
                    f"spec {spec!r} is not a cell or range reference"
                )
            cells.append((column_letters_to_number(match.group(1)),
                          int(match.group(2))))
        (col_a, row_a), (col_b, row_b) = cells[0], cells[-1]
        return (min(col_a, col_b), min(row_a, row_b),
                max(col_a, col_b), max(row_a, row_b))

    def sheet_in_scope(self, sheet_name: str) -> bool:
        return self.match_all or any(sheet_name in entry['sheets']
                                     for entry in self.entries)

    def cell_in_scope(self, sheet_name: str, column: int, row: int) -> bool:
        if self.match_all:
            return True
        for entry in self.entries:
            if sheet_name not in entry['sheets']:
                continue
            if 'cells' in entry:
                if any(c1 <= column <= c2 and r1 <= row <= r2
                       for c1, r1, c2, r2 in entry['cells']):
                    return True
            elif 'columns' in entry:
                if any(first <= column <= last
                       for first, last in entry['columns']):
                    return True
            elif 'rows' in entry:
                if any(first <= row <= last for first, last in entry['rows']):
                    return True
            else:
                return True
        return False


class StripFormulaCachesProcessor(FileOpsBaseProcessor):
    """Remove cached formula results from closed xlsx files, in place."""

    @classmethod
    def config_schema(cls) -> Schema:
        """Declared keys (2026-09-03); see core/config_schema.py."""
        return Schema([
            Key('files', 'list', item_kind='str', required=True),
            Key('create_backup', 'bool', default=True),
            Key('scope', 'list_of_mappings', schema=Schema([
                Key('sheet_names', 'list', item_kind='str', required=True),
                Key('cells', 'any'), Key('columns', 'any'), Key('rows', 'any'),
            ]), description='Sheets (and at most one of cells/columns/rows) to strip; absent = whole workbook'),
        ])

    @classmethod
    def get_minimal_config(cls):
        return {
            'files': ['some_workbook.xlsx'],
        }

    def __init__(self, step_config: dict):
        super().__init__(step_config)
        self.files = self.get_config_value('files', None)
        if not self.files or not isinstance(self.files, list):
            raise StepProcessorError(
                f"Step '{self.step_name}' requires 'files': a non-empty "
                f"list of xlsx paths"
            )
        self.create_backup = self.get_config_value('create_backup', True)
        self.scope = ScopeMatcher(self.get_config_value('scope', None),
                                  self.step_name)

    def perform_file_operation(self):
        reports = []
        for path_template in self.files:
            resolved = self._substitute(path_template)
            reports.append(self._strip_file(resolved))
        return '; '.join(reports)

    def _substitute(self, value: str) -> str:
        if hasattr(self, 'variable_substitution') and self.variable_substitution:
            return self.variable_substitution.substitute(value)
        return value

    # ---- per-file surgery -------------------------------------------------

    def _strip_file(self, path: str) -> str:
        if not os.path.isfile(path):
            raise StepProcessorError(
                f"Step '{self.step_name}': file not found: {path}"
            )
        before_bytes = os.path.getsize(path)

        # Backup FIRST, before any read or analysis (ruling,
        # 2026-08-17): whatever happens after this line, the original
        # bytes already exist twice.
        if self.create_backup:
            shutil.copy2(path, path + '.stripbak')
            logger.info(f"Backup written: {q(str(path) + '.stripbak')}")

        logger.info(f"Reading '{os.path.basename(path)}' "
                    f"({before_bytes:,} bytes)...")
        with zipfile.ZipFile(path, 'r') as archive:
            names = archive.namelist()
            contents = {name: archive.read(name) for name in names}

        sheet_map = self._map_sheet_names(contents)
        totals = {'stripped': 0, 'spill_removed': 0, 'spill_blanked': 0,
                  'external_skipped': [], 'inline_skipped': [],
                  'sheets': []}

        for sheet_name, part_name in sheet_map.items():
            if part_name not in contents:
                continue
            if not self.scope.sheet_in_scope(sheet_name):
                continue
            sheet_xml = contents[part_name].decode('utf-8')
            cell_count = sheet_xml.count('<c ')
            logger.info(f"  [{sheet_name}] scanning {cell_count:,} cell(s)...")
            new_xml, counts = self._strip_sheet(sheet_xml, sheet_name)
            contents[part_name] = new_xml.encode('utf-8')
            if any((counts['stripped'], counts['spill_removed'],
                    counts['spill_blanked'], counts['external_skipped'],
                    counts['inline_skipped'])):
                totals['sheets'].append((sheet_name, counts))
                totals['stripped'] += counts['stripped']
                totals['spill_removed'] += counts['spill_removed']
                totals['spill_blanked'] += counts['spill_blanked']
                totals['external_skipped'] += [
                    f"{sheet_name}!{ref}" for ref in counts['external_skipped']]
                totals['inline_skipped'] += [
                    f"{sheet_name}!{ref}" for ref in counts['inline_skipped']]

        contents['xl/workbook.xml'] = self._force_recalc_flags(
            contents['xl/workbook.xml'].decode('utf-8')).encode('utf-8')

        removed_calc_chain = False
        if 'xl/calcChain.xml' in contents:
            del contents['xl/calcChain.xml']
            removed_calc_chain = True
            ct = contents['[Content_Types].xml'].decode('utf-8')
            contents['[Content_Types].xml'] = calc_chain_override_rgx.sub(
                '', ct).encode('utf-8')
            rels_name = 'xl/_rels/workbook.xml.rels'
            if rels_name in contents:
                rels = contents[rels_name].decode('utf-8')
                contents[rels_name] = calc_chain_rel_rgx.sub(
                    '', rels).encode('utf-8')

        directory = os.path.dirname(os.path.abspath(path))
        handle, temp_path = tempfile.mkstemp(dir=directory, suffix='.tmp')
        os.close(handle)
        try:
            # compresslevel 9: measured 6.5% smaller than the default
            # on the production workbook, for milliseconds of CPU - a
            # file being archived should be packed like one.
            with zipfile.ZipFile(temp_path, 'w', zipfile.ZIP_DEFLATED,
                                 compresslevel=9) as out:
                for name, data in contents.items():
                    out.writestr(name, data)
            os.replace(temp_path, path)
        except Exception:
            if os.path.exists(temp_path):
                os.remove(temp_path)
            raise

        after_bytes = os.path.getsize(path)
        self._log_report(path, totals, removed_calc_chain,
                         before_bytes, after_bytes)
        return (f"{os.path.basename(path)}: {totals['stripped']} cache(s) "
                f"stripped, {totals['spill_removed']} spill cell(s) removed, "
                f"{before_bytes:,} -> {after_bytes:,} bytes")

    def _map_sheet_names(self, contents) -> dict:
        workbook_xml = contents['xl/workbook.xml'].decode('utf-8')
        rels_xml = contents.get('xl/_rels/workbook.xml.rels', b'').decode('utf-8')
        rel_targets = {}
        for element in workbook_rel_element_rgx.findall(rels_xml):
            rel_id = attr_id_rgx.search(element)
            target = attr_target_rgx.search(element)
            if rel_id and target:
                rel_targets[rel_id.group(1)] = target.group(1)
        mapping = {}
        for element in workbook_sheet_element_rgx.findall(workbook_xml):
            sheet_name = attr_name_rgx.search(element)
            rel_id = attr_rid_rgx.search(element)
            if not sheet_name or not rel_id:
                continue
            target = rel_targets.get(rel_id.group(1), '')
            if target:
                mapping[sheet_name.group(1)] = (
                    'xl/' + target.lstrip('/').removeprefix('xl/'))
        return mapping

    def _strip_sheet(self, xml: str, sheet_name: str):
        counts = {'stripped': 0, 'spill_removed': 0, 'spill_blanked': 0,
                  'external_skipped': [], 'inline_skipped': []}

        # Pass 1: collect in-scope array anchor extents. Gated on a
        # substring check first - most sheets have no array formulas,
        # and scanning three-quarters of a million cells to learn that
        # is the difference between milliseconds and seconds
        # (production-scale lesson, 2026-08-17).
        spill_spans = []
        if 't="array"' in xml:
            for match in cell_element_rgx.finditer(xml):
                body = match.group('body') or ''
                ref_match = array_formula_ref_rgx.search(body)
                if not ref_match:
                    continue
                anchor_ref = cell_ref_attr_rgx.search(match.group('attrs'))
                if not anchor_ref:
                    continue
                anchor_col = column_letters_to_number(anchor_ref.group(1))
                anchor_row = int(anchor_ref.group(2))
                if not self.scope.cell_in_scope(sheet_name, anchor_col,
                                                anchor_row):
                    continue
                first = ref_match.group(1)
                last = ref_match.group(2) or first
                # SINGLE-CELL array refs have no member cells and need
                # no span entry. This is not an edge case: Excel, on
                # resave, rewrites every cm-declared dynamic formula as
                # a single-cell array (<f t="array" ref="AV2:AV2">) -
                # 67,726 of them on the production VMS sheet, which
                # turned the member lookup into an O(anchors x cells)
                # scan and a 500x field slowdown (2026-08-17, found by
                # heartbeat rate + cProfile on the real file).
                if first == last:
                    continue
                fm = cell_ref_attr_rgx.match(f'r="{first}"')
                lm = cell_ref_attr_rgx.match(f'r="{last}"')
                spill_spans.append((
                    column_letters_to_number(fm.group(1)), int(fm.group(2)),
                    column_letters_to_number(lm.group(1)), int(lm.group(2)),
                    (anchor_col, anchor_row),
                ))

        def in_spill(column, row):
            for c1, r1, c2, r2, anchor in spill_spans:
                if c1 <= column <= c2 and r1 <= row <= r2:
                    return anchor
            return None

        unrestricted = self.scope.match_all and not spill_spans

        def cell_label_of(attrs):
            ref = cell_ref_attr_rgx.search(attrs)
            return f'{ref.group(1)}{ref.group(2)}' if ref else '?'

        def rewrite(match):
            body = match.group('body')
            # Fast bail for the overwhelming majority: a cell with no
            # formula, on a sheet slice with no spill spans, cannot be
            # touched - return it before any reference parsing. On a
            # production-scale sheet (745k cells, 68k formulas) this
            # single check removes ~90% of the callback cost.
            if (not body or '<f' not in body) and not spill_spans:
                return match.group(0)
            attrs = match.group('attrs')

            formula = body and formula_element_rgx.search(body)
            if formula:
                # Whole-workbook runs (the common case) need no cell
                # address at all unless refusing - parse lazily.
                if not unrestricted:
                    ref = cell_ref_attr_rgx.search(attrs)
                    if not ref:
                        return match.group(0)
                    column = column_letters_to_number(ref.group(1))
                    row = int(ref.group(2))
                    if not self.scope.cell_in_scope(sheet_name, column, row):
                        return match.group(0)
                if external_workbook_ref_rgx.search(formula.group(0)):
                    counts['external_skipped'].append(cell_label_of(attrs))
                    return match.group(0)
                if inline_string_rgx.search(body):
                    counts['inline_skipped'].append(cell_label_of(attrs))
                    return match.group(0)
                if '<v' not in body:
                    return match.group(0)
                new_body = value_element_rgx.sub('', body)
                new_attrs = cell_type_attr_rgx.sub('', attrs)
                counts['stripped'] += 1
                return f'<c{new_attrs}>{new_body}</c>'

            # Value-only cell: touch ONLY inside an in-scope spill.
            ref = cell_ref_attr_rgx.search(attrs)
            if not ref:
                return match.group(0)
            column = column_letters_to_number(ref.group(1))
            row = int(ref.group(2))
            anchor = in_spill(column, row)
            if anchor is None or (column, row) == anchor:
                return match.group(0)
            if cell_style_attr_rgx.search(attrs):
                new_attrs = cell_type_attr_rgx.sub('', attrs)
                new_attrs = cell_vm_attr_rgx.sub('', new_attrs)
                counts['spill_blanked'] += 1
                return f'<c{new_attrs}/>'
            counts['spill_removed'] += 1
            return ''

        # Row-chunked processing with a heartbeat (2026-08-17 field
        # ruling): a long-running strip must issue running commentary
        # every few seconds - and if any row range ever behaves
        # pathologically, the heartbeat pinpoints WHERE. Cells never
        # span <row> elements, so splitting on row boundaries is
        # surgery-neutral; the preamble and tail segments carry no
        # cells and pass through the same sub as a no-op.
        segments = re.split(r'(?=<row[ >])', xml)
        total_cells = xml.count('<c ')
        done_cells = 0
        started = time.monotonic()
        last_beat = started
        rebuilt = []
        for segment in segments:
            rebuilt.append(cell_element_rgx.sub(rewrite, segment))
            done_cells += segment.count('<c ')
            now = time.monotonic()
            if now - last_beat >= HEARTBEAT_SECONDS:
                last_beat = now
                elapsed = now - started
                rate = done_cells / elapsed if elapsed > 0 else 0
                remaining = ((total_cells - done_cells) / rate
                             if rate > 0 else 0)
                logger.info(
                    f"  [{sheet_name}] {done_cells:,}/{total_cells:,} "
                    f"cell(s), {rate:,.0f}/s, ~{remaining:,.0f}s left...")
        elapsed = time.monotonic() - started
        if elapsed >= HEARTBEAT_SECONDS:
            logger.info(
                f"  [{sheet_name}] done: {total_cells:,} cell(s) in "
                f"{elapsed:,.1f}s")
        return ''.join(rebuilt), counts

    def _force_recalc_flags(self, workbook_xml: str) -> str:
        def rebuild(match):
            element = match.group(0)
            element = element.replace('calcMode="manual"', 'calcMode="auto"')
            for attribute in ('fullCalcOnLoad', 'calcCompleted'):
                element = re.sub(attribute + r'="[^"]*"\s*', '', element)
            insertion = ' fullCalcOnLoad="1" calcCompleted="0"'
            if element.endswith('/>'):
                return element[:-2] + insertion + '/>'
            return element.replace('>', insertion + '>', 1)

        if calc_pr_rgx.search(workbook_xml):
            return calc_pr_rgx.sub(rebuild, workbook_xml, count=1)
        return workbook_xml.replace(
            '</workbook>',
            '<calcPr fullCalcOnLoad="1" calcCompleted="0"/></workbook>')

    def _log_report(self, path, totals, removed_calc_chain, before, after):
        logger.info(f"Stripped formula caches in '{path}':")
        for sheet_name, counts in totals['sheets']:
            parts = []
            if counts['stripped']:
                parts.append(f"{counts['stripped']} formula cache(s)")
            if counts['spill_removed']:
                parts.append(f"{counts['spill_removed']} spill cell(s) removed")
            if counts['spill_blanked']:
                parts.append(f"{counts['spill_blanked']} spill cell(s) "
                             f"blanked (style kept)")
            logger.info(f"  [{sheet_name}] " + ', '.join(parts))
        for label, skipped in (('external-workbook reference',
                                totals['external_skipped']),
                               ('inline-string oddity',
                                totals['inline_skipped'])):
            if skipped:
                shown = ', '.join(skipped[:REPORT_NAME_CAP])
                more = (f" (+{len(skipped) - REPORT_NAME_CAP} more)"
                        if len(skipped) > REPORT_NAME_CAP else '')
                logger.info(
                    f"  REFUSED ({label} - cached value may be the only "
                    f"copy of the data): {shown}{more}")
        if removed_calc_chain:
            logger.info("  calcChain.xml removed (rebuilds on demand)")
        logger.info(
            f"  calcPr: fullCalcOnLoad set; Excel recalculates on next open")
        saved = before - after
        logger.info(
            f"  {before:,} -> {after:,} bytes ({saved:+,} = "
            f"{100.0 * saved / before:.1f}% reclaimed)")

    def get_usage_examples(self) -> dict:
        from excel_recipe_processor.utils.processor_examples_loader import (
            load_processor_examples)
        return load_processor_examples('strip_formula_caches')

# End of file #
