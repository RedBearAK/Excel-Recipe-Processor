"""
Audit every tie a closed xlsx file has to other files. Read-only.

excel_recipe_processor/processors/audit_external_ties_processor.py

Excel's Break Links shows one list of linked workbooks and offers one
verb. It does not say WHERE the links live (cells, defined names,
conditional formatting, data validation, charts, pivot caches), cannot
see phantom references, and cannot tell a reference that could be
retargeted to a local sheet from one that can only be frozen. This
step produces that inventory and nothing else: the file is opened with
zipfile in read mode, so by construction it cannot change a byte.

The inventory is the shared substrate for the sever/repair surgery and
the tab-transplant pre-flight (2026-09-14 design). Building the audit
first means the later rewrite logic is written against categories
actually seen in real files rather than the whole OOXML spec.

What the completion log names, per file:
  - each external workbook by [N] index, target path and sheet list
  - every formula cell carrying [N] (capped per class), with whether
    the referenced sheet name exists LOCALLY - the retarget candidates
  - defined names, CF/DV features, chart series, file hyperlinks,
    connections, query tables, OLE objects
  - orphans: [N] with no reference behind it, and dangling link parts
  - pivots whose cache source is external
Optional: write the full inventory as sorted JSON (report_file), and
halt the recipe when any tie exists (fail_on_ties).
"""

import os
import time
import logging

from pathlib import Path

from excel_recipe_processor.core.log_format import q
from excel_recipe_processor.core.config_schema import Key, Schema
from excel_recipe_processor.core.base_processor import (
    FileOpsBaseProcessor,
    StepProcessorError,
)
from excel_recipe_processor.processors._helpers.external_ties_inventory import (
    inventory_to_json,
    inventory_external_ties,
    ExternalTiesInventoryError,
)


logger = logging.getLogger(__name__)

REPORT_NAME_CAP = 10
HEARTBEAT_SECONDS = 5.0


class AuditExternalTiesProcessor(FileOpsBaseProcessor):
    """Inventory external ties in closed xlsx files without touching them."""

    @classmethod
    def config_schema(cls) -> Schema:
        """Declared keys (2026-09-14); see core/config_schema.py."""
        return Schema([
            Key('files', 'list', item_kind='str', required=True,
                description='xlsx paths to audit; never modified'),
            Key('report_file', 'str',
                description='Optional path for the full inventory as JSON'),
            Key('fail_on_ties', 'bool', default=False,
                description='Halt the recipe when any file has a tie'),
            Key('name_cap', 'int', default=REPORT_NAME_CAP,
                description='Max named items per class in the log'),
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
        self.report_file = self.get_config_value('report_file', None)
        self.fail_on_ties = bool(self.get_config_value('fail_on_ties', False))
        self.name_cap = self.get_config_value('name_cap', REPORT_NAME_CAP)
        if not isinstance(self.name_cap, int) or self.name_cap < 1:
            raise StepProcessorError(
                f"Step '{self.step_name}': 'name_cap' must be a positive integer"
            )

    def perform_file_operation(self):
        inventories = []
        for path_template in self.files:
            resolved = self._substitute(path_template)
            inventories.append(self._audit_file(resolved))

        if self.report_file:
            self._write_report(self._substitute(self.report_file), inventories)

        tied = [Path(inv['file']).name for inv in inventories
                if inv['summary']['has_ties']]
        if tied and self.fail_on_ties:
            raise StepProcessorError(
                f"Step '{self.step_name}': fail_on_ties is set and "
                f"{len(tied)} file(s) have external ties: {', '.join(tied)}"
            )
        return (f"audited {len(inventories)} file(s); "
                f"{len(tied)} with external ties")

    def _substitute(self, value: str) -> str:
        if hasattr(self, 'variable_substitution') and self.variable_substitution:
            return self.variable_substitution.substitute(value)
        return value

    # ---- per-file audit -------------------------------------------------------

    def _audit_file(self, path: str) -> dict:
        if not os.path.isfile(path):
            raise StepProcessorError(
                f"Step '{self.step_name}': file not found: {path}"
            )
        name = Path(path).name
        logger.info(f"🔎 Auditing external ties in {q(name)} (read-only)")
        started = time.perf_counter()
        last_beat = [started]

        def heartbeat(position: int, count: int, sheet_name: str):
            now = time.perf_counter()
            if now - last_beat[0] >= HEARTBEAT_SECONDS:
                logger.info(f"   ... sheet {position}/{count} {q(sheet_name)} "
                            f"({now - started:.1f}s elapsed)")
                last_beat[0] = now

        try:
            inventory = inventory_external_ties(path, heartbeat=heartbeat)
        except ExternalTiesInventoryError as error:
            raise StepProcessorError(
                f"Step '{self.step_name}': {error}"
            )

        self._log_inventory(inventory, name)
        logger.info(f"   done in {time.perf_counter() - started:.2f}s")
        return inventory

    def _write_report(self, report_path: str, inventories: list) -> None:
        parent = Path(report_path).parent
        if not parent.is_dir():
            raise StepProcessorError(
                f"Step '{self.step_name}': report_file directory does not "
                f"exist: {parent}"
            )
        payload = inventories[0] if len(inventories) == 1 else inventories
        with open(report_path, 'w', encoding='utf-8') as handle:
            handle.write(inventory_to_json(payload))
            handle.write('\n')
        logger.info(f"📝 Wrote inventory to {q(report_path)}")

    # ---- reporting -------------------------------------------------------------

    def _named(self, labels: list) -> str:
        shown = labels[:self.name_cap]
        extra = len(labels) - len(shown)
        text = ', '.join(shown)
        return text + (f" (+{extra} more)" if extra > 0 else '')

    def _log_inventory(self, inventory: dict, name: str) -> None:
        summary = inventory['summary']
        local_sheets = set(inventory['sheets'])

        if not summary['has_ties']:
            logger.info(f"✅ {q(name)}: no external ties "
                        f"({len(local_sheets)} sheets, "
                        f"{summary['pivots']} pivot tables, all local)")
            return

        logger.info(f"⚠️  {q(name)}: external ties found")

        for link in inventory['external_links']:
            cached = 'cached values' if link['has_cached_values'] else 'no cache'
            logger.info(f"   [{link['index']}] {link['kind']} -> "
                        f"{link['target'] or '(no target)'}; "
                        f"sheets {link['sheet_names'] or '[]'}; {cached}")

        refs = inventory['formula_refs']
        if refs:
            retargetable = [ref for ref in refs
                            if ref['ref_sheets']
                            and all(sheet in local_sheets for sheet in ref['ref_sheets'])]
            frozen_only = [ref for ref in refs if ref not in retargetable]
            logger.info(f"   formula cells with [N]: {len(refs)} "
                        f"({len(retargetable)} name a sheet that exists "
                        f"locally, {len(frozen_only)} do not)")
            if retargetable:
                logger.info("   retarget candidates: " + self._named(
                    [f"{r['sheet']}!{r['cell']}" for r in retargetable]))
            if frozen_only:
                without_cache = [r for r in frozen_only if not r['has_cached_value']]
                logger.info("   freeze-or-refuse: " + self._named(
                    [f"{r['sheet']}!{r['cell']}" for r in frozen_only]))
                if without_cache:
                    logger.info(f"   NOTE {len(without_cache)} of those carry "
                                f"no cached value - nothing to freeze to")

        names = inventory['defined_names']
        if names:
            logger.info(f"   defined names: {len(names)} - " + self._named(
                [f"{n['name']}{' (hidden)' if n['hidden'] else ''}"
                 f"{' [' + n['scope'] + ']' if n['scope'] else ''}"
                 for n in names]))

        features = inventory['sheet_features']
        if features:
            by_kind = {}
            for feature in features:
                by_kind.setdefault(feature['kind'], []).append(
                    f"{feature['sheet']}!{feature['sqref'] or '?'}")
            for kind, labels in sorted(by_kind.items()):
                marker = ' <- UNKNOWN CARRIER' if kind == 'unclassified' else ''
                logger.info(f"   {kind}: {len(labels)}{marker} - "
                            + self._named(labels))

        charts = inventory['chart_refs']
        if charts:
            logger.info(f"   chart series: {len(charts)} - " + self._named(
                [c['part'].rsplit('/', 1)[-1] for c in charts]))

        external_pivots = [p for p in inventory['pivots'] if p['external_rid']]
        if external_pivots:
            logger.info(f"   pivots with external source: "
                        f"{len(external_pivots)} - " + self._named(
                [f"{p['on_sheet']}!{p['location'] or '?'}" for p in external_pivots]))

        links = inventory['file_hyperlinks']
        if links:
            logger.info(f"   file hyperlinks: {len(links)} - " + self._named(
                [f"{l['sheet']}: {l['target']}" for l in links]))

        for label, key in (('connections', 'connections'),
                           ('query tables', 'query_tables'),
                           ('OLE objects', 'ole_objects')):
            if inventory[key]:
                logger.info(f"   {label}: {len(inventory[key])}")

        orphans = inventory['orphans']
        if orphans['unresolved_indexes']:
            logger.info(f"   ORPHAN indexes with no externalReference: "
                        f"{orphans['unresolved_indexes']}")
        if orphans['unused_indexes']:
            logger.info(f"   phantom links (declared, never referenced): "
                        f"{orphans['unused_indexes']}")
        if orphans['dangling_parts']:
            logger.info(f"   dangling link parts: {orphans['dangling_parts']}")

    def get_capabilities(self) -> dict:
        return {
            'description': 'Inventory every external tie in closed xlsx files, read-only',
            'finds': [
                'external workbook links by index with target and sheet list',
                'formula cells, defined names, CF/DV features and chart series carrying [N]',
                'pivot cache sources (local and external), file hyperlinks, connections, OLE',
                'orphan indexes, phantom links, dangling link parts',
            ],
            'safety': ['never writes to an audited file', 'optional JSON report'],
        }

# End of file #
