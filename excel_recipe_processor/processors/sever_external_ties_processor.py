"""
Sever every external tie in a closed xlsx file, writing a NEW file.

excel_recipe_processor/processors/sever_external_ties_processor.py

Excel's Break Links freezes [N] cell formulas to their cached values
and drops the link parts; it cannot retarget a reference to a sheet
that already exists locally (the copied-tab case), cannot see links
in defined names, conditional formatting, data validation or charts,
and offers no inventory of what it did. This step does all of that
against a COPY: the source file is read once and never written, the
result lands beside it as <stem><suffix>_<timestamp>.xlsx, and a
report names every fix, every refusal and every limbo item.

write_mode in_place is the explicit opt-in (2026-09-14): the result
is written to a temp file beside the source, verified, the source is
copied to <name>.severbak, and the temp atomically replaces the
source. Refused when Excel has the file open or a .severbak already
exists. Both modes prove, byte for byte, that every part the surgery
did not claim to touch is unchanged (see _helpers/xlsx_package_write).

Resolution per carrier: retarget when every referenced sheet (or
name) exists locally, else the carrier's unresolved policy - freeze
for formulas, drop for defined names / CF / DV rules, or refuse.
ALL-OR-NOTHING: the link plumbing comes out only when zero [N]
survives; any refusal means no output file at all (the report is
still written). See _helpers/external_ties_sever.py for the surgery
doctrine.

Before the result is named anything, it is re-inventoried and must
come back free of links, [N] carriers and orphans, or the temp is
deleted and the step fails loud.
"""

import os
import json
import time
import logging

from pathlib import Path
from datetime import datetime

from excel_recipe_processor.core.log_format import q
from excel_recipe_processor.core.config_schema import Key, Schema
from excel_recipe_processor.core.base_processor import (
    FileOpsBaseProcessor,
    StepProcessorError,
)
from excel_recipe_processor.processors._helpers.external_ties_sever import (
    SeverPlan,
    POLICY_DROP,
    POLICY_FREEZE,
    FORMULA_POLICIES,
    DROPPABLE_POLICIES,
    validate_policies,
)
from excel_recipe_processor.processors._helpers.xlsx_package_write import (
    PackageWriteError,
    refuse_if_open_in_excel,
)
from excel_recipe_processor.processors._helpers.external_ties_inventory import (
    inventory_external_ties,
)


logger = logging.getLogger(__name__)

REPORT_NAME_CAP = 10
HEARTBEAT_SECONDS = 5.0
DEFAULT_SUFFIX = '_severed'
BACKUP_SUFFIX = '.severbak'
WRITE_MODE_NEW_FILE = 'new_file'
WRITE_MODE_IN_PLACE = 'in_place'
WRITE_MODES = (WRITE_MODE_NEW_FILE, WRITE_MODE_IN_PLACE)
NEW_FILE_ONLY_KEYS = ('output_dir', 'output_suffix', 'timestamp_format')
DEFAULT_TIMESTAMP_FORMAT = '%y%m%d_%H%M%S'

# Sections of the post-write inventory that must be empty for the
# output to count as severed. Limbo sections are excluded on purpose.
MUST_BE_CLEAN = ('external_links', 'formula_refs', 'defined_names',
                 'sheet_features', 'chart_refs')


class SeverExternalTiesProcessor(FileOpsBaseProcessor):
    """Retarget, freeze or drop every [N] carrier; write a new file."""

    @classmethod
    def config_schema(cls) -> Schema:
        """Declared keys (2026-09-14); see core/config_schema.py."""
        return Schema([
            Key('files', 'list', item_kind='str', required=True,
                description='xlsx paths to sever; never modified'),
            Key('output_dir', 'str',
                description='Directory for the new files; default beside each source'),
            Key('output_suffix', 'str', default=DEFAULT_SUFFIX,
                description='Appended to the stem before the timestamp'),
            Key('timestamp_format', 'str', default=DEFAULT_TIMESTAMP_FORMAT,
                description='strftime format for the output timestamp'),
            Key('retarget_if_possible', 'bool', default=True,
                description='Rewrite [N]Sheet! to a local Sheet! when that sheet exists here'),
            Key('unresolved_formula_policy', 'str', default=POLICY_FREEZE,
                choices=list(FORMULA_POLICIES),
                description='Formula cells that do not retarget: freeze to the cached value, or refuse'),
            Key('unresolved_defined_name_policy', 'str', default=POLICY_DROP,
                choices=list(DROPPABLE_POLICIES),
                description='Defined names that do not retarget: drop the name, or refuse'),
            Key('unresolved_conditional_formatting_policy', 'str', default=POLICY_DROP,
                choices=list(DROPPABLE_POLICIES),
                description='CF rules that do not retarget: drop the rule, or refuse'),
            Key('unresolved_data_validation_policy', 'str', default=POLICY_DROP,
                choices=list(DROPPABLE_POLICIES),
                description='DV rules that do not retarget: drop the rule, or refuse'),
            Key('write_mode', 'str', default=WRITE_MODE_NEW_FILE,
                choices=list(WRITE_MODES),
                description='new_file (default): timestamped copy beside the source; '
                            'in_place: temp + verify + .severbak backup + atomic replace'),
            Key('verify_with_openpyxl', 'bool', default=False,
                description='Also load the result with openpyxl before accepting it (slow on large files)'),
            Key('fail_on_limbo', 'bool', default=False,
                description='Halt when file hyperlinks, connections, query tables or OLE objects remain'),
            Key('report_file', 'str',
                description='Optional path for the JSON report of fixes, refusals and limbo'),
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
        self.output_dir = self.get_config_value('output_dir', None)
        self.output_suffix = self.get_config_value('output_suffix', DEFAULT_SUFFIX)
        self.timestamp_format = self.get_config_value(
            'timestamp_format', DEFAULT_TIMESTAMP_FORMAT)
        self.retarget_if_possible = bool(
            self.get_config_value('retarget_if_possible', True))
        self.policies = {
            'formula': self.get_config_value('unresolved_formula_policy', POLICY_FREEZE),
            'defined_name': self.get_config_value(
                'unresolved_defined_name_policy', POLICY_DROP),
            'conditional_formatting': self.get_config_value(
                'unresolved_conditional_formatting_policy', POLICY_DROP),
            'data_validation': self.get_config_value(
                'unresolved_data_validation_policy', POLICY_DROP),
        }
        validate_policies(self.policies, self.step_name, StepProcessorError)
        self.write_mode = self.get_config_value('write_mode', WRITE_MODE_NEW_FILE)
        if self.write_mode not in WRITE_MODES:
            raise StepProcessorError(
                f"Step '{self.step_name}': write_mode must be one of "
                f"{list(WRITE_MODES)}, got {self.write_mode!r}"
            )
        if self.write_mode == WRITE_MODE_IN_PLACE:
            named = [key for key in NEW_FILE_ONLY_KEYS if key in step_config]
            if named:
                raise StepProcessorError(
                    f"Step '{self.step_name}': {named} only apply to "
                    f"write_mode new_file; in_place writes over the source"
                )
        self.verify_with_openpyxl = bool(self.get_config_value('verify_with_openpyxl', False))
        self.fail_on_limbo = bool(self.get_config_value('fail_on_limbo', False))
        self.report_file = self.get_config_value('report_file', None)
        self.name_cap = self.get_config_value('name_cap', REPORT_NAME_CAP)
        if not isinstance(self.name_cap, int) or self.name_cap < 1:
            raise StepProcessorError(
                f"Step '{self.step_name}': 'name_cap' must be a positive integer"
            )
        if not isinstance(self.output_suffix, str) or '/' in self.output_suffix:
            raise StepProcessorError(
                f"Step '{self.step_name}': 'output_suffix' must be a plain "
                f"filename fragment"
            )

    def perform_file_operation(self):
        reports = []
        failures = []
        for path_template in self.files:
            resolved = self._substitute(path_template)
            report = self._sever_file(resolved)
            reports.append(report)
            if report['refused']:
                failures.append(Path(resolved).name)
            elif self.fail_on_limbo and report['limbo']:
                failures.append(Path(resolved).name + ' (limbo)')

        if self.report_file:
            self._write_report(self._substitute(self.report_file), reports)

        if failures:
            raise StepProcessorError(
                f"Step '{self.step_name}': no output written for "
                f"{len(failures)} file(s) - {', '.join(failures)}; see the "
                f"refusals in the log{' and report' if self.report_file else ''}"
            )
        written = [Path(report['output']).name for report in reports if report['output']]
        mode = 'in place' if self.write_mode == WRITE_MODE_IN_PLACE else 'to new files'
        return f"severed {len(written)} of {len(reports)} file(s) {mode}: {', '.join(written)}"

    def _substitute(self, value: str) -> str:
        if hasattr(self, 'variable_substitution') and self.variable_substitution:
            return self.variable_substitution.substitute(value)
        return value

    # ---- per-file ---------------------------------------------------------------

    def _output_path_for(self, source: str) -> str:
        source_path = Path(source)
        directory = Path(self._substitute(self.output_dir)) if self.output_dir \
            else source_path.parent
        if not directory.is_dir():
            raise StepProcessorError(
                f"Step '{self.step_name}': output directory does not exist: {directory}"
            )
        stamp = datetime.now().strftime(self.timestamp_format)
        output = directory / f"{source_path.stem}{self.output_suffix}_{stamp}{source_path.suffix}"
        if output.exists():
            raise StepProcessorError(
                f"Step '{self.step_name}': output already exists: {output}"
            )
        return str(output)

    def _sever_file(self, source: str) -> dict:
        if not os.path.isfile(source):
            raise StepProcessorError(
                f"Step '{self.step_name}': file not found: {source}"
            )
        name = Path(source).name
        in_place = self.write_mode == WRITE_MODE_IN_PLACE
        if in_place:
            try:
                refuse_if_open_in_excel(source)
            except PackageWriteError as error:
                raise StepProcessorError(f"Step '{self.step_name}': {error}")
            output = source
            logger.info(f"✂️  Severing external ties in {q(name)} IN PLACE "
                        f"(backup {q(name + BACKUP_SUFFIX)})")
        else:
            output = self._output_path_for(source)
            logger.info(f"✂️  Severing external ties in {q(name)} -> {q(Path(output).name)}")
        started = time.perf_counter()
        last_beat = [started]

        def heartbeat(position: int, count: int, sheet_name: str):
            now = time.perf_counter()
            if now - last_beat[0] >= HEARTBEAT_SECONDS:
                logger.info(f"   ... sheet {position}/{count} {q(sheet_name)} "
                            f"({now - started:.1f}s elapsed)")
                last_beat[0] = now

        plan = SeverPlan(source, self.policies, self.retarget_if_possible)
        if not plan.before['summary']['has_ties']:
            logger.info(f"   {q(name)} has no external ties; writing an "
                        f"unchanged copy would be pointless - skipping")
            return {'source': source, 'output': '', 'fixed': plan.fixed,
                    'refused': [], 'limbo': [], 'plumbing_removed': {},
                    'before_summary': plan.before['summary'],
                    'after_summary': plan.before['summary'], 'skipped': True}

        report = plan.run(heartbeat)
        report['output'] = ''
        self._log_report(report, name)

        if report['refused']:
            logger.error(f"❌ {q(name)}: {len(report['refused'])} refusal(s) - "
                         f"no output written")
            report['after_summary'] = {}
            return report

        def verify_clean(temp_path: str) -> None:
            after = inventory_external_ties(temp_path)
            report['after_summary'] = after['summary']
            dirty = [section for section in MUST_BE_CLEAN if after[section]]
            orphans = after['orphans']
            if dirty or orphans['unresolved_indexes'] or orphans['dangling_parts']:
                raise PackageWriteError(
                    f"post-write inventory still shows {dirty or orphans}")

        try:
            checks = plan.write(output, in_place, BACKUP_SUFFIX,
                                verify_output=verify_clean,
                                verify_with_openpyxl=self.verify_with_openpyxl)
        except PackageWriteError as error:
            raise StepProcessorError(
                f"Step '{self.step_name}': verification failed for {name}: "
                f"{error}; nothing was written or replaced"
            )
        report['output'] = output
        report['write_checks'] = checks
        where = 'in place' if in_place else q(Path(output).name)
        logger.info(f"✅ {where} verified: {checks['parts_untouched_verified']} "
                    f"untouched part(s) byte-identical, "
                    f"{len(checks['parts_changed'])} changed, "
                    f"{len(checks['parts_removed'])} removed "
                    f"({os.path.getsize(output) / 1e6:.2f} MB) in "
                    f"{time.perf_counter() - started:.2f}s")
        if in_place:
            logger.info(f"   backup: {q(checks['backup'])}")
        return report

    def _write_report(self, report_path: str, reports: list) -> None:
        parent = Path(report_path).parent
        if not parent.is_dir():
            raise StepProcessorError(
                f"Step '{self.step_name}': report_file directory does not "
                f"exist: {parent}"
            )
        payload = reports[0] if len(reports) == 1 else reports
        with open(report_path, 'w', encoding='utf-8') as handle:
            json.dump(payload, handle, indent=2, sort_keys=True, ensure_ascii=False)
            handle.write('\n')
        logger.info(f"📝 Wrote sever report to {q(report_path)}")

    # ---- reporting ----------------------------------------------------------------

    def _named(self, labels: list) -> str:
        shown = labels[:self.name_cap]
        extra = len(labels) - len(shown)
        return ', '.join(shown) + (f" (+{extra} more)" if extra > 0 else '')

    def _log_report(self, report: dict, name: str) -> None:
        for action in ('retargeted', 'frozen', 'dropped'):
            entries = report['fixed'][action]
            if not entries:
                continue
            by_kind = {}
            for entry in entries:
                by_kind.setdefault(entry['kind'], []).append(entry['where'])
            for kind, labels in sorted(by_kind.items()):
                logger.info(f"   {action} {kind}: {len(labels)} - " + self._named(labels))
        plumbing = report['plumbing_removed']
        if plumbing:
            logger.info(f"   plumbing removed: {len(plumbing['parts'])} part(s), "
                        f"{plumbing['rels']} rel(s), {plumbing['overrides']} "
                        f"content-type override(s); fullCalcOnLoad set")
        for entry in report['refused'][:self.name_cap]:
            logger.error(f"   REFUSED {entry['kind']} at {entry['where']}: "
                         f"{entry['reason']} - {entry['text']}")
        if len(report['refused']) > self.name_cap:
            logger.error(f"   (+{len(report['refused']) - self.name_cap} more refusals)")
        if report['limbo']:
            by_kind = {}
            for entry in report['limbo']:
                by_kind[entry['kind']] = by_kind.get(entry['kind'], 0) + 1
            logger.warning("   limbo (left in place): " + ', '.join(
                f"{kind} {count}" for kind, count in sorted(by_kind.items())))

    def get_capabilities(self) -> dict:
        return {
            'description': 'Sever every external tie in closed xlsx files, writing a new timestamped file',
            'resolution': [
                'retarget [N]Sheet! to a local sheet of the same name when it exists',
                'freeze unresolved formulas to cached values, or refuse',
                'drop unresolved defined names, CF rules and DV rules, or refuse',
                'remove externalReferences, link parts, rels, overrides and calcChain; force recalc',
            ],
            'safety': [
                'source file never written; output is a new file beside it',
                'all-or-nothing: any refusal means no output, report still written',
                'post-write re-inventory must be clean or the output is deleted',
            ],
        }

# End of file #
