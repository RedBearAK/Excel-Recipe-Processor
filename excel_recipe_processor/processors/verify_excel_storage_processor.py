"""
Verify a workbook's stored formula grammar and declaration state.

excel_recipe_processor/processors/verify_excel_storage_processor.py

The whole-file audits that pinned this project's shipped-and-repaired
incident classes, promoted from the test suite into a recipe step so
every run can hold its own output to the same standard:

- STORED GRAMMAR (core/excel_storage_audit.audit_stored_grammar):
  forbidden leading '=' in definedNames/DV formulas, literal '#'
  outside strings, chained storage prefixes, _xlfn on names outside
  the validated future-function map, LAMBDA/LET declaration slots
  lacking _xlpm. - each one a real production incident before it was
  a check.
- LEGACY CSE (audit_legacy_cse): any t="array" cell missing its cm
  dynamic-array declaration - the Dom_View braces incident class,
  where a spill collapses to one value until hand-re-entered.

SESSION-AWARE, the part that makes a mid-recipe audit honest: the
final save happens AFTER all steps, so a file still in the workbook
session has stale bytes on disk. When a listed file is the in-flight
session workbook, the processor serializes it through THE SAME
declaration pipeline the run-end save uses - to an in-memory buffer,
no disk writes - and audits the bytes as they WILL be written. Files
not in the session (previous outputs, foreign workbooks) audit
straight from disk.

on_violation: 'halt' (default) fails the run with every violation
listed; 'warn' logs them and continues.
"""

import io
import logging

from excel_recipe_processor.core.base_processor import FileOpsBaseProcessor, StepProcessorError
from excel_recipe_processor.core.config_schema import Key, Schema, name_list
from excel_recipe_processor.core.workbook_session import WorkbookSession
from excel_recipe_processor.core.dynamic_array_metadata import declare_dynamic_formulas_in_zip
from excel_recipe_processor.core.excel_storage_audit import (
    audit_legacy_cse,
    audit_stored_grammar,
)


from excel_recipe_processor.core.log_format import q

logger = logging.getLogger(__name__)


class VerifyExcelStorageProcessor(FileOpsBaseProcessor):
    """Audit workbooks for storage-grammar and declaration violations."""

    @classmethod
    def config_schema(cls) -> Schema:
        """Declared keys (2026-09-03); see core/config_schema.py."""
        return Schema([
            Key('files', 'list', item_kind='str', required=True),
            Key('on_violation', 'str', default='error', choices=['error', 'warn']),
        ])

    @classmethod
    def get_minimal_config(cls):
        return {
            'files': ['some_workbook.xlsx'],
        }

    def __init__(self, step_config: dict):
        super().__init__(step_config)

        self.files = self.get_config_value('files', None)
        self.on_violation = self.get_config_value('on_violation', 'halt')

        if not isinstance(self.files, list) or len(self.files) == 0:
            raise StepProcessorError(
                f"Step '{self.step_name}': 'files' must be a non-empty "
                f"list of workbook paths"
            )
        if self.on_violation not in ('halt', 'warn'):
            raise StepProcessorError(
                f"Step '{self.step_name}': on_violation must be 'halt' or "
                f"'warn', got {self.on_violation!r}"
            )

    def get_capabilities(self) -> dict:
        """Processor capabilities information."""
        return {
            'description': "Audit workbooks' stored formula grammar and dynamic-array declarations",
            'checks': ['stored grammar (prefixes, declarations, forbidden forms)',
                       'legacy CSE (t="array" without cm)'],
            'session_aware': 'in-flight workbooks audit their WILL-BE-WRITTEN bytes via an in-memory serialize',
            'on_violation': ['halt (default)', 'warn'],
        }

    def perform_file_operation(self) -> str:
        """Audit every listed file; halt or warn per configuration."""
        all_violations = []
        for path in self.files:
            source, provenance = self._bytes_source(path)
            grammar = audit_stored_grammar(source)
            if hasattr(source, 'seek'):
                source.seek(0)
            legacy = [f"{member}!{ref}: legacy CSE (t=\"array\" without cm)"
                      for member, ref in audit_legacy_cse(source)]
            found = grammar + legacy
            if found:
                all_violations.extend(f"{path} [{provenance}] {item}"
                                      for item in found)
            else:
                logger.info(f"🔎 Storage audit CLEAN ({provenance}): {q(path)}")

        if not all_violations:
            return f"audited {len(self.files)} workbook(s): clean"

        listing = '\n  '.join(all_violations)
        if self.on_violation == 'warn':
            logger.warning(
                f"⚠️  Storage audit found {len(all_violations)} "
                f"violation(s):\n  {listing}")
            return f"audited {len(self.files)} workbook(s): " \
                   f"{len(all_violations)} violation(s), warn mode"
        raise StepProcessorError(
            f"Step '{self.step_name}': storage audit found "
            f"{len(all_violations)} violation(s):\n  {listing}"
        )

    def _bytes_source(self, path: str):
        """(bytes source, provenance label) for one listed file.

        Session workbooks serialize through the same declaration
        pipeline the run-end save uses, so the audit sees the bytes as
        they WILL be written; everything else reads from disk.
        """
        cached = WorkbookSession.peek_workbook(path)
        if cached is None:
            return str(path), 'disk'

        raw = io.BytesIO()
        cached.save(raw)
        raw.seek(0)
        declared = io.BytesIO()
        declare_dynamic_formulas_in_zip(
            raw, declared,
            injected_cells=WorkbookSession._injected_formula_ranges.get(
                WorkbookSession._key(path)))
        declared.seek(0)
        return declared, 'session, will-be-written bytes'

# End of file #
