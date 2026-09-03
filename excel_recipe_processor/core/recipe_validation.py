"""
Pre-execution validation of a loaded, variable-resolved recipe.

excel_recipe_processor/core/recipe_validation.py

Runs as the FIRST phase of every run, after variables resolve and before
any step touches data; --validate stops the run after this phase. One
code path, two exits, so what the flag checks is exactly what a real run
checks (2026-09-03).

Two checks:

1. Step schemas. Every step whose processor declares a schema is validated
   against it: unknown keys (with a nearest-name suggestion), wrong types,
   missing required keys, variant mismatches. Processors without a schema
   are validated for their family's stage keys only and reported once per
   processor type so the remaining count stays visible.

2. Stage graph, walked in step order. Errors:
   - a stage read before any earlier step wrote it
   - a stage written twice without confirm_stage_replacement
   - a declared stage that no step writes or no step reads (declarations
     exist to catch drift; an unused one IS drift)
   Warnings:
   - a stage used but not declared (allowed, as today)

Stage reads and writes come from the schema (stage_in / stage_out kinds)
when a processor has one. Until every processor does, a schema-less step
falls back to a fixed list of stage-bearing keys; that list is temporary
and shrinks as schemas land.

Exit policy: errors fail; warnings do not.
"""

import logging

from excel_recipe_processor.core.config_schema import (
    Key, Schema, stage_references, validate_config,
)


logger = logging.getLogger(__name__)


# Stage-bearing keys for processors that have no schema yet. TEMPORARY:
# every entry here is a processor whose schema has not landed. Nested
# forms: sheets_to_create[].data_source (export), data_sources[].stage
# (combine). Delete this table when the last schema lands.
FALLBACK_STAGE_READ_KEYS = (
    'source_stage', 'lookup_stage', 'reference_stage', 'merge_source',
    'aggregation_source', 'raw_stage', 'expected_from_stage', 'target_stage',
    'output_stage', 'stage', 'expected_stage', 'filtered_stage',
)
FALLBACK_NESTED_READ_KEYS = ('data_source', 'stage', 'insert_from_stage')
FALLBACK_STAGE_WRITE_KEYS = ('save_to_stage', 'save_conflicts_to_stage')
FALLBACK_STAGE_LIST_KEYS = ('stages',)


class RecipeValidationReport:
    """Errors and warnings from the validation phase, with a summary."""

    def __init__(self):
        self.errors = []
        self.warnings = []
        self.schema_less_types = set()
        self.validated_steps = 0

    @property
    def ok(self) -> bool:
        return not self.errors

    def log(self) -> None:
        if self.schema_less_types:
            names = ', '.join(sorted(self.schema_less_types))
            logger.info(
                f"   no schema yet ({len(self.schema_less_types)} processor type(s), "
                f"stage keys checked only): {names}"
            )
        for warning in self.warnings:
            logger.warning(f"\u26a0\ufe0f  {warning}")
        for error in self.errors:
            logger.error(f"\u274c {error}")
        if self.ok:
            logger.info(f"\u2713 Validation passed: {self.validated_steps} step(s), "
                        f"{len(self.warnings)} warning(s)")
        else:
            logger.error(f"Validation failed: {len(self.errors)} error(s), "
                         f"{len(self.warnings)} warning(s)")


def _fallback_stage_references(config: dict, processor_class) -> tuple:
    """
    Stage reads/writes for a schema-less step, from the temporary key
    table, walking nested mappings and lists so keys like
    sheets_to_create[].data_source or insert_from_stage inside a list of
    parts are found wherever they sit.
    """
    reads, writes = [], []
    if not isinstance(config, dict):
        return reads, writes
    if config.get('processor_type') == 'free_stages':
        return [], []          # releases stages: neither a read nor a write

    def walk(node, top_level: bool):
        if isinstance(node, dict):
            for name, value in node.items():
                if isinstance(value, str):
                    if name in FALLBACK_STAGE_READ_KEYS or (not top_level and name in FALLBACK_NESTED_READ_KEYS):
                        reads.append(value)
                    elif name in FALLBACK_STAGE_WRITE_KEYS:
                        writes.append(value)
                    elif top_level and name == 'stage_name' and 'save_to_stage' not in node:
                        writes.append(value)   # copy_stage / create_stage output
                elif isinstance(value, list) and name in FALLBACK_STAGE_LIST_KEYS:
                    reads.extend(v for v in value if isinstance(v, str))
                else:
                    walk(value, False)
        elif isinstance(node, list):
            for item in node:
                walk(item, False)

    walk(config, True)
    return reads, writes


def _family_stage_schema(processor_class) -> Schema:
    """
    Stage-key-only schema for a schema-less processor.

    Family classes state their stage keys; a processor still on the bare
    base class (not yet audited into a family) is read through the
    requires_source_stage / requires_save_to_stage flags the loader has
    always used, so nothing loosens during the migration.
    """
    family = processor_class.family.name
    keys = []
    if family == 'base':
        if getattr(processor_class, 'requires_source_stage', True):
            keys.append(Key('source_stage', 'stage_in', required=True))
        if getattr(processor_class, 'requires_save_to_stage', True):
            keys.append(Key('save_to_stage', 'stage_out', required=True))
    else:
        if family in ('transform', 'export'):
            keys.append(Key('source_stage', 'stage_in', required=True))
        if family in ('transform', 'import'):
            keys.append(Key('save_to_stage', 'stage_out', required=True))
    return Schema(keys)


def validate_recipe(recipe_data: dict, registry, substitute) -> RecipeValidationReport:
    """
    Validate every step and the stage graph.

    Args:
        recipe_data: loaded recipe (settings + recipe)
        registry:    processor registry, for class lookup by processor_type
        substitute:  callable applying variable substitution to a step config
    """
    report = RecipeValidationReport()
    steps = recipe_data.get('recipe', [])
    settings = recipe_data.get('settings', {})
    declared = {
        entry.get('stage_name') for entry in settings.get('stages', [])
        if isinstance(entry, dict) and isinstance(entry.get('stage_name'), str)
    }
    written = {}       # stage -> step number that wrote it
    read = set()
    used = set()

    for index, raw in enumerate(steps, 1):
        if not isinstance(raw, dict):
            report.errors.append(f"step {index}: must be a mapping")
            continue
        processor_type = raw.get('processor_type')
        label = f"step {index} '{raw.get('step_description', processor_type)}'"
        if processor_type not in registry._processors:
            report.errors.append(f"{label}: unknown processor_type {processor_type!r}")
            continue
        processor_class = registry._processors[processor_type]
        try:
            config = substitute(raw)
        except Exception as error:
            report.errors.append(f"{label}: variable substitution failed: {error}")
            continue

        schema = processor_class.full_schema()
        if schema is None:
            report.schema_less_types.add(processor_type)
            stage_schema = _family_stage_schema(processor_class)
            for message in validate_config(config, Schema(list(stage_schema.keys.values())), '', True):
                if 'unknown key' in message:
                    continue
                report.errors.append(f"{label}: {message}")
            reads, writes = _fallback_stage_references(config, processor_class)
        else:
            for message in validate_config(config, schema, '', False):
                report.errors.append(f"{label}: {message}")
            reads, writes = stage_references(config, schema)
        report.validated_steps += 1

        for stage in reads:
            used.add(stage)
            read.add(stage)
            if stage not in written:
                report.errors.append(f"{label}: reads stage '{stage}' before any step writes it")
            if stage not in declared:
                report.warnings.append(f"{label}: stage '{stage}' is not declared in settings.stages")
        for stage in writes:
            used.add(stage)
            if stage in written and not config.get('confirm_stage_replacement', False):
                report.errors.append(
                    f"{label}: writes stage '{stage}' already written by step {written[stage]} "
                    f"without confirm_stage_replacement: true"
                )
            written[stage] = index
            if stage not in declared:
                report.warnings.append(f"{label}: stage '{stage}' is not declared in settings.stages")

    for stage in sorted(declared):
        if stage not in written:
            report.errors.append(f"declared stage '{stage}' is never written by any step")
        elif stage not in read:
            report.errors.append(f"declared stage '{stage}' is written but never read")
    return report


# End of file #
