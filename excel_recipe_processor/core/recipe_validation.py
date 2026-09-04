"""
Pre-execution validation of a loaded, variable-resolved recipe.

excel_recipe_processor/core/recipe_validation.py

Runs as the FIRST phase of every run, after variables resolve and before
any step touches data; --validate stops the run after this phase. One
code path, two exits, so what the flag checks is exactly what a real run
checks (2026-09-03).

Two checks:

1. Step schemas. Every step is validated against its processor's declared
   schema: unknown keys (with a nearest-name suggestion), wrong types,
   missing required keys, variant mismatches. Every registered processor
   declares one; a processor without a schema is a defect and errors.

2. Stage graph, walked in step order. Errors:
   - a stage read before any earlier step wrote it
   - a stage written twice without confirm_stage_replacement
   - a declared stage that no step writes or no step reads (declarations
     exist to catch drift; an unused one IS drift)
   Warnings:
   - a stage used but not declared (allowed, as today)

Stage reads, writes and releases come from the schema (stage_in /
stage_out / stage_release kinds) and from computed_stage_writes().

Exit policy: errors fail; warnings do not.
"""

import logging

from excel_recipe_processor.core.config_schema import stage_references, validate_config


logger = logging.getLogger(__name__)


class RecipeValidationReport:
    """Errors and warnings from the validation phase, with a summary."""

    def __init__(self):
        self.errors = []
        self.warnings = []
        self.validated_steps = 0

    @property
    def ok(self) -> bool:
        return not self.errors

    def log(self) -> None:
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
    released = {}      # stage -> step number that freed it
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
            # Every registered processor declares a schema (2026-09-04); a
            # missing one is a defect in the processor, not a recipe.
            report.errors.append(f"{label}: processor '{processor_type}' declares no schema")
            continue
        for message in validate_config(config, schema, '', False):
            report.errors.append(f"{label}: {message}")
        reads, writes, releases = stage_references(config, schema)
        writes = list(writes) + list(processor_class.computed_stage_writes(config))
        report.validated_steps += 1

        for stage in reads:
            if stage in released:
                report.errors.append(
                    f"{label}: reads stage '{stage}' after step {released[stage]} released it"
                )

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
            released.pop(stage, None)
            if stage not in declared:
                report.warnings.append(f"{label}: stage '{stage}' is not declared in settings.stages")
        for stage in releases:
            if stage not in written:
                report.errors.append(f"{label}: releases stage '{stage}' that no earlier step wrote")
            released[stage] = index

    for stage in sorted(declared):
        if stage not in written:
            report.errors.append(f"declared stage '{stage}' is never written by any step")
        elif stage not in read:
            report.errors.append(f"declared stage '{stage}' is written but never read")
    return report


# End of file #
