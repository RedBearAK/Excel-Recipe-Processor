"""
The erp_metadata block: what kind of ERP file this is, and what it last ran on.

excel_recipe_processor/core/erp_metadata.py

Every YAML the framework reads can open with one top-level block that
declares the file for a cold reader and for the tool:

    erp_metadata:
      erp_file_kind: recipe
      last_validated_via_erp_version: "20261014.2"

The block is frontmatter in spirit (YAML has no real frontmatter, so it is
an ordinary top-level key placed first by convention). It is OPTIONAL -
an older file without it keeps running - but its absence is reported at
the start and the end of the run so files get populated as they are
touched. When present it is validated as a CLOSED mapping with the same
machinery processor configs use: exactly the declared keys, typed, an
unknown key named with a nearest-name suggestion, the kind from a fixed
list, the version in the YYYYMMDD.N shape _version.py uses.

The version comparison is the payoff and is always reported: a runner
OLDER than the recipe's last validation is the "your ERP is behind this
recipe" situation (its unknown-key errors are otherwise puzzling); a
NEWER runner is a nudge to re-validate. The tag is a manual, honest
"last validated with", never a minimum-requirement claim the tool could
not verify. Writing it back is a deliberate act (--stamp), never a side
effect of validation.

A named_objects file (the manage_named_objects import/export format)
carries the same two keys plus the provenance an export knows: which
workbook it was harvested from, when, with which openpyxl, and how many
objects - the last of which the importer VERIFIES against the file.
"""

import logging

from excel_recipe_processor._version import __version__
from excel_recipe_processor.core.config_schema import Key, Schema, validate_config


logger = logging.getLogger(__name__)

ERP_METADATA_KEY = 'erp_metadata'
ERP_FILE_KINDS = ('recipe', 'named_objects')

# Keys every kind carries.
_common_keys = [
    Key('erp_file_kind', 'str', required=True, choices=list(ERP_FILE_KINDS),
        description='What this file is to the framework'),
    Key('last_validated_via_erp_version', 'str',
        description='ERP version (YYYYMMDD.N) that last validated this file clean; a manual tag'),
]

# Provenance an export writes into a named_objects file. Optional: a
# hand-authored library has no workbook it was harvested from.
_named_objects_keys = [
    Key('exported_from_workbook', 'str', description='Workbook the objects were harvested from'),
    Key('exported_on', 'str', description='ISO timestamp of the export'),
    Key('exported_with_openpyxl_version', 'str'),
    Key('named_object_count', 'int',
        description='Objects in the file across all four sections; verified on import'),
]

ERP_METADATA_SCHEMA = Schema(_common_keys, variants={'erp_file_kind': {
    'recipe': Schema([]),
    'named_objects': Schema(_named_objects_keys),
}})


class ErpMetadataError(ValueError):
    """The erp_metadata block is present but malformed, or declares the wrong kind."""
    pass


class ErpMetadataReport:
    """
    What inspecting one file's block found: whether it was there, what it
    said, and the lines to log about it. start_lines are (level, text)
    pairs for the beginning of the run; end_line is the one reminder worth
    repeating at the end (None when there is nothing to repeat).
    """

    def __init__(self):
        self.present = False
        self.file_kind = None
        self.validated_version = None
        self.comparison = None
        self.start_lines = []
        self.end_line = None


def parse_erp_version(text) -> tuple:
    """
    The YYYYMMDD.N version string as a comparable (date, n) tuple.

    Raises ErpMetadataError on any other shape, so a typo in the tag is
    caught at load instead of comparing as a string.
    """
    if not isinstance(text, str):
        raise ErpMetadataError(
            f"ERP version must be a quoted string like \"20261014.2\", got {text!r}"
        )
    parts = text.strip().split('.')
    if len(parts) != 2 or not parts[0].isdigit() or not parts[1].isdigit() or len(parts[0]) != 8:
        raise ErpMetadataError(
            f"ERP version {text!r} is not in the YYYYMMDD.N shape _version.py uses"
        )
    return (int(parts[0]), int(parts[1]))


def inspect_erp_metadata(document: dict, expected_kind: str, source_label: str) -> ErpMetadataReport:
    """
    Validate a loaded YAML document's erp_metadata block and prepare the
    log lines about it.

    Args:
        document:      The parsed YAML (a mapping)
        expected_kind: The erp_file_kind the caller is loading ('recipe'
                       or 'named_objects')
        source_label:  How to name the file in messages (its name or path)

    Returns:
        ErpMetadataReport

    Raises:
        ErpMetadataError: block present but malformed, or declaring a
                          different kind than the caller expected
    """
    if expected_kind not in ERP_FILE_KINDS:
        raise ErpMetadataError(f"expected_kind must be one of {ERP_FILE_KINDS}, got {expected_kind!r}")

    report = ErpMetadataReport()
    if not isinstance(document, dict) or ERP_METADATA_KEY not in document:
        text = (f"{source_label} has no erp_metadata block; add one declaring "
                f"erp_file_kind: {expected_kind} and last_validated_via_erp_version "
                f"(see core/erp_metadata.py)")
        report.start_lines.append(('warning', f"🏷️  {text}"))
        report.end_line = f"🏷️  Reminder: {text}"
        return report

    block = document[ERP_METADATA_KEY]
    report.present = True
    errors = validate_config(block, ERP_METADATA_SCHEMA, path=ERP_METADATA_KEY, allow_unresolved=False)
    if errors:
        raise ErpMetadataError(f"{source_label}: " + '; '.join(errors))

    kind = block.get('erp_file_kind')
    report.file_kind = kind
    if kind != expected_kind:
        raise ErpMetadataError(
            f"{source_label} declares erp_file_kind: {kind}, but a {expected_kind} was expected here"
            + (" - this is a named-objects library, not a recipe" if kind == 'named_objects' else '')
            + (" - this is a recipe, not a named-objects library" if kind == 'recipe' else '')
        )

    # Keys other than the block itself are expected to follow it, frontmatter style.
    if list(document.keys())[0] != ERP_METADATA_KEY:
        report.start_lines.append(('info', f"🏷️  {source_label}: erp_metadata is not the first top-level key; "
                                           f"convention puts it first, where frontmatter would be"))

    tagged = block.get('last_validated_via_erp_version')
    if tagged is None:
        report.start_lines.append(('info', f"🏷️  {source_label}: erp_file_kind {kind}; "
                                           f"no last_validated_via_erp_version tag (running ERP {__version__})"))
        return report

    tagged_tuple = parse_erp_version(tagged)
    running_tuple = parse_erp_version(__version__)
    report.validated_version = tagged
    if tagged_tuple == running_tuple:
        report.comparison = 'same'
        report.start_lines.append(('info', f"🏷️  {source_label}: erp_file_kind {kind}; last validated via "
                                           f"ERP {tagged}, running the same"))
    elif tagged_tuple < running_tuple:
        report.comparison = 'runner_newer'
        report.start_lines.append(('info', f"🏷️  {source_label}: erp_file_kind {kind}; last validated via "
                                           f"ERP {tagged}, running {__version__} (newer) - re-validate "
                                           f"and re-tag when convenient"))
    else:
        report.comparison = 'runner_older'
        text = (f"{source_label}: last validated via ERP {tagged}, but this ERP is {__version__} "
                f"(OLDER) - keys this file uses may not exist here yet; update ERP before "
                f"trusting unknown-key errors")
        report.start_lines.append(('warning', f"🏷️  {text}"))
        report.end_line = f"🏷️  Reminder: {text}"
    return report


def log_report_start(report: ErpMetadataReport) -> None:
    """Emit the start-of-run lines at their levels."""
    for level, text in report.start_lines:
        if level == 'warning':
            logger.warning(text)
        else:
            logger.info(text)


def log_report_end(report: ErpMetadataReport) -> None:
    """Repeat the one reminder worth repeating, if any."""
    if report.end_line:
        logger.warning(report.end_line)


def build_named_objects_metadata(object_count: int, exported_from_workbook: str = None,
                                 exported_on: str = None, openpyxl_version: str = None) -> dict:
    """
    The erp_metadata block an export writes into a named_objects file.
    Provenance keys are included only when known.
    """
    block = {
        'erp_file_kind': 'named_objects',
        'last_validated_via_erp_version': __version__,
        'named_object_count': int(object_count),
    }
    if exported_from_workbook is not None:
        block['exported_from_workbook'] = str(exported_from_workbook)
    if exported_on is not None:
        block['exported_on'] = str(exported_on)
    if openpyxl_version is not None:
        block['exported_with_openpyxl_version'] = str(openpyxl_version)
    return block


# End of file #
