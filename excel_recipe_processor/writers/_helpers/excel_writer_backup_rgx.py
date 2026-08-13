"""
Regex patterns for timestamped backup file names.

excel_recipe_processor/writers/_helpers/excel_writer_backup_rgx.py

Patterns live in their own module so editing the surrounding logic can never
corrupt them, and so the one pattern that authorises DELETION can be read and
audited on its own.

Backup names take the form:

    report_erpbkup_260812_144320.xlsx
    report_erpbkup_260812_144320_2.xlsx     (same-second collision)

The marker is "_erpbkup_" rather than the more natural "_bkup_" on purpose:
the trimmer deletes every file this pattern matches, so the marker must be
one that essentially cannot arise from anything but this tool. "_bkup_" is a
common human shorthand and would put hand-named files at risk.

YYMMDD_HHMMSS is zero-padded and monotonic, so sorting these names
lexicographically sorts them chronologically - which is what lets the
trimmer keep the newest N without consulting file modification times.
"""

import re


# Built for a specific file: the caller supplies the escaped stem and
# suffix, so the pattern can never match a backup belonging to a different
# source file that happens to sit in the same folder.
def build_backup_name_rgx(escaped_stem: str, escaped_suffix: str):
    """
    Return a compiled pattern matching this file's backups only.

    Args:
        escaped_stem:   re.escape'd file stem, e.g. "report"
        escaped_suffix: re.escape'd extension including the dot, e.g. "\\.xlsx"

    Returns:
        Compiled pattern with group 1 as the sortable timestamp portion
    """
    return re.compile(
        rf'^{escaped_stem}_erpbkup_(\d{{6}}_\d{{6}}(?:_\d+)?){escaped_suffix}$'
    )


# The legacy scheme this replaces: name.xlsx.backup, .backup1, .backup2 ...
# Matched only so those files can be RECOGNISED and reported. The trimmer
# never deletes them - they predate the marker and deleting files this tool
# did not create under the current scheme is not its business.
legacy_backup_rgx = re.compile(
    r'\.backup\d*$'
)

# End of file #
