# Timestamped backups with a hard cap (and the double-log fix)

Expand at the repo root. excel_writer.py, file_writer.py and
export_file_processor.py are WHOLE FILES - replace, don't merge
(excel_writer carries the export bridge and the modern-theme baseline).
New helper package: writers/_helpers/ holding the deletion pattern in its
own _rgx module, per convention.

## Naming: marker BEFORE the extension

    report_erpbkup_260812_144320.xlsx

The old scheme put ".backup" AFTER the extension, changing the file type as
far as the file manager was concerned so the backup would not open in its
default application. Fixed.

The marker is "_erpbkup_", not the more natural "_bkup_", precisely because
the trimmer DELETES what the pattern matches: "_bkup_" is a common human
shorthand and would put hand-named files at risk. "_erpbkup_" essentially
cannot come from anything but this tool.

## Why timestamps rather than a rolling sequence

Names are written once and never renamed. A rolling scheme shifts every
file on every run, and a crash mid-shift leaves the set ambiguous; here a
crash can only leave one extra file. And because YYMMDD_HHMMSS is
zero-padded and monotonic, sorting the names lexicographically sorts them
CHRONOLOGICALLY - so trimming is a pure deletion of the oldest, with no
reliance on modification times (which would not survive the files being
copied or moved).

Worth noting the old numbered scheme could not have been sorted safely at
all: your CMA folder held .backup1 ... .backup10, and lexicographically
".backup10" sorts between ".backup1" and ".backup2".

## The directive

    delete_backups_beyond: 2    # keep the 2 newest, delete every older one
                                # counts the one being made
                                # 0 means make no backup at all

Named for what it does to the SURPLUS, not for a ceiling. "max_backups_
allowed" was the first name and was rejected: it can be read as "refuse to
create new backups once two exist", which is the opposite behaviour, and a
dangerous thing to assume - someone could believe their oldest backup is
safe forever while it is in fact the next one deleted.

Your CMA lookup folder had reached ELEVEN legacy backups - unbounded growth
in a folder another project reads from, which is what prompted this. The
CMA export keeps create_backup: true as you asked; it now just stops at two.

## Deletion safety (the part that matters)

Only names matching the _erpbkup_ pattern for THIS file's stem AND
extension are ever deleted. Asserted explicitly in tests: a neighbouring
file's backups, a "_bkup_" human-shorthand file, and a marker-carrying but
malformed name all survive a trim. Legacy ".backup" files are recognised
only to be REPORTED once per run and are never deleted - they predate the
marker, and deleting files this tool did not create under the current
scheme is not its business. Sweep them by hand when convenient.

## The double log

FileWriter.create_backup logged a line that ExcelWriter.create_backup had
already logged - one file, two lines, never legitimate. The wrapper's call
is removed; the emitter stays where the copy happens.

Tests 7/7 (naming, cap, deletion safety, same-second collision, 0 and
negative caps, single log line, byte-for-byte copy). Full suite: 20
failures, baseline.

# End of file #
