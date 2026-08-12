# Duplicate "Created backup" log line

Expand at the repo root. core/file_writer.py is a WHOLE FILE - replace,
don't merge.

## The duplicate

FileWriter.create_backup delegates to ExcelWriter.create_backup, which
already logs "Created backup: {path}" at the point the copy happens. The
wrapper then logged the identical line a second time. ONE file, TWO lines -
a literal duplicate emission, never legitimate under any circumstance.

Proof it was one file, visible in your own log: both lines named the SAME
path (.backup2). Two real backups could not share a name - the numbering
loop picks the first free one, so a second would have been .backup3.

The wrapper's log call is removed; the emitter stays where the action is.
New test module asserts exactly one line and exactly one file per call.

## The real finding underneath: backups ACCUMULATE

The numbering loop never overwrites and never prunes:

    lookup.xlsx.backup, .backup1, .backup2, .backup3, ...

one more on every run, forever. Your CMA lookup folder was already at
.backup2, so it is on its third and growing. This is by design in the
current code, and the new test documents it rather than changing it - but
for a file rewritten on every VMS run it is unbounded growth in a folder
another project reads from.

Options, none implemented: cap the count and recycle the oldest; date-stamp
instead of numbering, with a retention window; or simply skip the backup
for regenerated artefacts, since the CMA lookup is fully reproducible by
rerunning the recipe. That last one is probably right for this case - a
backup of a derived file is not protecting anything - but it is a
behaviour change and yours to call.

Full suite: 20 failures, baseline. New tests 3/3.

# End of file #
