# The export bridge: the workbook never round-trips through disk

Expand at the repo root. Whole files - replace, don't merge. Requires
erp_workbook_session.tgz applied FIRST (this builds on it; the processor
files here supersede that archive's copies, and format_excel here carries
the autofit_scan_rows option too).

## What changed

The export step used to save the workbook to disk (34s on a 51K merge)
only for the very next step's session load to read the identical bytes
back (33s). The export now populates its workbook through a throwaway
buffer and hands the live book straight to the session
(WorkbookSession.adopt_workbook); nothing touches disk until the one save
at run end. The old active-sheet setter - a THIRD full load-and-save round
trip - collapses into a flag flip on the session book.

Measured at 10,500 rows: export 0.7s (was 2.2s), ranges 0.0s (was 2.4s),
whole run 12.6s (was 23.1s). At 51K merge scale, projected ~65s savings
on top of the session's 96s - toward roughly 75-90s total runtime.

## The subtle bug the equivalence gate caught

pandas' to_excel materializes every NaN as a literal '' cell (na_rep
default) - and the disk round-trip was silently ERASING them all, because
openpyxl serializes '' as an empty cell. The live bridged book skipped
that laundering, so formula seeding suddenly saw '' where a reload would
have shown None, and its collision check halted the run. Two fixes:

1. The bridge NORMALIZES the book to disk equivalence at adoption ('' ->
   None across all sheets), so every downstream consumer sees exactly what
   a reload would have shown - the general fix, not a per-consumer patch.
2. seed_donor_formulas' collision checks now treat '' as empty on their
   own merits - an empty string is what na_rep leaves behind, not data.

## Failure semantics (changed, deliberately)

A crash between export and flush now leaves NO output file - the same
outcome as crashing earlier while stages exist only in memory. The run
failed; rerun it. A recipe wanting a post-export checkpoint on disk can
place a flush_workbooks step right after its export, at the cost of the
save the bridge skips. Documented in the session module docstring.

Standalone (non-pipeline) callers are untouched: without a deferred
session, the export writes to disk exactly as before, and adopt_workbook
degenerates to an immediate save.

Equivalence-gated: 15-attribute snapshot (including data spot checks at
three coordinates) captured pre-bridge matches the bridged output exactly.
Session tests 9/9. Full suite: 20 failures, baseline.

# End of file #
