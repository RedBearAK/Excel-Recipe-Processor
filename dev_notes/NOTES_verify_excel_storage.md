# NOTES: verify_excel_storage - the audits become a recipe step (2026-08-16)

Every check in this processor was a production incident before it was
a check: the '=' definedName repair-deletion (fn_blank_safe), the
Dom_View legacy-CSE braces, prefix-chain corruption, unmapped _xlfn
names, bare LAMBDA/LET declaration slots. The whole-file sweeps that
pinned those incidents lived in the test suite; now a recipe can hold
its own output to the same standard.

## The promotion (one truth)

audit_stored_grammar and the legacy-CSE sweep moved to
core/excel_storage_audit.py; both test modules import from there
(local names preserved), and the processor imports the same functions.
Both auditors take PATHS OR FILE-LIKE sources.

## Session-awareness (the part that makes a mid-recipe audit honest)

The run-end save happens AFTER all steps, so a file still in the
workbook session has STALE bytes on disk. A listed file found in the
session serializes through THE SAME declaration pipeline the run-end
save uses - to an in-memory buffer, no disk writes - and the audit
reads the bytes as they WILL be written. The test pins this with the
hardest case: a violation planted only in the live object, invisible
to the clean disk copy, caught. Supporting change:
dynamic_array_metadata's writer and report line accept file-like
destinations (the READER already did - see incident below).

## An incident during the build, on the record

A splice targeting the reader matched 'source_path = Path(source)' as
a SUBSTRING of a deeper-indented line and mangled the function -
written WITHOUT the ast parse-proof the house convention demands, and
caught only by the test run after. Two lessons re-learned: (1) the
actual BytesIO failure was a LOG LINE, not the reader - diagnose
before splicing; (2) parse-proof is per-write, no exceptions, even
for a two-line edit.

## Wiring

VMS recipe: final step audits {output_dir}/{output_basename}.xlsx
(74 steps, validates). on_violation: halt (default) lists every
violation; warn logs and continues. Demo end-to-end: audit reports
CLEAN with 'session, will-be-written bytes' provenance; log/terminal
byte-parity re-verified. Tests 2/2 (clean/dirty/warn + the session
path); the two donor test modules stay green through their imports.
