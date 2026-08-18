# NOTES: the Dom_View braces incident (2026-08-16)

## Symptom (first production run with the fn_vms_view views)

Both view anchors showed a single value ("2026" - the first cell of
the spill) with {braces} in the edit line: legacy CSE array formulas.
Double-click + Enter converted them to live spills. Run time also
regressed 18s -> 45s.

## Root cause: THREE defects interacting

1. **openpyxl drops cm on a round trip.** The declaration pass is
   zip-level surgery; the in-memory workbook object never knows about
   cm. flush wrote correctly-declared bytes, the next format step
   reloaded them (openpyxl parses t="array" into ArrayFormula but
   discards cm), and the final save re-serialized WITHOUT cm.
2. **The provenance registry was popped after each save**, on the
   assumption "already-marked cells are recognized on re-saves" - true
   within one set of bytes, false across an openpyxl reload. So the
   final save's re-declaration had no provenance and could rescue only
   vocabulary-hit cells (the log's "19352 completed"); ~48k
   provenance-only cells reverted to bare t="array". The seven filled
   VMS columns wore invisible braces too (scalar results, so no
   visible breakage); the views broke VISIBLY because a spill under
   CSE collapses to one value.
3. **The vocabulary could not see through the named lambda.** The old
   direct '=fn_blank_safe(FILTER(...))' text carried a vocabulary hit;
   '=fn_vms_view(...)' hides FILTER inside the name's definition -
   neither vocabulary nor (per defect 2) provenance reached it.

An honest accounting: the demo verification for the Dom_View delivery
checked formula TEXT and formatting but never the cm declaration
state - the bug shipped through it. The no-legacy-CSE sweep below is
the check that was missing, now part of the test suite.

## Fixes (all three, plus the check that was missing)

- **Lambda vocabulary** (dynamic_array_metadata): defined names whose
  stored content declares _xlfn.LAMBDA( join the detection vocabulary
  VERBATIM (calls are stored case-preserving; the pattern is
  case-sensitive, so no uppercasing). Safe by construction: LAMBDA
  does not exist pre-365, so a call to one can never be a legacy
  implicit-intersection formula.
- **Registry persistence** (workbook_session): the save-path pop is
  gone; provenance lives until the session resets, so every save of
  the same file can re-declare.
- **assert_no_legacy_cse(path)**: reusable sweep - any t="array"
  formula cell without cm is a violation. Pinned in
  tests/test_declaration_lambda_and_registry.py (3/3) including the
  exact flush -> reload -> save shape of the incident.

## The 45s -> ~18s fix (same insight, other direction)

The regression decomposed exactly: flush save 5.4s + disk profile
13.1s + format-step reload 7.0s ≈ the 27s delta. All three existed
only to read applied formats OFF DISK. profile_sheets now resolves
input_file through WorkbookSession.peek_workbook() FIRST: when the
path is the run's own in-flight workbook it profiles the LIVE object
(formatting already on it), falling back to disk reads otherwise
(previous-run files, external workbooks - peek never loads, so
arbitrary files are not dragged into the session). The recipe's flush
step is REMOVED (73 steps); the pipeline is again: format mains ->
profile (in-memory) -> format views -> one save at run end.

Frame facts from a live sheet: formula cells (ArrayFormula or '='
text) become NA, matching what a disk read of the same unopened file
yields - documented in the processor.

## iTerm2 buffer (user question)

Settings -> Profiles -> Terminal -> Scrollback lines (raise it, or
check "Unlimited scrollback"). Steadier: pipe runs through tee -
`... 2>&1 | tee run_$(date +%H%M%S).log` - and the whole log survives
regardless of buffer.
