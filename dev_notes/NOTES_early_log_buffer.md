# NOTES: log file == terminal, byte for byte (2026-08-16)

Three gaps closed across three user reports, converging on one
acceptance criterion the delivery now MEETS AND TESTS: running the
demo with stdout+stderr captured to a file and diffing it against the
recipe-triggered log yields ZERO differences.

## Gap 1 - the head (loading lines)

The recipe log_file directive attaches at the post-external-variables
seam. A buffer handler installs at startup, holds startup output, and
attach_log_file() replays it as the file's opening lines; the seam
discards it when nobody wants a file.

## Gap 2 - the tail (the summary block)

"✓ Recipe completed successfully..." and the stage counts are
deliberately UNPREFIXED print() output, invisible to logging.
mirror_print() writes the same bytes to terminal and every attached
log stream raw (no formatter), so the file matches exactly - including
the absence of 'INFO:'. A dead stream is caught and ignored.

## Gap 3 - the rhythm (blank separators, user-caught)

The terminal's readability comes from print() blanks: one before
EVERY step (_log_step_separator) and several between the startup
sections. Fix in two parts: (a) the buffer holds ORDERED MIXED items
- ('record', LogRecord) from logging plus ('text', str) from
mirror_print - so pre-attach blanks replay in true emission order
among the loading lines; (b) every run-flow separator converted to
mirror_print (per-step, pre-load, pre-external, pre-execution,
post-last-step, summary block bounds). A double blank introduced by
the gap-2 fix (a new leading blank atop the pre-existing separator)
was removed in the same pass. Capabilities/help/dump displays keep
plain print - they are not run flow.

Tests 5/5 including interleaved-blank ordering; the byte-parity diff
is the standing acceptance check for any future change to run output.
