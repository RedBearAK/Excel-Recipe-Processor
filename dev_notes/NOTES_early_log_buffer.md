# NOTES: complete log-file coverage - both ends (2026-08-16)

Two gaps closed in one delivery; a recipe-triggered log file is now
COMPLETE, opening line to summary, identical in coverage to --log-file.

## The head: the early-record buffer

The recipe log_file directive attaches at the post-external-variables
seam, which used to mean the loading lines existed only in the
terminal. A lightweight buffer handler installs at startup, holds
those records (capped at 1000), and attach_log_file() flushes them as
the FILE'S OPENING LINES before the live handler joins. When the seam
finds no directive and no CLI attach consumed the buffer, it is
discarded on the spot. Buffered records flush to the FILE handler
only - they already reached the terminal once.

## The tail: mirror_print (user-caught, second gap)

The run summary block ("✓ Recipe completed successfully...", the
stage counts) and the blank separators are deliberately UNPREFIXED
print() output - outside the logging system, so no FileHandler ever
saw them and the log's tail was silently incomplete. mirror_print()
writes the same bytes to both places: terminal via print, each
attached log stream raw (no formatter - the file matches the terminal
exactly, including the absence of 'INFO:'). A dead stream is caught
and ignored: a log line must never kill a run. Converted sites: the
whole summary block and the two separators that post-date attachment;
pre-attach separators remain terminal-only cosmetics.

## Sub-recipe concern (resolved structurally)

There is NO recipe include/import mechanism - recipes are single YAML
files - so nothing can introduce a log directive mid-run. The only
recipe-source attach call is the one seam reading the MAIN settings,
and the single 'recipe' slot refuses a second attach regardless.

Tests 5/5 in tests/test_log_file_output.py: UTF-8 round trip, CLI
precedence, opening-line ordering, the discard path, and summary
parity (present AND unprefixed).
