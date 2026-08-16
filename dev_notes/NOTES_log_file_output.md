# NOTES: log-file mirroring (2026-08-16)

## The finding that shaped it

The "nice" log form is Unicode emoji in the MESSAGE TEXT - plain
logging.basicConfig, zero ANSI codes in the stream. So the terminal
content is already tee-able losslessly (`... 2>&1 | tee run.log`),
and nothing isatty-strips on pipe. What tee cannot give is the reason
this earned real code: a recipe-triggered log that NAMES ITSELF AFTER
THE RUN.

## The feature

- `--log-file PATH` (CLI): attaches at startup, captures everything
  including recipe loading. Path taken literally.
- `settings: log_file:` (recipe): attaches at the post-external-
  variables seam, because the paths worth writing need them -
  `log_file: "{output_dir}/{output_basename}_log.txt"` pairs each log
  with the output workbook it describes, provenance for free. Lines
  before the seam (loading, variable resolution) live only in the
  terminal; documented in the setting's comment.
- PRECEDENCE: CLI wins outright; a recipe attachment is skipped with
  an explicit 🪵 line naming the active file. FileHandler is mode='w'
  (one log per run, like one output per run) with encoding='utf-8'
  stated so the symbols survive every platform.

attach_log_file() lives in core/main.py beside the basicConfig it
mirrors; tests/test_log_file_output.py (2/2) pins the UTF-8 round
trip and the precedence. The VMS recipe carries the setting as of
this delivery; iTerm2 scrollback (the original question) is Settings
-> Profiles -> Terminal -> Scrollback lines / Unlimited, but with the
recipe setting in place the buffer stops mattering.
