# Every run writes a log (2026-09-04)

## Why

A recipe without a `settings.log_file` directive wrote nothing, and the
terminal's scrollback was the only record. The duplicate-key investigation
that started this work lost its first 30 lines that way. Logs are cheap;
losing the record of a run is not.

## Semantics of `settings.log_file`

- absent, or `true`: the platform's per-user log folder, file named
  `<recipe stem>_<YYMMDD>_<HHMMSS>_log.txt`
  - macOS    `~/Library/Logs/excel_recipe_processor/`
  - Windows  `%LOCALAPPDATA%/excel_recipe_processor/logs/`
  - other    `$XDG_STATE_HOME/excel_recipe_processor/logs/`
             (`~/.local/state/...` when unset)
- a string: a path template, external variables substituted, as before
  (the VMS recipe pairs its log with its output this way)
- `false`: no file
- anything else, or an empty string: halts with a message

`ERP_LOG_DIR` overrides the platform folder. `--log-file` on the CLI
still outranks all of it. The startup lines buffered before the seam
become the file's head in every case.

Not beside the recipe or the output by default: a recipe can have several
inputs and outputs, so no single folder is "the" run location.

## Consequence

Anything that runs a pipeline - including tests - now leaves a log in the
platform folder unless it opts out. Two test suites did on the first run.

## Where

`core/default_log_path.py` holds the folder mapping and the setting
resolver; the seam in `core/recipe_pipeline.py` calls it.
`tests/test_default_log_path.py` 4/4.
