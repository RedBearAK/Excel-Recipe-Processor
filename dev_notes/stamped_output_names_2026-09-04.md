# Stamp every output file name with the run time (2026-09-04)

## Why

Excel keeps showing a workbook it already has open. Rewrite the same
file name underneath it and nothing on screen changes - and the hunt
for "why doesn't this look any different" begins, usually well before
anyone thinks to check the window title. A run stamp in the name makes
every run a new file, and Excel opens the new one as a new window.

## House pattern

One settings variable, every output and the log named from it, so a
run's files pair up by name:

    output_basename: "{source_stem}_proc_{hour}{minute}{second}"
    log_file: "{output_dir}/{output_basename}_log.txt"
    ...
    output_file: "{output_dir}/{output_basename}.xlsx"

Time only when the stem already carries the date (a download stem does);
`{timestamp}` when it does not. `vms_process.yaml`,
`vms_merge_downloads.yaml`, and `only_in_one_file.yaml` all follow it.

## Where the guidance now lives

- `export_file` schema: the `output_file` key description (which feeds
  the generated `docs/processors/export_file.md`)
- `export_file_examples.yaml`: the basic example's comment, the
  parameter details, and a `naming_convention` entry
- `_rules_for_valid_usage_examples.md`: examples that write files carry
  a stamp and say why, so the habit propagates
- `README.md`: the first-recipe example
