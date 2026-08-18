# Fill-down tests, and the recipe development CLI options

## Part 1 — the missing fill_down tests

`tests/test_seed_donor_fill_down.py`, 10 tests. Every assertion is on cell
CONTENT rather than on reported counts, because the counts were what lied: the
original bug logged `Filled 5 column(s)` while writing nothing to one of them.

| Test | Pins |
|---|---|
| plain formulas reach the last row | basic fill, references translated |
| **array formula is not skipped** | the original silent bug |
| array ref is per row | each cell gets its own `ref`, not the origin's |
| convert mode writes plain formulas | `array_formula_mode` |
| named ranges do not shift | `rng_carrier` stays put while `C11` moves |
| short donor leaves no gap | donor with fewer rows than `row_count` |
| on_existing_cell policies | error / skip / overwrite |
| anchor columns accept header names | not just column letters |
| column without a formula is not filled | a constant is not smeared downward |
| empty transplant raises | rather than writing a formula-free file |

**Verified the tests actually catch the bug.** Reintroducing the original
`is_array = False` takes the module from 10/10 to 8/10, failing on exactly the
two array tests. A test that cannot fail is not protecting anything.

## Part 2 — the CLI options

All external to the recipe file. Editing a recipe to observe it changes the
thing being observed, and the recipe is usually what is under test.

### `--dump-stage NAME[:SPEC]`

Writes a stage to CSV as it is produced, then carries on. Repeatable.

| Spec | Rows |
|---|---|
| *(omitted)* | all |
| `20` | first 20 |
| `-20` | last 20 |
| `100-150` | 100 through 150, 1-based inclusive |
| `20,-20` | first 20 and last 20, with a `...` marker between |

The marker matters: without it, a both-ends dump reads as contiguous rows and
invites a wrong conclusion about the data.

CSV rather than xlsx on purpose — faster to write, diffable and greppable once
written. These are throwaway artifacts.

### `--dump-dir DIR`

Where the CSVs go. Defaults to the current directory.

### `--stop-after STAGE`

Halts once that stage has been written. Most inspection cycles only need the
first part of a pipeline, and the rest is waiting.

```
🛑 Stopping after 'stg_vms_shaped' as requested (--stop-after). 36 of 41 steps ran.
```

### `--list-stages RECIPE`

Prints the declared stages with the step that writes each one, without running
anything. Answers "what can I ask for" without reading the YAML.

```
vms_process.yaml: 41 steps, 37 declared stages

  step  1  stg_vms_import_raw
            Untouched VMS download
  step  2  stg_vms_cleaned
            Whitespace and invisible characters removed from all columns
```

It also reports any stage written but not declared, which is the same drift the
validator warns about.

### Specs are validated before step 1

A typo initially surfaced at **step 37 of 41**, after everything upstream had
run — the precise late-failure complaint that prompted these options. Specs are
now checked while parsing arguments:

```
$ ... --dump-stage stg_vms_sorted:banana
Error: Cannot understand row spec 'banana'. Accepted forms: 20 (first 20),
       -20 (last 20), 100-150 (a range), 20,-20 (both ends)

$ ... --dump-stage stg_vms_sorted:90-40
Error: Row range '90-40' ends before it starts
```

Nothing runs.

## Verification

`tests/test_stage_inspection.py`, 11 tests: argument parsing, all four spec
forms, the ellipsis marker, both-ends on a frame smaller than the request, early
rejection of six malformed specs, acceptance of every documented form, the
readable descriptions, and the file actually landing on disk.

Exercised together against the real recipe:

```
🔎 Dumped 'stg_vms_filter_notes' -> ... (5 of 82 rows, 66 columns, first 5 rows)
🔎 Dumped 'stg_lookup_carriers'  -> ... (8 of 8 rows, 1 columns, all rows)
🔎 Dumped 'stg_vms_enriched'     -> ... (3 of 82 rows, 71 columns, last 3 rows)
🔎 Dumped 'stg_vms_shaped'       -> ... (13 of 82 rows, 76 columns, first 10 and last 2 rows)
🛑 Stopping after 'stg_vms_shaped'
```

Full suite: 20 failures, matching baseline.

## Still untested

Named for honesty rather than left implied:

- CLI override precedence (`--var` beating `--set`, and both surviving
  re-resolution)
- `create_stage` through the pipeline path, where the bug actually was —
  the existing test exercises the processor directly
- auto-fit ignoring formula source text
- the `aggregate_data` / `group_data` double-save fix

None are silent-failure classes except the first, which is the one I would do
next.

# End of file #
