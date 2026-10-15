# next_workday: WORKDAY with a holiday table from another stage, statically

`dev_notes/2026-10-14_next_workday_calculation_type.md`

## The ask

GridCraft (an open-source Rust Excel reimplementation Kris uses as a
second opener) took 283 s to open a processed VMS workbook and minutes per
edit. Profiling showed the cost was formula evaluation, not file size:
49K live cells, no cached values (openpyxl writes none), every one
re-evaluated on open and - because they reference defined names - on
every edit. The recipe already fixed the worst of it the same day (the
PID decode now matches a pre-flattened text key, 94M TEXT() calls down
to 26K). The next largest block was DUE DATE: 12.4K injected
`fn_next_workday(Proj Due Date)` formulas computing a fixed answer from
inputs that were ALL already static - Proj Due Date from the first_match
cascade, the holidays from `stg_lookup_holidays`. Kris: solidify it.

## Why it needed a processor change

`pandas_formula` evaluates against ONE DataFrame, the step's source
stage. `{col:Name}` becomes `df['Name']`; the vocabulary is that stage's
columns, `{var:...}` constants, `np` and `pd`. Every previous cross-stage
need in the recipe was one of three shapes with a dedicated processor:
a row-wise join (`lookup_data`, `merge_data`), a membership filter
(`filter_data` `in_stage`), or a constant (a settings variable). The
holidays are a fourth shape: a whole column from another stage used as
a vector PARAMETER with no row key. `np.busday_offset(dates, 0,
roll='forward', holidays=H)` is the one-liner, but H had no spelling.

Two ways to close the gap were weighed. A general `{stage_col:Stage!
Column}` token for `pandas_formula` would serve any future vector
parameter but makes expressions less inspectable and lets them depend
silently on another stage's row order and dtype. The specific type won
for now: explicit keys, fail-loud validation, typed like the existing
`date` category. The general token stays on the table if a second
vector-parameter need appears; its fragility should be judged then.

## What changed

`add_calculated_column` gains `calculation_type: "next_workday"`:

```yaml
calculation_type: "next_workday"
calculation:
  date_column: "Proj Due Date"          # REQ - blank in, blank out
  holidays_stage: "stg_lookup_holidays" # OPT - with holidays_column, or neither
  holidays_column: "Date"
  weekmask: "1111100"                   # OPT - Mon..Sun flags, default Mon-Fri
```

Semantics: Excel's `WORKDAY(date - 1, 1, holidays)` - the first working
day on or after the date. A date already on a working day stays; a
weekend or holiday rolls FORWARD. Blank (NaT, or '' in a text column)
stays blank. The result column is datetime64.

`holidays_stage` is typed `stage_in`, so `stage_references()` finds it
inside the variant schema: the validator's stage graph, the runtime
declaration contract (`StageManager.begin_step`) and the auto-free plan
all account for the read. Without that typing the holidays stage could
be freed before this step runs.

Fail-loud: a non-blank non-date in `date_column` or the holiday column,
a missing stage or column, `holidays_stage` without `holidays_column`
(or the reverse), and a weekmask that is not seven 0/1 flags with at
least one 1 all raise StepProcessorError naming the culprit. Blank
holiday rows are skipped and counted in the log. The log reports dated
rows, rows moved, blank rows, holidays honored and the weekmask.

Files: `processors/add_calculated_column_processor.py` (schema variant,
dispatch, `_apply_next_workday`, `_load_holiday_dates`, capabilities),
`processors/_examples/add_calculated_column_examples.yaml`
(`next_workday_example`, `parameter_details.next_workday`),
`tests/test_add_calculated_column_next_workday.py`.

## Verification

- `tests/test_add_calculated_column_next_workday.py`: 5/5 - WORKDAY
  semantics row by row (weekday stays, Sat/Sun to Monday, holiday Friday
  to Monday, holiday Monday to Tuesday, blank), weekends-only without a
  holiday stage, a six-day weekmask, text-typed dates with '' blanks and
  the result dtype, seven fail-loud paths.
- Related modules unchanged and green: `test_add_calculated_column_
  processor`, `..._spill_first_match`, `test_all_processor_examples`
  (9/9), `test_examples_validate_against_schemas`, `test_usage_examples`.
- Real-data parity: the processor run over the 261014 file's Proj Due
  Date column with its Holidays tab (86 dates) against the live DUE DATE
  column GridCraft had computed for the same file: 12,465 of 12,465 rows
  identical, 670 of them moved onto a later working day. The test file
  was the pre-change workbook, so this is the live formula's own answer.

## Recipe side (delivered as loose YAML, not in this archive)

`vms_process.yaml`: a `next_workday` step after the Proj Due Date
cascade (`stg_vms_due_date_workday`), DUE DATE removed from
`var_columns_to_create` and from the inject step. `vms_named_lambdas.
yaml`: `fn_next_workday` moves to the `zz_` archival tier as
`zz_fn_next_workday` with the pandas twin named. Validates on dev_beta:
128 steps, 0 warnings. Live formulas in the processed file: ~53.7K to
~41.3K.

## Lesson

The three-shape inventory (join, membership, constant) is worth
remembering: a stage column used as a vector parameter is the shape the
framework did not have, and the symptom is "I can see the data in a
stage but cannot name it in an expression".

# End of file #
