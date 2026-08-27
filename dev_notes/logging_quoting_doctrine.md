# Logging quoting doctrine and staged sweep plan

*2026-08-26. Stage 1 shipped in erp_log_quoting_stage1.tgz.*

## The doctrine

Any **user-originated identifier** in log output - sheet names, column
names, stage names, range names, file names, template names - is
quoted at the point of emission via `core/log_format.py` (`q()` for
one name, `qlist()` for lists, which caps with an explicit `+N more`
tail). Names routinely contain spaces, and a comma inside one member
of a bare joined list is silently indistinguishable from a list
separator - quoting makes every name's boundary unambiguous.

**Not quoted**: counts, durations, letters/positions, fixed internal
vocabulary (processor types, option constants like
`Available conditions: ...`), and names already inside an accepted
delimiter form - the `[Sheet]` bracket prefix counts as quoting.

## Stage 1 - shipped (most egregious: joined name lists + the rider)

- format_excel: worksheet census line, loading-file line, both pulse
  tick labels (`auto-fit 'Can Sizes': column 2/2 (B)`).
- format helper: column_formats description, width descriptions,
  hidden-columns pairs (`'Notes' (BX)`), cell_formats description.

## Stage 2 - name lists in other processors - SHIPPED (erp_log_quoting_stage23.tgz)

Highest traffic first:
- lookup_data: members were already pre-quoted at the append site
  (`'{col}' (98.2%)`) - marked compliant, join left alone.
- clean_data: skipped-columns list.
- select_columns: created-columns list.
- inject_formulas: sheet name quoted at the member-build site
  (`'VMS': 10; 'Van_Mech': 3`). strip_formula_caches parts are count
  phrases under a [sheet] bracket prefix - compliant, left alone.
- conditional_format: range joins are A1 refs (leave); check rule
  summaries for name emission.

## Stage 3 - single names in prose - SHIPPED (same archive)

Grep pattern: `logger.(info|warning)(f"` lines interpolating
`{sheet_name}`/`{column`/`{stage` without adjacent quotes or
brackets. Convert to `q()`. The `[{sheet_name}]` prefix form stays.

## Stage 4 - error messages

Guided errors interpolate names heavily and are ALREADY mostly
quoted by house style (`'{spec}'`); sweep for stragglers with the
same grep, lowest priority since errors quote the offending value in
context far more often than info lines do.

## Explicitly out of scope

- generate_column_config joins: those WRITE YAML output, already
  double-quoting members - file content, not logging.
- deduplicate `Conflicting Columns` join: report DATA cell content,
  a deliberate output format.
- aggregate `'_'.join` flatten: column-name construction, not logging.

## Verification per stage

Run a full recipe, diff the log against the prior run's log with
names normalized: every changed line must differ only by added
quotes. The tests that assert on log text (none currently) would
need updating if introduced.
