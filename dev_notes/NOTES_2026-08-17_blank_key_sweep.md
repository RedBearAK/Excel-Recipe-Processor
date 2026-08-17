# Blank-key retention sweep: the remaining five groupby sites

dev_notes/NOTES_2026-08-17_blank_key_sweep.md

Completes the doctrine begun in
NOTES_2026-08-17_blank_key_group_loss.md (aggregate_data): pandas'
dropna=True default does not merge NaN-keyed rows, it silently
DELETES them. Per ruling, every remaining grouping site now passes
dropna=False:

- add_subtotals_processor.py: the group iteration. A blank-keyed
  group now gets its own subtotal row like any other group.
- group_data_processor.py: all three iteration groupbys (stage split,
  groups listing, lookup split). Blank-keyed slices now surface as a
  group instead of vanishing between the others.
- pivot_table_processor.py, two sites:
  * The main pd.pivot_table call already exposed a 'dropna' config
    key - its DEFAULT flipped True -> False. Empirically verified on
    pandas 3 before flipping: with True, 4 rows summing 10 pivot to
    1 row summing 3; the NaN-keyed rows are deleted, not merged. The
    key stays configurable, so a recipe that WANTS the pruning makes
    a visible per-step choice instead of inheriting a silent default.
  * The crosstab path had no dropna at all - now passes False.

## Tests

New tests/test_blank_key_retention_doctrine.py (3/3): add_subtotals
(blank group emits its own 700.0 subtotal; 6 rows, 1460 conserved),
group_data (dropna=False iteration contract yields the NaN group),
pivot_table (flipped default keeps the blank-keyed row; 730
conserved). aggregate_data's retention stays pinned in its own suite.
All four processor suites plus the comprehensive-processors and
examples checkers green - no existing test depended on the deletion,
including through pivot_table's flipped default.

## Runtime question from the same review (no action)

The ~10s jump between the 011628 and 125054 runs is environmental:
EVERY step slowed by the same ~1.7x, including importing the
identical raw file (0.85s -> 1.53s) and free_stages memory frees -
work no recipe change touches. The structural additions (two more
sheets through export/format/audit, the summary profiles) are
sub-second class. Expect ~17s under quiet machine conditions.

# End of file #
