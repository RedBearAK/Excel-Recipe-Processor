# Two processor fixes found while inserting a column

Expand at the repo root. Both are WHOLE FILES - replace, don't merge.
This archive REPLACES erp_seed_column_positions.tgz (that fix is included
here).

## add_calculated_column: column substitution in expression formulas

_make_formula_safe replaced column names with df['name'] references in a
LOOP, one column at a time. That cannot be correct when one column name
contains another. With "Species" and "Major Species" both present:

  - short name first: "Major Species" becomes "Major df['Species']"
  - long name first is no better - the later short-name pass then matches
    INSIDE the text just substituted

Either way the formula fails to parse, so any recipe with nested column
names could not use an expression formula referring to them at all.

Now a single pass over an alternation of all column names, longest first so
the regex prefers the longer match. One pass cannot re-enter its own output.

## seed_donor_formulas: each workbook uses its own column position

When a column is NAMED, the resolver looked it up in both workbooks and, if
the positions differed, warned and then used the DONOR's letter for the
target - writing formulas into whatever column happened to sit there.
Exposed by inserting a column into the VMS output: everything after it
shifted, and the seed step tried to write a formula over 'Australia'. It
failed loudly only because on_existing_cell defaults to error; with 'skip'
or 'overwrite' it would have silently corrupted data.

The resolver now returns (donor, target) letter pairs - read from the
donor's position, write to the target's. Naming a column means "find it
wherever it lives". Explicit letters are still taken literally on both
sides. The divergence is now an INFO line, since it is normal and handled.

Full suite: 20 failures, baseline.

# End of file #
