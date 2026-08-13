# seed_donor_formulas: each workbook uses its own column position

Expand at the repo root. WHOLE FILE - replace, don't merge (also carries the
workbook-session and empty-string-is-not-data changes).

## The bug

When a column is named rather than lettered, the resolver looked the name up
in BOTH workbooks - and if the positions differed, it logged a warning and
then used the DONOR's letter for the target anyway:

    ⚠️ Column 'Test Fresh' found at different positions:
       source=AW, target=AX. Using source position.

So formulas were written into whatever column happened to sit at the donor's
letter in the target file. Found the moment REAL_MAJOR_SPECIES was inserted
into the VMS output: every column after it shifted one right, and the seed
step tried to write a formula over 'Australia' in the Country column. It
failed loudly only because on_existing_cell defaults to error - with 'skip'
or 'overwrite' set, it would have silently written formulas into the wrong
columns and overwritten real data.

## The fix

The resolver now returns (donor_letter, target_letter) pairs: formulas are
READ from the donor's position and WRITTEN to the target's. Naming a column
means "find it wherever it lives", which is the whole point of naming it.
An explicit letter is still taken literally on both sides. The divergence is
now an INFO line rather than a warning, because it is normal and handled:

    📋 Column 'Test Fresh': donor AW -> target AX (layouts differ; each side
       uses its own position)

Verified end to end on a recipe whose output has one more column than the
donor: formulas land in the correct target columns, all 15 transplanted,
fill-down follows the target positions.

Full suite: 20 failures, baseline.

# End of file #
