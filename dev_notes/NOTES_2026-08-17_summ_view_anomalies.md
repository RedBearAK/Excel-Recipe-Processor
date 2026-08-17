# Summary view anomalies: one bug, one collation truth, one swallow

dev_notes/NOTES_2026-08-17_summ_view_anomalies.md

Production observation on the new summary view tabs: Exp_Summ_View's
first spill row (Sort Order 1) had blank keys with CUSTOM1/CUSTOM2 in
the two value columns; Exp_Summ_CMA_View started at Sort Order 2; and
row order differed from the static tabs.

## Anomalies 1 + 2: one cause - a synthetic GROUPBY header row

In production Excel, GROUPBY emitted a generated header row even with
field_headers 0: blank cells over the seven headerless key ranges and
CUSTOM1/CUSTOM2 over the two LAMBDA-computed value columns (Excel's
auto-names for computed fields). SEQUENCE numbered it Sort Order 1;
the first REAL row became 2. The CMA view then LOOKED healed because
its FILTER on Carrier="CMA" dropped the header row (blank Carrier
fails the test) - starting at the parent's 2, consistent with the
gap-keeping rule. Both symptoms, one row.

Fix (library, delivered alongside): enum-proof rather than
enum-dependent. The base now passes GROUPBY's output through
FILTER(raw, CHOOSECOLS(raw,1)<>"") before numbering: every real row
HAS a Booking (the filter_array guarantees it), so header rows - and
any future totals rows - drop out regardless of how any Excel build
interprets field_headers. Stored form byte-verified
(_xlfn._xlws.FILTER, _xlpm.raw params); drill audit clean.

## Second bug found by the first: importer swallowed a rejected name

The guard's intermediate was first named 'g0'. Letter+digit LET names
look like cell references, which Excel forbids - the storage
transformer correctly REJECTS them. But manage_named_objects wrapped
definition translation in the same try/except as the collision-policy
write: with on_existing != 'error' the rejection became a WARNING,
failed[] was never shown in the completion line ("3 written, 0
replaced, 0 skipped" - no failed count), and the library formula
silently never reached the workbook.

Fixed (this TGZ): definition-translation errors raise UNCONDITIONALLY
- on_existing is a name-COLLISION policy and must not decide whether a
broken definition is fatal. The completion line now appends
", N FAILED: names" whenever the failed list is non-empty. Suite
green. The LET name is 'raw' with the constraint recorded in the
library comment.

## Anomaly 3: row order - collation, not a bug, and a ruling to make

Both sides sort by the same seven keys, but under different collation:
pandas (aggregate_data sort_by_groups default) sorts ASCII
case-sensitive - ALL uppercase before any lowercase - while Excel's
GROUPBY sort is case-insensitive locale collation. Concretely:

  pandas: ACME Fish | Acme | BALTIC-2 | Baltic Star | acme fish co
  Excel:  Acme | ACME Fish | acme fish co | Baltic Star | BALTIC-2

Same keys, two legal orders. Numbers-inside-text (booking codes)
lexicograph identically in both, so the visible divergence comes from
mixed-case values - most plausibly Customer / Destination /
Fishery Group. NOT fixed here: aligning is a ruling, not a patch.
Options, in rough order of honesty:
  (a) Accept the divergence - the views are self-consistent and the
      user is choosing between tabs anyway.
  (b) Align the STATIC side to Excel's collation: sort the aggregate
      output case-insensitively (e.g. casefolded sort keys) so pandas
      matches GROUPBY. Changes the accepted static tabs' order - the
      end user should bless it.
  (c) Chase exact parity in-formula - not recommended; Excel exposes
      no case-sensitive sort, so parity can only come from (b).

# End of file #
