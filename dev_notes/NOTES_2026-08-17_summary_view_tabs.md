# Summary view tabs + injector xlpm wiring (2026-08-17)

dev_notes/NOTES_2026-08-17_summary_view_tabs.md

Exp_Summ and Exp_Summ_CMA replicated as FILTER-spill-style live view
tabs (Exp_Summ_View, Exp_Summ_CMA_View), placed immediately right of
their static parents. Originals KEPT: the user judges whether the
read-only views suffice, and only then do the static tabs retire for
the file-size win.

## Framework fix in this TGZ: cell-level LET/LAMBDA storage

The storage drill's audit caught a cell-injected LET stored with a
bare declared name ("LET declaration 'b' lacks _xlpm."). Root cause:
the 2026-08-14 refusal guard in inject_formulas was retired in favor
of the xlpm_name_storage transformer, but only manage_named_objects
ever called it - the injector's live path never did. Second finding
while wiring it: the transformer must run FIRST, on the DISPLAY form,
because prefix_future_functions rewrites LET( to _xlfn.LET(, which
the name scanner does not recognize.

Order in _apply_formula_to_sheet is now: transform_xlpm_names (live
only) -> prefix_future_functions -> transform_storage_forms (live
only) -> column placeholders. Stale refusal-era comment corrected.
Suites green: inject_formulas, dynamic_array_metadata,
declaration_lambda_and_registry, session_declaration_wiring,
manage_named_objects, verify_excel_storage, xlpm_adversarial.

## Open question recorded: eta storage inside definedNames

manage_named_objects' import path does NOT eta-transform definitions,
and _xleta. forms were harvested from real Excel CELL storage only -
their shape inside a definedName has never been verified. So the
library's GROUPBY uses FULL LAMBDAS for both aggregation slots
(LAMBDA(v, ROWS(UNIQUE(v))) beside LAMBDA(w, SUM(w))) instead of a
bare SUM: proven storage, zero guesswork. If a bare eta name in a
definition is ever wanted, harvest real Excel output first, then
teach both the import transform and the audit.

## The derivation (library, delivered alongside)

fml_exp_summ_base (named formula): the Exp_Summ pipeline live -
GROUPBY over the seven keys (Booking, Tracking, Carrier, Customer,
Product Origin, Fishery Group, Destination) with distinct-van count
and net-weight sum, filtered to Booking<>"", sorted by every key in
order ({1,2,3,4,5,6,7}), no headers (the frame supplies them), no
totals; HSTACK(SEQUENCE(...)) numbers the grouped order = Sort Order.

fn_summ_display (lambda): blank-repeats display treatment - shift-
compare (prev = "" stacked over column minus last row) blanks Booking
and Tracking on continuation rows, mirroring clean_data blank_repeats.

Views (recipe, delivered alongside):
  Exp_Summ_View!A2     =fn_blank_safe(fn_summ_display(fml_exp_summ_base))
  Exp_Summ_CMA_View!A2 =LET(b, fml_exp_summ_base,
      fn_blank_safe(fn_summ_display(FILTER(b, CHOOSECOLS(b,4)="CMA",
      "no CMA rows"))))
CMA inherits both sibling-derivation rules from the static pipeline:
Sort Order keeps parent numbering with gaps (filter AFTER numbering),
blanking computed WITHIN the subset (display AFTER filter).

## Recipe additions (78 steps, was 74)

- 7 new Customer-anchored ranges (booking/tracking/carrier/prodorig/
  fishgrp/dest/vannum; customer + netwt already existed) - one shared
  extent per the 2026-08-14 house note.
- Zero-row header frame from stg_export_summary_ordered (impossible-
  filter pattern) + summary width profile (stg_summ_sheet_profile,
  NOT freed), both BEFORE the summary free step.
- Two sheets_to_create entries right of Exp_Summ_CMA.
- Two inject_formulas steps; formatting entries with inherited widths,
  direct navy header keys (NOT tpl_header - auto_filter cannot
  reorder a spill), freeze, whole_column number formats for the spill
  columns, paler family blues (E3F2FB / C4E0F4).

## Drill (real pipeline, synthetic 5-row seed)

ranges -> library import -> both injections -> verify_excel_storage:
audit CLEAN on will-be-written bytes; both anchors cm="1" with
metadata.xml under declare_dynamic_formulas (matching the recipe);
stored base verified byte-level (_xlfn.GROUPBY, _xlpm-prefixed
lambda params, sort array intact, zero bare eta names).

## Known limits, stated

- GROUPBY's text collation may order keys slightly differently than
  pandas' sort in edge cases (case/locale); the views recompute from
  live data, so tiny ordering differences vs the static tabs are
  possible and are the user's acceptance call.
- The views are read-only projections: no per-row edits, no filter
  UI reordering - same economics and limits as Exp_View.

# End of file #
