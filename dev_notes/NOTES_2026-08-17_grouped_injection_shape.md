# sheets_to_receive_formulas: grouped injection shape (2026-08-17)

dev_notes/NOTES_2026-08-17_grouped_injection_shape.md

SUPERSEDES the same-day sheets_to_inject TGZ (do not push that one;
this delivery contains the final name throughout).

Ruling from the six-injection-steps review: inject_formulas was the
only multi-sheet workbook processor still shaped one-directive-per-
step. The grouped sibling shape fixes that.

## Naming (two rulings, recorded)

First pass borrowed export_file's precedent as sheets_to_inject -
rejected on grammar: the verb attached to the wrong noun. It is not
sheets being injected; the sheets already EXIST and RECEIVE formulas.
sheets_to_create works precisely because its entries ARE sheets being
created. Candidates weighed: formulas_to_inject reads naturally
against the processor name but misdescribes the structure (the list's
entries are per-sheet groups, not formulas); sheets_to_receive_formulas
says exactly what the list contains - sheets, each receiving formulas.
Longer, and self-explanatory beats short, per the columns_to_keep
doctrine. The entry ANATOMY still mirrors sheets_to_create
(sheet_name + payload), so the two shapes rhyme where it helps a
reader and diverge where the grammar demands it.

## Why nesting, not repeated keys (recorded from the discussion)

A YAML mapping's keys must be unique per level: a second sheet_names:
beside the first is not iteration - PyYAML silently keeps the LAST
duplicate and discards the rest, no error, no warning. A sequence of
entries is YAML's only repeatable-group construct, which is why
format_excel's formatting: and export_file's sheets_to_create: have
this shape already.

## The vocabulary

    sheets_to_receive_formulas:  # grouped: per-sheet formulas
      - sheet_name: "Exp_View"
        formulas:
          - cell: "A2"
            formula: '=...'
      - sheet_name: "Dom_View"
        formulas: [...]

    sheet_names: [...]           # broadcast: SAME formulas to many
    formulas: [...]              # sheets - unchanged, still legal

Exactly ONE shape per step: both given -> guided error; grouped shape
with awaken mode -> guided error (awaken addresses sheets with
sheet_names). Entry validation fails loud naming the entry index;
unknown per-entry keys are named, not ignored (mode and target_file
are step-level). Per-sheet failures are attributed:
"sheets_to_receive_formulas entry 2 (sheet 'View_B'): ...". The
completion log enumerates per-sheet counts, so a consolidated step
still tells the story tab by tab: "injected live formulas across 2
sheet(s) - Exp_Summ_View: 1; Exp_Summ_CMA_View: 1".

Implementation note: _inject_grouped iterates entries around the
UNCHANGED per-formula path (_apply_formula_to_sheet - the freshly
drilled xlpm/prefix/storage pipeline), with the same live-cell
registration and mark_dirty flow as the broadcast path.

## Verification

tests/test_inject_formulas_grouped.py (6/6): per-sheet targeting,
shape exclusivity, entry-index naming, unknown-key refusal, per-sheet
failure attribution, broadcast regression. Collateral green:
inject_formulas, all_processor_examples (grouped example under the
schema's yaml field; CLI renders it). Storage drill re-run through
the renamed shape: audit clean on will-be-written bytes, both summary
view anchors cm-marked.

## Delivered alongside (recipe project file, not in this TGZ)

vms_process.yaml: the five read-only view injections (Exp_View,
Dom_View, Cust_List, Exp_Summ_View, Exp_Summ_CMA_View) consolidated
into one sheets_to_receive_formulas step; the interactive Cust_Summ
pair keeps its own step - genuinely a different story. 79 -> 75 steps.

# End of file #
