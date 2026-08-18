# sheets_to_receive_formulas: THE live injection shape (2026-08-17)

dev_notes/NOTES_2026-08-17_grouped_injection_shape.md

SUPERSEDES both same-day grouped-injection TGZs (sheets_to_inject and
the renamed transitional one). This delivery contains the final
vocabulary throughout: per-entry sheet_names LISTS, broadcast retired.

## Evolution in three rulings, recorded

1. Six one-directive injection steps -> a grouped shape, initially
   sheets_to_inject by anatomy-parallel to export_file's
   sheets_to_create.
2. Naming: the verb attached to the wrong noun (sheets are not
   injected; they already exist and RECEIVE) ->
   sheets_to_receive_formulas. formulas_to_inject was weighed and
   rejected: the list's entries are per-sheet groups, not formulas.
3. Per-entry multi-sheet: each entry takes a sheet_names LIST
   (plural REQUIRED, one spelling per the columns_to_keep doctrine;
   singular sheet_name and bare-string values get guided errors).
   With that, ONE entry naming several sheets IS the old broadcast -
   so the grouped shape subsumes it, and the top-level
   sheet_names + formulas pair is RETIRED for live mode with a
   guided error. This also removed the implicit active-sheet
   default, the last silent addressing in injection. Note the
   deliberate divergence from sheets_to_create's singular entry key:
   created sheets map 1:1 to a data_source and MUST be singular;
   receiving sheets are legitimately N:1 and MAY be plural - the
   grammar diverges, not the style.

## Why nesting, not repeated keys (recorded from the discussion)

A YAML mapping's keys must be unique per level: a second sheet_names:
beside the first is not iteration - PyYAML silently keeps the LAST
duplicate and discards the rest, no error, no warning. A sequence of
entries is YAML's only repeatable-group construct.

## The vocabulary (live mode; dead keeps top-level formulas on the
stage path; awaken keeps top-level sheet_names addressing)

    sheets_to_receive_formulas:
      - sheet_names: ["Exp_View"]          # LIST, even for one
        formulas:
          - cell: "A2"
            formula: '=...'
      - sheet_names: ["Notes_A", "Notes_B"]  # one entry, many sheets
        formulas: [...]                      # = the old broadcast

Guided errors: retired top-level pair (teaches the one-entry form);
missing sheets_to_receive_formulas in live mode; singular sheet_name;
bare-string sheet_names; entry missing either field (names the entry
index); unknown per-entry keys (named, not ignored); per-sheet
failures attributed "entry N (sheet 'X'): ...". Completion log
enumerates per-sheet counts.

## Verification

tests/test_inject_formulas_grouped.py rebuilt (8/8): per-sheet
targeting, one-entry-many-sheets, broadcast retirement, singular-key
guidance, scalar-value guidance, entry-index naming, unknown-key
refusal, per-sheet attribution. Main suite's two live configs
migrated (they had relied on the retired implicit active sheet).
Twelve-suite collateral ring green including all grammar/storage
suites and both example checkers. All four live examples in
inject_formulas_examples.yaml migrated (dead/awaken untouched);
grouped example demonstrates the one-entry-many-sheets form. Storage
drill re-run through the final shape: audit clean, anchors cm-marked.

## Delivered alongside (recipe project file, not in this TGZ)

vms_process.yaml: all three live inject steps on the sole shape -
the five-view grouped step (entries now carry sheet_names lists),
the seven-formula VMS step and the Cust_Summ step (both converted
from broadcast). 75 steps; the recipe validator reports the same 5
pre-existing no-substitution issues as baseline.

# End of file #
