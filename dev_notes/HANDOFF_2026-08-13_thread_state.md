# HANDOFF: 2026-08-13 thread - dynamic arrays, reshape, CF, verify_data

State capture for the next working session. The companion note
NOTES_dynamic_array_declaration.md carries the pass-by-pass technical
detail; this is the orientation layer.

## Where things stand

Framework at 39 processors on dev_beta + this thread's patch set
(erp_framework_current.tgz, extracting at repo root - the running SUPERSET
of the whole thread; each rebuild replaced the previous archive).

Shipped and Excel-verified by the user:
- Dynamic-array declaration (cm="1" + XLDAPR metadata): the @ is gone from
  World Region, Country, AND SALE TYPE1 - the last via injection
  provenance (recipe-authored cells declared regardless of function).
  Enabled recipe-wide via settings declare_dynamic_formulas: true.
- All seven formula columns injected from the recipe with {col:} names;
  donor and seed step retired from vms_process.yaml (the
  seed_donor_formulas PROCESSOR remains, recipe just stopped using it).
- conditional_format proving-ground rule (contract-without-price row
  highlight on VMS_Data) confirmed working, formula clean:
  =AND($AA2<>"", $AC2="").

Shipped, awaiting first real-run eyes:
- verify_data (this session's last build): warn/halt per rule, full
  filter_data vocabulary via live borrow, stage AND file modes, run-end
  🔎 summary via core/verification_ledger.py. NOT yet wired into either
  production recipe - see next steps.
- columns_to_rows / rows_to_columns reshape pair ("unpivot" deliberately
  not used as a name).
- slice_data transpose; tab colors (yellow data / blue summaries / green
  lookups / plain Source_Files); No_Price_Product_Summary tab (8 columns,
  three exclusions); recipe element count log line; packaging repair
  (pyproject [project], deps dynamic from requirements.txt, harmful
  formatter tooling removed); docs currency pass (all 38... now 39
  processors have examples YAMLs).

## Recipes

- vms_process.yaml: current copy in the conversation outputs. Header
  comments modernized (phase framing retired, filename fixed, counts
  corrected). Settings: declare_dynamic_formulas: true.
- cma_invoices_process.yaml: imports the standalone
  lookup_source_files/export_summary_cma.xlsx (vms_processed prompt
  removed). The export summary now carries Fishery Group instead of Major
  Species - a non-event for this recipe (reads only Booking + Carrier
  Tracking No).

## Natural next steps (in rough order)

1. Wire verify_data into the CMA recipe: the "hard fail-loud verification
   of unresolved lookups" horizon item is now one step -
   {column: SHIP REF, condition: not_empty, severity: halt} on the
   enriched stage (plus the BL Ref twin). Also a candidate on the VMS
   side: Carrier in_stage against the carriers lookup.
2. CMA horizon remainder: drift alarm via diff_data against previous
   output; curated payables filter (groups containing Open/Overdue;
   six-credit exclusion stays human-managed).
3. Test-debt cleanup thread (deliberately deferred, "hasn't really hurt
   anything so far"): ~20 modules fail on PRISTINE dev_beta. Triage done
   2026-08-13, in categories: (a) ~11 stale-config tests predating
   now-required fields (sort_type, source_stage, list-shaped formatting) -
   repair or retire; (b) test_load_stage_processor tests a DELETED module
   - retire; (c) test_basic imports pytest, likely passes on the dev
   machine (sandbox artifact; test_openpyxl_performance possibly similar);
   (d) needs real investigation: test_fast_column_config ('' vs 'Empty: 0'
   header expectation - probably deliberate feature drift),
   test_filter_terms_detector (auto-detection empty - sklearn drift or
   regression?), test_cli_integration (output format drift),
   test_usage_examples (its "8 missing examples" claim is ITSELF stale -
   coverage is complete as of this thread).
4. conditional_format deferred extras, user's caution recorded: any
   clear_existing semantics need their own careful design; also
   top/bottom-N, above/below average, icon sets, case-sensitive text,
   date operands. _xlfn-in-CF-formulas remains unverified in Excel (moot
   until a modern function appears in a rule; the prefix pass already
   rides along).
5. Back burner, user's call: merge/split-per-group processor (prior art:
   https://github.com/RedBearAK/Simple-Excel-Merge); directive-count
   logging upgrade (per-processor semantic counts; the YAML element
   counter in recipe_pipeline.count_step_elements becomes its fallback);
   verify_data possible growth (row_count rules, a not-formula-aware
   file-mode warning if a rule targets a column whose cells are formulas).

## Stage-key families (post-standardization, 2026-08-13)

Bare 'stage' is gone. source_stage/save_to_stage/lookup_stage for
step-level flow (copy_stage and create_stage included, as of the
thirteenth pass); 'stage_name' only for settings declarations and rule/
sub-config references. copy_stage was latently pipeline-broken before the
sweep - see the notes file's thirteenth pass before touching it.

## Sharp edges worth re-reading before touching related code

- pandas 3 null detection in the CMA SHIP REF coalesce: positive
  contains "" test, NOT not_contains "nan" - dated do-not-simplify
  comment in the recipe.
- The dynamic-array metadata bytes in core/dynamic_array_metadata.py are
  VERBATIM Excel output (and byte-identical to xlsxwriter's) - do not
  reformat.
- WorkbookSession.flush_all() must NOT full-reset() - that silently
  dropped _deferred mid-run once already (fixed this thread; comment at
  the site).
- inject_formulas' _store_formula is the ONE live-write funnel; the
  provenance registry depends on every writer routing through it.
- verify_data borrows filter_data._apply_filter via a shim instance -
  deliberate live reuse; if filter_data's seam changes shape, verify_data
  follows.

# End of file #
