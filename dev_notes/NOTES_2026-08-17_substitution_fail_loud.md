# Pipeline substitution swallow fixed; VMS recipe swept (2026-08-17)

dev_notes/NOTES_2026-08-17_substitution_fail_loud.md

The list-family retirement met production and exposed a framework bug
in the process - the migration working exactly as intended, and better
than intended.

## The bug the production log exposed

RecipePipeline._substitute_variables_in_config caught ALL substitution
exceptions, logged a WARNING, and returned the ORIGINAL config. So
when the retired {list:var_expected_download_columns} raised its
guided error, the guidance scrolled past as a warning, the
unsubstituted config flowed onward, and verify_columns halted on a
misleading shape complaint ("requires exactly one of
'expected_columns' (a list) or ...") - because it was looking at the
literal template STRING, not a list.

Fixed: substitution failure on a step config now raises
StepProcessorError carrying the underlying error as the halt. The
guided messages (retirement, unconvertible members, bracket typos,
unknown variables) are now what the operator SEES, which is their
entire purpose. Chained with `from e` for the traceback.

Pinned by a new test in tests/test_typed_list_references.py (now 6/6):
the pipeline layer must surface the retirement guidance AS the
StepProcessorError. Pipeline-touching collateral verified green:
test_recipepipeline_functionality, both integration suites,
interactive variables, comprehensive substitution.

## VMS recipe swept (delivered alongside, NOT in this TGZ)

vms_process.yaml: all nine bare {list:...} sites -> {list_str:...},
verified per-site against their variable declarations (all nine are
string lists: expected download columns, the five drop lists, temp key
columns, final order, columns to create). The full recipe was then
substituted end to end against its own settings variables: clean, with
the column-list sites landing as list[str].

cma_invoices_process.yaml: audited, ZERO bare structural references -
no changes needed.

vms_named_lambdas.yaml: no variable templates at all - untouched.

# End of file #
