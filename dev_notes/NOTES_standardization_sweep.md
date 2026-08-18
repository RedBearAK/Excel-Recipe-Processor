# Standardization sweep

No behaviour changes. Structure and documentation only.

## What was actually missing

A survey of five methods across all 29 registered processors found far less
inconsistency than expected. Two of the five are not standards at all:

- **`get_supported_features`** — present on 1 of 29. A `format_excel` one-off,
  not a convention. Left alone.
- **`get_operation_type`** — present on 4 of 29, and all four are FileOps
  processors. It is a FileOps convention, correctly absent elsewhere. Left alone.

The real gaps were narrow:

| Gap | Processor | Effect |
|---|---|---|
| `get_minimal_config` | `manage_named_objects` | dropped out of capability discovery entirely |
| examples file | `manage_named_objects` | `--get-usage-examples` returned nothing |
| stray file | repo root | `add_calc_col_examples.yaml`, a duplicate outside the examples folder |

`export_file` and `import_file` appeared to lack `get_capabilities` and
`get_usage_examples`, but both resolve through the base class and the filename
convention, and both have example files. Not gaps.

**Capability discovery now reports 29 of 29 processors with zero errors.**

## Example files: 171 examples, 4 were broken

167 of 171 were already complete runnable recipes. The four exceptions were all
in `seed_donor_formulas_examples.yaml` — bare step fragments with no `settings:`
section, which the rules file lists as a hard requirement. Wrapped into complete
recipes.

One further defect: `export_file_examples.yaml`'s `multi_sheet_example` omitted
`source_stage`. Multi-sheet mode still needs it — the `sheets` list says what
goes on each tab, but `source_stage` is what the step consumes. That example had
never validated.

Every example in the five touched files now validates cleanly:

```
export_file              6/6
seed_donor_formulas      7/7
manage_named_objects     6/6
format_excel             7/7
clean_data               8/8
```

## New functionality now documented

None of the options added during the VMS work had reached the example files.

**`manage_named_objects_examples.yaml`** — new file, six examples covering
list, export, `create_from_columns`, import, `copy_direct` and `validate_yaml`,
plus parameter details, the four `row_mode` values, and the two limitations
worth knowing before relying on it: tables are not created, and named ranges
must exist before any formula referencing them.

**`seed_donor_formulas_examples.yaml`** — `fill_down`, `fill_anchor_columns`,
`array_formula_mode`, `on_existing_cell`. Plus notes on the four behaviours that
are surprising until explained: `start_row` applying to both files, a short
donor not leaving a gap, a constant not being filled down, and an empty
transplant being an error rather than a warning.

**`format_excel_examples.yaml`** — `column_formats` with all eleven rule keys,
`hidden_columns`, the thirteen number format aliases, and the ordering rules
inside the processor (formats before auto-fit, explicit widths after, hiding
last) along with why auto-fit ignores formula cells.

**`export_file_examples.yaml`** — `template_file` mode, with what survives an
openpyxl round trip, what does not, and the warning against chaining outputs
into templates.

**`clean_data_examples.yaml`** — the `columns: "*"` form, and why it is only
safe because typed columns are skipped.

**`recipe_settings_examples.yaml`** — `{recipe_dir}` and `{recipe_parent_dir}`
for recipes that run from any directory, the full built-in variable list with a
note on why `input_basename` does not resolve, and every command-line option
including `--set`, `--dump-stage` with its four row specs, `--stop-after` and
`--list-stages`.

## Not done

`get_minimal_config` over-states requirements on 11 of 28 processors, listing
keys that have defaults. That blocks using it directly as a required-parameter
source for validation. Correcting it means auditing 11 processors and deciding
per key whether it belongs, which is a judgement call per processor rather than
a sweep.

# End of file #
