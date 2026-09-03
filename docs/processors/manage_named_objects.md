# `manage_named_objects`

**Family:** `file_ops`

Manage Excel named ranges, formulas, lambda functions, and tables

## Keys

Generated from the declared schema; keys not listed are refused at recipe load.

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy
- `operation`: str; REQUIRED; one of export_all, export_filtered, import_all, import_filtered, list_objects, validate_yaml, copy_direct, create_from_columns
- `name_validation`: str; default "excel"; one of none, excel, house
- when `operation` = `export_all`:
  - `source_file`: str; REQUIRED
  - `yaml_file`: str
  - `vba_file`: str
  - `export_formats`: mapping - Alternative grouped form of the two output paths
    - `yaml_file`: str
    - `vba_file`: str
- when `operation` = `export_filtered`:
  - `source_file`: str; REQUIRED
  - `yaml_file`: str
  - `vba_file`: str
  - `include_patterns`: list of str
  - `exclude_patterns`: list of str
- when `operation` = `import_all`:
  - `target_file`: str; REQUIRED
  - `yaml_file`: str; REQUIRED
  - `on_existing`: str; default "error"; one of error, replace, skip
  - `prune_orphans_with_prefix`: str
- when `operation` = `import_filtered`:
  - `target_file`: str; REQUIRED
  - `yaml_file`: str; REQUIRED
  - `on_existing`: str; default "error"; one of error, replace, skip
  - `include_patterns`: list of str
  - `exclude_patterns`: list of str
  - `prune_orphans_with_prefix`: str
- when `operation` = `list_objects`:
  - `source_file`: str; REQUIRED
- when `operation` = `validate_yaml`:
  - `yaml_file`: str; REQUIRED
- when `operation` = `create_from_columns`:
  - `target_file`: str; REQUIRED
  - `ranges`: list_of_mappings; REQUIRED
    - `name`: str; REQUIRED - Defined name to create
    - `sheet_name`: any; REQUIRED - Tab name, number, or ?sheet_NNN? token
    - `column_names`: list of str; REQUIRED - Header names spanning the range
    - `anchor_columns`: list of str - Columns to measure the row extent from; default the range columns
    - `row_mode`: str; default "data_with_header"; one of data, data_with_header, full_col, full_col_no_header
    - `header_row`: int; default 1
    - `expand_span`: bool; default true
    - `absolute`: bool; default true
    - `on_missing`: str; default "error"; one of error, warn, skip
    - `scope`: str; default "global"; one of global, local
    - `name_mgr_comment`: str - Name Manager comment text
  - `on_existing`: str; default "error"; one of error, replace, skip
  - `prune_orphans_with_prefix`: str
- when `operation` = `copy_direct`:
  - `source_file`: str; REQUIRED
  - `target_file`: str; REQUIRED
  - `include_local`: bool; default true
  - `include_patterns`: list of str
  - `exclude_patterns`: list of str
  - `on_existing`: str; default "error"; one of error, replace, skip

## Examples

Every step below validates against the schema (tests/test_examples_validate_against_schemas.py).

### basic

List every named object in a workbook

```yaml
# Report what a workbook already defines, without changing anything.
# Useful for auditing an inherited file before automating around it.

settings:
  description: "Inventory the named objects in a lookup workbook"

recipe:
  - step_description: "List named objects"
    processor_type: "manage_named_objects"
    # REQ - Operation to perform
    operation: "list_objects"
    # REQ - Workbook to read
    source_file: "lookup_tables.xlsx"
```

### export

Export named objects to YAML, and to a VBA-friendly format

```yaml
# export_all writes both a YAML file for humans and recipes, and a
# pipe-delimited file that VBA can parse without a YAML library.

settings:
  description: "Capture the named objects from a workbook"

recipe:
  - step_description: "Export all named objects"
    processor_type: "manage_named_objects"
    operation: "export_all"
    source_file: "reference_workbook.xlsx"
    # OPT - YAML output path
    yaml_file: "named_objects.yaml"
    # OPT - Pipe-delimited output for VBA
    vba_file: "named_objects.txt"
```

### create from columns

Define named ranges from column names on generated data

```yaml
# The operation that makes named ranges survive a regenerated file.
#
# Rather than copying a fixed address, each range is recomputed from the
# actual data extent on every run. A hand-maintained range goes stale the
# moment someone appends a row; a regenerated one cannot.
#
# Must run AFTER the workbook exists, and BEFORE any formula that
# references these names. Excel resolves a formula naming a range that does
# not exist to #NAME?, and openpyxl writes it without complaint.

settings:
  description: "Define lookup ranges on a freshly written workbook"
  variables:
    output_file: "output/report.xlsx"

recipe:
  - step_description: "Define the lookup named ranges"
    processor_type: "manage_named_objects"
    operation: "create_from_columns"
    # REQ - Workbook to write the names into
    target_file: "{output_file}"
    # OPT - "error" (default), "replace", or "skip"
    on_existing: "replace"
    # OPT - Delete inherited names with this prefix that are not defined
    # below, instead of only reporting them. Useful when the workbook was
    # copied from a template that may carry renamed leftovers.
    prune_orphans_with_prefix: "rng_"
    # REQ - The ranges to define
    ranges:
      - # REQ - Defined name. House style: rng_ prefix, and a separator
        # before any digit so the name can never read as a cell reference
        name: "rng_PID"
        # REQ - Sheet holding the column
        sheet_name: "Product_IDs"
        # REQ - Column names, or Excel column letters
        column_names: ["Product ID"]
        # OPT - "data" (default: data rows only), "data_with_header",
        # "full_col", or "full_col_no_header"
        row_mode: "data"

      - # Adjacent entries expand into a span covering everything between,
        # so this becomes $B$2:$E$854 rather than two separate columns
        name: "rng_payterms"
        sheet_name: "Sales_Orders"
        column_names: ["Payment Terms", "Deposit Application"]
        row_mode: "data"
        # OPT - Columns used to measure the extent. Defaults to the columns
        # being defined, which truncates a sparse column - anchor on a dense
        # one instead.
        anchor_columns: ["SO No."]

      - # full_col_no_header has no extent to fall behind, which suits a
        # validation list someone extends by hand
        name: "rng_carrier"
        sheet_name: "Carriers"
        column_names: ["Carrier"]
        row_mode: "full_col_no_header"
```

### import

Recreate named objects in another workbook from a YAML export

```yaml
# The reverse of export_all. Names are validated before being written,
# because Excel silently rejects some and refuses to open files containing
# others.

settings:
  description: "Copy named objects into a generated workbook"

recipe:
  - step_description: "Import named objects from YAML"
    processor_type: "manage_named_objects"
    operation: "import_all"
    # REQ - YAML file produced by export_all
    yaml_file: "named_objects.yaml"
    # REQ - Workbook to write into, which must already exist
    target_file: "output/report.xlsx"
    # OPT - "error" (default), "replace", or "skip"
    on_existing: "replace"
    # OPT - "none", "excel" (default), or "house"
    #   excel  Excel's own rules: no spaces, cannot look like a cell
    #          reference, R and C are reserved
    #   house  adds a three character minimum and requires a separator
    #          before digits, so TAX24 must be TAX_24
    # Names read out of a foreign workbook are already valid by
    # construction, so "excel" is the sensible level for an import.
    name_validation: "excel"
```

### copy direct

Copy named objects straight from one workbook to another

```yaml
# Skips the YAML round trip when the source workbook is available.

settings:
  description: "Transfer named objects between workbooks"

recipe:
  - step_description: "Copy named objects"
    processor_type: "manage_named_objects"
    operation: "copy_direct"
    # REQ - Workbook to read from
    source_file: "reference_workbook.xlsx"
    # REQ - Workbook to write to
    target_file: "output/report.xlsx"
    on_existing: "replace"
    # OPT - Restrict to matching names
    include_patterns: ["rng_*"]
```

### validate

Check a YAML export without writing anything

```yaml
# Reports every problem found rather than stopping at the first, so a
# recipe author sees the whole picture in one pass.

settings:
  description: "Validate named object definitions before use"

recipe:
  - step_description: "Validate the YAML export"
    processor_type: "manage_named_objects"
    operation: "validate_yaml"
    yaml_file: "named_objects.yaml"
    name_validation: "house"
```

## Parameter notes

- `operation` (required): Which operation to perform
- `source_file`: Workbook to read named objects from
- `target_file`: Workbook to write into. Must already exist - named objects are added to a workbook, not used to create one.
- `yaml_file`: YAML output path for export operations
- `vba_file`: Pipe-delimited output path, for VBA that cannot parse YAML
- `ranges`: List of range definitions, each naming a sheet and its columns
- `on_existing` (default `error`): What to do when a name already exists
- `name_validation` (default `excel for import and copy, house for create_from_columns`): How strictly names are checked before writing
- `prune_orphans_with_prefix`: For create_from_columns. Delete inherited names carrying this prefix that this step does not define. Without it they are only reported. Matters when the target was copied from a template, since a name renamed by hand rides along and the regenerated name appears beside it.
- `include_patterns`: Wildcard patterns limiting which names are handled
- `exclude_patterns`: Wildcard patterns excluding names from being handled
- `include_local` (default `True`): Include sheet-scoped names as well as workbook-scoped ones

