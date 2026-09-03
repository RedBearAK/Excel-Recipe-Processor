# Writing a new processor

Read this before adding a processor. It is short because the framework
enforces most of it: a processor in the wrong family, with an undeclared
key, or with a selector construct its family does not offer, fails when
its module is imported - not when a recipe runs.

## 1. Pick the family by what the step addresses

| The step... | Family class | Contributes | May name columns by |
|---|---|---|---|
| reads one stage, returns one stage | `TransformBaseProcessor` | `source_stage`, `save_to_stage`, `confirm_stage_replacement` | header name only (`name_list`) |
| reads one stage, writes nothing (a check) | `TransformBaseProcessor` with `writes_stage = False` | `source_stage` | header name only |
| creates a stage from outside the pipeline (file, inline data, profile) | `ImportBaseProcessor` | `save_to_stage`, `confirm_stage_replacement` | header name only |
| consumes a stage into a file | `ExportBaseProcessor` | `source_stage` | header name only |
| opens a workbook and changes it in place | `FileOpsBaseProcessor` | nothing (declare `target_file` yourself) | name (`column_names`), positional ref (`column_refs`), or an ordered typed list |
| frees or copies stages, touches no data | `BaseStepProcessor` (family `base`) | nothing (declare stage keys yourself, e.g. kind `stage_release`) | - |

Two rules of thumb:

- A processor that needs BOTH a file and a stage is a bridge (Import or
  Export), never a Transform with a file key bolted on.
- A processor that wants two families depending on a mode is two
  processors. `verify_data` became `verify_stage_data` and
  `verify_sheet_data`; `inject_formulas` `mode: dead` is the remaining
  candidate.

## 2. Declare the schema

```python
from excel_recipe_processor.core.base_processor import StepProcessorError, TransformBaseProcessor
from excel_recipe_processor.core.config_schema import Key, Schema, name_list


class DELETE_THIS_DELETE_THIS_DELETE_THIS:

    @classmethod
    def config_schema(cls) -> Schema:
        """Declared keys (date); one line on anything non-obvious."""
        rule = Schema([
            Key('when', 'str', required=True),
            Key('then', 'list', required=True, item_kind='any'),
        ])
        return Schema([
            name_list('columns', required=True),
            Key('mode', 'str', default='strict', choices=['strict', 'lenient']),
            Key('pandas_rules', 'list_of_mappings', schema=rule),
            Key('lookup_stage', 'stage_in'),
        ], at_least_one=[['columns', 'pandas_rules']])
```

The family's keys are merged in for you; never redeclare `source_stage`
or `save_to_stage`. Extra stages a processor reads or writes are its own
keys with kinds `stage_in` / `stage_out`, so the stage graph sees them.
Output stages whose names are computed at run time are returned from
`computed_stage_writes(config)`.

Kinds: `str`, `int`, `number`, `bool`, `list` (with `item_kind`),
`mapping` (closed, its own `Schema`), `open_mapping` (keys are the
author's data - column names, rename maps), `list_of_mappings`, `any`
(unchecked; use rarely and say why), `stage_in`, `stage_out`,
`stage_release`.

A key whose value selects which sibling keys are legal is a variant:

```python
        return Schema([
            Key('slice_type', 'str', required=True, choices=['row_range', 'column_range']),
        ], variants={'slice_type': {
            'row_range': Schema([Key('start_row', 'int', default=1), Key('end_row', 'int')]),
            'column_range': Schema([Key('start_col', 'any'), Key('end_col', 'any')]),
        }})
```

## 3. Naming rules (the schema enforces shape; you enforce these)

- One key per concept. No aliases, no "also accepts". A rename is a
  breaking change made in the same commit as the recipe updates.
- An evaluated string never sits under a bare key: `pandas_formula`,
  `pandas_rules`, `pandas_default`, `excel_formula`. Plain names (`when`,
  `then`) are structure inside a dialect-declared container.
- Column-name lists are lists of strings, never positions. In `file_ops`,
  `column_names` and `column_refs` are siblings; a header named `BQ` is a
  name, a letter `BQ` is a ref, and the resolver refuses a bare letter
  shape in a name list.
- Enum values are snake_case ERP vocabulary; a library's own spelling
  (`greaterThan`, `notBetween`) is storage, never recipe vocabulary.
- `case_sensitive: false` is the default everywhere; `ignore_case` does not exist.
- Singular for one, plural for many: `sheet_name`, `sheet_names`,
  `sheets_to_create`, `columns_to_keep`.
- File roles: `input_file` read once, `output_file` created, `target_file`
  changed in place, `source_file` / `template_file` read as a donor.
- `on_*` policies take values from one set: `error`, `warn`, `skip`, plus
  a processor's own specific actions (`create_empty`, `replace`, `halt`).

## 4. Ship it with

- `get_minimal_config()` that validates against the schema (the tests instantiate it).
- `_examples/<name>_examples.yaml` whose every step validates
  (`tests/test_examples_validate_against_schemas.py` runs it).
- A standalone test module in house style (no pytest style, no `unittest`).
- `--export-schemas md` shows the result; that rendering is what the next
  author reads.
