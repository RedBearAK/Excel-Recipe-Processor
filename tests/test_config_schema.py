"""
Test the schema vocabulary and the family rules it enforces.

File: excel_recipe_processor/tests/test_config_schema.py

Covers the four constructs (closed mapping, open mapping, list of
mappings, discriminated variant), the selector constructs, unknown-key
suggestions, stage reference extraction, and the family rules checked at
class definition: a Transform processor cannot hold a positional
selector, a FileOps processor can, and no processor may redefine a
family key.

Run: PYTHONPATH=. python3 tests/test_config_schema.py
"""

import sys

from excel_recipe_processor.core.base_processor import (
    FileOpsBaseProcessor, TransformBaseProcessor,
)
from excel_recipe_processor.core.config_schema import (
    Key, Schema, SchemaDefinitionError, name_list, name_ref_pair, stage_references,
    typed_item_list, validate_config,
)


def report(label: str, good: bool, detail: str = '') -> bool:
    print(f"  {label}: {'OK' if good else 'FAIL'}{'  ' + detail if detail else ''}")
    return good


def test_closed_mapping_rejects_unknown_with_suggestion() -> bool:
    print('\nTesting closed mapping, unknown key suggestion, required, choices, types...')
    schema = Schema([
        Key('new_column', 'str', required=True),
        Key('overwrite', 'bool', default=False),
        Key('mode', 'str', choices=['fast', 'careful']),
        Key('limit', 'int'),
    ])
    errors = validate_config({'new_colum': 'x', 'overwrite': 'yes', 'mode': 'slow', 'limit': 2.5}, schema)
    joined = '\n'.join(errors)
    ok = report('unknown key suggested', "unknown key 'new_colum'; did you mean 'new_column'?" in joined)
    ok &= report('missing required', "missing required key 'new_column'" in joined)
    ok &= report('bool type', 'overwrite: expected bool' in joined)
    ok &= report('choices', "mode: 'slow' is not one of" in joined)
    ok &= report('int rejects float', 'limit: expected int' in joined)
    ok &= report('bool is not int', bool(validate_config({'new_column': 'x', 'limit': True}, schema)))
    ok &= report('clean config passes', validate_config({'new_column': 'x'}, schema) == [])
    return ok


def test_open_mapping_and_nested() -> bool:
    print('\nTesting open mapping, nested mapping, list of mappings...')
    inner = Schema([Key('when', 'str', required=True), Key('then', 'list', required=True, item_kind='any')])
    schema = Schema([
        Key('mapping', 'open_mapping'),
        Key('calculation', 'mapping', schema=Schema([Key('pandas_formula', 'str', required=True)])),
        Key('rules', 'list_of_mappings', schema=inner),
    ])
    ok = report('open mapping takes any keys',
                validate_config({'mapping': {'Any Header': 'X', 'Other': 'Y'}}, schema) == [])
    errors = validate_config({'calculation': {'formula': 'x'}}, schema)
    ok &= report('nested unknown key with path', any(e.startswith('calculation: unknown key') for e in errors))
    errors = validate_config({'rules': [{'when': 'a', 'then': []}, {'wen': 'b', 'then': []}]}, schema)
    ok &= report('list element path', any(e.startswith('rules[2]: unknown key') for e in errors))
    return ok


def test_discriminated_variant() -> bool:
    print('\nTesting discriminated variant...')
    schema = Schema([
        Key('slice_type', 'str', required=True, choices=['row_range', 'column_range']),
    ], variants={'slice_type': {
        'row_range': Schema([Key('start_row', 'int', required=True), Key('end_row', 'int')]),
        'column_range': Schema([Key('start_col', 'str', required=True), Key('end_col', 'str')]),
    }})
    ok = report('branch keys legal', validate_config({'slice_type': 'row_range', 'start_row': 1}, schema) == [])
    errors = validate_config({'slice_type': 'row_range', 'start_col': 'A'}, schema)
    ok &= report('other branch keys unknown', any('unknown key' in e and 'start_col' in e for e in errors))
    ok &= report('branch required enforced', any("missing required key 'start_row'" in e for e in errors))
    return ok


def test_selector_constructs() -> bool:
    print('\nTesting selector constructs...')
    pair_keys, group = name_ref_pair()
    schema = Schema([name_list('columns')] + pair_keys + [typed_item_list('order')], at_least_one=[group])
    ok = report('name list of strings', validate_config({'columns': ['A', 'B'], 'column_names': ['x']}, schema) == [])
    errors = validate_config({'columns': ['A', 2], 'column_refs': ['A']}, schema)
    ok &= report('name list rejects ints', any('columns: expected list of str' in e for e in errors))
    errors = validate_config({'columns': []}, schema)
    ok &= report('pair at least one', any('requires at least one of' in e for e in errors))
    errors = validate_config({'column_names': ['x'], 'order': [{'name': 'A'}, {'ref': 'B'}, {}]}, schema)
    ok &= report('typed item needs name or ref', any('order[3]' in e and 'at least one' in e for e in errors))
    return ok


def test_stage_references_from_schema() -> bool:
    print('\nTesting stage reference extraction...')
    schema = Schema([
        Key('source_stage', 'stage_in'), Key('save_to_stage', 'stage_out'),
        Key('sheets_to_create', 'list_of_mappings',
            schema=Schema([Key('sheet_name', 'str'), Key('data_source', 'stage_in')])),
    ])
    reads, writes, releases = stage_references({
        'source_stage': 'stg_a', 'save_to_stage': 'stg_b',
        'sheets_to_create': [{'sheet_name': 'X', 'data_source': 'stg_c'}]}, schema)
    return report('reads and writes', reads == ['stg_a', 'stg_c'] and writes == ['stg_b'], f'{reads} {writes}')


def test_family_rules_at_class_definition() -> bool:
    print('\nTesting family rules enforced at class definition...')
    ok = True
    try:
        class BadTransform(TransformBaseProcessor):
            @classmethod
            def config_schema(cls):
                keys, group = name_ref_pair()
                return Schema(keys, at_least_one=[group])
            def execute(self, data):
                return data
        ok &= report('transform with column_refs', False)
    except SchemaDefinitionError as error:
        ok &= report('transform with column_refs rejected', 'does not offer' in str(error))
    try:
        class GoodFileOps(FileOpsBaseProcessor):
            @classmethod
            def config_schema(cls):
                keys, group = name_ref_pair()
                return Schema([Key('target_file', 'str', required=True)] + keys + [typed_item_list('order')],
                              at_least_one=[group])
            def perform_file_operation(self):
                return ''
        ok &= report('file_ops with refs and typed list accepted',
                     'column_refs' in GoodFileOps.full_schema().keys)
    except SchemaDefinitionError as error:
        ok &= report('file_ops with refs', False, str(error))
    try:
        class Redefiner(TransformBaseProcessor):
            @classmethod
            def config_schema(cls):
                return Schema([Key('source_stage', 'str')])
            def execute(self, data):
                return data
        ok &= report('family key redefinition', False)
    except SchemaDefinitionError as error:
        ok &= report('family key redefinition rejected', 'redefines family key' in str(error))

    class Plain(TransformBaseProcessor):
        @classmethod
        def config_schema(cls):
            return Schema([name_list('columns', required=True)])
        def execute(self, data):
            return data
    merged = Plain.full_schema()
    ok &= report('family contribution merged',
                 {'source_stage', 'save_to_stage', 'processor_type', 'columns'} <= set(merged.keys))
    return ok


def main() -> int:
    tests = [
        test_closed_mapping_rejects_unknown_with_suggestion,
        test_open_mapping_and_nested,
        test_discriminated_variant,
        test_selector_constructs,
        test_stage_references_from_schema,
        test_family_rules_at_class_definition,
    ]
    passed = 0
    for test in tests:
        try:
            if test():
                passed += 1
        except Exception as error:
            print(f'  EXCEPTION in {test.__name__}: {error}')
    print(f'\n{passed}/{len(tests)} tests passed')
    return 0 if passed == len(tests) else 1


if __name__ == '__main__':
    sys.exit(main())


# End of file #
