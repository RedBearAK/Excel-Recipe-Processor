"""
Test the schema export renders every registered processor.

File: excel_recipe_processor/tests/test_schema_export.py

The exported document is what recipe authors and models read; it must
cover every processor, agree with the live declarations, and render both
forms without error.

Run: PYTHONPATH=. python3 tests/test_schema_export.py
"""

import sys
import json

from excel_recipe_processor.core.pipeline import registry
from excel_recipe_processor.core.schema_export import export_schemas, render_markdown


def test_every_processor_present() -> bool:
    print('\nTesting coverage...')
    exported = export_schemas(registry)
    missing = sorted(set(registry._processors) - set(exported['processors']))
    good = not missing
    print(f"  {len(exported['processors'])} processors exported; missing={missing} -> {'OK' if good else 'FAIL'}")
    return good


def test_json_round_trip_and_markdown() -> bool:
    print('\nTesting JSON serialisation and Markdown rendering...')
    exported = export_schemas(registry)
    text = json.dumps(exported, default=str)
    back = json.loads(text)
    markdown = render_markdown(exported)
    good = (back['processors']['sort_data']['family'] == 'transform'
            and 'case_sensitive' in back['processors']['sort_data']['schema']['keys']
            and '### `sort_data`' in markdown
            and '## Families' in markdown)
    print(f"  json {len(text):,} chars, markdown {len(markdown):,} chars -> {'OK' if good else 'FAIL'}")
    return good


def test_schema_less_are_marked() -> bool:
    print('\nTesting that schema-less processors are marked, not omitted...')
    exported = export_schemas(registry)
    markdown = render_markdown(exported)
    schema_less = [n for n, p in exported['processors'].items() if p['schema'] is None]
    good = all(f'### `{n}`' in markdown for n in schema_less)
    print(f"  schema-less: {schema_less} -> {'OK' if good else 'FAIL'}")
    return good


def main() -> int:
    tests = [test_every_processor_present, test_json_round_trip_and_markdown, test_schema_less_are_marked]
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
