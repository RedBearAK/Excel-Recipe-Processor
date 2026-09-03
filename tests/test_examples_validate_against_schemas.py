"""
Validate every processor example recipe against the declared schemas.

File: excel_recipe_processor/tests/test_examples_validate_against_schemas.py

The examples are the documentation authors and models read; if they drift
from the code they teach the wrong keys. Every example YAML block is
loaded as a recipe and each of its steps is validated against its
processor's schema (schema-less processors are skipped and counted). Any
unknown key, wrong type or missing required key in an example fails.

Run: PYTHONPATH=. python3 tests/test_examples_validate_against_schemas.py
"""

import sys
import glob

from pathlib import Path

import yaml

from excel_recipe_processor.core.pipeline import registry
from excel_recipe_processor.core.config_schema import validate_config


EXAMPLES_DIR = Path(__file__).resolve().parent.parent / 'excel_recipe_processor' / 'processors' / '_examples'


def example_recipes():
    """Yield (file, section, recipe_dict) for every example block that parses."""
    for path in sorted(glob.glob(str(EXAMPLES_DIR / '*_examples.yaml'))):
        if Path(path).name.startswith('__DEPRECATED__'):
            continue
        document = yaml.safe_load(Path(path).read_text())
        if not isinstance(document, dict):
            continue
        for section, block in document.items():
            if not isinstance(block, dict) or 'yaml' not in block:
                continue
            try:
                recipe = yaml.safe_load(block['yaml'])
            except yaml.YAMLError as error:
                yield Path(path).name, section, {'_parse_error': str(error)}
                continue
            if isinstance(recipe, dict) and isinstance(recipe.get('recipe'), list):
                yield Path(path).name, section, recipe


def main() -> int:
    failures = []
    checked = 0
    skipped_types = set()
    for file_name, section, recipe in example_recipes():
        if '_parse_error' in recipe:
            failures.append(f"{file_name} [{section}]: YAML parse error: {recipe['_parse_error'][:80]}")
            continue
        for index, step in enumerate(recipe['recipe'], 1):
            if not isinstance(step, dict):
                failures.append(f"{file_name} [{section}] step {index}: not a mapping")
                continue
            processor_type = step.get('processor_type')
            processor_class = registry._processors.get(processor_type)
            if processor_class is None:
                failures.append(f"{file_name} [{section}] step {index}: unknown processor_type {processor_type!r}")
                continue
            schema = processor_class.full_schema()
            if schema is None:
                skipped_types.add(processor_type)
                continue
            checked += 1
            for message in validate_config(step, schema, '', True):
                failures.append(f"{file_name} [{section}] step {index} ({processor_type}): {message}")

    print(f"Checked {checked} example step(s); schema-less types skipped: {sorted(skipped_types)}")
    for failure in failures:
        print(f"  \u2717 {failure}")
    if failures:
        print(f"\n{len(failures)} example problem(s)")
        return 1
    print("\nAll example steps validate against their schemas")
    return 0


if __name__ == '__main__':
    sys.exit(main())


# End of file #
