"""
Validate every YAML recipe fragment in the Markdown docs against the schemas.

File: excel_recipe_processor/tests/test_docs_fragments_validate.py

Hand-written pages drift: a README or troubleshooting example keeps
teaching a key long after the processor stopped accepting it. This test
reads every ```yaml block in README.md and docs/**/*.md, finds the steps
in it (a bare step, a list of steps, or a recipe: list), and validates
each against its processor's construction schema. A block that contains
an elision marker line (`# ...`) is an illustration and is skipped.

Run: PYTHONPATH=. python3 tests/test_docs_fragments_validate.py
"""

import re
import sys
import glob

from pathlib import Path

import yaml

from excel_recipe_processor.core.pipeline import registry
from excel_recipe_processor.core.config_schema import validate_config


ROOT = Path(__file__).resolve().parent.parent
ELISION = re.compile(r'^\s*#\s*\.\.\.', re.M)


def markdown_files():
    yield ROOT / 'README.md'
    for path in sorted(glob.glob(str(ROOT / 'docs' / '**' / '*.md'), recursive=True)):
        yield Path(path)


def steps_in(document):
    if isinstance(document, dict) and 'processor_type' in document:
        return [document]
    if isinstance(document, dict) and isinstance(document.get('recipe'), list):
        return [s for s in document['recipe'] if isinstance(s, dict)]
    if isinstance(document, list):
        return [s for s in document if isinstance(s, dict) and 'processor_type' in s]
    return []


def main() -> int:
    failures = []
    checked = 0
    skipped = 0
    for path in markdown_files():
        text = path.read_text()
        for index, block in enumerate(re.findall(r'```yaml\n(.*?)```', text, re.S), 1):
            if ELISION.search(block):
                skipped += 1
                continue
            try:
                document = yaml.safe_load(block)
            except yaml.YAMLError as error:
                failures.append(f"{path.relative_to(ROOT)} block {index}: YAML error {str(error)[:70]}")
                continue
            for step in steps_in(document):
                processor_class = registry._processors.get(step.get('processor_type'))
                if processor_class is None:
                    failures.append(f"{path.relative_to(ROOT)} block {index}: unknown processor {step.get('processor_type')!r}")
                    continue
                checked += 1
                for message in validate_config(step, processor_class.construction_schema(), '', True):
                    failures.append(f"{path.relative_to(ROOT)} block {index} ({step['processor_type']}): {message}")
    print(f"Checked {checked} step(s) in doc fragments; {skipped} elided block(s) skipped")
    for failure in failures:
        print(f"  \u2717 {failure}")
    if failures:
        print(f"\n{len(failures)} problem(s)")
        return 1
    print("\nAll doc fragments validate against their schemas")
    return 0


if __name__ == '__main__':
    sys.exit(main())


# End of file #
