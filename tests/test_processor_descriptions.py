"""
Processor description conventions, enforced.

tests/test_processor_descriptions.py

Every registered processor's get_capabilities()['description'] is
swept for the house cap: AT MOST 80 CHARACTERS, non-empty. The cap
exists because the capabilities listing prints name-padded lines
(~26 chars of name column), so an 80-char description keeps every
line comfortably inside a terminal; the 144-char excel_data_validation
description that prompted this (2026-08-15) wrapped ugly everywhere it
appeared. A long description is usually a description doing a
docstring's job - move the detail into the module docstring or the
examples file, and keep the listing line to ONE clause plus at most a
parenthetical.

This is the "note somewhere" made mechanical: a new processor with a
long description fails here at commit time instead of waiting for an
eyeball on the listing. Runnable directly or with pytest; direct runs
are the authoritative score.
"""

import sys

from excel_recipe_processor.core.pipeline import registry


DESCRIPTION_CAP = 80


def test_description_cap():
    """Every processor description exists and fits the cap."""
    print(f"\nSweeping all processor descriptions (cap {DESCRIPTION_CAP})...")

    offenders = []
    for name in sorted(registry.get_registered_types()):
        processor_class = registry.get_processor_class(name)
        config = processor_class.get_minimal_config()
        config['processor_type'] = name
        try:
            capabilities = processor_class(config).get_capabilities()
        except Exception as error:
            offenders.append((name, f"could not instantiate for sweep: {error}"))
            continue
        description = capabilities.get('description', '')
        if not isinstance(description, str) or not description.strip():
            offenders.append((name, "missing or empty description"))
        elif len(description) > DESCRIPTION_CAP:
            offenders.append(
                (name, f"{len(description)} chars: {description[:90]}..."))

    if offenders:
        print(f"✗ {len(offenders)} processor(s) violate the description cap:")
        for name, problem in offenders:
            print(f"    {name}: {problem}")
        return False
    count = len(registry.get_registered_types())
    print(f"✓ All {count} descriptions present and within {DESCRIPTION_CAP} chars")
    return True


def main():
    """Run all tests and report results."""
    print("Processor description convention tests")
    print("=" * 50)

    tests = [
        test_description_cap,
    ]

    passed = 0
    for test in tests:
        try:
            if test():
                passed += 1
            else:
                print(f"FAILED: {test.__name__}")
        except Exception as error:
            print(f"FAILED with exception: {test.__name__}: {error}")

    print("=" * 50)
    print(f"Final score: {passed}/{len(tests)} tests passed")
    return passed == len(tests)


if __name__ == '__main__':
    sys.exit(0 if main() else 1)

# End of file #
