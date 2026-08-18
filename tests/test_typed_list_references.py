"""
Typed list-reference family: member conversion, retirement, bracket net.

tests/test_typed_list_references.py

Bare {list:name} is retired (2026-08-17): every list reference declares
its member typing, even when that declaration is "any". The family:

    {list_int:name}    members converted to int, loudly on failure
    {list_float:name}  members converted to float, loudly on failure
    {list_str:name}    members converted to str
    {list_any:name}    members pass through as declared (acknowledged
                       untyped/mixed)

The bracketed alternative {list[int]:name} was considered and rejected:
unknown underscore variants fail loud through the existing typed path,
while unknown bracket variants would pass through as literal text. The
syntax is tempting enough to type accidentally, so it is caught as a
typo with the real vocabulary in the message.

Runnable standalone or under pytest; the exit code carries the verdict.
"""

import sys

from excel_recipe_processor.core.variable_substitution import (
    VariableSubstitution,
    VariableSubstitutionError,
)


def make_substitution():
    """Substitution armed with fixtures for every case."""
    return VariableSubstitution(custom_variables={
        'quoted_numbers': ['100', '200'],
        'mixed_floats': ['1.5', 2],
        'mixed_bag': [1, '2', 3.0],
        'unconvertible': ['100', 'abc'],
        'cli_string': '100,200',
    })


def test_member_conversion_per_family():
    """Each family converts (or passes) members as documented."""
    print("Testing member conversion across the family...")

    substitution = make_substitution()
    cases = [
        ('{list_int:quoted_numbers}', [100, 200], [int, int]),
        ('{list_float:mixed_floats}', [1.5, 2.0], [float, float]),
        ('{list_str:mixed_bag}', ['1', '2', '3.0'], [str, str, str]),
        ('{list_any:mixed_bag}', [1, '2', 3.0], [int, str, float]),
    ]
    passed = True
    for template, expected_values, expected_types in cases:
        result = substitution.substitute_structure(template)
        types_ok = [type(member) for member in result] == expected_types
        if result == expected_values and types_ok:
            print(f"  ✓ {template} -> {result!r}")
        else:
            print(f"  ✗ {template} -> {result!r} "
                  f"({[type(m).__name__ for m in result]})")
            passed = False
    return passed


def test_unconvertible_member_fails_loud_with_index():
    """Conversion failure names the member index, value, and target type."""
    print("\nTesting the per-member conversion error...")

    substitution = make_substitution()
    try:
        substitution.substitute_structure('{list_int:unconvertible}')
        print("  ✗ unconvertible member was accepted")
        return False
    except VariableSubstitutionError as error:
        message = str(error)
        if 'member 1' in message and "'abc'" in message and 'int' in message:
            print("  ✓ error names member index, value, and target type")
            return True
        print(f"  ✗ error lacks specifics: {message[:110]}")
        return False


def test_bare_list_is_retired_with_guidance():
    """{list:name} fails loud naming the whole replacement family."""
    print("\nTesting the bare-list retirement...")

    substitution = make_substitution()
    try:
        substitution.substitute_structure('{list:mixed_bag}')
        print("  ✗ bare list reference was accepted")
        return False
    except VariableSubstitutionError as error:
        message = str(error)
        family = ('{list_int:' in message and '{list_float:' in message
                  and '{list_str:' in message and '{list_any:' in message)
        if 'retired' in message and family:
            print("  ✓ retirement error teaches all four replacements")
            return True
        print(f"  ✗ guidance incomplete: {message[:110]}")
        return False


def test_container_check_still_guards_cli_strings():
    """A CLI-style string variable still fails the container check."""
    print("\nTesting the container check on non-list variables...")

    substitution = make_substitution()
    try:
        substitution.substitute_structure('{list_int:cli_string}')
        print("  ✗ string variable accepted as a list")
        return False
    except VariableSubstitutionError as error:
        if 'expects a list' in str(error):
            print("  ✓ non-list variable rejected loudly")
            return True
        print(f"  ✗ wrong error: {error}")
        return False


def test_bracketed_syntax_caught_as_typo():
    """{list[int]:...} and half-typed forms fail with the real vocabulary."""
    print("\nTesting the bracket typo net...")

    substitution = make_substitution()
    passed = True
    for template in ('{list[int]:quoted_numbers}', '{list[int:quoted_numbers}'):
        try:
            result = substitution.substitute_structure(template)
            print(f"  ✗ {template} passed through as {result!r}")
            passed = False
        except VariableSubstitutionError as error:
            message = str(error)
            if 'not ERP vocabulary' in message and '{list_int:' in message:
                print(f"  ✓ {template} caught, underscore family taught")
            else:
                print(f"  ✗ {template} wrong guidance: {message[:100]}")
                passed = False
    return passed


def test_pipeline_substitution_fails_loud():
    """The pipeline halts on substitution failure with the real error.

    The warn-and-continue swallow (found 2026-08-17 in production)
    returned the UNSUBSTITUTED config, so the step died on a misleading
    shape complaint while the guided retirement error scrolled past as
    a warning. The pipeline layer must surface the substitution error
    AS the halt.
    """
    print("\nTesting the pipeline-layer fail-loud...")

    from excel_recipe_processor.core.base_processor import StepProcessorError
    from excel_recipe_processor.core.recipe_pipeline import RecipePipeline

    pipeline = RecipePipeline.__new__(RecipePipeline)
    pipeline.variable_substitution = make_substitution()
    config = {'expected_columns': '{list:quoted_numbers}'}
    try:
        pipeline._substitute_variables_in_config(config)
        print("  ✗ substitution failure was swallowed")
        return False
    except StepProcessorError as error:
        if 'retired' in str(error) and '{list_str:quoted_numbers}' in str(error):
            print("  ✓ pipeline halts with the guided error itself")
            return True
        print(f"  ✗ wrong error surfaced: {error}")
        return False


def main():
    """Run every test and report a final score."""
    print("=== typed list-reference family tests ===")

    tests = [
        test_member_conversion_per_family,
        test_unconvertible_member_fails_loud_with_index,
        test_bare_list_is_retired_with_guidance,
        test_container_check_still_guards_cli_strings,
        test_bracketed_syntax_caught_as_typo,
        test_pipeline_substitution_fails_loud,
    ]

    passed = 0
    for test_func in tests:
        try:
            if test_func():
                passed += 1
        except Exception as error:
            print(f"✗ {test_func.__name__} crashed: {error}")

    print(f"\n=== Results: {passed}/{len(tests)} tests passed ===")
    return passed == len(tests)


if __name__ == '__main__':
    sys.exit(0 if main() else 1)

# End of file #
