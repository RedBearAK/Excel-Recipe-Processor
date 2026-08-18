"""
Typed dict-reference family: value conversion, key doctrine, retirement.

tests/test_typed_dict_references.py

Bare {dict:name} is retired (2026-08-17), mirroring the list-family
migration: every dict reference declares its VALUE typing, even when
that declaration is "any". Keys are ALWAYS normalized to str - recipe
dict variables are string-keyed mappings (column names, labels, status
codes) by doctrine, which is what collapses the key-x-value combination
space to the value axis alone and makes an underscore family workable
where a bracketed mini-grammar was rejected. The family:

    {dict_int:name}    values converted to int, loudly on failure
    {dict_float:name}  values converted to float, loudly on failure
    {dict_str:name}    values converted to str
    {dict_any:name}    values pass through as declared (acknowledged
                       untyped/mixed/nested); keys still become str

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
        'quoted_values': {'a': '100', 'b': '200'},
        'mixed_values': {'a': 1, 'b': '2', 'c': 3.0},
        'int_keys': {1: 'x', 2: 'y'},
        'nested': {'level1': {'level2': ['deep']}},
        'unconvertible': {'a': '100', 'b': 'abc'},
        'cli_string': 'a=1,b=2',
    })


def test_value_conversion_per_family():
    """Each family converts (or passes) values as documented."""
    print("Testing value conversion across the family...")

    substitution = make_substitution()
    cases = [
        ('{dict_int:quoted_values}', {'a': 100, 'b': 200}),
        ('{dict_float:quoted_values}', {'a': 100.0, 'b': 200.0}),
        ('{dict_str:mixed_values}', {'a': '1', 'b': '2', 'c': '3.0'}),
        ('{dict_any:nested}', {'level1': {'level2': ['deep']}}),
    ]
    passed = True
    for template, expected in cases:
        result = substitution.substitute_structure(template)
        types_match = all(
            type(result[key]) is type(expected[key]) for key in expected
        )
        if result == expected and types_match:
            print(f"  ✓ {template} -> {result!r}")
        else:
            print(f"  ✗ {template} -> {result!r}")
            passed = False
    return passed


def test_keys_always_normalized_to_str():
    """Non-string keys become strings in every family, including any."""
    print("\nTesting the string-key doctrine...")

    substitution = make_substitution()
    passed = True
    for template in ('{dict_str:int_keys}', '{dict_any:int_keys}'):
        result = substitution.substitute_structure(template)
        key_types = sorted({type(key).__name__ for key in result})
        if key_types == ['str'] and set(result) == {'1', '2'}:
            print(f"  ✓ {template} normalized keys: {sorted(result)}")
        else:
            print(f"  ✗ {template} keys: {sorted(result, key=str)} ({key_types})")
            passed = False
    return passed


def test_unconvertible_value_fails_loud_with_key():
    """Conversion failure names the key, value, and target type."""
    print("\nTesting the per-value conversion error...")

    substitution = make_substitution()
    try:
        substitution.substitute_structure('{dict_int:unconvertible}')
        print("  ✗ unconvertible value was accepted")
        return False
    except VariableSubstitutionError as error:
        message = str(error)
        if "key 'b'" in message and "'abc'" in message and 'int' in message:
            print("  ✓ error names key, value, and target type")
            return True
        print(f"  ✗ error lacks specifics: {message[:110]}")
        return False


def test_bare_dict_is_retired_with_guidance():
    """{dict:name} fails loud naming the whole replacement family."""
    print("\nTesting the bare-dict retirement...")

    substitution = make_substitution()
    try:
        substitution.substitute_structure('{dict:mixed_values}')
        print("  ✗ bare dict reference was accepted")
        return False
    except VariableSubstitutionError as error:
        message = str(error)
        family = ('{dict_int:' in message and '{dict_float:' in message
                  and '{dict_str:' in message and '{dict_any:' in message)
        if 'retired' in message and family and 'normalized to str' in message:
            print("  ✓ retirement error teaches all four replacements and key doctrine")
            return True
        print(f"  ✗ guidance incomplete: {message[:110]}")
        return False


def test_container_check_still_guards_non_dicts():
    """A CLI-style string variable still fails the container check."""
    print("\nTesting the container check on non-dict variables...")

    substitution = make_substitution()
    try:
        substitution.substitute_structure('{dict_int:cli_string}')
        print("  ✗ string variable accepted as a dict")
        return False
    except VariableSubstitutionError as error:
        if 'expects a dict' in str(error):
            print("  ✓ non-dict variable rejected loudly")
            return True
        print(f"  ✗ wrong error: {error}")
        return False


def test_bracketed_dict_syntax_caught_as_typo():
    """{dict[str,int]:...} fails with both underscore families taught."""
    print("\nTesting the bracket typo net covers dicts...")

    substitution = make_substitution()
    try:
        result = substitution.substitute_structure('{dict[str,int]:quoted_values}')
        print(f"  ✗ bracketed dict syntax passed through as {result!r}")
        return False
    except VariableSubstitutionError as error:
        message = str(error)
        if ('not ERP vocabulary' in message and '{dict_int:' in message
                and '{list_int:' in message):
            print("  ✓ bracketed dict caught; both families taught")
            return True
        print(f"  ✗ wrong guidance: {message[:110]}")
        return False


def main():
    """Run every test and report a final score."""
    print("=== typed dict-reference family tests ===")

    tests = [
        test_value_conversion_per_family,
        test_keys_always_normalized_to_str,
        test_unconvertible_value_fails_loud_with_key,
        test_bare_dict_is_retired_with_guidance,
        test_container_check_still_guards_non_dicts,
        test_bracketed_dict_syntax_caught_as_typo,
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
