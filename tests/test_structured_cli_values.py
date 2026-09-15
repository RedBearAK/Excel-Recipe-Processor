"""
A --set / --var value written as a YAML/JSON list or mapping becomes that
structure; anything else stays the string it was.

File: excel_recipe_processor/tests/test_structured_cli_values.py

Run: PYTHONPATH=. python3 tests/test_structured_cli_values.py
"""

import sys

from excel_recipe_processor.core.interactive_variables import parse_cli_variables, structured_cli_value


def test_lists_and_mappings_parse_strings_do_not() -> bool:
    print('\nA bracketed value becomes a list, a braced one a mapping, everything else stays a string...')
    cases = [
        ('["Packages", "Units"]', ['Packages', 'Units']),
        ('[]', []),
        ('{"a": 1}', {'a': 1}),
        ('plain text', 'plain text'),
        ('downloads/[latest].xlsx', 'downloads/[latest].xlsx'),   # a path with a bracket inside stays a path
        ('[not, closed', '[not, closed'),                          # does not parse: the string
        ('  ["x"]  ', ['x']),
        ('', ''),
    ]
    bad = [(given, expected, structured_cli_value(given)) for given, expected in cases if structured_cli_value(given) != expected]
    for given, expected, got in bad:
        print(f"  FAIL {given!r} -> {got!r}, expected {expected!r}")
    print(f"  {len(cases) - len(bad)}/{len(cases)} -> {'OK' if not bad else 'FAIL'}")
    return not bad


def test_var_form_too() -> bool:
    print('\n--var name=value goes through the same rule...')
    parsed = parse_cli_variables(['cols=["A", "B"]', 'name=plain'])
    good = parsed == {'cols': ['A', 'B'], 'name': 'plain'}
    print(f"  {parsed} -> {'OK' if good else 'FAIL'}")
    return good


def main() -> int:
    tests = [test_lists_and_mappings_parse_strings_do_not, test_var_form_too]
    passed = sum(1 for test in tests if test())
    print(f"\n{passed}/{len(tests)} tests passed")
    return 0 if passed == len(tests) else 1


if __name__ == '__main__':
    sys.exit(main())


# End of file #
