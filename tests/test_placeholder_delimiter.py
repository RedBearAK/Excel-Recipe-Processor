"""
The doubled-brace placeholder grammar, drilled at every parser.

tests/test_placeholder_delimiter.py

Born 2026-11-02 with the delimiter change ({name} -> {{name}}). Four
parsers recognize placeholders - the variable pass, the pandas-expression
column tokenizer, the Excel range resolver and the formula injector's
opaque-span scan - and each is planted with: the live forms; the things
single braces must now be free to mean (a regex quantifier, an Excel
array constant, a dict literal); the retired single-brace token, which
must be REFUSED naming the doubled form; the mistyped shapes (one side
doubled, padding, empties); and the verbatim rule (a name with a
trailing space is that name, untrimmed).

Run: PYTHONPATH=. python3 tests/test_placeholder_delimiter.py
"""

import sys

from excel_recipe_processor.core.column_tokens import (
    ColumnTokenError, build_dataframe_expression, tokenize_formula,
)
from excel_recipe_processor.core.variable_substitution import (
    VariableSubstitution, VariableSubstitutionError,
)
from excel_recipe_processor.processors._helpers.inject_formulas_rgx import (
    column_placeholder_rgx, opaque_span_rgx,
)


def check(label: str, good: bool, detail: str = '') -> bool:
    print(f"  {'✓' if good else '✗'} {label}{'  ' + detail if detail else ''}")
    return good


def refuses(fn, text, error_type, needle) -> bool:
    try:
        result = fn(text)
    except error_type as error:
        return needle in str(error)
    except Exception as error:
        print(f"    (wrong error type {type(error).__name__}: {error})")
        return False
    print(f"    (accepted: {result!r})")
    return False


def make_vs() -> VariableSubstitution:
    return VariableSubstitution(custom_variables={
        'out': 'OUT', 'names': ['a', 'b'], 'count': 3, 'nav': '1F4E79',
    })


def test_variable_pass_live_forms() -> bool:
    print('\nVariable pass: the live forms substitute...')
    vs = make_vs()
    ok = True
    ok &= check('{{name}} in a string', vs.substitute('{{out}}/file.xlsx') == 'OUT/file.xlsx')
    ok &= check('{{str:name}} in a string', vs.substitute('x_{{str:out}}') == 'x_OUT')
    ok &= check('{{date:FMT}} renders', vs.substitute('{{date:YYYY}}') == vs.now.strftime('%Y'))
    ok &= check('{{list_str:name}} as the whole string returns the structure',
                vs.substitute_structure('{{list_str:names}}') == ['a', 'b'])
    ok &= check('{{int:name}} as the whole string returns the int',
                vs.substitute_structure('{{int:count}}') == 3)
    ok &= check('{{col:Name}} passes through verbatim for the processors',
                vs.substitute('{{col:Ship Date}} > 1') == '{{col:Ship Date}} > 1')
    ok &= check('two placeholders around an A1 range colon',
                vs.substitute('{{str:out}}2:{{str:out}}5') == 'OUT2:OUT5')
    return ok


def test_variable_pass_single_braces_are_text() -> bool:
    print('\nVariable pass: single braces are ordinary text now...')
    vs = make_vs()
    ok = True
    for text in [r'\d{2,4}', r'[A-Z]{3}', 'IF(A1={1,2,3},1,0)', '{"a","b"}', "{'a': 1}", 'x {} y']:
        ok &= check(f'{text!r} unchanged', vs.substitute(text) == text)
    ok &= check('a live token beside an array constant',
                vs.substitute('{{out}} {1,2,3}') == 'OUT {1,2,3}')
    return ok


def test_variable_pass_retired_form_is_refused() -> bool:
    print('\nVariable pass: the retired single-brace token is refused, naming the doubled form...')
    vs = make_vs()
    ok = True
    for old, doubled in [('{out}', '{{out}}'), ('{str:out}', '{{str:out}}'),
                         ('{date:YYYYMMDD}', '{{date:YYYYMMDD}}'), ('{col:Ship Date}', '{{col:Ship Date}}'),
                         ('{list_str:names}', '{{list_str:names}}')]:
        ok &= check(f'{old} refused with {doubled}',
                    refuses(vs.substitute_structure, f'pre {old} post', VariableSubstitutionError, doubled))
    return ok


def test_variable_pass_mistyped_shapes_are_refused() -> bool:
    print('\nVariable pass: one side doubled, padding, empties...')
    vs = make_vs()
    ok = True
    ok &= check('{{out} mismatched', refuses(vs.substitute, '{{out}', VariableSubstitutionError, 'Mismatched'))
    ok &= check('{out}} mismatched', refuses(vs.substitute, '{out}}', VariableSubstitutionError, 'Mismatched'))
    ok &= check('{{ out }} padded', refuses(vs.substitute, '{{ out }}', VariableSubstitutionError, 'Whitespace'))
    ok &= check('{{out}}{{ unbalanced', refuses(vs.substitute, '{{out}} {{', VariableSubstitutionError, 'Unbalanced'))
    ok &= check('{{:x}} empty type', refuses(vs.substitute, '{{:x}}', VariableSubstitutionError, 'Empty type'))
    ok &= check('{{str:}} empty name', refuses(vs.substitute, '{{str:}}', VariableSubstitutionError, 'Empty variable'))
    ok &= check('{{list[int]:names}} bracketed', refuses(vs.substitute, '{{list[int]:names}}',
                                                          VariableSubstitutionError, 'underscore'))
    return ok


def test_column_tokenizer() -> bool:
    print('\nColumn tokenizer (pandas expressions)...')
    ok = True
    segs = tokenize_formula("{{col:Price}} * 2 + {{col:Net Weight}}")
    ok &= check('two column segments, verbatim names',
                [t for k, t in segs if k == 'column'] == ['Price', 'Net Weight'], f'{segs}')
    segs = tokenize_formula("{{col:Ship Date }}")
    ok &= check('trailing space inside the token is part of the name (verbatim)',
                [t for k, t in segs if k == 'column'] == ['Ship Date '])
    segs = tokenize_formula("x.str.match(r'\\d{2,4}') and {'a': 1}['a']")
    ok &= check('regex quantifier and dict literal are code, not references',
                not [t for k, t in segs if k == 'column'])
    expr = build_dataframe_expression("{{col:Price}} > {{col:Cost}}", ['Price', 'Cost'])
    ok &= check('builds frame accesses', expr == "df['Price'] > df['Cost']", expr)
    ok &= check('retired {col:X} refused with the doubled form',
                refuses(tokenize_formula, "{col:Price} > 1", ColumnTokenError, '{{col:Price}}'))
    ok &= check('unterminated {{col:X refused',
                refuses(tokenize_formula, "{{col:Price > 1", ColumnTokenError, 'Unterminated'))
    ok &= check('unknown column named in the doubled form',
                refuses(lambda f: build_dataframe_expression(f, ['Price']), "{{col:Cost}}",
                        ColumnTokenError, '{{col:Cost}}'))
    return ok


def test_formula_and_range_patterns() -> bool:
    print('\nFormula injector and range resolver patterns...')
    ok = True
    formula = '=IF({{col:Test Dest}}2=1, XLOOKUP({{col:Destination}}2, rng_a, rng_b), "")'
    ok &= check('column_placeholder_rgx finds both names',
                column_placeholder_rgx.findall(formula) == ['Test Dest', 'Destination'])
    ok &= check('array constant is not a placeholder',
                column_placeholder_rgx.findall('=SUM({1,2,3})') == [])
    ok &= check('retired {col:X} is not matched (left for the variable pass to refuse)',
                column_placeholder_rgx.findall('={col:X}2') == [])
    spans = [m.group(0) for m in opaque_span_rgx.finditer('"a {{col:X}} b" & {{col:Y}}1')]
    ok &= check('opaque spans: the string literal whole, then the placeholder',
                spans == ['"a {{col:X}} b"', '{{col:Y}}'], f'{spans}')
    rng = '{{col:Filter: SALE TYPE1}}2:{{col:Filter: Process Year}}5'
    ok &= check('range with a colon inside a name still yields both names',
                column_placeholder_rgx.findall(rng) == ['Filter: SALE TYPE1', 'Filter: Process Year'])
    return ok


def main() -> bool:
    print('=== placeholder delimiter: {{ }} ===')
    tests = [
        test_variable_pass_live_forms,
        test_variable_pass_single_braces_are_text,
        test_variable_pass_retired_form_is_refused,
        test_variable_pass_mistyped_shapes_are_refused,
        test_column_tokenizer,
        test_formula_and_range_patterns,
    ]
    passed = sum(1 for test in tests if test())
    print(f'\n=== Results: {passed}/{len(tests)} tests passed ===')
    return passed == len(tests)


if __name__ == '__main__':
    sys.exit(0 if main() else 1)


# End of file #
