"""
Adversarial tests for the formula storage pipeline.

tests/test_xlpm_adversarial.py

The happy paths are pinned by the oracle corpus; this module ATTACKS
the pipeline instead. Three lines of assault:

1. OUTPUT INVARIANTS - properties every transformed formula must hold,
   checked by inspection functions reusable across all cases: string
   literals byte-preserved, paren balance preserved, no double
   prefixes, no unknown _xlfn names (the blanket-prefixing regression
   class), every LAMBDA/LET declaration slot _xlpm.-prefixed, no bare
   spill '#' outside strings, idempotence.
2. HOSTILE INPUTS - malformed, truncated, empty, keyword-adjacent, and
   Excel-forbidden constructions: must refuse loudly or pass through
   untouched, never corrupt.
3. SEEDED FUZZ - a deterministic generator composes hundreds of nested
   construct/string/ref/array combinations; every one must satisfy the
   invariants. A seed failure prints the offending formula verbatim so
   it can be promoted to a named regression case.

Runnable directly or with pytest; the direct python3 run is the
authoritative score.
"""

import re
import sys
import random

from excel_recipe_processor.processors._helpers.xlpm_name_storage import (
    transform_xlpm_names,
)
from excel_recipe_processor.processors._helpers.inject_formulas_functions import (
    FUTURE_FUNCTION_PREFIXES,
    prefix_future_functions,
    transform_storage_forms,
)


STRING_RGX = re.compile(r'"(?:[^"]|"")*"')
SHEET_QUOTE_RGX = re.compile(r"'(?:[^']|'')*'")
XLFN_NAME_RGX = re.compile(r'_xlfn\.(?:_xlws\.)?([A-Za-z][A-Za-z0-9.]*)')
BAD_PREFIX_CHAIN_RGX = re.compile(
    r'_xl(?:pm|eta|ws)\._xl|_xlfn\._xl(?!ws\.)'
)
DECL_CONSTRUCT_RGX = re.compile(r'(?<![A-Za-z0-9_.\\])(LAMBDA|LET)\s*\(',
                                re.IGNORECASE)


def live(formula: str) -> str:
    """The injector's full live storage pipeline."""
    return transform_storage_forms(
        prefix_future_functions(transform_xlpm_names(formula)))


def outside_strings(text: str) -> str:
    """Text with both quote styles blanked, for structural inspection."""
    return SHEET_QUOTE_RGX.sub("''", STRING_RGX.sub('""', text))


def check_invariants(given: str, produced: str) -> str:
    """Empty string when all invariants hold, else a violation report."""
    problems = []

    if sorted(STRING_RGX.findall(given)) != sorted(STRING_RGX.findall(produced)):
        problems.append("string literals not byte-preserved")

    bare_given = outside_strings(given)
    bare_out = outside_strings(produced)

    # ANCHORARRAY rewriting legitimately ADDS paren pairs, so the paren
    # invariant is "output stays balanced", not "count preserved".
    if bare_out.count('(') != bare_out.count(')'):
        problems.append("output parentheses unbalanced")
    for char in '{}[]':
        if bare_given.count(char) != bare_out.count(char):
            problems.append(f"count of {char!r} changed")
            break

    if BAD_PREFIX_CHAIN_RGX.search(produced):
        problems.append("chained storage prefixes")

    for match in XLFN_NAME_RGX.finditer(bare_out):
        if match.group(1).upper() not in FUTURE_FUNCTION_PREFIXES:
            problems.append(f"_xlfn on unmapped name {match.group(1)!r}")

    if '#' in bare_out:
        problems.append("literal '#' survived outside strings")

    if not _declarations_all_prefixed(bare_out):
        problems.append("a LAMBDA/LET declaration slot lacks _xlpm.")

    if transform_storage_forms(produced) != produced:
        problems.append("not idempotent under storage transforms")
    if transform_xlpm_names(produced) != produced:
        problems.append("not idempotent under the _xlpm transform")

    return '; '.join(problems)


def _declarations_all_prefixed(bare_text: str) -> bool:
    """Every declaration slot in every construct starts with _xlpm."""
    for match in DECL_CONSTRUCT_RGX.finditer(bare_text):
        open_paren = match.end() - 1
        depth = 0
        args = []
        start = open_paren + 1
        index = open_paren
        while index < len(bare_text):
            char = bare_text[index]
            if char in '({[':
                depth += 1
            elif char in ')}]':
                depth -= 1
                if depth == 0:
                    args.append(bare_text[start:index])
                    break
            elif char == ',' and depth == 1:
                args.append(bare_text[start:index])
                start = index + 1
            index += 1
        if not args:
            continue
        keyword = match.group(1).upper()
        if keyword == 'LAMBDA':
            declaration_slots = args[:-1]
        else:
            declaration_slots = args[0:len(args) - 1:2]
        for slot in declaration_slots:
            if not slot.strip().startswith('_xlpm.'):
                return False
    return True


def test_invariants_on_known_corpus():
    """Every named case from this session's work holds all invariants."""
    print("\nTesting invariants across the known corpus...")

    corpus = [
        '=GROUPBY(a,b,SUM)',
        '=GROUPBY(A1:A6,B1:B6,LAMBDA(x,SUM(x)))',
        '=LET(v,FILTER(a,b),IF(v="","",v))',
        '=LET(a,b+1,b,2,a+b)',
        '=LAMBDA(f,f(1)+f(2))(LAMBDA(n,n*2))',
        '=SUM(D1#)+COUNTA(Lookups!$Z$2#)',
        '=IF(A1="LAMBDA(x)","see Z1# or SUM",TEXTJOIN(",",TRUE,A1:A3))',
        "=LAMBDA(x,'x sheet'!A1+Sheet1!x+x)(1)",
        '=LET(vms_rows,FILTER(rng_vms_all,rng_vms_saletype="Export","none"),fnBlankSafe(vms_rows))',
    ]
    for given in corpus:
        produced = live(given)
        report = check_invariants(given, produced)
        if report:
            print(f"✗ {given}\n  violated: {report}\n  produced: {produced}")
            return False
        print(f"✓ {given}")
    return True


def test_hostile_inputs():
    """Malformed and forbidden inputs refuse loudly or pass untouched."""
    print("\nTesting hostile inputs...")

    # Must RAISE (never emit corrupt storage)
    raising = [
        ("unbalanced parens", '=LAMBDA(x,SUM(x', 'Unbalanced'),
        ("cell-ref name", '=LET(A1,5,A1*2)', 'cell reference'),
        ("bare R name", '=LAMBDA(R,R*2)(1)', 'cell reference'),
        ("R1C1 name", '=LET(R1C1,5,R1C1)', 'cell reference'),
        ("boolean name", '=LET(TRUE,1,TRUE+1)', 'boolean'),
        ("empty declaration", '=LET(,1,2)', 'not a legal name'),
        ("numeric declaration", '=LAMBDA(2x,2x)(1)', 'not a legal name'),
    ]
    for label, formula, fragment in raising:
        try:
            transform_xlpm_names(formula)
            print(f"✗ {label}: should have raised")
            return False
        except ValueError as error:
            if fragment not in str(error):
                print(f"✗ {label}: guidance missing {fragment!r}: {error}")
                return False
            print(f"✓ {label}: refused")

    # Must pass through UNTOUCHED (no construct present, or degenerate)
    untouched = [
        '',
        '   ',
        '=MYLAMBDA(x,1)',          # construct keyword as name substring
        '=LETTERS(A1)',
        '=SUM(LAMBDANESS)',        # keyword inside a longer identifier
        '="LET(a,1,a)"',           # entirely a string literal
    ]
    for formula in untouched:
        if transform_xlpm_names(formula) != formula:
            print(f"✗ mangled a construct-free input: {formula!r}")
            return False
    print("✓ construct-free and degenerate inputs pass through untouched")

    # Unterminated string INSIDE a construct: the construct's parens can
    # never close - refusing loudly is the safe behavior, not guessing
    try:
        transform_xlpm_names('=LAMBDA(x,IF(x="dangling')
        print("✗ unterminated string inside a construct should refuse")
        return False
    except ValueError as error:
        if 'Unbalanced' not in str(error):
            print(f"✗ wrong refusal: {error}")
            return False
        print("✓ unterminated string inside a construct refused (unbalanced)")

    # Unterminated string with NO construct: nothing to transform, the
    # trailing text is literal - passthrough, no crash
    dangling_free = '=IF(x="dangling'
    if transform_xlpm_names(dangling_free) != dangling_free:
        print("✗ construct-free dangling string was mangled")
        return False
    print("✓ construct-free dangling string passes through untouched")

    # Duplicate parameters: not Excel-legal, but must not corrupt (both
    # occurrences prefixed identically; Excel rejects at its own layer)
    result = transform_xlpm_names('LAMBDA(x,x,x+1)')
    if result != 'LAMBDA(_xlpm.x,_xlpm.x,_xlpm.x+1)':
        print(f"✗ duplicate-param handling changed: {result}")
        return False
    print("✓ duplicate params transformed consistently (Excel's problem)")
    return True


def test_seeded_fuzz():
    """Hundreds of generated nightmares; every one holds the invariants."""
    print("\nTesting seeded fuzz (deterministic)...")

    rng = random.Random(20260814)
    names = ['v', 'amt', 'my_val', 'acc', 'row_set', 'f']
    hostile_strings = ['"LAMBDA(x)"', '"a,b"', '"Z1#"', '"say ""hi"""',
                       '"LET(v,1,v)"', '"#N/A-ish"']
    atoms = ['A1', '$B$2', 'A2:A9', 'rng_data', '42', '3.5',
             "'My Sheet'!C3", 'Sheet1!v', '{1,2;3,4}']
    legacy_calls = ['SUM', 'IF', 'MAX', 'COUNTA']
    future_calls = ['FILTER', 'TEXTJOIN', 'UNIQUE', 'SEQUENCE']

    def expr(depth, in_scope):
        roll = rng.random()
        if depth <= 0 or roll < 0.30:
            pool = atoms + hostile_strings + (in_scope * 2)
            return rng.choice(pool)
        if roll < 0.45:
            return f"{rng.choice(legacy_calls)}({expr(depth - 1, in_scope)})"
        if roll < 0.60:
            return (f"{rng.choice(future_calls)}({expr(depth - 1, in_scope)},"
                    f"{expr(depth - 1, in_scope)})")
        if roll < 0.72:
            return f"{expr(depth - 1, in_scope)}+{expr(depth - 1, in_scope)}"
        if roll < 0.80:
            return f"SUM({rng.choice(['D1', 'rng_pick'])}#)"
        if roll < 0.90:
            params = rng.sample(names, rng.randint(1, 2))
            body = expr(depth - 1, in_scope + params)
            call_args = ','.join(expr(0, in_scope) for _ in params)
            suffix = f"({call_args})" if rng.random() < 0.5 else ''
            return f"LAMBDA({','.join(params)},{body}){suffix}"
        pairs = []
        scope = list(in_scope)
        for _ in range(rng.randint(1, 2)):
            name = rng.choice([n for n in names if n not in scope] or names)
            pairs.append(name)
            pairs.append(expr(depth - 1, scope))
            if name not in scope:
                scope.append(name)
        return f"LET({','.join(pairs)},{expr(depth - 1, scope)})"

    failures = 0
    for case_number in range(300):
        formula = '=' + expr(3, [])
        try:
            produced = live(formula)
        except ValueError:
            continue  # Generator can build refusable shapes; refusal is fine
        report = check_invariants(formula, produced)
        if report:
            failures += 1
            print(f"✗ seed case {case_number}: {report}")
            print(f"  formula:  {formula}")
            print(f"  produced: {produced}")
            if failures >= 3:
                print("  (stopping after 3 reports)")
                return False
    if failures:
        return False
    print("✓ 300 seeded cases hold every invariant")
    return True


def main():
    """Run all tests and report results."""
    print("Adversarial storage-pipeline tests")
    print("=" * 50)

    tests = [
        test_invariants_on_known_corpus,
        test_hostile_inputs,
        test_seeded_fuzz,
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
