"""
Tests for the _xlpm. name-storage transformer (LAMBDA / LET).

tests/test_xlpm_name_storage.py

The stored grammar is harvest-verified (2026-08-14): declared names get
_xlpm. at declaration and every in-scope occurrence; legacy functions
stay bare. The subtle behaviors under test are Excel's actual scoping
rules - LET binds sequentially, LAMBDA params scope to the body,
nesting accumulates - plus the string/boundary safety that keeps refs
and literals untouched. Runnable directly or with pytest.
"""

import sys

from excel_recipe_processor.processors._helpers.xlpm_name_storage import (
    transform_xlpm_names,
    parse_lambda_parameters,
)


def check(cases) -> bool:
    """Run (given, expected) pairs through the transformer."""
    for given, expected in cases:
        result = transform_xlpm_names(given)
        if result != expected:
            print(f"✗ {given}\n  got      {result}\n  expected {expected}")
            return False
        print(f"✓ {given}")
    return True


def test_scoping_rules():
    """Sequential LET binding, body-only LAMBDA params, nesting."""
    print("\nTesting Excel's scoping rules...")
    return check([
        # LET binds sequentially: b in v1 is an OUTER name, stays bare
        ('LET(a,b+1,b,2,a+b)',
         'LET(_xlpm.a,b+1,_xlpm.b,2,_xlpm.a+_xlpm.b)'),
        # A LET name in its own value expression is not yet bound
        ('LET(x,x+1,x*2)',
         'LET(_xlpm.x,x+1,_xlpm.x*2)'),
        # Closures: outer params reach inner bodies
        ('LAMBDA(x,LAMBDA(y,x+y))',
         'LAMBDA(_xlpm.x,LAMBDA(_xlpm.y,_xlpm.x+_xlpm.y))'),
        # LET inside a LAMBDA body sees the parameter
        ('LAMBDA(v,LET(t,v*2,t+v))',
         'LAMBDA(_xlpm.v,LET(_xlpm.t,_xlpm.v*2,_xlpm.t+_xlpm.v))'),
        # Lambda-valued parameters may be CALLED
        ('LAMBDA(f,f(1)+f(2))',
         'LAMBDA(_xlpm.f,_xlpm.f(1)+_xlpm.f(2))'),
        # Optional parameters (harvested): declaration _xlop, uses _xlpm
        ('LAMBDA(x,[y],IF(ISOMITTED(y),x,x+y))(5)',
         'LAMBDA(_xlpm.x,_xlop.y,IF(ISOMITTED(_xlpm.y),_xlpm.x,_xlpm.x+_xlpm.y))(5)'),
        # Case-insensitive binding, case-preserving replacement
        ('LAMBDA(amt,AMT+amt)',
         'LAMBDA(_xlpm.amt,_xlpm.AMT+_xlpm.amt)'),
    ])


def test_boundary_and_string_safety():
    """Refs, sheet qualifiers, and literals are never rewritten."""
    print("\nTesting boundary and string safety...")
    return check([
        # Param inside a double-quoted string stays text
        ('LAMBDA(v,IF(v="v","literal v",v))',
         'LAMBDA(_xlpm.v,IF(_xlpm.v="v","literal v",_xlpm.v))'),
        # Quoted sheet names are protected; ! qualifier blocks the prefix
        ("LAMBDA(x,'x sheet'!A1+Sheet1!x+x)",
         "LAMBDA(_xlpm.x,'x sheet'!A1+Sheet1!x+_xlpm.x)"),
        # $-adjacent tokens are cell refs, not names (param 'A')
        ('LAMBDA(A,$A$1+A)',
         'LAMBDA(_xlpm.A,$A$1+_xlpm.A)'),
        # Token boundaries: no partial hits inside longer names
        ('LAMBDA(v,IF(rng_values=v,v,rng_values))',
         'LAMBDA(_xlpm.v,IF(rng_values=_xlpm.v,_xlpm.v,rng_values))'),
        # Array-constant commas do not split arguments
        ('LAMBDA(x,SUM({1,2;3},x))',
         'LAMBDA(_xlpm.x,SUM({1,2;3},_xlpm.x))'),
        # No constructs, active set empty: byte-identical passthrough
        ('IF(A1="LAMBDA(x)","yes","no")',
         'IF(A1="LAMBDA(x)","yes","no")'),
    ])


def test_idempotence_and_parsing():
    """Re-running never double-prefixes; parameter parsing reads the text."""
    print("\nTesting idempotence and parameter parsing...")

    once = transform_xlpm_names('LET(v,FILTER(a,b),LAMBDA(x,v+x))')
    if transform_xlpm_names(once) != once:
        print("✗ Transformer is not idempotent")
        return False
    print("✓ Idempotent on re-run")

    params = parse_lambda_parameters('LAMBDA(rate, term, rate*term)')
    if params != ['rate', 'term']:
        print(f"✗ Parameter parse wrong: {params}")
        return False
    print("✓ parse_lambda_parameters reads declarations from the text")
    return True


def test_guided_refusals():
    """Malformed constructs and unharvested syntax fail loud."""
    print("\nTesting guided refusals...")

    cases = [
        ("required after optional", 'LAMBDA([y],x,x+y)', 'ordering'),
        ("bracketed LET name", 'LET([a],1,a)', 'LAMBDA parameters only'),
        ("LAMBDA missing body", 'LAMBDA(x)', 'at least one parameter'),
        ("LET even arg count", 'LET(a,1,a+1,extra)', 'odd count'),
        ("declaration not a name", 'LET(a+b,1,2)', 'not a legal name'),
        ("keyword as name", 'LAMBDA(LET,LET+1)', 'construct keywords'),
        ("storage namespace grab", 'LAMBDA(_xlpm_x,1)', '_xl storage namespace'),
    ]
    for label, formula, fragment in cases:
        try:
            transform_xlpm_names(formula)
            print(f"✗ {label}: should have raised")
            return False
        except ValueError as error:
            if fragment not in str(error):
                print(f"✗ {label}: error lacks guidance: {error}")
                return False
            print(f"✓ {label}: refused with guidance")
    return True


def main():
    """Run all tests and report results."""
    print("_xlpm name-storage transformer tests")
    print("=" * 50)

    tests = [
        test_scoping_rules,
        test_boundary_and_string_safety,
        test_idempotence_and_parsing,
        test_guided_refusals,
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
