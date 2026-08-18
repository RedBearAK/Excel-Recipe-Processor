"""
Tests for formula storage-form transforms (spill '#', eta, LAMBDA guard).

tests/test_inject_formula_storage_forms.py

The expected outputs are Excel's own storage, harvested verbatim from a
hand-built workbook (2026-08-14, data-validation-test.xlsx): spilled
references stored as _xlfn.ANCHORARRAY(...), eta-reduced aggregation
names stored with _xleta., LAMBDA parameters stored with _xlpm. (the
latter unimplemented, therefore refused loudly). Runnable directly or
with pytest.
"""

import sys

from excel_recipe_processor.processors._helpers.inject_formulas_functions import (
    prefix_future_functions,
    transform_storage_forms,
)


def stored(formula: str) -> str:
    """The full storage pipeline: call prefixes, then storage forms."""
    return transform_storage_forms(prefix_future_functions(formula))


def test_harvested_forms():
    """The exact shapes Excel wrote in the harvest workbook."""
    print("\nTesting harvested storage forms...")

    cases = [
        # (recipe syntax, expected storage)
        ('=SUM(D1#)',
         '=SUM(_xlfn.ANCHORARRAY(D1))'),
        ('=GROUPBY(A1:A6,B1:B6,SUM)',
         '=_xlfn.GROUPBY(A1:A6,B1:B6,_xleta.SUM)'),
        ('=COUNTA(Lookups!$Z$2#)',
         '=COUNTA(_xlfn.ANCHORARRAY(Lookups!$Z$2))'),
        ("=SUM('Look Ups'!A2#)",
         "=SUM(_xlfn.ANCHORARRAY('Look Ups'!A2))"),
        ('=SUM(rng_pick#)',
         '=SUM(_xlfn.ANCHORARRAY(rng_pick))'),
        ('=GROUPBY(a,b,AVERAGE,0,1,1,c=$C$1)',
         '=_xlfn.GROUPBY(a,b,_xleta.AVERAGE,0,1,1,c=$C$1)'),
    ]
    for recipe_form, expected in cases:
        result = stored(recipe_form)
        if result != expected:
            print(f"✗ {recipe_form}\n  got      {result}\n  expected {expected}")
            return False
        print(f"✓ {recipe_form} -> {expected}")
    return True


def test_untouched_forms():
    """Ordinary calls, strings, and prefixed text stay exactly as written."""
    print("\nTesting forms that must NOT change...")

    cases = [
        # SUM as a call is not an eta reference
        ('=SUM(A1:A6)', '=SUM(A1:A6)'),
        # '#' and aggregation names inside string literals are text
        ('=TEXTJOIN(",",TRUE,"see Z1# or SUM",A1)',
         '=_xlfn.TEXTJOIN(",",TRUE,"see Z1# or SUM",A1)'),
        # Error literals are not spill references
        ('=IF(ISNA(A1),"#N/A text",A1)', '=IF(ISNA(A1),"#N/A text",A1)'),
        # Already-transformed text passes through (idempotence)
        ('=_xlfn.GROUPBY(a,b,_xleta.SUM)', '=_xlfn.GROUPBY(a,b,_xleta.SUM)'),
    ]
    for given, expected in cases:
        result = stored(given)
        if result != expected:
            print(f"✗ {given}\n  got      {result}\n  expected {expected}")
            return False
        print(f"✓ unchanged: {given}")

    # Full idempotence: transforming twice equals transforming once
    once = stored('=SUM(D1#)+GROUPBY(a,b,SUM)')
    if transform_storage_forms(once) != once:
        print("✗ transform is not idempotent")
        return False
    print("✓ idempotent on re-run")
    return True


def test_xlpm_constructs_transform():
    """LAMBDA/LET now TRANSFORM (guard retired); strings still safe."""
    print("\nTesting _xlpm construct transformation via the live pipeline...")

    from excel_recipe_processor.processors._helpers.xlpm_name_storage import (
        transform_xlpm_names,
    )

    def live(formula):
        return transform_storage_forms(
            prefix_future_functions(transform_xlpm_names(formula)))

    cases = [
        # The harvest workbook's H8, byte-exact
        ('=GROUPBY(A1:A6,B1:B6,LAMBDA(x,SUM(x)))',
         '=_xlfn.GROUPBY(A1:A6,B1:B6,_xlfn.LAMBDA(_xlpm.x,SUM(_xlpm.x)))'),
        # The LET wrapper that was previously refused
        ('=LET(v,FILTER(a,b),IF(v="","",v))',
         '=_xlfn.LET(_xlpm.v,_xlfn._xlws.FILTER(a,b),IF(_xlpm.v="","",_xlpm.v))'),
        # Sequential LET scoping: outer b in v1 stays bare
        ('=LET(a,b+1,b,2,a+b)',
         '=_xlfn.LET(_xlpm.a,b+1,_xlpm.b,2,_xlpm.a+_xlpm.b)'),
    ]
    for given, expected in cases:
        result = live(given)
        if result != expected:
            print(f"✗ {given}\n  got      {result}\n  expected {expected}")
            return False
        print(f"✓ {given}")

    benign = '=IF(A1="LAMBDA(x)","yes","no")'
    if live(benign) != benign:
        print("✗ LAMBDA text inside a string literal was rewritten")
        return False
    print("✓ 'LAMBDA(' inside a string literal untouched")

    # Optional parameters: harvested 2026-08-14, now a capability -
    # declaration stores _xlop.y (brackets vanish), uses store _xlpm.y
    result = transform_xlpm_names('=LAMBDA(x,[y],x+y)')
    if result != '=LAMBDA(_xlpm.x,_xlop.y,_xlpm.x+_xlpm.y)':
        print(f"✗ Optional-parameter storage wrong: {result}")
        return False
    print("✓ Optional [y] stores as _xlop declaration + _xlpm uses")
    return True


def main():
    """Run all tests and report results."""
    print("Formula storage-form transform tests")
    print("=" * 50)

    tests = [
        test_harvested_forms,
        test_untouched_forms,
        test_xlpm_constructs_transform,
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
