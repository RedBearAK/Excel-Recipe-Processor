"""
Tests for named-lambda human<->Excel translation in manage_named_objects.

tests/test_named_lambda_translation.py

The stored grammar is harvest-verified (2026-08-14, real Excel output):
_xlfn.LAMBDA with _xlpm.-prefixed parameters at declaration and every
body occurrence, LEGACY function names BARE (SUM inside a stored lambda
carries no prefix), future functions _xlfn.-prefixed via the shared map.
Guards the 2026-08-14 regression where a hand-rolled list blanket-
prefixed legacy names. Runnable directly or with pytest.
"""

import sys

from excel_recipe_processor.processors.manage_named_objects_processor import (
    ManageNamedObjectsProcessor,
)


def make_processor():
    """A processor instance for calling the translation methods."""
    config = ManageNamedObjectsProcessor.get_minimal_config()
    config['processor_type'] = 'manage_named_objects'
    return ManageNamedObjectsProcessor(config)


def test_human_to_excel():
    """Human definitions store with harvest-verified grammar."""
    print("\nTesting human -> Excel storage translation...")
    processor = make_processor()

    cases = [
        ('LAMBDA(v, IF(v="", "", v))', ['v'],
         '_xlfn.LAMBDA(_xlpm.v,IF(_xlpm.v="","",_xlpm.v))'),
        # Legacy stays bare, future gets the map prefix
        ('LAMBDA(x, SUM(x) + TEXTJOIN(",", TRUE, x))', ['x'],
         '_xlfn.LAMBDA(_xlpm.x,SUM(_xlpm.x) + _xlfn.TEXTJOIN(",",TRUE,_xlpm.x))'),
        # Parameter name inside a string literal must NOT be prefixed
        ('LAMBDA(v, IF(v="v", "literal v", v))', ['v'],
         '_xlfn.LAMBDA(_xlpm.v,IF(_xlpm.v="v","literal v",_xlpm.v))'),
        # Token boundaries: param must not fire inside longer names
        ('LAMBDA(v, IF(rng_values=v, v, rng_values))', ['v'],
         '_xlfn.LAMBDA(_xlpm.v,IF(rng_values=_xlpm.v,_xlpm.v,rng_values))'),
    ]
    for human, params, expected in cases:
        result = processor.translate_lambda_to_excel(human, params)
        if result != expected:
            print(f"✗ {human}\n  got      {result}\n  expected {expected}")
            return False
        print(f"✓ {human}")
    return True


def test_excel_to_human_round_trip():
    """Excel storage translates to readable form and back losslessly."""
    print("\nTesting Excel -> human -> Excel round trip...")
    processor = make_processor()

    stored = '_xlfn.LAMBDA(_xlpm.v,IF(_xlpm.v="","",_xlpm.v))'
    human, params = processor.translate_lambda_to_human(stored)
    if params != ['v'] or 'IF(v=""' not in human.replace(' ', ''):
        print(f"✗ To-human wrong: {human!r} params {params}")
        return False
    print(f"✓ To human: {human}")

    back = processor.translate_lambda_to_excel(human, params)
    if back != stored:
        print(f"✗ Round trip drifted:\n  {stored}\n  {back}")
        return False
    print("✓ Round trip byte-identical")
    return True


def test_house_prefix_underscore_convention():
    """Group prefixes require the underscore; ordinary words do not trip."""
    print("\nTesting the prefix-underscore house convention...")

    from excel_recipe_processor.processors._helpers.defined_name_validator import (
        check_defined_name,
    )

    for good in ('fn_blank_safe', 'rng_customers', 'tbl_orders',
                 'fml_net_calc', 'fnord_thing', 'final_total'):
        problems = check_defined_name(good)
        if problems:
            print(f"✗ {good} wrongly flagged: {problems}")
            return False
    print("✓ Underscored group names and ordinary words pass")

    for bad in ('fn_BlankSafe'.replace('_', ''), 'rngCustomers', 'tblOrders'):
        problems = check_defined_name(bad)
        if not any('underscore' in p for p in problems):
            print(f"✗ {bad} not flagged for the missing underscore")
            return False
    print("✓ camelCase-after-prefix names refused with guidance")
    return True


def main():
    """Run all tests and report results."""
    print("Named-lambda translation tests")
    print("=" * 50)

    tests = [
        test_human_to_excel,
        test_excel_to_human_round_trip,
        test_house_prefix_underscore_convention,
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
