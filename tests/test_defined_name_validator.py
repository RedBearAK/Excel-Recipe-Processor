"""
Tests for Excel defined-name validation.

tests/test_defined_name_validator.py

Runnable with pytest, but written to run standalone and report a score.
Covers Excel's own naming rules and the stricter house style on top.
"""

from excel_recipe_processor.processors._helpers.defined_name_validator import (
    DefinedNameError,
    check_defined_name,
    validate_defined_name,
    is_valid_defined_name,
)


def test_conventional_names_pass():
    """Names following the rng_ convention are accepted."""
    print("\nTesting conventional names...")

    names = [
        'rng_PID',
        'rng_country',
        'rng_Prod_Form',
        'rng_payterms',
        'rng_EXdest',
        'rng_PID_2026',
        'Q_1',
    ]

    passed = True

    for name in names:
        problems = check_defined_name(name)
        if problems:
            print(f"  ✗ {name!r} rejected: {problems}")
            passed = False
        else:
            print(f"  ✓ {name!r} accepted")

    return passed


def test_cell_reference_names_rejected():
    """Names Excel reads as cell references are rejected."""
    print("\nTesting cell-reference-shaped names...")

    names = ['A1', 'Q1', 'TAX24', 'XFD1048576', 'q1']
    passed = True

    for name in names:
        if is_valid_defined_name(name):
            print(f"  ✗ {name!r} was accepted")
            passed = False
        else:
            print(f"  ✓ {name!r} rejected")

    return passed


def test_reserved_names_rejected():
    """Excel's reserved shorthand and built-in names are rejected."""
    print("\nTesting reserved names...")

    names = ['R', 'C', 'r', 'R1C1', 'RC', 'Print_Area', 'Database']
    passed = True

    for name in names:
        if is_valid_defined_name(name):
            print(f"  ✗ {name!r} was accepted")
            passed = False
        else:
            print(f"  ✓ {name!r} rejected")

    return passed


def test_illegal_characters_rejected():
    """Spaces and punctuation Excel disallows are rejected."""
    print("\nTesting illegal characters...")

    names = ['rng country', '1rng_PID', 'rng-PID', 'rng!PID', 'rng@PID', '']
    passed = True

    for name in names:
        if is_valid_defined_name(name):
            print(f"  ✗ {name!r} was accepted")
            passed = False
        else:
            print(f"  ✓ {name!r} rejected")

    return passed


def test_house_style_digit_separation():
    """Digits must be separated from preceding letters."""
    print("\nTesting house style digit separation...")

    passed = True

    if is_valid_defined_name('rng_PID2026'):
        print("  ✗ 'rng_PID2026' was accepted under house style")
        passed = False
    else:
        print("  ✓ 'rng_PID2026' rejected under house style")

    if is_valid_defined_name('rng_PID_2026'):
        print("  ✓ 'rng_PID_2026' accepted")
    else:
        print("  ✗ 'rng_PID_2026' was rejected")
        passed = False

    if is_valid_defined_name('rng_PID2026', enforce_house_style=False):
        print("  ✓ 'rng_PID2026' accepted with house style disabled")
    else:
        print("  ✗ 'rng_PID2026' rejected even with house style disabled")
        passed = False

    return passed


def test_minimum_length():
    """House style requires at least three characters."""
    print("\nTesting minimum length...")

    passed = True

    if is_valid_defined_name('AB'):
        print("  ✗ 'AB' was accepted")
        passed = False
    else:
        print("  ✓ 'AB' rejected as too short")

    if is_valid_defined_name('ABC'):
        print("  ✓ 'ABC' accepted")
    else:
        print("  ✗ 'ABC' was rejected")
        passed = False

    return passed


def test_validate_raises_with_detail():
    """validate_defined_name raises and explains every problem."""
    print("\nTesting validate_defined_name...")

    try:
        validate_defined_name('Q1')
    except DefinedNameError as error:
        text = str(error)
        if 'cell reference' in text:
            print(f"  ✓ Raised with useful detail: {text[:70]}...")
            return True
        print(f"  ✗ Detail missing the key reason: {text}")
        return False

    print("  ✗ No error raised for 'Q1'")
    return False


def main():
    """Run every test and report a final score."""
    print("=== Defined Name Validator Tests ===")

    tests = [
        test_conventional_names_pass,
        test_cell_reference_names_rejected,
        test_reserved_names_rejected,
        test_illegal_characters_rejected,
        test_house_style_digit_separation,
        test_minimum_length,
        test_validate_raises_with_detail,
    ]

    passed = 0

    for test_func in tests:
        try:
            if test_func():
                passed += 1
        except Exception as error:
            print(f"  ✗ {test_func.__name__} crashed: {error}")

    print(f"\n=== Results: {passed}/{len(tests)} tests passed ===")

    if passed == len(tests):
        print("✅ All defined name validator tests passed!")
        return 1

    print("❌ Some defined name validator tests failed!")
    return 0


if __name__ == '__main__':
    exit(0 if main() else 1)


# End of file #
