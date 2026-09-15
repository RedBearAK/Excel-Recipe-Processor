"""
Test log_format.duration: elapsed time at the precision that suits it.

File: excel_recipe_processor/tests/test_log_format_duration.py

A completion line stays in the log, so "0s total" for a 7 ms phase
says nothing; the live pulse line ticks in whole seconds on purpose.
This pins the scale-aware formatter both completion lines now use, and
that a real pulse completion carries it.

Run: PYTHONPATH=. python3 tests/test_log_format_duration.py
"""

import io
import os
import sys
import logging

from excel_recipe_processor.core.log_format import duration


def test_scales() -> bool:
    print('\nEach scale gets the precision that suits it...')
    cases = [(0.007, '0.007s'), (0.0004, '0.000s'), (0.43, '0.43s'), (1.4, '1.4s'),
             (12.04, '12.0s'), (59.99, '60.0s'), (60, '1m 0.0s'), (134.2, '2m 14.2s'), (-1, '0.000s')]
    bad = [(given, expected, duration(given)) for given, expected in cases if duration(given) != expected]
    for given, expected, got in bad:
        print(f"  FAIL {given} -> {got!r}, expected {expected!r}")
    print(f"  {len(cases) - len(bad)}/{len(cases)} -> {'OK' if not bad else 'FAIL'}")
    return not bad


def test_pulse_completion_line_uses_it() -> bool:
    print('\nA pulse completion line logs a precise duration, never a bare whole-second 0s...')
    os.environ['ERP_PULSE'] = 'force'
    from excel_recipe_processor.core import terminal_pulse
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    logger = logging.getLogger('excel_recipe_processor.core.terminal_pulse')
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    try:
        pulse = terminal_pulse.TerminalPulse('Formatting sheet')
        pulse.tick("1/1: 'Rule_Codes'")
        pulse.done()
    finally:
        logger.removeHandler(handler)
    text = stream.getvalue()
    completion = [line for line in text.splitlines() if 'total' in line]
    good = bool(completion) and not any(line.rstrip().endswith(' 0s total') for line in completion) \
        and all('s total' in line for line in completion)
    print(f"  {completion[-1] if completion else 'no completion line'!r} -> {'OK' if good else 'FAIL'}")
    return good


def main() -> int:
    tests = [test_scales, test_pulse_completion_line_uses_it]
    passed = sum(1 for test in tests if test())
    print(f"\n{passed}/{len(tests)} tests passed")
    return 0 if passed == len(tests) else 1


if __name__ == '__main__':
    sys.exit(main())


# End of file #
