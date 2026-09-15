#!/usr/bin/env python3
"""
Bump excel_recipe_processor/_version.py to today's date with the next suffix.

dev_notes/bump_version.py  (Excel-Recipe-Processor)

    python3 dev_notes/bump_version.py            -> 20260913.1 (or 20260914.0 tomorrow)
    python3 dev_notes/bump_version.py --check    -> prints the version, exit 1 if it is not today's

Ported from SBS-Corp-DB-Replica (2026-09-13), where a version bumped by
text replacement stood still for a day and a half while the code moved
on. ERP's had stood still for a YEAR - 20250807.0 until Kris set it by
hand on 2026-09-10. This reads the current value, computes the next,
writes it, and prints both: a bump that cannot no-op. ERP's convention
starts each day at .0 (the replica's starts at .1); kept.
"""

import os
import re
import sys
import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
PATH = os.path.join(HERE, '..', 'excel_recipe_processor', '_version.py')
version_rgx = re.compile(r'__version__ = "(\d{8})\.(\d+)"')


def main() -> int:
    text = open(PATH, encoding='utf-8').read()
    match = version_rgx.search(text)
    if not match:
        print(f"no version line in {PATH}")
        return 1
    day, suffix = match.group(1), int(match.group(2))
    today = datetime.date.today().strftime('%Y%m%d')
    current = f"{day}.{suffix}"
    if '--check' in sys.argv:
        print(current)
        return 0 if day == today else 1
    new = f"{today}.{suffix + 1 if day == today else 0}"
    open(PATH, 'w', encoding='utf-8').write(text.replace(match.group(0), f'__version__ = "{new}"'))
    print(f"{current} -> {new}")
    return 0


if __name__ == '__main__':
    sys.exit(main())


# End of file #
