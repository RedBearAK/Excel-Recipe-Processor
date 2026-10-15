"""
Code changed, version bumped: the one cannot happen without the other.

tests/test_version_bumped.py

The version stood still at 20260914.3 through two deliveries (2026-09-28
"Empty policies", 2026-10-06 the optional-columns keys) because bumping
was a habit, and a habit is not a backstop. This asks git two questions
and fails on either:

    working tree   a file under the package or tests is modified, added or
                   staged while _version.py is not: a delivery is being
                   prepared without a bump
    history        the newest commit touching the package or tests is newer
                   than the newest commit touching _version.py: a delivery
                   was committed without one

Outside a git checkout it cannot judge, says so, and passes - a drill
that cannot see must not block. Run `python3 dev_notes/bump_version.py`
to make it pass; a bump that cannot no-op.

Run: PYTHONPATH=. python3 tests/test_version_bumped.py
"""

import os
import sys
import subprocess


HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.normpath(os.path.join(HERE, '..'))
PACKAGE = 'excel_recipe_processor'
VERSION_FILE = f'{PACKAGE}/_version.py'
WATCHED = [PACKAGE, 'tests']


def git(*arguments) -> str:
    completed = subprocess.run(['git', *arguments], cwd=ROOT, capture_output=True, text=True)
    if completed.returncode != 0:
        raise RuntimeError(completed.stderr.strip() or 'git failed')
    return completed.stdout


def changed_in_working_tree() -> list:
    """Paths under the watched folders with uncommitted changes (modified,
    added, staged, untracked). Porcelain lines are 'XY path': the leading
    space of ' M' is significant, so nothing here strips the left side."""
    lines = git('status', '--porcelain', '--untracked-files=all', '--', *WATCHED).splitlines()
    return sorted(line[3:].rstrip() for line in lines if len(line) > 3)


def newest_commit_time(*paths) -> int:
    text = git('log', '-1', '--format=%ct', '--', *paths).strip()
    return int(text) if text else 0


def test_working_tree_changes_come_with_a_bump() -> bool:
    print('\nUncommitted code or test changes must come with a changed _version.py...')
    changed = changed_in_working_tree()
    code_changed = [path for path in changed if path != VERSION_FILE and not path.endswith('.pyc') and '__pycache__' not in path]
    version_changed = VERSION_FILE in changed
    if not code_changed:
        print('  ✓ nothing changed in the working tree')
        return True
    if version_changed:
        print(f'  ✓ {len(code_changed)} changed file(s) and _version.py is changed too')
        return True
    print(f'  ✗ {len(code_changed)} file(s) changed but _version.py is not: {code_changed[:6]}'
          f'{" ..." if len(code_changed) > 6 else ""} - run python3 dev_notes/bump_version.py')
    return False


def test_history_never_moved_the_code_without_the_version() -> bool:
    print('\nThe newest commit touching code or tests is no newer than the newest touching _version.py...')
    code_time = newest_commit_time(*WATCHED)
    version_time = newest_commit_time(VERSION_FILE)
    if code_time <= version_time:
        print('  ✓ history is consistent')
        return True
    if VERSION_FILE in changed_in_working_tree():
        # the bump that history lacks is sitting in the working tree: the
        # next commit carries it, which is the only remedy there is
        print('  ✓ history lacked a bump; the uncommitted _version.py supplies it - commit it with the code')
        return True
    offender = git('log', '-1', '--format=%h %cs %s', '--', *WATCHED).strip()
    last_bump = git('log', '-1', '--format=%h %cs %s', '--', VERSION_FILE).strip()
    print(f'  ✗ commit "{offender}" changed code or tests after the last bump "{last_bump}"')
    return False


def main() -> bool:
    print('=== version bump backstop ===')
    try:
        git('rev-parse', '--is-inside-work-tree')
    except (RuntimeError, OSError) as error:
        print(f'  not a git checkout ({error}); cannot judge, passing')
        return True
    results = [test_working_tree_changes_come_with_a_bump(), test_history_never_moved_the_code_without_the_version()]
    passed = sum(1 for good in results if good)
    print(f'\n=== Results: {passed}/{len(results)} checks passed ===')
    return passed == len(results)


if __name__ == '__main__':
    sys.exit(0 if main() else 1)


# End of file #
