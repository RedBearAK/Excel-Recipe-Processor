"""
Drift alarm for the committed capabilities reference snapshot.

tests/test_capabilities_snapshot_drift.py

current_capabilities.json at the repo root is a reference document,
like the README: the output of `--list-capabilities --json`, committed
so capability changes are deliberate and reviewed. This test compares
the committed snapshot against the LIVE output and fails loud with an
itemized delta when they diverge - an unintended processor
disappearance, a renamed capability key, or an accidental registration
change becomes a red test naming exactly what moved, instead of a
snapshot that silently rots (the previous copy sat at 17 processors
while the tool grew to 44).

When the drift is INTENDED, refresh the snapshot and commit it:

    PYTHONPATH=. python3 -m excel_recipe_processor \\
        --list-capabilities --json > current_capabilities.json

Runnable standalone or under pytest; the exit code carries the verdict.
"""

import sys
import json
import subprocess

from pathlib import Path


REPO_ROOT = Path(__file__).parent.parent
SNAPSHOT_PATH = REPO_ROOT / 'current_capabilities.json'
REFRESH_COMMAND = (
    "PYTHONPATH=. python3 -m excel_recipe_processor "
    "--list-capabilities --json > current_capabilities.json"
)


def load_live_capabilities():
    """Run the CLI and parse its JSON capabilities output."""
    result = subprocess.run(
        [sys.executable, '-m', 'excel_recipe_processor',
         '--list-capabilities', '--json'],
        capture_output=True, text=True, timeout=120, cwd=str(REPO_ROOT)
    )
    if result.returncode != 0:
        print(f"✗ CLI failed (rc {result.returncode}): {result.stderr.strip()}")
        return None
    return json.loads(result.stdout)


def test_snapshot_file_exists():
    """The reference document must exist at the repo root."""
    print("Checking that the snapshot reference exists...")
    if SNAPSHOT_PATH.exists():
        print(f"✓ Found {SNAPSHOT_PATH.name}")
        return True
    print(f"✗ {SNAPSHOT_PATH.name} is missing from the repo root")
    print(f"  Create it with: {REFRESH_COMMAND}")
    return False


def test_snapshot_matches_live_output():
    """The committed snapshot must match the live capabilities exactly."""
    print("\nComparing the committed snapshot against live output...")

    try:
        snapshot = json.loads(SNAPSHOT_PATH.read_text())
    except (OSError, json.JSONDecodeError) as error:
        print(f"✗ Snapshot unreadable: {error}")
        return False

    live = load_live_capabilities()
    if live is None:
        return False

    if snapshot == live:
        total = live.get('system_info', {}).get('total_processors', '?')
        print(f"✓ Snapshot matches live output ({total} processors)")
        return True

    # Itemize the drift so the alarm names what moved
    print("✗ Capabilities have DRIFTED from the committed snapshot:")

    snap_procs = set(snapshot.get('processors', {}).keys())
    live_procs = set(live.get('processors', {}).keys())

    for name in sorted(live_procs - snap_procs):
        print(f"  + processor added:   {name}")
    for name in sorted(snap_procs - live_procs):
        print(f"  - processor removed: {name}")

    for name in sorted(snap_procs & live_procs):
        if snapshot['processors'][name] != live['processors'][name]:
            snap_keys = set(snapshot['processors'][name])
            live_keys = set(live['processors'][name])
            added_keys = sorted(live_keys - snap_keys)
            removed_keys = sorted(snap_keys - live_keys)
            changed_keys = sorted(
                key for key in (snap_keys & live_keys)
                if snapshot['processors'][name][key] != live['processors'][name][key]
            )
            detail_parts = []
            if added_keys:
                detail_parts.append(f"keys added {added_keys}")
            if removed_keys:
                detail_parts.append(f"keys removed {removed_keys}")
            if changed_keys:
                detail_parts.append(f"values changed {changed_keys}")
            print(f"  ~ processor changed: {name} ({'; '.join(detail_parts)})")

    if snapshot.get('system_info') != live.get('system_info'):
        print("  ~ system_info changed")

    print("\n  If this drift is INTENDED, refresh the reference and commit it:")
    print(f"    {REFRESH_COMMAND}")
    return False


def main():
    """Run every check and report a final score."""
    print("=== capabilities snapshot drift alarm ===")

    tests = [
        test_snapshot_file_exists,
        test_snapshot_matches_live_output,
    ]

    passed = 0
    for test_func in tests:
        try:
            if test_func():
                passed += 1
        except Exception as error:
            print(f"✗ {test_func.__name__} crashed: {error}")

    print(f"\n=== Results: {passed}/{len(tests)} checks passed ===")
    return passed == len(tests)


if __name__ == '__main__':
    sys.exit(0 if main() else 1)

# End of file #
