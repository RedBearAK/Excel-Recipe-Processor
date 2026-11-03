# X_broken_tests

Parking lot, not a test suite. Modules moved here stopped passing when the
code they drilled changed shape, and were kept rather than deleted in case
a check in them is still worth porting. Nothing here is run by anything.
The rule for a file in this folder (2026-09): if it does something useful,
fix it and move it back to `tests/`; if it is obsolete, delete it.
