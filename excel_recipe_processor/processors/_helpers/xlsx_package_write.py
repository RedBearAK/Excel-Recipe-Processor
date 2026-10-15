"""
Write a surgically edited xlsx package and PROVE the surgery stayed
inside its claims - for a new output file, or in place of the original.

excel_recipe_processor/processors/_helpers/xlsx_package_write.py

Shared by sever_external_ties and transplant_worksheet so the write
path, the verification and the in-place doctrine exist once.

THE IN-PLACE DOCTRINE (2026-09-14):
  The surgery logic is identical in both modes; only the write differs.
  In-place therefore reduces to a write that cannot leave the file in
  a half state:
    1. refuse if Excel has the file open (a ~$ lock file beside it)
    2. write the result to a temp file in the SAME directory
    3. verify the temp (below); on any failure delete it and stop
    4. copy the original to <name><backup_suffix> (refuse if that
       backup already exists - a rerun must not bury the true original)
    5. os.replace(temp, original) - atomic on the same filesystem
  A crash at any step leaves either the untouched original or the
  verified result, never a truncated zip.

VERIFICATION, both modes, on the temp before it is named anything:
  a. ZipFile.testzip() finds no bad member
  b. the part list equals the original's, minus removed parts, plus
     added parts, exactly
  c. every part the plan did NOT claim to change or remove is
     byte-identical to the original - the surgery touched only what
     it said it would
  d. the caller's own check (post-write inventory, sheet set, ...)
  e. optionally an openpyxl load, a second parser's opinion (slow on
     large files, so off by default)
"""

import os
import shutil
import zipfile
import hashlib


class PackageWriteError(Exception):
    """The write or its verification failed; nothing was replaced."""
    pass


def lock_file_for(path: str) -> str:
    """Excel's owner-lock beside an open workbook: ~$Name.xlsx"""
    directory, name = os.path.split(path)
    return os.path.join(directory, '~$' + name)


def refuse_if_open_in_excel(path: str) -> None:
    lock = lock_file_for(path)
    if os.path.exists(lock):
        raise PackageWriteError(
            f"{os.path.basename(path)} appears to be open in Excel "
            f"(lock file present: {os.path.basename(lock)}); close it first")


def _digest(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _temp_path_beside(target: str) -> str:
    """Hidden, pid-tagged, same directory (so the final rename is atomic),
    and still ending in the real extension so a second parser will open it."""
    directory, name = os.path.split(target)
    stem, ext = os.path.splitext(name)
    return os.path.join(directory, f'.{stem}.tmp-{os.getpid()}{ext}')


def write_and_verify(original_path: str, order: list, parts: dict,
                     changed: set, removed: set, output_path: str,
                     in_place: bool, backup_suffix: str,
                     verify_output=None, verify_with_openpyxl: bool = False) -> dict:
    """
    Write `parts` (in `order`) as a zip, verify it, then either rename
    it to output_path (new file) or replace original_path (in place).

    changed / removed: the part names the surgery claims to have
    rewritten / dropped. Any other difference from the original is a
    verification failure. Added parts are those in `order` that the
    original lacks; they count as changed.

    verify_output: optional callable(temp_path) raising on failure.

    Returns a dict of what was checked, for the report.
    """
    final_path = original_path if in_place else output_path
    if in_place:
        refuse_if_open_in_excel(original_path)
        if not os.access(original_path, os.W_OK):
            raise PackageWriteError(f"not writable: {original_path}")
        backup_path = original_path + backup_suffix
        if os.path.exists(backup_path):
            raise PackageWriteError(
                f"backup already exists, move it aside first: {backup_path}")
    else:
        backup_path = ''
        if os.path.exists(output_path):
            raise PackageWriteError(f"output already exists: {output_path}")

    temp_path = _temp_path_beside(final_path)
    try:
        with zipfile.ZipFile(temp_path, 'w', zipfile.ZIP_DEFLATED) as archive:
            for name in order:
                archive.writestr(name, parts[name])
        checks = _verify_temp(original_path, temp_path, order, changed, removed)
        if verify_output is not None:
            verify_output(temp_path)
            checks['caller_check'] = True
        if verify_with_openpyxl:
            _verify_with_openpyxl(temp_path)
            checks['openpyxl_load'] = True
        if in_place:
            shutil.copy2(original_path, backup_path)
            checks['backup'] = backup_path
        os.replace(temp_path, final_path)
    except Exception:
        if os.path.exists(temp_path):
            os.remove(temp_path)
        raise
    checks['written'] = final_path
    return checks


def _verify_temp(original_path: str, temp_path: str, order: list,
                 changed: set, removed: set) -> dict:
    with zipfile.ZipFile(temp_path, 'r') as archive:
        bad = archive.testzip()
        if bad is not None:
            raise PackageWriteError(f"zip integrity check failed at member {bad}")
        written_names = archive.namelist()
        written = {name: archive.read(name) for name in written_names}

    with zipfile.ZipFile(original_path, 'r') as archive:
        original_names = archive.namelist()
        original_digests = {name: _digest(archive.read(name)) for name in original_names}

    if written_names != list(order):
        raise PackageWriteError("written part order differs from the plan")

    still_present = set(removed) & set(written_names)
    if still_present:
        raise PackageWriteError(
            f"parts claimed removed are still present: {sorted(still_present)[:10]}")
    expected = (set(original_names) - set(removed)) | (set(order) - set(original_names))
    if set(written_names) != expected:
        unexpected = sorted(set(written_names) ^ expected)
        raise PackageWriteError(
            f"part set differs from the plan: {unexpected[:10]}")

    added = set(order) - set(original_names)
    untouched_claimed = set(original_names) - set(changed) - set(removed)
    drifted = [name for name in sorted(untouched_claimed)
               if _digest(written[name]) != original_digests[name]]
    if drifted:
        raise PackageWriteError(
            f"{len(drifted)} part(s) changed that the surgery did not claim: "
            f"{drifted[:10]}")

    return {
        'zip_integrity': True,
        'parts_original': len(original_names),
        'parts_written': len(written_names),
        'parts_changed': sorted(set(changed) & set(original_names)),
        'parts_added': sorted(added),
        'parts_removed': sorted(set(removed) & set(original_names)),
        'parts_untouched_verified': len(untouched_claimed),
    }


def _verify_with_openpyxl(temp_path: str) -> None:
    import openpyxl
    try:
        workbook = openpyxl.load_workbook(temp_path, read_only=True)
        workbook.close()
    except Exception as error:
        raise PackageWriteError(f"openpyxl could not load the result: {error}")

# End of file #
