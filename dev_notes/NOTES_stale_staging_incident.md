# NOTES: the stale staging directory incident (2026-08-16)

The erp_log_extension_policy.tgz (first build) was supposed to carry
ONE file - core/main.py with the extension-policy docstring - and
instead carried six: five injection-cluster files swept in from a
STALE pre-compaction staging directory (/home/claude/xp_patch, almost
certainly the old xlop-optionals build). mkdir -p succeeded silently
on the existing directory; tar archived everything in it; and the
delivery was presented WITHOUT the tar-tzf listing that most other
deliveries got. Three of the five were genuinely regressive - worst,
xlpm_name_storage.py was 96 lines behind, which would have stripped
post-delivery adversarial hardening from the storage transformer had
it been committed.

Caught by the user reading git status before committing - which is
the last line of defense working, not the process working.

## Conventions corrected (both now mandatory for every delivery)

1. STAGING DIRECTORIES ARE ALWAYS CREATED FRESH: rm -rf the staging
   path before mkdir. mkdir -p's silence on existing directories is
   exactly the wrong behavior for a build root that must contain
   only this delivery's files.
2. TAR LISTINGS ARE PRINTED BEFORE PRESENTING, EVERY TIME - the
   tar tzf output is part of the delivery, not an optional check.
   A TGZ whose contents were never displayed is unverified cargo.

Recovery: git restore the five uninvited files (three stale, two
identical); keep the main.py docstring change. This rebuilt archive
contains exactly core/main.py plus this note.
