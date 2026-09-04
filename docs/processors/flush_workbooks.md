# `flush_workbooks`

**Family:** `file_ops`

Write every session-held workbook to disk now, instead of at run end

## Notes

- **when to use**: an external tool or risky operation needs the file on disk mid-run
- **after effects**: the session empties; later file operations reload from disk

## Keys

Generated from the declared schema; keys not listed are refused at recipe load.

- `step_description`: str - Human-readable step name; apostrophe-free by house style
- `processor_type`: str; REQUIRED - Registered processor name
- `on_error`: str; one of halt, skip, continue - Per-step override of the recipe error policy

## Examples

Every step below validates against the schema (tests/test_examples_validate_against_schemas.py).

### basic

Checkpoint the output file mid-run

```yaml
# File operations (named ranges, formula seeding, formatting) share one
# live in-memory workbook per file and normally save ONCE, after the
# last step succeeds. If an external tool needs the file on disk
# mid-run, or you want a checkpoint before a risky operation, flush
# explicitly. After a flush the session is empty; a later file
# operation reloads from disk, which is correct because the disk copy
# is now the truth.

settings:
  description: "Checkpoint pending workbook writes before a risky step"

recipe:
  - # OPT - Human-readable step description
    # Default value: "Unnamed flush_workbooks step"
    step_description: "Write all pending workbook changes to disk"
    # REQ - Must be "flush_workbooks" for this processor type
    processor_type: "flush_workbooks"
```

