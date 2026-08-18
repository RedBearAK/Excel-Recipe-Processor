# The four missing format features, plus a create_stage blocker

Built on dev_beta @ 7badc89 plus the earlier fixes. All four formatting features
turned out to be straightforward, because the hard part — resolving a column
NAME to a column letter — was already solved by `excel_range_resolver` for
named ranges.

## New: `column_formats` and `hidden_columns`

```yaml
formatting:
  - sheet: "VMS Data"
    on_missing_column: "error"        # "error", "warn" (default), "skip"
    column_formats:
      - columns: ["Cases (24)", "Gross Wt", "Packages", "Units", "Net Weight"]
        number_format: "thousands"
      - columns: ["Price", "Total Price", "Freight Invoice", "Freight Total"]
        number_format: "accounting"
      - columns: ["Product ID"]
        alignment_horizontal: "center"
    hidden_columns: ["Notes", "Original Van Numbers"]
```

Rules accept `number_format`, `alignment_horizontal`, `alignment_vertical` and
`wrap_text`. They apply to **data rows only** — the header keeps its own styling.

### Format aliases

Excel format codes are cryptic and easy to get subtly wrong, so the common ones
have readable names. Any literal Excel code still works.

| Alias | Code |
|---|---|
| `thousands` | `#,##0` |
| `accounting` | `_($* #,##0.00_);_($* (#,##0.00);_($* "-"??_);_(@_)` |
| `currency` | `$#,##0.00` |
| `percent`, `date`, `integer`, `text` | … |

The accounting code is the one worth having an alias for: currency symbol pinned
left, negatives in parentheses, zero as a dash. Hand-typing it is a coin flip.

### Ordering inside format_excel

Column formats run **before** auto-fit, because `1,234` is wider than `1234` and
an accounting format wider still. Hiding runs **after** sizing, so auto-fit does
not spend effort measuring a column nobody will see.

## Bug: create_stage could not be used in any recipe

Its own documented usage example failed validation:

```
Step 'x': missing required field 'source_stage'
Step 'x': missing required field 'save_to_stage'
```

`recipe_loader` has two validation paths. The **legacy** one exempts
`create_stage`, `debug_breakpoint`, `format_excel` and `generate_column_config`
by name. The **class-based** one runs first and has no exemptions at all, so
anything that is not Import, Export or FileOps fell through to "needs both
stages". `create_stage` and `copy_stage` are plain `BaseStepProcessor`, so both
were unusable.

Rather than copy the hardcoded list into the second path, the requirement is now
declared on the processor:

```python
class BaseStepProcessor(ABC):
    requires_source_stage  = True
    requires_save_to_stage = True

class CreateStageProcessor(BaseStepProcessor):
    requires_source_stage  = False
    requires_save_to_stage = False

class CopyStageProcessor(BaseStepProcessor):
    requires_save_to_stage = False      # names its output with 'stage_name'
```

Both the validator and the pipeline dispatch read the flags, so a new processor
declares its own shape and no list can drift.

The pipeline dispatch checks the attribute, **not** `isinstance` — the existing
comment warning against `isinstance(processor, BaseStepProcessor)` still holds.

`create_stage` also still demands a pass-through DataFrame it ignores, a
leftover from before the stages-only migration. The pipeline hands it an empty
frame. Worth cleaning up properly at some point.

## Verification

`test_format_excel_column_formats.py` — 7/7. Covers alias resolution, formats
landing on data cells, the header row being left alone, hiding, per-column
alignment not disturbing neighbours, the missing-column policy, and rejection of
a rule that specifies no action.

Full suite: **20 failures before and after**, identical to baseline.

# End of file #
