# Documentation

| Read | For |
|---|---|
| [`CAPABILITIES.md`](CAPABILITIES.md) | what the tool can do, in prose: the run, the stage model, every processor by purpose |
| [`STEP_SCHEMAS.md`](STEP_SCHEMAS.md) | every processor's step keys - kinds, required, defaults, allowed values, variants. Generated: `python -m excel_recipe_processor --export-schemas md` |
| [`WRITING_A_PROCESSOR.md`](WRITING_A_PROCESSOR.md) | adding a processor: family, schema, naming rules |
| [`cli/commands.md`](cli/commands.md) | the command line, generated from `--help` |
| `../dev_notes/KEY_MIGRATIONS_LEDGER.md` | every renamed or removed term, old beside new, for maintenance sweeps |

Worked examples per processor: `python -m excel_recipe_processor --get-usage-examples <name>`.
The settings block: `--get-settings-examples`. Both are validated in the test suite.
