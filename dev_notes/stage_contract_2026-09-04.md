# The stage contract: use requires declaration (2026-09-04)

## What a stage is, and what this does not change

A named stage is a handle on a DataFrame that any step, anywhere in the
recipe, can point at for any purpose: read, filter, stack, verify
against, look up in, export. Nothing here touches that. This is about the
bookkeeping around the handles - who is counting references, from what
evidence - which is where today's failures lived.

## One source of truth, enforced from both sides

A processor declares its stage keys once, in `config_schema()`:
`stage_in` for stages it reads, `stage_out` for stages it writes (plus
`computed_stage_writes()` for names it derives). Everything derives from
that single statement:

- the validator checks the recipe's stage graph from it
- the auto-free plan builds its usage table from it
  (`plan_auto_free`, same function, same resolved configs)
- and now the runtime refuses any use that was not declared

`begin_step()` opens a contract window around each step's execution;
inside it `load_stage()` accepts only the step's declared reads and
`save_stage()` only its declared writes. A violation halts at the use:

    Step 11 ('...') read stage 'stg_x' without declaring it: no stage_in
    key in the processor's config_schema() names that stage (declared:
    [...]). The validator and the auto-free plan are built from those
    declarations, so an undeclared use cannot be counted, checked, or
    freed correctly. Declare the key.

So: declared ⇒ counted (the audit test proves every declared key at any
nesting is in the plan) and used ⇒ declared (the runtime refuses the
rest). An undercount can no longer exist silently; the only way to
create one is an undeclared use, and that fails the first time the code
path runs - on the author's desk, naming the processor - not later as a
stage freed too early in someone's recipe.

## Peeks

`peek_stage()` reads without consuming and is exempt by construction
(it suspends the window for the call). It is for inspection, dumps, and
`free_stages`, whose stages are `stage_release`, not reads. A processor
that transforms data uses `load_stage()` and declares.

## Scope

The window exists only while the pipeline runs a step; direct calls
from tests and tooling are unconstrained, and `cleanup_stages()` clears
the plan and the window. `--validate` builds the plan without opening
any window.

## Verified

Every processor's examples through the real pipeline (CLI suite and the
288-example suite): zero violations. The VMS merge and only-in recipes:
clean. `vms_process.yaml` validates; its first live run under the
contract is the acceptance test for its 109 steps.
`tests/test_auto_free_default.py` 6/6, including the contract window.
