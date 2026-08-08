# on_existing_cell

Supersedes the seed_donor_formulas in `fix_sentinel_overwritten.tgz`.

## The answer to the question

No, it could not overwrite. Worse, the two halves disagreed and neither was
configurable:

| Half | On an occupied cell |
|---|---|
| transplant (seeded rows) | raised |
| fill_down | silently skipped |

The skip was hardcoded while fixing the sentinel row, which turned a policy
choice into a fixed rule and left the same collision producing different
outcomes depending on which row it landed on.

## Now

```yaml
on_existing_cell: "error"      # default
                  "skip"
                  "overwrite"
```

Applies to both halves, so the behaviour no longer depends on which row a
collision falls in.

| Value | Behaviour |
|---|---|
| `error` | stop, naming the cell and its current contents |
| `skip` | leave it alone, count it, report the total |
| `overwrite` | write anyway |

Verified against a cell deliberately occupied mid-fill:

```
error      -> halted: Fill target ... already contains data: '...'
skip       -> ran; the cell kept its original value
overwrite  -> ran; the cell now holds the translated formula
```

## Note on the changed default

`fill_down` previously skipped occupied cells always; the default is now
`error`. That is a behaviour change, chosen because:

- the recipe writes the formula columns blank, so nothing legitimately occupies
  them and a collision means something unexpected happened
- the old silent skip is exactly how the sentinel-overwrite problem stayed
  invisible for several runs
- `skip` is one line away when a trailing marker row is genuinely wanted

The current recipes run clean on the new default — 41 steps, formulas filled
rows 5 to 83, no collisions.

## Spotted while testing

The donor at `lookup_source_files/vms_processed_donor.xlsx` still carries
content at **row 8682** — the leftover sentinel row that survived the
Shift-Cmd-Down selection. It does no harm as a donor, since only rows 2-4 are
read, but it is what keeps the file at 0.66 MB and it will trip `error` mode for
anyone using that file as a fill target rather than a source.

Worth deleting that row now that the sentinel concept is retired.

# End of file #
