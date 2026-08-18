# Tab completion for `--var`

## Why it fails on zsh but not bash

`--var name=path` is one shell word. Whether the shell will complete a filename
inside it depends on whether `=` breaks words:

```
bash  COMP_WORDBREAKS:  \t\n"'@><=;|&(:      <- contains =
zsh   default:          no = in WORDCHARS    <- does not
```

So bash sees `path` as its own completable fragment and zsh sees the whole
`--var name=path` blob. Nothing to do with the tool.

Verified in bash here. zsh was not available in the test environment, so the zsh
side is from documented behaviour rather than a run.

There are four ways out, in rough order of effort.

---

## 1. `--set NAME VALUE` — no shell configuration at all

Added to the tool. The value is a separate argument, so it is an ordinary word
and both shells complete it natively:

```sh
excel-recipe-processor recipe_files/vms_process.yaml \
    --set source_download VMS_Downloads_unprocessed/<TAB>
```

`--var NAME=VALUE` still works and is unchanged. Both can be mixed; `--var` wins
if the same name appears in both, so a scripted `--set` default can be
overridden by hand on the same line.

This is the answer to "is there an intrinsic CLI solution" — there is now.

---

## 2. `setopt magic_equal_subst` — one line, fixes it everywhere

In `~/.zshrc`:

```sh
setopt magic_equal_subst
```

zsh then performs filename completion and expansion after `=` in any
`anything=expression` argument, for every command, not just this one. It also
makes `--var file=~/downloads/x.xlsx` expand the tilde, which it otherwise does
not.

Worth knowing it is global. That is usually welcome but it is a shell-wide
behaviour change, not a per-command one.

---

## 3. A completion function

`_excel-recipe-processor` in this archive. Install:

```sh
mkdir -p ~/.zsh/completions
cp _excel-recipe-processor ~/.zsh/completions/
```

and in `~/.zshrc`, **before** `compinit`:

```sh
fpath=(~/.zsh/completions $fpath)
autoload -Uz compinit && compinit
```

It completes recipe files for the positional argument, offers the external
variable names actually declared in the recipe on the command line, and
completes paths for `--set` values.

**The alias is the catch.** zsh resolves completion for an alias to the aliased
command word, which for

```sh
alias excel-recipe-processor="python -m excel_recipe_processor "
```

is `python`. So tab offers python's options. The `#compdef` line registers the
completion under the alias name, which is what makes it apply.

---

## 4. Symlinks in `~/.local/bin` instead of aliases

Worth considering for the reason you raised, though note it does not by itself
solve completion — a symlinked `excel-recipe-processor` still has no completion
function, it just stops zsh resolving to `python`.

What it does buy:

- Commands work in non-interactive contexts where the RC file is not sourced:
  scripts, cron, `find -exec`, editor build commands
- One name per tool rather than an alias that only exists in interactive shells
- Completion functions attach to a real command name, which is more predictable

The trade-off is that a symlink cannot carry `python -m`, so each needs either a
console-entry-point install (`pip install -e .`, which `setup.py` already
declares) or a two-line wrapper script.

Given `setup.py` has an `entry_points` console_scripts block, `pip install -e .`
in each repo would give you real commands with no aliases and no wrappers. That
is probably the cleanest end state, but it is a bigger change than the rest of
this file.

---

## Recommendation

`--set` alone solves the immediate problem with nothing to install. Add
`setopt magic_equal_subst` if you want `--var` to behave too — it costs one line
and helps with every other command that takes `name=path` arguments.

The completion function is worth it only if you want recipe-aware variable name
completion.

# End of file #
