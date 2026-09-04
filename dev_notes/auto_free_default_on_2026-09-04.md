# auto_free_stages defaults to on (2026-09-04)

The VMS merge recipe, which said nothing about it, ended its run with all
13 stages alive and 149 MB held - and the assumption had been that
freeing was automatic. Now it is. `settings: auto_free_stages: false` is
the opt-out; `true` is the (now redundant) opt-in the VMS recipe carries.

The model is unchanged: consuming-STEP counting from a structural scan
of the recipe, freeing when a stage's last consuming step completes
(never on load). Undercount fails loud at the next load; overcount only
holds memory longer. A run-end `--dump-stage` of a stage whose last
consumer already ran will not find it, as it never did under opt-in.

`tests/test_auto_free_default.py` pins the default, the opt-out, and the
step-completion trigger. `current_capabilities.json` is refreshed for the
combine_data and deduplicate_data capability text changed by this same
patch; the drift test confirms it.
