# aggregate_data: empty input produces an empty summary

Expand at the repo root. Touches one file.

Previously an empty input DataFrame halted the pipeline via the base class's
validate_data_not_empty. For an aggregation that is the wrong trade: it turns
"this download has no matching rows yet" into "no output file at all".

aggregate_data now returns an empty frame carrying the declared output columns
(group_by names + each aggregation's output_name), with a warning:

    ⚠️  '<step>': input is empty; producing an empty summary with columns [...]

Downstream export sheets and any ranges keep their headers and line up.

Verified: empty in -> (0, N) with correct columns; normal aggregation
unchanged; a full 43-step recipe run whose summary filter matches nothing
completes with an intact empty sheet. Full suite: 20 failures, baseline.

# End of file #
