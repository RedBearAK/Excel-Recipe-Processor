# clean_data: new blank_repeats action

Expand at the repo root. One processor file + its examples + a test module
(5/5).

Reproduces the pivot-table "don't repeat item labels" display: the first row
of a run keeps its values; continuation rows show blanks. The rule's columns
move AS A GROUP - a row is a continuation only when every listed column
equals the row above, and then all are blanked together. Blanking columns
independently would wrongly blank a tracking number whose booking differs;
the group semantics make that impossible (tested).

    rules:
      - columns: ["Booking", "Carrier Tracking No"]
        action: "blank_repeats"

Cautions, also in the docstring and example:
- Display transformation - apply LAST, after all sorting, on sheets meant
  for reading. The blanks are static; re-sorting in Excel will not restore
  the values the way a live pivot does.
- NaN-safe: two missing values count as a repeat.

Also in this file: clean_data now passes an EMPTY input through with a
warning instead of halting, matching aggregate_data. Required for recipe
correctness: the summary-display step cleans a stage that is legitimately
empty when a download has no booked rows, and halting there would undo the
empty-summary robustness. Verified end to end: a no-bookings download
completes all 45 steps with an intact empty Export_Summary.

Full suite: 20 failures, baseline.

# End of file #
