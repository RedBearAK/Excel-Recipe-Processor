r"""
Regex for excel_data_validation bound-value recognition.

excel_recipe_processor/processors/_helpers/excel_data_validation_rgx.py

Excel stores data-validation date and time bounds as serial numbers or
formulas, never as the ISO strings a recipe author naturally writes. These
patterns recognize the friendly forms so the processor can convert them to
DATE(y,m,d) / TIME(h,m,s) formulas that Excel actually evaluates. A bound
that matches neither pattern passes through verbatim (a cell reference,
a named range, or an explicit formula).
"""

import re


# ISO calendar date, the recipe-friendly bound form for date validations.
# Matches: "2026-01-01", "1999-12-31"
# Rejects: "2026-1-1", "01/01/2026", "2026-01-01 08:00"
iso_date_rgx = re.compile(r'^(\d{4})-(\d{2})-(\d{2})$')

# Clock time, the recipe-friendly bound form for time validations.
# Matches: "8:00", "08:00", "17:30:00"
# Rejects: "8", "8:0", "8:00 AM", "25:00" (hour range checked by caller)
clock_time_rgx = re.compile(r'^(\d{1,2}):(\d{2})(?::(\d{2}))?$')

# End of file #
