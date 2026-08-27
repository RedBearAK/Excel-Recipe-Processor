"""
Quoting for user-originated identifiers in log output.

excel_recipe_processor/core/log_format.py

THE DOCTRINE (2026-08-26): any user-originated identifier - sheet
names, column names, stage names, range names, file names, template
names - is quoted at the point of log emission, and lists of them are
emitted as quoted comma lists. Names routinely contain spaces; without
quotes, "Formatting Can Sizes: column 2/2" reads as sentence words,
and a comma inside one member of a bare joined list is silently
indistinguishable from a list separator. Quotes make the boundary of
every name unambiguous.

Not quoted: counts, durations, fixed internal vocabulary (processor
types, option constants), and names already delimited by an accepted
bracket form like the [Sheet] log prefix - brackets are quoting.

Helpers:
    q(value)            -> 'Van List'   (escapes any internal quote)
    qlist(values, n)    -> 'A', 'B, C', 'D'  (each member quoted; a
                           comma inside a member can never masquerade
                           as a separator; optional cap appends
                           "... +N more")
"""


def q(value) -> str:
    """One user-originated name, quoted for a log line."""
    text = str(value).replace("'", "\\'")
    return f"'{text}'"


def qlist(values, limit: int = 0) -> str:
    """A list of user-originated names as a BRACKETED quoted list.

    Always bracketed - ['VMS', 'Van_List'] and ['VMS'] alike - so a
    list of one is visibly a list and the whole collection has an
    unambiguous boundary, not just each member. limit > 0 caps the
    members shown and appends a "+N more" tail inside the brackets so
    high-cardinality lists stay readable without hiding their size.
    """
    values = [q(value) for value in values]
    if limit and len(values) > limit:
        shown = values[:limit]
        return f"[{', '.join(shown)} ... +{len(values) - limit} more]"
    return f"[{', '.join(values)}]"

# End of file #
