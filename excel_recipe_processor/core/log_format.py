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


def now_stamp() -> str:
    """Full wall-clock datetime for run boundaries: 2026-08-26 18:56:08."""
    from datetime import datetime
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def clock() -> str:
    """Time-of-day only, for step starts: 18:56:08. TIMESTAMP DOCTRINE
    (2026-08-26): the log records WHEN at phase boundaries - recipe
    start/end carry the full datetime, each step-start line carries
    the clock - and durations everywhere else. Deliberately not a
    stamp-every-line log; the step clocks bracket everything between
    them, so any line's moment is recoverable to within its step.
    """
    from datetime import datetime
    return datetime.now().strftime('%H:%M:%S')


def q(value) -> str:
    """One user-originated name, quoted for a log line."""
    text = str(value).replace("'", "\\'")
    return f"'{text}'"


def qblock(values, indent: str = '        ', width: int = 96) -> str:
    """A FULL quoted list as a wrapped block after a prompting line.

    Log output never truncates lists (2026-08-26 ruling): a capped
    list withholds exactly what a troubleshooting session needs. Long
    lists instead continue on their own lines - the prompting log line
    ends where the block begins, members wrap at ~width characters,
    and every continuation line carries the indent so the block reads
    as one tidy unit under its header. Bracketed like qlist.

    A single member too long for a whole line is hard-wrapped with
    paired ellipsis marks - `…` ending the broken line and opening its
    continuation (the keyszer/Toshy keymap-name technique). Quotes
    still mark TRUE member boundaries; `…` pairs mark artificial ones,
    so the block stays dense without a reader (or a parser removing
    `…\n …` seams) ever mistaking a wrap for a member break.

    Usage: logger.info(f"... on 27 column(s):{qblock(names)}")
    """
    members = [q(value) for value in values]
    if not members:
        return ' []'
    continuation = indent + ' '

    def split_long(member: str, room_first: int) -> list:
        # Chunk one over-long quoted member; each non-final chunk ends
        # with the ellipsis, each non-first chunk begins with it
        chunks = []
        remaining = member
        room = max(room_first, 12)
        first = True
        while remaining:
            reserve = 0 if len(remaining) <= room else 1  # trailing …
            take = room - reserve
            if len(remaining) <= room and (first or True):
                chunk = ('' if first else '…') + remaining
                chunks.append(chunk)
                break
            chunk = ('' if first else '…') + remaining[:take - (0 if first else 1)]
            remaining = remaining[take - (0 if first else 1):]
            chunks.append(chunk + '…')
            first = False
            room = width - len(continuation)
        return chunks

    lines = []
    current = indent + '['
    for position, member in enumerate(members):
        tail = ', ' if position < len(members) - 1 else ']'
        candidate = current + member + tail
        if len(candidate) <= width:
            current = candidate
            continue
        # Would a fresh line hold the whole member?
        if len(continuation + member + tail) <= width:
            lines.append(current.rstrip())
            current = continuation + member + tail
            continue
        # Hard-wrap the member itself with ellipsis seams
        room_first = width - len(current)
        if room_first < 16:
            lines.append(current.rstrip())
            current = continuation
            room_first = width - len(current)
        chunks = split_long(member, room_first)
        for chunk in chunks[:-1]:
            lines.append(current + chunk)
            current = continuation
        current = current + chunks[-1] + tail
    lines.append(current)
    return '\n' + '\n'.join(lines)


def qlist(values, limit: int = 0) -> str:
    """A list of user-originated names as a BRACKETED quoted list.

    Always bracketed - ['VMS', 'Van_List'] and ['VMS'] alike - so a
    list of one is visibly a list and the whole collection has an
    unambiguous boundary, not just each member. The limit parameter
    survives for API stability but LOG SITES MUST NOT USE IT: capped
    log lists withhold what troubleshooting needs - use qblock for
    anything that might run long.
    """
    values = [q(value) for value in values]
    if limit and len(values) > limit:
        shown = values[:limit]
        return f"[{', '.join(shown)} ... +{len(values) - limit} more]"
    return f"[{', '.join(values)}]"

# End of file #
