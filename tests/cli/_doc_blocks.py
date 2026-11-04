'''Splicing generated blocks into hand-written docs, and the tables the generators write.'''


def replace_block(text: str, begin: str, end: str, body: str, doc: str) -> str:
    '''Swap what sits between the `begin` and `end` markers for `body`, leaving the rest alone.

    `doc` names the document in the error raised when the markers were removed by hand.
    '''
    if begin not in text or end not in text:
        raise AssertionError(
            f'Expected markers not found in {doc}: {begin!r} / {end!r}. '
            'They were removed by hand -- restore them before regenerating.'
        )
    start = text.index(begin) + len(begin)
    stop = text.index(end, start)
    return text[:start] + '\n\n' + body + '\n\n' + text[stop:]


def exempt_table(rows) -> list[str]:
    '''The lines of the "Source | Why | Expression" table of call sites a census could not resolve.'''
    return [
        '| Source | Why | Expression |',
        '|---|---|---|',
        *(f'| `{source}` | {reason} | `{expression}` |' for source, reason, expression in rows),
    ]
