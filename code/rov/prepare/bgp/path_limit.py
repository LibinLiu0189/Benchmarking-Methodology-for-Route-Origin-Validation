"""Bound complete AS_PATH text for the local BIRD 2.19.2 CLI and INFO readers."""

from rov.data.bgp import attributes

# buffer_puts marks a buffer full even when the last byte is the terminating NUL.
CLI_ATTRIBUTE_LIMIT = 500 - len("BGP.as_path: ") - 2
CLI_LINE_LIMIT = 512 - 5 - 1 - len("BGP.as_path: ") - 1 - 1
INFO_PATH_LIMIT = 1000 - 2
# Reserve IPv6 mixed notation, peer_65534, Linux IFNAMSIZ, enum/uint32, all
# separators, the "(path )" wrapper, newline and terminating NUL.
INFO_OTHER_BYTES = (
    len("ROVLOG RIB|") + 49 + 10 + 45 + 15 + len("(path )")
    + len("(enum 31)4294967295") + 10 + 6 + 2
)
INFO_LINE_LIMIT = 1024 - INFO_OTHER_BYTES
MAX_AS_PATH_TEXT_BYTES = min(
    CLI_ATTRIBUTE_LIMIT, CLI_LINE_LIMIT, INFO_PATH_LIMIT, INFO_LINE_LIMIT,
)


# Decode the four-octet segments without changing their order or multiplicity.
def path_segments(raw):
    path = next(
        (raw[start:end] for kind, _, start, end in attributes(raw) if kind == 2),
        None,
    )
    if path is None:
        raise ValueError("Missing AS_PATH")
    position = 0
    while position < len(path):
        if position + 2 > len(path):
            raise ValueError("Truncated AS_PATH segment")
        kind, count = path[position:position + 2]
        position += 2
        end = position + 4 * count
        if kind not in (1, 2, 3, 4) or not count or end > len(path):
            raise ValueError("Invalid AS_PATH segment")
        yield kind, [
            int.from_bytes(path[offset:offset + 4], "big")
            for offset in range(position, end, 4)
        ]
        position = end


# Match as_path_format decimal ASNs, wrappers and intra/inter-segment spaces.
def path_text_length(raw):
    length = 0
    for index, (kind, asns) in enumerate(path_segments(raw)):
        length += bool(index) + sum(len(str(asn)) for asn in asns) + len(asns) - 1
        length += {1: 2, 2: 0, 3: 2, 4: 4}[kind]
    return length


# Record candidate drops separately from prefixes retaining another candidate.
def filter_report(dropped, affected, retained):
    return dict(
        rule="drop_candidate_when_complete_bird_as_path_text_exceeds_limit",
        bird_version="2.19.2",
        max_as_path_text_bytes=MAX_AS_PATH_TEXT_BYTES,
        limits=dict(
            cli_attribute=CLI_ATTRIBUTE_LIMIT, cli_line=CLI_LINE_LIMIT,
            info_path=INFO_PATH_LIMIT, info_line=INFO_LINE_LIMIT,
        ),
        dropped_candidates={str(a): dropped[a] for a in (4, 6)},
        affected_prefixes={str(a): len(affected[a]) for a in (4, 6)},
        affected_prefixes_with_retained_candidates={
            str(a): len(affected[a] & retained[a]) for a in (4, 6)
        },
        affected_prefixes_without_retained_candidates={
            str(a): len(affected[a] - retained[a]) for a in (4, 6)
        },
    )
