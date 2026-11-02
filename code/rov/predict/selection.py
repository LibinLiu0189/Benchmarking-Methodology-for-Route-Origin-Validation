"""Decode BGP attributes and select among ROV-eligible directly connected eBGP candidates."""

import ipaddress as ip
import struct

from rov.data.bgp import attributes
from .routes import BestPath


# Decode path attributes into the AS segments and selection fields needed
# for offline best-path prediction.
def path_info(raw):
    """Parse four-octet AS_PATH while preserving segment types; follow BIRD nest/a-path.c for path length and origin semantics."""
    values = {k: raw[a:b] for k, _, a, b in attributes(raw)}
    if 1 not in values or len(values[1]) != 1 or values[1][0] > 2 or 2 not in values:
        raise ValueError("Missing or invalid ORIGIN/AS_PATH")
    segments = []
    pos = 0
    data = values[2]
    while pos < len(data):
        if pos + 2 > len(data):
            raise ValueError("Truncated AS_PATH segment")
        kind, count = data[pos : pos + 2]
        pos += 2
        if kind not in (1, 2, 3, 4) or not count or pos + 4 * count > len(data):
            raise ValueError("Invalid AS_PATH segment")
        numbers = struct.unpack_from(f"!{count}I", data, pos)
        pos += 4 * count
        segments.append((kind, numbers))
    if 4 in values and len(values[4]) != 4:
        raise ValueError("Invalid MED length")
    origin_as = segments[-1][1][-1] if segments and segments[-1][0] in (2, 3) else 0
    return dict(
        next_hop=(
            str(ip.IPv6Address(values[14][4:20]))
            if 14 in values
            else str(ip.IPv4Address(values[3]))
        ),
        segments=segments,
        origin_as=origin_as,
        origin=values[1][0],
        med=int.from_bytes(values.get(4, b"\0"), "big"),
        has_set=any(k in (1, 4) for k, _ in segments),
        has_confed=any(k in (3, 4) for k, _ in segments),
        kinds=tuple(values),
    )


# Compare two selection keys, applying MED only within the same neighbor AS.
def better(a, b):
    if a[0] != b[0]:
        return a[0] < b[0]
    if a[1] == b[1] and a[2] != b[2]:
        return a[2] < b[2]
    return a[3] < b[3]


# Select among already accepted candidates; report ambiguous MED comparisons or ties.
def select_path(routes, peers, policy):
    """Return a definite result only for a unique candidate that beats all others."""
    accepted = []
    for route in routes:
        peer = peers[route.peer]
        length = sum(
            len(numbers) if kind == 2 else int(kind == 1)
            for kind, numbers in route.segments
        )
        first_as = next(
            (numbers[0] if kind == 2 else None
             for kind, numbers in route.segments if kind in (1, 2)),
            None,
        )
        router_id = peer["router_id"]
        key = (
            (length, route.origin),
            first_as if first_as is not None else policy["dut_asn"],
            route.med,
            (
                int(ip.IPv4Address(router_id)),
                int(ip.ip_address(peer["source_address"])),
            ),
        )
        accepted.append((route, key))
    if not accepted:
        return BestPath()
    winners = [
        r
        for r, key in accepted
        if all(r is other or better(key, k) for other, k in accepted)
    ]
    if len(winners) != 1:
        return BestPath(
            peers=tuple(r.peer for r, k in accepted),
        )
    r = winners[0]
    return BestPath(path=r)


# Format a predicted path, an absent path, or an unresolved result for display.
def describe(value):
    if value["status"] != "known":
        return "Cannot uniquely predict: " + value["reason"]
    p = value["path"]
    if p is None:
        return "No available BGP path"
    return f'peer {p["peer"]}, next hop {p["gateway"]}, origin AS{p["origin_as"]}'

