"""Predict per-batch candidate ROV changes from local BGP, initial VRPs, and incremental updates."""

import ipaddress as ip

from rov.data.bgp import attributes
from .preprocess import inputs


# Extract only the origin ASN from four-octet AS_PATH for ROV validation.
def origin_as(raw):
    data = next(
        (raw[start:end] for kind, _, start, end in attributes(raw) if kind == 2),
        None,
    )
    if data is None:
        raise ValueError("Missing AS_PATH")
    position = 0
    origin = 0
    while position < len(data):
        if position + 2 > len(data):
            raise ValueError("Truncated AS_PATH segment")
        kind, count = data[position : position + 2]
        position += 2
        end = position + 4 * count
        if kind not in (1, 2, 3, 4) or not count or end > len(data):
            raise ValueError("Invalid AS_PATH segment")
        # Preserve the existing origin rule: empty paths and final sets use ASN 0.
        origin = int.from_bytes(data[end - 4 : end], "big") if kind in (2, 3) else 0
        position = end
    return origin


# Distinguish an unavailable origin from a genuine origin AS0 for input preparation.
def candidate_origin(raw):
    try:
        origin = origin_as(raw)
    except ValueError:
        return None

    path = next(
        (raw[start:end] for kind, _, start, end in attributes(raw) if kind == 2),
        None,
    )
    position = 0
    final_kind = None
    while position < len(path):
        final_kind = path[position]
        position += 2 + 4 * path[position + 1]
    return origin if final_kind in (2, 3) else None


# Apply VRP batches in order and report candidates whose ROV state changes.
def predict(bgp, vrp, sequence):
    batches, groups, _, index, prefixes = inputs(
        bgp, vrp, sequence, lambda raw: {"origin_as": origin_as(raw)}
    )
    for info, batch in batches:
        targets = prefixes.covered(batch)
        before = {
            p: [index.state(ip.ip_network(p), r["origin_as"]) for r in groups[p]]
            for p in targets
        }
        index.apply(batch)
        changes = []
        for prefix in targets:
            for route, old in zip(groups[prefix], before[prefix]):
                new = index.state(ip.ip_network(prefix), route["origin_as"])
                if old[0] != new[0]:
                    changes.append(
                        dict(
                            prefix=prefix,
                            peer=route["peer"],
                            origin_as=route["origin_as"],
                            before=old[0],
                            after=new[0],
                            before_reason=old[1],
                            after_reason=new[1],
                        )
                    )
        print(
            f'\nBatch {info["number"]}: {len(batch)} VRP operations; {len(changes)} candidate ROV changes'
        )
        for c in changes:
            print(
                f'  {c["prefix"]}, peer {c["peer"]}, AS{c["origin_as"]}: {c["before"]} → {c["after"]}'
            )
        yield dict(
            batch=info["number"],
            snapshot=info.get("snapshot", info.get("source")),
            vrp_operations=len(batch),
            covered_prefixes=len(targets),
            changes=changes,
        )
