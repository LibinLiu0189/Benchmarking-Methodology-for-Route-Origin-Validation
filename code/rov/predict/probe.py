"""Select probe addresses and predict their final forwarding next hops."""

import ipaddress as ip
import random


# Subtract more-specific selected or unresolved routes from both endpoint tables.
def probe_ranges(network, before_table, after_table, progress=None, prefix_index=None):
    excluded = set()
    if network.version == 4 and prefix_index is not None:
        descendants = list(prefix_index.covered(str(network)))
        candidates = (
            (prefix, table[prefix])
            for table in (before_table, after_table)
            for prefix in descendants
        )
        total = 2 * len(descendants)
    else:
        # Preserve the existing IPv6 and standalone behavior.
        candidates = (
            (prefix, result)
            for table in (before_table, after_table)
            for prefix, result in table.items()
        )
        total = len(before_table) + len(after_table)

    for scanned, (prefix, result) in enumerate(candidates, 1):
        if progress is not None and scanned % 1000 == 0:
            progress("probe candidate scan", scanned, total)
        if result["status"] == "known" and result["path"] is None:
            continue
        candidate = ip.ip_network(prefix)
        if (
            candidate.version == network.version
            and candidate.prefixlen > network.prefixlen
            and candidate.subnet_of(network)
        ):
            excluded.add(
                (int(candidate.network_address), int(candidate.broadcast_address))
            )
    remaining = []
    cursor = int(network.network_address)
    end = int(network.broadcast_address)
    for first, last in sorted(excluded):
        if cursor < first:
            remaining.append((cursor, first - 1))
        cursor = max(cursor, last + 1)
    if cursor <= end:
        remaining.append((cursor, end))
    return remaining


# Choose one address uniformly from the allowed ranges and resolve its final next hop.
def prepare_probe(prefix, before_table, after_table, progress=None, prefix_index=None):
    network = ip.ip_network(prefix)
    address_type = ip.IPv4Address if network.version == 4 else ip.IPv6Address
    ranges = probe_ranges(
        network, before_table, after_table, progress, prefix_index
    )
    probe = dict(
        status="unavailable",
        ranges=[
            dict(start=str(address_type(first)), end=str(address_type(last)))
            for first, last in ranges
        ],
        destination=None,
        expected_prefix=None,
        expected_next_hop=None,
    )
    if not ranges:
        probe["reason"] = "fully_covered_by_more_specific_routes"
        return probe

    offset = random.randrange(sum(last - first + 1 for first, last in ranges))
    for first, last in ranges:
        size = last - first + 1
        if offset < size:
            probe["destination"] = str(address_type(first + offset))
            break
        offset -= size

    # Longer routes were excluded above; check the target and then its ancestors.
    candidate = network
    while True:
        result = after_table.get(str(candidate))
        if result is not None:
            if result["status"] != "known":
                probe["status"] = "unresolved"
                probe["reason"] = "unresolved_covering_route"
                return probe
            if result["path"] is not None:
                probe["expected_prefix"] = str(candidate)
                probe["expected_next_hop"] = result["path"]["gateway"]
                break
        if candidate.prefixlen == 0:
            break
        candidate = candidate.supernet()
    probe["status"] = "ready"
    return probe
