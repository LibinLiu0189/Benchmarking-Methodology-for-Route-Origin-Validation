"""Select probe IPs whose predicted forwarding next hop changes in one batch."""

from collections import defaultdict
import ipaddress as ip
import json
from pathlib import Path


# Index complete next-hop histories and the unchanged covering routes used by probes.
def _route_histories(initial, changes, predictions_path):
    histories = {
        prefix: (None if path is None else path[1], [])
        for prefix, path in initial.items()
    }
    for number in sorted(changes):
        for prefix, before, after in changes[number]:
            before_hop = None if before is None else before[1]
            after_hop = None if after is None else after[1]
            if before_hop != after_hop:
                histories[prefix][1].append((number, after_hop))

    # Changed routes already have their initial state and every subsequent update.
    # Covering snapshots supply routes that never change anywhere in the input.
    # Prepared probe addresses exclude unchanged active more-specific routes.
    with Path(predictions_path).open() as stream:
        for line in stream:
            if not line.strip():
                continue
            row = json.loads(line)
            for change in row["changes"]:
                for field in ("before_covering", "after_covering"):
                    covering = change.get(field)
                    if covering is None or covering["prefix"] in initial:
                        continue
                    prefix = covering["prefix"]
                    next_hop = str(ip.ip_address(covering["path"]["gateway"]))
                    existing = histories.get(prefix)
                    if existing is None:
                        histories[prefix] = (next_hop, [])
                    elif existing[0] != next_hop:
                        raise ValueError(f"Conflicting unchanged covering route: {prefix}")

    tables = {4: defaultdict(dict), 6: defaultdict(dict)}
    for prefix, history in histories.items():
        network = ip.ip_network(prefix)
        tables[network.version][network.prefixlen][int(network.network_address)] = history
    return {
        version: [
            (length, (32 if version == 4 else 128) - length, table)
            for length, table in sorted(branches.items(), reverse=True)
        ]
        for version, branches in tables.items()
    }


# Merge only routes containing this IP and compare forwarding after each full batch.
def _single_change_batch(address, tables):
    next_hops = {}
    updates = defaultdict(list)
    value = int(address)
    for length, shift, table in tables[address.version]:
        history = table.get(value >> shift << shift)
        if history is None:
            continue
        initial_hop, changes = history
        if initial_hop is not None:
            next_hops[length] = initial_hop
        for number, next_hop in changes:
            updates[number].append((length, next_hop))

    previous = next_hops[max(next_hops)] if next_hops else None
    changed_batch = None
    for number in sorted(updates):
        for length, next_hop in updates[number]:
            if next_hop is None:
                next_hops.pop(length, None)
            else:
                next_hops[length] = next_hop
        current = next_hops[max(next_hops)] if next_hops else None
        if current != previous:
            if changed_batch is not None:
                return None
            changed_batch = number
            previous = current
    return changed_batch


# Return the owning batch only for destinations with one complete-input transition.
def single_transition_batches(destinations, initial, changes, predictions_path):
    destinations = set(destinations)
    if not destinations:
        return {}
    tables = _route_histories(initial, changes, predictions_path)
    result = {}
    for destination in destinations:
        number = _single_change_batch(ip.ip_address(destination), tables)
        if number is not None:
            result[destination] = number
    return result
