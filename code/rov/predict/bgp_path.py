"""Predict best-path changes from BGP batches with fixed VRPs and an explicit ROV policy."""

import ipaddress as ip
from pathlib import Path
import random

from rov.data.bgp import read_raw
from rov.data.bgp_updates import read_batch_updates
from .selection import path_info, select_path
from .routes import BestPath, Candidate, PrefixRoutes


# Decode and validate every announcement against the fixed authorization table.
def make_candidate(prefix, peer, raw, authorizations):
    fields = path_info(raw)
    rov = authorizations.state(ip.ip_network(prefix), fields["origin_as"])[0]
    return Candidate(
        peer=peer,
        next_hop=fields["next_hop"],
        origin_as=fields["origin_as"],
        segments=tuple(fields["segments"]),
        origin=fields["origin"],
        med=fields["med"],
        rov=rov,
        unsupported_attributes=bool(set(fields["kinds"]) & {9, 10, 17, 18, 26}),
    )


# Apply import policy to stored candidates, then use the existing selection rules.
def choose(prefix, candidates, peers, policy):
    eligible = []
    for candidate in candidates:
        if policy["rov"] == "on" and candidate.rov == "Invalid":
            continue
        if policy["as_sets"] == "reject" and any(
            kind in (1, 4) for kind, _ in candidate.segments
        ):
            continue
        if any(policy["dut_asn"] in numbers for _, numbers in candidate.segments):
            continue
        if candidate.unsupported_attributes or any(
            kind in (3, 4) for kind, _ in candidate.segments
        ):
            raise ValueError(
                f"{prefix} from {candidate.peer}: unsupported selection attributes or confederation"
            )
        eligible.append(candidate)
    return select_path(eligible, peers, policy)


# Build one prefix table containing all neighbor candidates and each selected path.
def load_initial(bgp, authorizations, peers, policy):
    table: dict[str, PrefixRoutes] = {}
    for peer in peers.values():
        for prefix, _, raw in read_raw(Path(bgp) / peer["file"]):
            candidate = make_candidate(prefix, peer["name"], raw, authorizations)
            entry = table.get(prefix)
            if entry is None:
                entry = table[prefix] = PrefixRoutes()
            entry.candidates[peer["name"]] = candidate
    print(f"Selecting initial best paths for {len(table):,} prefixes.", flush=True)
    for prefix, entry in table.items():
        entry.best = choose(prefix, entry.candidates.values(), peers, policy)
    return table


# Apply every operation before selecting again; remember each touched prefix's old result.
def apply_batch(table, directory, batch, events, authorizations, peers):
    before: dict[str, BestPath] = {}
    counts = {"announce": 0, "withdraw": 0}
    for peer, operation, prefix, raw in read_batch_updates(directory, batch, events):
        if peer not in peers:
            raise ValueError(f"Update peer is absent from the baseline: {peer}")
        entry = table.get(prefix)
        if entry is None:
            entry = table[prefix] = PrefixRoutes()
        before.setdefault(prefix, entry.best)
        counts[operation] += 1
        if operation == "withdraw":
            entry.candidates.pop(peer, None)
        else:
            # Always replace the old candidate, including when the new one is Invalid.
            entry.candidates[peer] = make_candidate(prefix, peer, raw, authorizations)
    return before, counts


# Recalculate touched prefixes and return their updated best paths as objects.
def recalculate(table, before, peers, policy):
    after: dict[str, BestPath] = {}
    for prefix in before:
        entry = table[prefix]
        entry.best = choose(prefix, entry.candidates.values(), peers, policy)
        after[prefix] = entry.best
        if not entry.candidates:
            del table[prefix]
    return after


# Read a batch endpoint result, falling back to the unchanged entry in the full table.
def batch_path(prefix, results, table):
    if prefix in results:
        return results[prefix]
    entry = table.get(prefix)
    return entry.best if entry is not None else BestPath()


# Select a probe using only indexed descendants and ancestors of a changed prefix.
def prepare_probe(prefix, before, after, table, prefix_index, rng=None):
    network = ip.ip_network(prefix)
    address_type = ip.IPv4Address if network.version == 4 else ip.IPv6Address
    excluded = []
    for child in prefix_index.covered(prefix):
        old = batch_path(child, before, table)
        new = batch_path(child, after, table)
        if old.known and old.path is None and new.known and new.path is None:
            continue
        child_network = ip.ip_network(child)
        excluded.append((
            int(child_network.network_address),
            int(child_network.broadcast_address),
        ))

    # Merge overlapping exclusions while retaining the remaining address intervals.
    ranges = []
    cursor = int(network.network_address)
    end = int(network.broadcast_address)
    for first, last in sorted(excluded):
        if cursor < first:
            ranges.append((cursor, first - 1))
        cursor = max(cursor, last + 1)
    if cursor <= end:
        ranges.append((cursor, end))

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

    # Give every remaining address the same probability of being selected.
    offset = (rng if rng is not None else random).randrange(
        sum(last - first + 1 for first, last in ranges)
    )
    for first, last in ranges:
        size = last - first + 1
        if offset < size:
            probe["destination"] = str(address_type(first + offset))
            break
        offset -= size

    # Descendants are excluded, so resolve the target and then its closest ancestors.
    for candidate in (prefix, *prefix_index.covering(prefix)):
        result = batch_path(candidate, after, table)
        if not result.known:
            probe["status"] = "unresolved"
            probe["reason"] = "unresolved_covering_route"
            return probe
        if result.path is not None:
            probe["expected_prefix"] = candidate
            probe["expected_next_hop"] = result.path.next_hop
            break
    probe["status"] = "ready"
    return probe


# Format a selected, absent, or unresolved path using the existing prediction fields.
def format_best(result):
    if not result.known:
        return dict(
            status="unresolved",
            reason="no_unique_winner_med_or_tie",
            peers=list(result.peers),
        )
    route = result.path
    path = None if route is None else dict(
        peer=route.peer, gateway=route.next_hop, origin_as=route.origin_as,
    )
    return dict(status="known", path=path)


# Preserve the complete selected candidate in a change's before or after path.
def format_change_path(route):
    if route is None:
        return None

    segment_types = {
        1: "AS_SET",
        2: "AS_SEQUENCE",
        3: "AS_CONFED_SEQUENCE",
        4: "AS_CONFED_SET",
    }
    return dict(
        peer=route.peer,
        gateway=route.next_hop,
        origin_as=route.origin_as,
        as_path=[
            dict(type=segment_types[kind], asns=list(numbers))
            for kind, numbers in route.segments
        ],
        origin=route.origin,
        med=route.med,
    )


# Format one completed batch and its covering routes without selection or file I/O.
def format_results(batch, counts, policy, before, after, probes, table, prefix_index):
    changes = []
    unresolved = []
    for prefix, new in after.items():
        old = before[prefix]
        if not old.known or not new.known:
            unresolved.append(dict(
                prefix=prefix, before=format_best(old), after=format_best(new),
            ))
            continue
        if old.path == new.path:
            continue
        record = dict(
            prefix=prefix,
            before=format_change_path(old.path),
            after=format_change_path(new.path),
        )
        if old.path is None:
            record["type"] = "installed"
        elif new.path is None:
            record["after_covering"] = None
            for ancestor in prefix_index.covering(prefix):
                result = batch_path(ancestor, after, table)
                if result.known and result.path is not None:
                    record["after_covering"] = dict(
                        prefix=ancestor, path=format_best(result)["path"],
                    )
                    break
            record["type"] = (
                "withdrawn_to_covering_route"
                if record["after_covering"] is not None else "withdrawn"
            )
        else:
            record["type"] = "replaced"
        record["probe"] = probes[prefix]
        changes.append(record)
    return dict(
        batch=batch["number"],
        source=batch["source"],
        counts=counts,
        policy=policy,
        prefixes_recalculated=len(after),
        changes=changes,
        unresolved=unresolved,
    )
