"""Read prepared ROV, selected-path and probe predictions in batch order."""

from collections import Counter, defaultdict
import ipaddress as ip
import json
from pathlib import Path

# Read prepared candidate changes in order and retain each candidate's last state.
def load_rov_targets(directory, batches, predictions):
    with Path(predictions).open() as stream:
        rows = [json.loads(line) for line in stream if line.strip()]
    if [row.get("batch") for row in rows] != [batch["number"] for batch in batches]:
        raise ValueError("ROV prediction batches do not match the VRP sequence")

    targets = {}
    last_states = {}
    transitions = []
    for row in rows:
        for change in row["changes"]:
            prefix = str(ip.ip_network(change["prefix"]))
            peer = change["peer"]
            origin_as = int(change["origin_as"])
            key = (prefix, peer)
            transitions.append((
                row["batch"], prefix, peer, origin_as,
                change["before"], change["after"],
            ))
            current = targets.get(key)
            if current is None:
                targets[key] = dict(
                    prefix=prefix,
                    peer=peer,
                    origin_as=origin_as,
                    initial=change["before"],
                )
            else:
                if current["origin_as"] != origin_as or last_states[key] != change["before"]:
                    raise ValueError(f"Conflicting offline ROV changes for {prefix} {peer}")
            last_states[key] = change["after"]
    return targets, transitions


# Read per-batch selected-path predictions and retain only paths needed for matching.
def load_rib_targets(source, batches):
    with Path(source).open() as stream:
        rows = [json.loads(line) for line in stream if line.strip()]
    if [row.get("batch") for row in rows] != [batch["number"] for batch in batches]:
        raise ValueError("RIB prediction batches do not match the VRP sequence")

    changes = {}
    initial = {}
    last_paths = {}
    for row, batch in zip(rows, batches):
        number = row["batch"]
        if row["vrp_operations"] != batch["total"] or row["unresolved"]:
            raise ValueError(f"RIB predictions are incomplete for batch {number}")
        changes[number] = []
        for change in row["changes"]:
            prefix = str(ip.ip_network(change["prefix"]))
            before = rib_path(change["before"])
            after = rib_path(change["after"])
            if before == after:
                raise ValueError(f"RIB prediction has no selected-path change: {prefix}")
            if prefix in last_paths and last_paths[prefix] != before:
                raise ValueError(f"Conflicting offline RIB changes for {prefix}")
            initial.setdefault(prefix, before)
            last_paths[prefix] = after
            changes[number].append((prefix, before, after))
    return changes, initial


# Read the prepared probe destination and expected forwarding for each batch.
def load_fib_targets(source, batches):
    with Path(source).open() as stream:
        rows = [json.loads(line) for line in stream if line.strip()]
    if [row.get("batch") for row in rows] != [batch["number"] for batch in batches]:
        raise ValueError("FIB prediction batches do not match the VRP sequence")

    changes = defaultdict(list)
    unavailable = []
    for row in rows:
        number = row["batch"]
        batch_targets = {}
        for change in row["changes"]:
            probe = change["probe"]
            if probe["status"] != "ready":
                unavailable.append((
                    number, probe["status"], probe.get("reason", "unspecified"),
                ))
                continue
            destination = str(ip.ip_address(probe["destination"]))
            prefix = str(ip.ip_network(change["prefix"]))
            before_path = change["before"]
            if before_path is None:
                covering = change["before_covering"]
                before_path = None if covering is None else covering["path"]
            before = None if before_path is None else str(ip.ip_address(before_path["gateway"]))
            after = probe["expected_next_hop"]
            if after is not None:
                after = str(ip.ip_address(after))
            if destination in batch_targets:
                raise ValueError(f"Duplicate FIB destination {destination} in batch {number}")
            batch_targets[destination] = after
            changes[prefix].append((number, destination, before, after))
    return dict(changes), unavailable


# Keep the selected path identity represented in both predictions and BIRD observations.
def rib_path(path):
    if path is None:
        return None
    return (path["peer"], str(ip.ip_address(path["gateway"])))
