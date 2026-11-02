"""Generate VRP changes against uniformly sampled local BGP candidates."""

import ipaddress as ip
import json
import os
import random
import tempfile
from pathlib import Path

from rov.data.bgp import load_bgp
from rov.data.bgp import read_raw
from rov.data.vrp import load_vrp_text
from rov.predict.preprocess import Prefixes
from rov.predict.preprocess import VRPs
from rov.predict.rov import origin_as


# Read every BGP candidate in peer order and preserve its position in each group.
def read_candidates(folder):
    metadata = load_bgp(folder)
    candidates = []
    groups = {}
    for peer in metadata["peers"]:
        for prefix, _, raw in read_raw(Path(folder) / peer["file"]):
            origin = origin_as(raw)
            candidate_id = len(candidates)
            candidates.append((prefix, peer["name"], origin))
            groups.setdefault(prefix, []).append(candidate_id)
    return candidates, groups


# Check the prepared inputs once before building indexes or generating edits.
def validate_inputs(initial, candidates):
    if len(initial) != len(set(initial)):
        raise ValueError("Duplicate initial VRPs would make withdrawal indices ambiguous")
    for prefix, _, asn in initial:
        if asn == 0:
            raise ValueError(f"Initial VRP must have nonzero ASN: {prefix} AS{asn}")

    if not candidates:
        raise ValueError("BGP input contains no candidates")
    seen = set()
    for prefix, peer, origin in candidates:
        if ip.ip_network(prefix).prefixlen == 0:
            raise ValueError(
                f"BGP candidate cannot have a /0 prefix: peer={peer}, prefix={prefix}"
            )
        if origin == 0:
            raise ValueError(
                f"BGP candidate must have nonzero origin: "
                f"peer={peer}, prefix={prefix}, origin_as={origin}"
            )
        key = prefix, peer
        if key in seen:
            raise ValueError(f"Duplicate BGP candidate: {key}")
        seen.add(key)


# Return all current covering VRPs in deterministic order.
def covering_vrps(index, network):
    address = int(network.network_address)
    bits = network.max_prefixlen
    address_type = ip.IPv4Address if network.version == 4 else ip.IPv6Address
    result = []
    for length, table in index.tables[network.version].items():
        if length > network.prefixlen:
            continue
        parent_address = address >> (bits - length) << (bits - length)
        for maximum, asn in table.get(parent_address, ()):
            prefix = str(ip.ip_network((address_type(parent_address), length)))
            result.append((prefix, maximum, asn))
    return sorted(result)


# Choose the complete legal edit for one selected candidate.
def choose_edit(index, candidate, state, rng):
    prefix, _, origin = candidate
    network = ip.ip_network(prefix)
    covers = covering_vrps(index, network)

    if state == "Valid":
        authorized = [
            row for row in covers
            if row[2] == origin and row[1] >= network.prefixlen
        ]
        edits = [("withdraw", row) for row in authorized]
        if len(covers) > len(authorized):
            return "withdraw_authorizations", edits

        followup = rng.choice(["keep_not_found", "add_invalid_covering"])
        if followup == "keep_not_found":
            return "withdraw_authorizations", edits

        invalid_choice, invalid_row = invalid_covering_row(network, origin, rng)
        return f"withdraw_then_{invalid_choice}", edits + [("add", invalid_row)]

    if state == "Invalid":
        choices = ["withdraw_all_covering", "add_authorization"]
        choice = rng.choice(choices)
        if choice == "withdraw_all_covering":
            return choice, [("withdraw", row) for row in covers]

        return choice, [("add", authorization_row(network, origin))]

    choices = ["add_invalid_covering", "add_authorization"]
    choice = rng.choice(choices)
    if choice == "add_authorization":
        return choice, [("add", authorization_row(network, origin))]

    invalid_choice, invalid_row = invalid_covering_row(network, origin, rng)
    return invalid_choice, [("add", invalid_row)]


# Return the exact authorization for a candidate route.
def authorization_row(network, origin):
    return str(network), network.prefixlen, origin


# Draw one of the two invalid VRPs for a currently uncovered candidate.
def invalid_covering_row(network, origin, rng):
    choice = rng.choice(["asn_mismatch", "length_violation"])
    if choice == "asn_mismatch":
        row = (str(network), network.prefixlen, mismatching_asn(origin, rng))
    else:
        if network.prefixlen == 0:
            raise ValueError("Cannot make a length-violation VRP for a /0 candidate")
        row = (str(network.supernet()), network.prefixlen - 1, origin)
    return choice, row


# Draw a nonzero four-octet ASN different from the candidate origin.
def mismatching_asn(origin, rng):
    value = rng.randrange(1, 4294967295)
    return value + (value >= origin)


# Apply a logical edit and return its net and per-operation candidate changes.
def apply_edit(index, prefixes, candidates, groups, states, edits):
    affected_prefixes = prefixes.covered([{"vrp": row} for _, row in edits])
    affected_candidates = [
        candidate_id
        for prefix in affected_prefixes
        for candidate_id in groups[prefix]
    ]
    before = {candidate_id: states[candidate_id] for candidate_id in affected_candidates}
    current_results = {}
    for candidate_id in affected_candidates:
        prefix, _, origin = candidates[candidate_id]
        current_results[candidate_id] = index.state(ip.ip_network(prefix), origin)

    covered_candidates = {}
    operation_changes = []

    for operation, row in edits:
        index.edit(row, operation == "add")
        if row[0] not in covered_candidates:
            covered_candidates[row[0]] = [
                candidate_id
                for prefix in prefixes.covered([{"vrp": row}])
                for candidate_id in groups[prefix]
            ]

        step_changes = []
        for candidate_id in covered_candidates[row[0]]:
            prefix, peer, origin = candidates[candidate_id]
            previous_state, previous_reason = current_results[candidate_id]
            after_state, after_reason = index.state(ip.ip_network(prefix), origin)
            current_results[candidate_id] = after_state, after_reason
            if previous_state == after_state:
                continue

            change = dict(
                prefix=prefix,
                peer=peer,
                origin_as=origin,
                before=previous_state,
                after=after_state,
            )
            if previous_state == "Invalid":
                change["before_invalid_reason"] = previous_reason
            if after_state == "Invalid":
                change["after_invalid_reason"] = after_reason
            step_changes.append(change)
        operation_changes.append(step_changes)

    changes = []
    for candidate_id in affected_candidates:
        prefix, peer, origin = candidates[candidate_id]
        after = current_results[candidate_id][0]
        if before[candidate_id] != after:
            changes.append(dict(
                prefix=prefix,
                peer=peer,
                origin_as=origin,
                before=before[candidate_id],
                after=after,
            ))
        states[candidate_id] = after
    return changes, operation_changes


# Write an ordered VRP event and update its BRIO cache position.
def write_event(stream, cache, operation_number, batch, operation, row, changes):
    if operation == "withdraw":
        position = cache.index(row)
        command = f"remove {position + 1}"
        cache.pop(position)
        cache.append(None)
    else:
        cache.append(row)
        command = f"add {row[0]} {row[1]} {row[2]}"
    event = dict(
        number=operation_number,
        batch=batch,
        operation=operation,
        vrp=row,
        command=command,
        changes=changes,
    )
    stream.write(json.dumps(event, separators=(",", ":")) + "\n")
    return command


# Generate complete batches up to a soft operation target in a new directory.
def generate(bgp, vrp, output, batches, operations_per_batch, seed):
    bgp = Path(bgp).resolve()
    vrp = Path(vrp).resolve()
    output = Path(output).resolve()
    if batches < 1 or operations_per_batch < 1:
        raise ValueError("Batch count and operation target must be positive")
    if output.exists() or output == bgp or bgp in output.parents or output == vrp:
        raise ValueError("Output must be a new directory outside the input data")

    initial = load_vrp_text(vrp)
    candidates, groups = read_candidates(bgp)
    validate_inputs(initial, candidates)

    index = VRPs(initial)
    prefix_index = Prefixes(groups)
    networks = {prefix: ip.ip_network(prefix) for prefix in groups}
    states = [
        index.state(networks[prefix], origin)[0]
        for prefix, _, origin in candidates
    ]

    rng = random.Random(seed)
    cache = list(initial)
    operation_number = 0
    metadata = []

    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=output.parent, prefix=".generated-vrp-") as temp:
        stage = Path(temp) / "result"
        stage.mkdir()
        with (
            (stage / "events.jsonl").open("w") as events,
            (stage / "transitions.jsonl").open("w") as transitions,
        ):
            for batch in range(1, batches + 1):
                commands = []
                added = 0
                withdrawn = 0
                logical = 0

                while added + withdrawn < operations_per_batch:
                    selected_id = rng.randrange(len(candidates))
                    selected = candidates[selected_id]
                    before = states[selected_id]

                    choice, edits = choose_edit(index, selected, before, rng)
                    changes, operation_changes = apply_edit(
                        index, prefix_index, candidates, groups, states, edits,
                    )

                    selected_record = dict(
                        prefix=selected[0],
                        peer=selected[1],
                        origin_as=selected[2],
                        before=before,
                        after=states[selected_id],
                    )
                    if before == "Valid" and edits[-1][0] == "add":
                        selected_record["after_withdrawals"] = "NotFound"

                    for (operation, row), step_changes in zip(edits, operation_changes):
                        operation_number += 1
                        command = write_event(
                            events, cache, operation_number, batch, operation, row,
                            step_changes,
                        )
                        commands.append(command)
                        if operation == "add":
                            added += 1
                        else:
                            withdrawn += 1

                    logical += 1
                    transitions.write(json.dumps(dict(
                        batch=batch,
                        conversion=logical,
                        selected=selected_record,
                        choice=choice,
                        operations=len(edits),
                        changes=changes,
                    ), separators=(",", ":")) + "\n")

                script = f"{batch:04}.script"
                (stage / script).write_text("\n".join(commands + ["notify"]) + "\n")

                total = added + withdrawn
                excess = total - operations_per_batch
                metadata.append(dict(
                    number=batch,
                    source="generated",
                    through_operation=operation_number,
                    added=added,
                    withdrawn=withdrawn,
                    total=total,
                    excess=excess,
                    script=script,
                ))

                print(
                    f"Batch {batch}: add {added}, withdraw {withdrawn}, "
                    f"total {total}, excess {excess}",
                    flush=True,
                )

        manifest = dict(
            format="vrp-batches-v1",
            initial=os.path.relpath(vrp, output),
            initial_count=len(initial),
            candidate_count=len(candidates),
            seed=seed,
            operations_per_batch=operations_per_batch,
            batches=metadata,
            brio_assumption=(
                "Untouched cache loaded in initial file order; replay in order, "
                "before any 3600-second withdrawal expiry; no other cache operations."
            ),
        )
        (stage / "sequence.json").write_text(json.dumps(manifest, indent=2) + "\n")
        stage.rename(output)

    print(f"Generated sequence: {output}")
