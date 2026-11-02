"""Sample VRPs and encode exact-count update workloads with explicit caller constraints."""

from collections import Counter
import ipaddress as ip
import json
import os
from pathlib import Path
import random

from rov.data.vrp import load_vrp_text
from rov.predict.preprocess import Prefixes
from rov.predict.preprocess import VRPs
from rov.prepare.vrp.generate_rov_batches import read_candidates
from rov.prepare.vrp.generate_rov_batches import validate_inputs
from rov.prepare.vrp.generate_rov_batches import choose_edit
from rov.prepare.vrp.generate_rov_batches import apply_edit
from rov.prepare.vrp.generate_rov_batches import write_event


# Count the stated coverage, overlap and AFI units using the actual sampled input.
def describe_input(initial, candidates, groups, index):
    prefixes = {prefix: ip.ip_network(prefix) for prefix in groups}
    covered = sum(index.state(network, 0)[0] != "NotFound" for network in prefixes.values())
    overlap = 0
    for prefix, _, _ in initial:
        network = ip.ip_network(prefix)
        value, bits = int(network.network_address), network.max_prefixlen
        overlap += any(length < network.prefixlen and (value >> (bits - length) << (bits - length)) in table
                       for length, table in index.tables[network.version].items())
    return dict(vrp_count=len(initial), roa_count=None,
                roa_count_unavailable="Input is a VRP text dataset; original ROA-to-VRP mapping is not present",
                vrp_afi=dict(Counter(str(ip.ip_network(row[0]).version) for row in initial)),
                bgp_candidates_afi=dict(Counter(str(ip.ip_network(row[0]).version) for row in candidates)),
                bgp_unique_prefixes_afi=dict(Counter(str(net.version) for net in prefixes.values())),
                coverage_definition="Fraction of distinct announced prefixes with at least one covering VRP, irrespective of maxLength or ASN",
                covered_prefixes=covered, announced_prefixes=len(prefixes),
                coverage_fraction=covered / len(prefixes),
                overlap_definition="Fraction of VRPs strictly covered by another shorter-prefix VRP; same-prefix authorizations excluded",
                overlapping_vrps=overlap, overlap_fraction=overlap / len(initial),
                vrp_prefix_lengths=dict(Counter(f"{ip.ip_network(row[0]).version}/{ip.ip_network(row[0]).prefixlen}" for row in initial)))


# Write an exact operation count with fixed small batches and net ROA predictions.
def write_sequence(output, vrp_path, initial, candidates, groups, seed, operations, batch_size,
                   unique_batch_vrps=False):
    if operations <= 0 or batch_size <= 0 or operations % batch_size:
        raise ValueError("Positive operation count must be divisible by the fixed batch size")
    output.mkdir()
    index, prefixes = VRPs(initial), Prefixes(groups)
    networks = {prefix: ip.ip_network(prefix) for prefix in groups}
    states = [index.state(networks[prefix], origin)[0] for prefix, _, origin in candidates]
    cache = list(initial)
    rng = random.Random(seed)
    pending = []
    metadata = []
    number = 0
    rejected_edits = 0
    with (output / "events.jsonl").open("x") as events, (output / "predictions.jsonl").open("x") as predictions:
        for batch in range(1, operations // batch_size + 1):
            commands, changes, counts = [], {}, Counter()
            touched = set()
            for _ in range(batch_size):
                if not pending:
                    for attempt in range(10000):
                        chosen = rng.randrange(len(candidates))
                        _, pending = choose_edit(index, candidates[chosen], states[chosen], rng)
                        identities = [tuple(vrp) for _, vrp in pending]
                        if not unique_batch_vrps or (not touched.intersection(identities) and len(set(identities)) == len(identities)):
                            break
                        rejected_edits += 1
                    else:
                        raise ValueError("Unable to fill fixed batch without repeated VRP identities")
                    if not pending:
                        raise ValueError("Selected edit contained no operations")
                operation, vrp = pending.pop(0)
                if unique_batch_vrps and tuple(vrp) in touched:
                    raise ValueError("Pending edit repeats a VRP identity within one notification batch")
                touched.add(tuple(vrp))
                _, per_operation = apply_edit(index, prefixes, candidates, groups, states, [(operation, vrp)])
                step_changes = per_operation[0]
                number += 1
                commands.append(write_event(events, cache, number, batch, operation, vrp, step_changes))
                counts[operation] += 1
                for row in step_changes:
                    key = (row["prefix"], row["peer"])
                    if key not in changes:
                        changes[key] = dict(row)
                    else:
                        changes[key]["after"] = row["after"]
                        changes[key]["after_reason"] = row.get("after_reason")
            script = f"{batch:04}.script"
            (output / script).write_text("\n".join(commands + ["notify"]) + "\n")
            metadata.append(dict(number=batch, script=script, total=batch_size,
                                 added=counts["add"], withdrawn=counts["withdraw"], through_operation=number))
            net = [row for row in changes.values() if row["before"] != row["after"]]
            predictions.write(json.dumps(dict(batch=batch, vrp_operations=batch_size, changes=net)) + "\n")
    sequence = dict(format="vrp-batches-v1", initial=os.path.relpath(vrp_path, output),
                    initial_count=len(initial), seed=seed, batches=metadata,
                    operation_count=operations, batch_size=batch_size,
                    unique_vrp_identity_per_batch=unique_batch_vrps,
                    rejected_in_batch_repeat_edits=rejected_edits,
                    modify_definition="Withdrawal plus addition count as two real operations",
                    truncated_logical_edit=bool(pending),
                    brio_assumption="Untouched cache in initial file order, ordered replay before 3600-second withdrawal expiry")
    (output / "sequence.json").write_text(json.dumps(sequence, indent=2) + "\n")
    return sequence


# Bind one real sample and random workload without overwriting any prior directory.
def prepare_workload(bgp, source, output, size, seed, operations, batch_size, unique_batch_vrps):
    bgp, source, output = Path(bgp).resolve(), Path(source).resolve(), Path(output).absolute()
    if output.exists() or output.is_symlink():
        raise FileExistsError(f"Output already exists: {output}")
    source_rows = load_vrp_text(source)
    if not 0 < size <= len(source_rows):
        raise ValueError("VRP sample size exceeds the real source")
    selected = sorted(random.Random(seed).sample(range(len(source_rows)), size))
    initial = [source_rows[position] for position in selected]
    candidates, groups = read_candidates(bgp)
    validate_inputs(initial, candidates)
    index = VRPs(initial)
    description = describe_input(initial, candidates, groups, index)
    output.mkdir(parents=True)
    vrp = output / "initial.vrps"
    vrp.write_text("".join(f"{prefix} {maximum} {asn}\n" for prefix, maximum, asn in initial))
    sequence = write_sequence(output / "sequence", vrp, initial, candidates, groups, seed, operations, batch_size,
                              unique_batch_vrps=unique_batch_vrps)
    manifest = dict(bgp=str(bgp), vrp="initial.vrps",
                    source_vrp=str(source), sequence="sequence", predictions="sequence/predictions.jsonl",
                    seed=seed, sampling="Independent uniform VRP sample without replacement; original file order retained",
                    update_sampling="Uniform random BGP candidates; existing choose_edit rules; no fixed operation-type quota",
                    update_mode="incremental", size=size, operations=operations, batch_size=batch_size,
                    operation_counts=dict(add=sum(row["added"] for row in sequence["batches"]),
                                          withdraw=sum(row["withdrawn"] for row in sequence["batches"])),
                    rejected_in_batch_repeat_edits=sequence["rejected_in_batch_repeat_edits"],
                    policy=dict(rov="on", dut_asn=65000, as_sets="reject"),
                    input_statistics=description,
                    limitations=["No raw ROA mapping", "ROA queries are external observations, not internal validation timers"])
    return manifest
