"""Prepare shared distinct-prefix VRP inputs for Experiments 6.2 and 6.3A."""

import argparse
from collections import Counter, defaultdict
import ipaddress as ip
import json
import os
from pathlib import Path
import random
import shutil
import tempfile

from rov.data.vrp import load_vrp_text
from rov.predict.affected_paths import predict_paths
from rov.predict.preprocess import Prefixes, VRPs
from rov.prepare.bgp.quotas import afi_quotas
from rov.prepare.vrp.generate_rov_batches import (
    apply_edit,
    choose_edit,
    read_candidates,
    validate_inputs,
    write_event,
)


BRANCH_LENGTHS = {4: 8, 6: 32}


# Group eligible BGP prefixes by their IPv4 /8 or IPv6 /32 address region.
def candidate_pools(groups):
    pools = {4: defaultdict(list), 6: defaultdict(list)}
    for prefix in sorted(groups):
        network = ip.ip_network(prefix)
        length = BRANCH_LENGTHS[network.version]
        if network.prefixlen < length:
            continue
        region = str(network.supernet(new_prefix=length))
        pools[network.version][region].append(prefix)
    return pools


# Choose eligible address regions while preserving Experiment 6.2 locality rules.
def case_populations(pools, quotas, distribution):
    populations = {}
    selected_regions = {}
    for afi in (4, 6):
        count = quotas[afi]
        regions = {name: rows for name, rows in pools[afi].items() if rows}
        available = sum(len(rows) for rows in regions.values())
        if available < count:
            raise ValueError(f"AFI {afi} has only {available} eligible prefixes; {count} are required")

        names = sorted(regions)
        if distribution == "localized":
            names = sorted(regions, key=lambda name: (-len(regions[name]), name))
            capacity = 0
            for number, name in enumerate(names, 1):
                capacity += len(regions[name])
                if capacity >= count:
                    names = names[:number]
                    break
            if 2 * len(names) > len(regions):
                raise ValueError(f"AFI {afi} cannot support localized sampling")
            selected_regions[afi] = names
        elif distribution != "distributed":
            raise ValueError(f"Unknown distribution: {distribution}")

        populations[afi] = [prefix for name in names for prefix in regions[name]]
    return populations, selected_regions


# Apply complete ROV-changing edits to distinct selected prefixes without repeating a VRP identity.
def write_case(directory, vrp, initial, candidates, groups, prefix_index,
               baseline_index, baseline_states, populations, quotas, seed, rng):
    directory.mkdir(parents=True)
    index = VRPs(initial)
    states = list(baseline_states)
    cache = list(initial)
    touched_vrps = set()
    selected = []
    operations = []
    commands = []
    transitions = []

    with (directory / "events.jsonl").open("x") as events:
        for afi in (4, 6):
            population = list(populations[afi])
            rng.shuffle(population)
            accepted = 0
            for prefix in population:
                if accepted == quotas[afi]:
                    break
                for _ in range(32):
                    selected_id = rng.choice(groups[prefix])
                    candidate = candidates[selected_id]
                    before = states[selected_id]
                    choice_seed = rng.randrange(2**63)
                    choice, edits = choose_edit(
                        index, candidate, before, random.Random(choice_seed)
                    )
                    identities = [tuple(row) for _, row in edits]
                    if (len(identities) == len(set(identities))
                            and touched_vrps.isdisjoint(identities)):
                        break
                else:
                    continue

                changes, per_operation = apply_edit(
                    index, prefix_index, candidates, groups, states, edits
                )
                after = states[selected_id]
                if before == after:
                    raise ValueError(f"Selected candidate did not change ROV state: {candidate}")

                first_operation = len(operations) + 1
                for (operation, row), affected in zip(edits, per_operation, strict=True):
                    operations.append((operation, row))
                    commands.append(write_event(
                        events, cache, len(operations), 1, operation, row, affected,
                    ))
                    touched_vrps.add(tuple(row))
                selected.append((selected_id, baseline_states[selected_id]))
                transitions.append(dict(
                    batch=1, conversion=len(transitions) + 1,
                    choice_seed=choice_seed,
                    selected=dict(prefix=candidate[0], peer=candidate[1],
                                  origin_as=candidate[2], before=before, after=after),
                    choice=choice, first_operation=first_operation,
                    operations=len(edits), changes=changes,
                ))
                accepted += 1
            if accepted != quotas[afi]:
                raise ValueError(
                    f"AFI {afi} supplied only {accepted} non-conflicting transitions; "
                    f"{quotas[afi]} are required"
                )

    (directory / "0001.script").write_text("\n".join(commands + ["notify"]) + "\n")
    (directory / "transitions.jsonl").write_text(
        "".join(json.dumps(row) + "\n" for row in transitions)
    )

    affected = prefix_index.covered([{"vrp": row} for _, row in operations])
    changes = []
    for prefix in affected:
        network = ip.ip_network(prefix)
        for candidate_id in groups[prefix]:
            _, peer, origin = candidates[candidate_id]
            before, before_reason = baseline_index.state(network, origin)
            after, after_reason = index.state(network, origin)
            if before != after:
                changes.append(dict(
                    prefix=prefix, peer=peer, origin_as=origin,
                    before=before, after=after,
                    before_reason=before_reason, after_reason=after_reason,
                ))

    selected_prefixes = {row["selected"]["prefix"] for row in transitions}
    changed_prefixes = {row["prefix"] for row in changes}
    if len(selected_prefixes) != len(selected):
        raise ValueError("Selected BGP prefixes must be distinct")
    counts = Counter(operation for operation, _ in operations)
    summary = dict(
        selected_prefixes=len(selected_prefixes),
        selected_candidates=len(selected),
        selected_prefixes_net_changed=len(selected_prefixes & changed_prefixes),
        selected_candidates_net_changed=sum(
            baseline != index.state(
                ip.ip_network(candidates[candidate_id][0]), candidates[candidate_id][2]
            )[0]
            for candidate_id, baseline in selected
        ),
        operations=len(operations), additions=counts["add"],
        withdrawals=counts["withdraw"],
        distinct_vrp_identities=len(touched_vrps),
        affected_bgp_candidates=len(changes),
        affected_bgp_prefixes=len(changed_prefixes),
    )
    sequence = dict(
        format="vrp-batches-v1", initial=os.path.relpath(vrp, directory),
        initial_count=len(initial), candidate_count=len(candidates), seed=seed,
        scenario=directory.name, counts=summary,
        batches=[dict(
            number=1, source="shared-prefix-workload",
            through_operation=len(operations), added=counts["add"],
            withdrawn=counts["withdraw"], total=len(operations), script="0001.script",
        )],
    )
    (directory / "sequence.json").write_text(json.dumps(sequence, indent=2) + "\n")
    prediction = dict(
        batch=1, snapshot="shared-prefix-workload", vrp_operations=len(operations),
        covered_prefixes=len(affected), changes=changes,
    )
    (directory / "predictions.jsonl").write_text(json.dumps(prediction) + "\n")
    return summary


# Generate all localized and distributed prefix workloads with ROV and path predictions.
def generate(bgp, vrp, output, prefix_counts=(100, 500, 1000), seed=620001):
    bgp = Path(bgp).resolve(strict=True)
    vrp = Path(vrp).resolve(strict=True)
    output = Path(output).absolute()
    if output.exists() or output.is_symlink():
        raise FileExistsError(f"Output already exists: {output}")
    if not (bgp / "bgp.json").is_file() or not vrp.is_file():
        raise ValueError("BGP metadata or VRP baseline is missing")
    counts = sorted(set(prefix_counts))
    if not counts or any(count < 2 for count in counts):
        raise ValueError("Prefix counts must be at least two")
    if output == bgp or bgp in output.parents or output == vrp or output in vrp.parents:
        raise ValueError("Output must be outside both input datasets")

    initial = load_vrp_text(vrp)
    candidates, groups = read_candidates(bgp)
    validate_inputs(initial, candidates)
    baseline_index = VRPs(initial)
    prefix_index = Prefixes(groups)
    networks = {prefix: ip.ip_network(prefix) for prefix in groups}
    population = Counter(network.version for network in networks.values())
    quotas = {count: afi_quotas(count, population) for count in counts}
    baseline_states = [
        baseline_index.state(networks[prefix], origin)[0]
        for prefix, _, origin in candidates
    ]
    pools = candidate_pools(groups)
    rng = random.Random(seed)
    manifest = dict(
        format="exp62-prefix-sets-v1", bgp=os.path.relpath(bgp, output),
        vrp=os.path.relpath(vrp, output), seed=seed,
        prefix_counts=counts, cases=[],
    )

    output.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=".prefix-workloads-", dir=output.parent))
    try:
        for count in counts:
            for distribution in ("localized", "distributed"):
                populations, regions = case_populations(pools, quotas[count], distribution)
                relative = Path(f"prefixes-{count:06d}") / distribution
                case = stage / relative
                summary = write_case(
                    case, vrp, initial, candidates, groups, prefix_index,
                    baseline_index, baseline_states, populations, quotas[count], seed, rng,
                )
                path_predictions = case / "path-predictions.jsonl"
                predict_paths(bgp, vrp, case, path_predictions)
                path_row = json.loads(path_predictions.read_text())
                if path_row["unresolved"]:
                    raise ValueError(f"Unresolved path predictions for {relative}")
                summary.update(
                    rib_best_path_changes=len(path_row["changes"]),
                    fib_probe_targets=sum(
                        row["probe"]["status"] == "ready"
                        for row in path_row["changes"]
                    ),
                    fib_unavailable=sum(
                        row["probe"]["status"] != "ready"
                        for row in path_row["changes"]
                    ),
                )
                scenario = dict(
                    format="exp63a-scenario-v1",
                    bgp=os.path.relpath(bgp, case),
                    sequence="sequence.json", predictions="path-predictions.jsonl",
                )
                (case / "scenario.json").write_text(json.dumps(scenario, indent=2) + "\n")
                entry = dict(path=str(relative), **summary)
                if regions:
                    entry["local_regions"] = regions
                manifest["cases"].append(entry)
                print(
                    f"Prepared {relative}: {summary['operations']} VRP operations, "
                    f"{summary['affected_bgp_candidates']} ROV changes, "
                    f"{summary['rib_best_path_changes']} RIB changes",
                    flush=True,
                )
        (stage / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
        stage.rename(output)
    finally:
        if stage.exists():
            shutil.rmtree(stage)
    return manifest


# Parse one shared prefix-workload generation request.
def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bgp", required=True, type=Path)
    parser.add_argument("--vrp", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--prefix-counts", type=int, nargs="+", default=[100, 500, 1000])
    parser.add_argument("--seed", type=int, default=620001)
    args = parser.parse_args()
    generate(args.bgp, args.vrp, args.output, args.prefix_counts, args.seed)


if __name__ == "__main__":
    main()
