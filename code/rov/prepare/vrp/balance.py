"""Add VRPs until initial Invalid and NotFound candidate counts are close."""

import argparse
from collections import Counter
import ipaddress as ip
from pathlib import Path
import random
import shutil

from rov.data.bgp import load_bgp
from rov.data.bgp import read_raw
from rov.data.vrp import load_vrp_text
from rov.predict.preprocess import Prefixes
from rov.predict.preprocess import VRPs
from rov.prepare.vrp.generate_rov_batches import invalid_covering_row
from rov.predict.rov import candidate_origin


# Read each peer announcement as a separate candidate and index shared prefixes.
def read_candidates(folder):
    metadata = load_bgp(folder)
    candidates = []
    groups = {}
    for peer in metadata["peers"]:
        for prefix, _, raw in read_raw(Path(folder) / peer["file"]):
            network = ip.ip_network(prefix)
            if network.prefixlen == 0:
                raise ValueError(
                    f"BGP candidate cannot have a /0 prefix: "
                    f"peer={peer['name']}, prefix={prefix}"
                )

            origin = candidate_origin(raw)
            if origin is None:
                raise ValueError(
                    f"Cannot determine origin AS: peer={peer['name']}, prefix={prefix}"
                )

            candidate_id = len(candidates)
            candidates.append((network, origin))
            groups.setdefault(prefix, []).append(candidate_id)
    return candidates, groups


# Remove a candidate from the randomly selectable NotFound pool in constant time.
def remove_not_found(pool, positions, candidate_id):
    position = positions.pop(candidate_id)
    last_id = pool.pop()
    if position < len(pool):
        pool[position] = last_id
        positions[last_id] = position


# Add invalid-covering VRPs and update only candidates covered by each new row.
def balance(bgp, vrp, output, seed):
    bgp = Path(bgp).resolve()
    vrp = Path(vrp).resolve()
    output = Path(output).resolve()
    if output.exists() or output == bgp or bgp in output.parents:
        raise ValueError("Output must be a new file outside the BGP input directory")

    initial_rows = load_vrp_text(vrp)
    candidates, groups = read_candidates(bgp)
    index = VRPs(initial_rows)
    prefixes = Prefixes(groups)
    states = []
    counts = {4: Counter(), 6: Counter()}
    not_found = []
    positions = {}

    for candidate_id, (network, origin) in enumerate(candidates):
        state = index.state(network, origin)[0]
        states.append(state)
        counts[network.version][state] += 1
        if state == "NotFound":
            positions[candidate_id] = len(not_found)
            not_found.append(candidate_id)

    initial_counts = {family: counts[family].copy() for family in (4, 6)}
    added_rows = []
    added_counts = Counter()
    rng = random.Random(seed)

    while counts[4]["Invalid"] + counts[6]["Invalid"] < len(not_found):
        selected_id = rng.choice(not_found)
        network, origin = candidates[selected_id]
        _, row = invalid_covering_row(network, origin, rng)
        affected_prefixes = prefixes.covered([{"vrp": row}])

        index.edit(row, True)
        added_rows.append(row)
        added_counts[network.version] += 1
        for prefix in affected_prefixes:
            for candidate_id in groups[prefix]:
                candidate_network, candidate_origin_as = candidates[candidate_id]
                before = states[candidate_id]
                after = index.state(candidate_network, candidate_origin_as)[0]
                if after == before:
                    continue

                states[candidate_id] = after
                counts[candidate_network.version][before] -= 1
                counts[candidate_network.version][after] += 1
                if before == "NotFound":
                    remove_not_found(not_found, positions, candidate_id)

    output.parent.mkdir(parents=True, exist_ok=True)
    with vrp.open("rb") as source, output.open("xb") as stream:
        shutil.copyfileobj(source, stream)
        if added_rows and stream.tell():
            source.seek(-1, 2)
            if source.read(1) not in (b"\n", b"\r"):
                stream.write(b"\n")
        for prefix, maximum, asn in added_rows:
            stream.write(f"{prefix} {maximum} {asn}\n".encode("ascii"))

    return initial_counts, counts, added_counts


# Print initial and final ROV counts for both address families.
def print_counts(initial_counts, final_counts, added_counts):
    print("Family  Stage    Valid  Invalid  NotFound  AddedVRPs")
    for family in (4, 6):
        for stage, counts in (("Initial", initial_counts), ("Final", final_counts)):
            states = counts[family]
            added = added_counts[family] if stage == "Final" else 0
            print(
                f"IPv{family}   {stage:<7}  {states['Valid']}  {states['Invalid']} "
                f"{states['NotFound']}  {added}"
            )
    print(f"Added VRPs: {sum(added_counts.values())}")


# Read the input paths and save a new balanced initial VRP text file.
def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bgp", required=True, type=Path, help="Converted BGP RAW directory")
    parser.add_argument("--vrp", required=True, type=Path, help="Initial BRIO VRP text")
    parser.add_argument("--output", required=True, type=Path, help="New BRIO VRP text")
    parser.add_argument("--seed", type=int, default=0, help="Random seed (default: 0)")
    args = parser.parse_args()

    initial_counts, final_counts, added_counts = balance(
        args.bgp, args.vrp, args.output, args.seed
    )
    print_counts(initial_counts, final_counts, added_counts)
    print(f"VRPs saved: {args.output}")


if __name__ == "__main__":
    main()
