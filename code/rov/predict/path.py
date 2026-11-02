"""Predict per-batch best-path changes offline using an explicit BIRD directly connected eBGP policy; do not access the DUT."""

from pathlib import Path
import argparse
import ipaddress as ip
import time

from .preprocess import inputs, write_results
from .prefix_index import PrefixIndex
from .probe import prepare_probe
from .routes import Candidate
from .selection import path_info, select_path, describe


# Apply the VRP-update experiment's acceptance policy before shared selection.
def choose(prefix, routes, peers, index, policy):
    network = ip.ip_network(prefix)
    eligible = []
    unknown = []
    for route in routes:
        if policy["as_sets"] == "reject" and route["has_set"]:
            continue
        if any(policy["dut_asn"] in numbers for _, numbers in route["segments"]):
            continue
        rov = index.state(network, route["origin_as"])[0]
        if policy["rov"] == "on" and rov == "Invalid":
            continue
        if route["has_confed"] or set(route["kinds"]) & {9, 10, 17, 18, 26}:
            unknown.append(route["peer"])
            continue
        eligible.append(Candidate(
            peer=route["peer"], next_hop=route["next_hop"], origin_as=route["origin_as"],
            segments=tuple(route["segments"]), origin=route["origin"], med=route["med"],
            rov=rov,
        ))
    if unknown:
        return dict(
            status="unresolved", reason="unsupported_selection_attributes_or_confederation",
            peers=unknown,
        )
    result = select_path(eligible, peers, policy)
    if not result.known:
        return dict(
            status="unresolved", reason="no_unique_winner_med_or_tie",
            peers=list(result.peers),
        )
    selected = result.path
    return dict(
        status="known",
        path=None if selected is None else dict(
            peer=selected.peer, gateway=selected.next_hop, origin_as=selected.origin_as,
        ),
    )


# Find the most-specific selected route that covers, but is shorter than, the
# supplied prefix in a complete best-path table.
def covering_route(prefix, table, progress=None, prefix_index=None):
    network = ip.ip_network(prefix)
    if network.version == 4 and prefix_index is not None:
        for ancestor in prefix_index.covering(prefix):
            result = table[ancestor]
            if result["status"] == "known" and result["path"] is not None:
                return dict(prefix=ancestor, path=result["path"])
        return None

    # Keep the existing full-table behavior for IPv6 and standalone calls.
    selected = None
    for scanned, (candidate_prefix, result) in enumerate(table.items(), 1):
        if progress is not None and scanned % 1000 == 0:
            progress("covering route scan", scanned, len(table))
        if result["status"] != "known" or result["path"] is None:
            continue
        candidate = ip.ip_network(candidate_prefix)
        if (
            candidate.version == network.version
            and candidate.prefixlen < network.prefixlen
            and network.subnet_of(candidate)
            and (selected is None or candidate.prefixlen > selected[0].prefixlen)
        ):
            selected = candidate, result["path"]
    if selected is None:
        return None
    candidate, path = selected
    return dict(prefix=str(candidate), path=path)


# Build one path-change record and identify a covering route after an exact
# prefix loses its selected path.
def path_change(
    prefix, before, after, before_table, after_table, progress=None,
    prefix_index=None,
):
    record = dict(prefix=prefix, before=before, after=after)
    if before is None:
        record["before_covering"] = covering_route(
            prefix, before_table, progress, prefix_index
        )
        record["type"] = "installed"
    elif after is None:
        record["after_covering"] = covering_route(
            prefix, after_table, progress, prefix_index
        )
        record["type"] = (
            "withdrawn_to_covering_route"
            if record["after_covering"] is not None
            else "withdrawn"
        )
    else:
        record["type"] = "replaced"
    record["probe"] = prepare_probe(
        prefix, before_table, after_table, progress, prefix_index
    )
    return record


# Format an available covering route for terminal output.
def describe_covering(value):
    if value is None:
        return "No covering route"
    return f'{value["prefix"]}: {describe(dict(status="known", path=value["path"]))}'


# Calculate a complete initial best-path table, apply each VRP batch, and
# recalculate every BGP prefix before comparing the complete tables.
def predict(bgp, vrp, sequence, policy):
    last_progress = time.monotonic()

    # Print periodic progress, plus each stage's completion.
    def progress(stage, completed, total, started, force=False):
        nonlocal last_progress
        now = time.monotonic()
        if force or now - last_progress >= 5:
            print(
                f"{stage}: {completed:,}/{total:,} ({now - started:.1f}s)",
                flush=True,
            )
            last_progress = now

    loading_started = time.monotonic()
    print("Loading VRP batches, BGP candidates, and initial VRPs.", flush=True)

    # Keep shared input loading quiet for other callers while reporting this run.
    def input_progress(message, force=False):
        nonlocal last_progress
        now = time.monotonic()
        if force or now - last_progress >= 5:
            print(f"{message} ({now - loading_started:.1f}s)", flush=True)
            last_progress = now

    batches, groups, peers, index, _ = inputs(
        bgp,
        vrp,
        sequence,
        path_info,
        retain_all_candidates=True,
        progress=input_progress,
    )
    print(f"Inputs loaded in {time.monotonic() - loading_started:.1f}s.", flush=True)

    prefixes = sorted(
        groups,
        key=lambda prefix: (
            ip.ip_network(prefix).version,
            int(ip.ip_network(prefix).network_address),
            ip.ip_network(prefix).prefixlen,
        ),
    )
    total_prefixes = len(prefixes)
    index_started = time.monotonic()
    print("Building IPv4 prefix index for path-change queries.", flush=True)
    prefix_index = PrefixIndex(
        prefix for prefix in prefixes if ip.ip_network(prefix).version == 4
    )
    print(
        f"IPv4 prefix index built in {time.monotonic() - index_started:.1f}s.",
        flush=True,
    )
    selection_started = time.monotonic()
    print(
        f"Calculating initial best paths for {total_prefixes:,} BGP prefixes.",
        flush=True,
    )
    current = {}
    for position, prefix in enumerate(prefixes, 1):
        current[prefix] = choose(prefix, groups[prefix], peers, index, policy)
        if position % 1000 == 0:
            progress("Initial best paths", position, total_prefixes, selection_started)
    progress(
        "Initial best paths complete", total_prefixes, total_prefixes,
        selection_started, True,
    )

    for info, batch in batches:
        batch_number = info["number"]
        apply_started = time.monotonic()
        print(f"Batch {batch_number}: applying {len(batch)} VRP operations.", flush=True)
        index.apply(batch)
        print(
            f"Batch {batch_number}: VRP operations applied in "
            f"{time.monotonic() - apply_started:.1f}s.",
            flush=True,
        )

        selection_started = time.monotonic()
        print(
            f"Batch {batch_number}: recalculating {total_prefixes:,} best paths.",
            flush=True,
        )
        updated = {}
        for position, prefix in enumerate(prefixes, 1):
            updated[prefix] = choose(prefix, groups[prefix], peers, index, policy)
            if position % 1000 == 0:
                progress(
                    f"Batch {batch_number} best paths", position,
                    total_prefixes, selection_started,
                )
        progress(
            f"Batch {batch_number} best paths complete",
            total_prefixes, total_prefixes, selection_started, True,
        )

        changes = []
        unresolved = []
        comparison_started = time.monotonic()
        change_details_seconds = 0.0
        print(
            f"Batch {batch_number}: comparing {total_prefixes:,} prefixes; "
            "change details query indexed IPv4 ancestors and descendants.",
            flush=True,
        )
        for position, prefix in enumerate(prefixes, 1):
            old = current[prefix]
            new = updated[prefix]
            if old["status"] != "known" or new["status"] != "known":
                if old != new:
                    unresolved.append(dict(prefix=prefix, before=old, after=new))
                if position % 1000 == 0:
                    progress(
                        f"Batch {batch_number} compared prefixes", position,
                        total_prefixes, comparison_started,
                    )
                continue
            if old["path"] != new["path"]:
                details_started = time.monotonic()

                # Show progress inside a long probe candidate scan.
                def scan_progress(stage, scanned, total):
                    progress(
                        f"Batch {batch_number} prefix {position:,}/{total_prefixes:,} "
                        f"({prefix}) {stage}",
                        scanned,
                        total,
                        details_started,
                    )

                changes.append(
                    path_change(
                        prefix,
                        old["path"],
                        new["path"],
                        current,
                        updated,
                        scan_progress,
                        prefix_index,
                    )
                )
                change_details_seconds += time.monotonic() - details_started
            if position % 1000 == 0:
                progress(
                    f"Batch {batch_number} compared prefixes", position,
                    total_prefixes, comparison_started,
                )
        progress(
            f"Batch {batch_number} comparison complete",
            total_prefixes, total_prefixes, comparison_started, True,
        )
        print(
            f"Batch {batch_number}: covering-route and probe details for "
            f"{len(changes)} changes took {change_details_seconds:.1f}s within comparison.",
            flush=True,
        )
        current = updated
        print(
            f'\nBatch {info["number"]}: {len(batch)} VRP operations; '
            f'{len(prefixes)} prefixes recalculated; '
            f'{len(changes)} prefixes with best-path changes; '
            f'{len(unresolved)} changed unresolved predictions'
        )
        for c in changes:
            print(
                f'  {c["prefix"]}\n    Before: {describe(dict(status="known",path=c["before"]))}\n    After: {describe(dict(status="known",path=c["after"]))}'
            )
            if "after_covering" in c:
                new_covering = describe_covering(c["after_covering"])
                print(f"    New covering route: {new_covering}")
            probe = c["probe"]
            if probe["status"] == "ready":
                next_hop = probe["expected_next_hop"]
                expected = (
                    f"{probe['expected_prefix']} via {next_hop}"
                    if next_hop is not None
                    else "No forwarding route"
                )
                print(f"    Probe: {probe['destination']} -> {expected}")
            else:
                print(f"    Probe {probe['status']}: {probe['reason']}")
            for addresses in probe["ranges"]:
                print(f"    Probe range: {addresses['start']} - {addresses['end']}")
        for c in unresolved:
            print(
                f'  {c["prefix"]}: before: {describe(c["before"])}; after: {describe(c["after"])}'
            )
        yield dict(
            batch=info["number"],
            vrp_operations=len(batch),
            policy=policy,
            prefixes_recalculated=len(prefixes),
            changes=changes,
            unresolved=unresolved,
        )


# Parse input paths and DUT policy parameters, then save path predictions.
def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--bgp", required=True, type=Path)
    p.add_argument("--vrp", required=True, type=Path)
    p.add_argument("--sequence", required=True, type=Path)
    p.add_argument("--output", required=True, type=Path)
    p.add_argument("--dut-asn", required=True, type=int)
    p.add_argument(
        "--as-sets",
        choices=("accept", "reject"),
        required=True,
        help="DUT acceptance policy used for prediction; does not modify BGP data",
    )
    p.add_argument("--rov", choices=("on", "off"), default="on")
    a = p.parse_args()
    if not 1 <= a.dut_asn <= 4294967295:
        p.error("ASN is out of range")
    policy = {
        k: getattr(a, k)
        for k in (
            "dut_asn",
            "as_sets",
            "rov",
        )
    }
    started = time.monotonic()

    # Report only actual temporary-file writes and final output replacement.
    def output_progress(event, value):
        if event == "opened":
            print("Temporary JSONL opened; starting prediction.", flush=True)
        elif event == "writing":
            print(f"Batch {value['batch']}: writing temporary JSONL record.", flush=True)
        elif event == "written":
            row, elapsed = value
            print(
                f"Batch {row['batch']}: record written to temporary JSONL "
                f"in {elapsed:.1f}s.",
                flush=True,
            )
        else:
            print(
                f"Final JSONL published; total elapsed "
                f"{time.monotonic() - started:.1f}s.",
                flush=True,
            )

    write_results(
        a.output,
        predict(a.bgp, a.vrp, a.sequence, policy),
        (a.bgp, a.vrp, a.sequence),
        progress=output_progress,
    )


if __name__ == "__main__":
    main()
