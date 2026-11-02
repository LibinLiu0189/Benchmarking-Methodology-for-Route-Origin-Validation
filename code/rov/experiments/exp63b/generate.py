"""Generate one BGP change per sampled prefix, with fixed VRPs and on/off predictions."""

import argparse
from collections import Counter
import ipaddress as ip
import json
from pathlib import Path
import random
import struct
import tempfile

from rov.data.bgp import attributes
from rov.data.bgp import load_bgp
from rov.data.bgp import read_raw
from rov.data.bgp_updates import attribute
from rov.data.bgp_updates import load_update_sequence
from rov.data.vrp import load_vrp_text
from rov.prepare.bgp.encode import encode_update
from rov.predict import bgp_path
from rov.predict.prefix_index import PrefixIndex
from rov.predict.preprocess import VRPs

TYPES = ("withdraw_best", "add_valid", "add_invalid", "invalid_to_valid", "best_to_invalid")
BASELINE = Path(__file__).resolve().parents[3] / "data/baseline"


# Allocate by largest remainder, then sample without replacement within each AFI.
def sample_prefixes(prefixes, count, rng):
    pools = {4: [], 6: []}
    for prefix in sorted(set(prefixes)):
        pools[ip.ip_network(prefix).version].append(prefix)
    population = {afi: len(pool) for afi, pool in pools.items()}
    total = sum(population.values())
    if not 1 <= count <= total:
        raise ValueError(f"Need 1..{total} distinct prefixes; requested {count}")
    quotas = {afi: count * size // total for afi, size in population.items()}
    order = sorted(quotas, key=lambda afi: (-(count * population[afi] % total), afi))
    for afi in order[:count - sum(quotas.values())]:
        quotas[afi] += 1
    selected = [p for afi in (4, 6) for p in rng.sample(pools[afi], quotas[afi])]
    return selected, quotas, population


# Collect prefix identities and one same-family next-hop template for each peer.
def scan_baseline(bgp, peers):
    prefixes, templates = set(), {}
    for name, peer in peers.items():
        for prefix, afi, raw in read_raw(bgp / peer["file"]):
            prefixes.add(prefix)
            templates.setdefault((name, afi), raw)
    return prefixes, templates


# Read only the sampled prefixes' complete candidate sets and original attributes.
def read_selected(bgp, peers, selected, authorizations):
    groups = {prefix: {} for prefix in selected}
    raws = {prefix: {} for prefix in selected}
    for name, peer in peers.items():
        for prefix, _, raw in read_raw(bgp / peer["file"]):
            if prefix in groups:
                groups[prefix][name] = bgp_path.make_candidate(prefix, name, raw, authorizations)
                raws[prefix][name] = raw
    return groups, raws


# Find covering VRP origins that also authorize this prefix length.
def valid_origins(prefix, authorizations):
    net = ip.ip_network(prefix)
    value, bits = int(net.network_address), net.max_prefixlen
    origins = set()
    for length, table in authorizations.tables[net.version].items():
        if length <= net.prefixlen:
            cover = table.get(value >> (bits - length) << (bits - length), ())
            origins.update(asn for maximum, asn in cover if maximum >= net.prefixlen and asn)
    return sorted(origins)


# Replace AS_PATH and optionally the next hop, retaining all other attributes.
def rewrite(prefix, raw, segments, hop_template=None):
    net = ip.ip_network(prefix)
    path = b"".join(bytes((kind, len(asns))) + struct.pack(f"!{len(asns)}I", *asns)
                    for kind, asns in segments)
    hops = {}
    if hop_template is not None:
        hops = {kind: hop_template[start:end]
                for kind, start, _, end in attributes(hop_template) if kind in (3, 14)}
    parts = []
    for kind, start, payload, end in attributes(raw):
        if kind == 2:
            parts.append(attribute(raw[start], kind, path))
        elif kind == 3 and hop_template is not None and net.version == 4:
            parts.append(hops[3])
        elif kind == 14:
            source = hops.get(14, raw[start:end])
            _, a, b, c = next(attributes(source))
            value = source[b:c]
            # Baseline MP_REACH uses one next hop, a zero SNPA count, and one NLRI.
            boundary = 5 + value[3]
            if value[boundary - 1] != 0:
                raise ValueError("MP_REACH with SNPA entries is unsupported")
            nlri = bytes((net.prefixlen,)) + net.network_address.packed[:(net.prefixlen + 7) // 8]
            parts.append(attribute(source[a], 14, value[:boundary] + nlri))
        else:
            parts.append(raw[start:end])
    return b"".join(parts)


# Change only the final origin ASN without changing segment types or path length.
def change_origin(candidate, origin):
    segments = [(kind, tuple(asns)) for kind, asns in candidate.segments]
    if not segments or segments[-1][0] != 2:
        return None
    kind, asns = segments[-1]
    segments[-1] = (kind, asns[:-1] + (origin,))
    return tuple(segments)


# Select both modes under the same explicit import and best-path policies.
def selections(prefix, candidates, peers, policy):
    return {mode: bgp_path.choose(prefix, candidates.values(), peers, dict(policy, rov=mode))
            for mode in ("on", "off")}


# Check a proposed update through the shared predictor before accepting its type.
def feasible_update(prefix, candidates, peers, policy, authorizations, before,
                    kind, peer, raw, state=None):
    updated = dict(candidates)
    candidate = None
    if raw is None:
        updated.pop(peer)
    else:
        candidate = bgp_path.make_candidate(prefix, peer, raw, authorizations)
        if state is not None and candidate.rov != state:
            return None
        updated[peer] = candidate
    after = selections(prefix, updated, peers, policy)
    if not all(result.known for result in after.values()):
        return None
    changed = [mode for mode in before if before[mode].path != after[mode].path]
    if not changed:
        return None
    if kind in ("add_valid", "add_invalid", "invalid_to_valid"):
        if not any(after[mode].path == candidate for mode in changed):
            return None
    return dict(type=kind, peer=peer, raw=raw, before=before, after=after)


# Enumerate feasible types, choosing a random feasible candidate within each type.
def options_for(prefix, candidates, raws, templates, peers, policy, authorizations,
                reference, rng, requested_type=None):
    before = selections(prefix, candidates, peers, policy)
    if not all(result.known for result in before.values()):
        return [], {kind: "baseline_selection_unresolved" for kind in TYPES}
    best = before[reference].path
    origins = [asn for asn in valid_origins(prefix, authorizations) if asn != policy["dut_asn"]]
    rng.shuffle(origins)
    options, reasons = [], {}
    afi = ip.ip_network(prefix).version

    # Search candidate proposals in randomized order; all types retain equal selection weight.
    def proposals(kind):
        if kind == "withdraw_best":
            if best is not None:
                yield best.peer, None, None
        elif kind in ("add_valid", "add_invalid"):
            available = [name for name in peers if name not in candidates and (name, afi) in templates]
            rng.shuffle(available)
            bases = list(raws)
            rng.shuffle(bases)
            base = raws[best.peer if best is not None else bases[0]]
            for name in available:
                peer_as = peers[name]["asn"]
                targets = origins if kind == "add_valid" else [peer_as, 64496, 64497, 4294967294]
                for origin in targets:
                    if origin == policy["dut_asn"] or peer_as == policy["dut_asn"]:
                        continue
                    numbers = (peer_as,) if peer_as == origin else (peer_as, origin)
                    yield name, rewrite(prefix, base, ((2, numbers),), templates[name, afi]), (
                        "Valid" if kind == "add_valid" else "Invalid")
        else:
            targets = list(candidates.values()) if kind == "invalid_to_valid" else ([best] if best else [])
            rng.shuffle(targets)
            for candidate in targets:
                if kind == "invalid_to_valid" and candidate.rov != "Invalid":
                    continue
                if kind == "best_to_invalid" and candidate.rov == "Invalid":
                    continue
                for origin in (origins if kind == "invalid_to_valid" else [64496, 64497, 4294967294]):
                    if origin == policy["dut_asn"]:
                        continue
                    # A one-AS path cannot change origin while retaining its announcing AS.
                    if sum(len(asns) for _, asns in candidate.segments) == 1 and origin != peers[candidate.peer]["asn"]:
                        continue
                    segments = change_origin(candidate, origin)
                    if segments is not None:
                        yield candidate.peer, rewrite(prefix, raws[candidate.peer], segments), (
                            "Valid" if kind == "invalid_to_valid" else "Invalid")

    for kind in (requested_type,) if requested_type else TYPES:
        found = None
        for peer, raw, state in proposals(kind):
            found = feasible_update(prefix, candidates, peers, policy, authorizations,
                                    before, kind, peer, raw, state)
            if found is not None:
                break
        if found is not None:
            options.append(found)
        else:
            reasons[kind] = (
                "no_authorized_origin_at_prefix_length" if kind in ("add_valid", "invalid_to_valid") and not origins
                else "no_reference_best" if kind in ("withdraw_best", "best_to_invalid") and best is None
                else "no_existing_invalid_candidate" if kind == "invalid_to_valid" and not any(c.rov == "Invalid" for c in candidates.values())
                else "no_unused_peer_for_afi" if kind.startswith("add_") and not any(name not in candidates and (name, afi) in templates for name in peers)
                else "no_supported_proposal_with_required_state_and_unique_winning_change"
            )
    return options, reasons


# Write one compatible sequence containing exactly one operation per selected prefix.
def write_sequence(directory, bgp, metadata, selected):
    directory.mkdir()
    counts, peer_rows, events = Counter(), {}, []
    for number, (prefix, choice) in enumerate(selected.items(), 1):
        peer, raw = choice["peer"], choice["raw"]
        operation = "withdraw" if raw is None else "announce"
        row = peer_rows.setdefault(peer, dict(peer=peer, file=f"{peer}.bgp", announce=0, withdraw=0))
        message = encode_update(operation, prefix, raw)
        with (directory / row["file"]).open("ab") as stream:
            offset = stream.tell()
            stream.write(message)
        row[operation] += 1
        counts[operation] += 1
        events.append(dict(number=number, batch=1, timestamp_us=metadata["snapshot_timestamp"] * 1000000,
                           peer=peer, operation=operation, prefix=prefix, file=row["file"],
                           offset=offset, length=len(message)))
    batch = dict(number=1, source="generated-exp63b-prefix-set",
                 counts={key: counts[key] for key in ("announce", "withdraw")}, peers=list(peer_rows.values()))
    families = sorted({ip.ip_network(prefix).version for prefix in selected})
    manifest = dict(format="bgp-update-batches-v1", initial=str(bgp),
                    snapshot_timestamp=metadata["snapshot_timestamp"],
                    selection="generated-distinct-prefixes", afi=families[0] if len(families) == 1 else families,
                    batches=[batch])
    (directory / "sequence.json").write_text(json.dumps(manifest, indent=2) + "\n")
    (directory / "events.jsonl").write_text("".join(json.dumps(row) + "\n" for row in events))
    return batch


# Resolve the pre-update next hop for a probe whose more-specific ranges were excluded.
def before_forwarding(prefix, before, table, index):
    for covering in (prefix, *index.covering(prefix)):
        best = bgp_path.batch_path(covering, before, table)
        if not best.known:
            return dict(status="unresolved", next_hop=None)
        if best.path is not None:
            return dict(status="known", next_hop=best.path.next_hop)
    return dict(status="known", next_hop=None)


# Intersect both modes' safe ranges and choose exactly one shared destination.
def common_probe(prefix, probes, seed):
    ranges = [[(int(ip.ip_address(row["start"])), int(ip.ip_address(row["end"])))
               for row in probes[mode]["ranges"]] for mode in ("on", "off")]
    shared = []
    left = right = 0
    while left < len(ranges[0]) and right < len(ranges[1]):
        a, b = ranges[0][left], ranges[1][right]
        start, end = max(a[0], b[0]), min(a[1], b[1])
        if start <= end:
            shared.append((start, end))
        if a[1] < b[1]:
            left += 1
        else:
            right += 1
    ready = shared and all(probes[mode]["status"] == "ready" for mode in probes)
    address_type = ip.IPv4Address if ip.ip_network(prefix).version == 4 else ip.IPv6Address
    destination = None
    if ready:
        offset = random.Random(f"{seed}:{prefix}").randrange(sum(b - a + 1 for a, b in shared))
        for start, end in shared:
            if offset <= end - start:
                destination = str(address_type(start + offset))
                break
            offset -= end - start + 1
    return {mode: dict(probe, status="ready" if ready else "unavailable",
                       destination=destination,
                       ranges=[dict(start=str(address_type(a)), end=str(address_type(b))) for a, b in shared],
                       **({} if ready else dict(reason="no_common_safe_probe_in_both_modes")))
            for mode, probe in probes.items()}


# Independently decode saved RAW, then bind identical probe targets to both modes.
def predict_saved(bgp, authorizations, peers, policy, directory, selected, seed):
    batch, = load_update_sequence(directory / "update")
    reports, predictions = {}, {}
    for mode in ("on", "off"):
        print(f"Validating saved RAW and predicting ROV-{mode}.", flush=True)
        current_policy = dict(policy, rov=mode)
        table = bgp_path.load_initial(bgp, authorizations, peers, current_policy)
        index = PrefixIndex(table)
        with (directory / "update/events.jsonl").open() as events:
            before, counts = bgp_path.apply_batch(table, directory / "update", batch, events, authorizations, peers)
            if any(line.strip() for line in events):
                raise ValueError("Unexpected trailing events")
        after = bgp_path.recalculate(table, before, peers, current_policy)
        probes = {}
        for prefix, choice in selected.items():
            if before[prefix] != choice["before"][mode] or after[prefix] != choice["after"][mode]:
                raise ValueError(f"Saved RAW prediction differs from generated proposal: {prefix} {mode}")
            probes[prefix] = bgp_path.prepare_probe(prefix, before, after, table, index,
                                                   rng=random.Random(f"{seed}:{prefix}"))
            detail = dict(before=bgp_path.format_change_path(before[prefix].path),
                          after=bgp_path.format_change_path(after[prefix].path),
                          best_path_changed=before[prefix].path != after[prefix].path)
            old, new = before[prefix].path, after[prefix].path
            detail["selected_next_hop_changed"] = (old.next_hop if old else None) != (new.next_hop if new else None)
            detail["selected_route_absent_after"] = new is None
            detail["attributes_only"] = bool(old and new and old != new and old.peer == new.peer and old.next_hop == new.next_hop)
            detail["probe"] = probes[prefix]
            detail["forwarding_before"] = before_forwarding(prefix, before, table, index)
            choice.setdefault("results", {})[mode] = detail
        prediction = bgp_path.format_results(batch, counts, current_policy, before, after, probes, table, index)
        predictions[mode] = prediction
        reports[mode] = dict(best_path_changes=len(prediction["changes"]), unresolved=len(prediction["unresolved"]),
                             change_types=dict(Counter(row["type"] for row in prediction["changes"])))
        del table, index
    for prefix, choice in selected.items():
        shared = common_probe(prefix, {mode: choice["results"][mode]["probe"] for mode in predictions}, seed)
        for mode, probe in shared.items():
            detail = choice["results"][mode]
            detail["probe"] = probe
            previous = detail["forwarding_before"]
            detail["sampled_forwarding_changed"] = (
                previous["next_hop"] != probe["expected_next_hop"]
                if previous["status"] == "known" and probe["status"] == "ready" else None)
    for mode, prediction in predictions.items():
        prediction["probes"] = [dict(prefix=prefix, **choice["results"][mode]["probe"],
                                          before_next_hop=choice["results"][mode]["forwarding_before"]["next_hop"])
                                 for prefix, choice in selected.items()]
        prediction["baseline_selected"] = [dict(prefix=prefix, path=choice["results"][mode]["before"])
                                            for prefix, choice in selected.items()]
        for change in prediction["changes"]:
            change["probe"] = selected[change["prefix"]]["results"][mode]["probe"]
        (directory / f"predictions-{mode}.jsonl").write_text(json.dumps(prediction) + "\n")
        reports[mode]["probe_targets"] = sum(probe["status"] == "ready" for probe in prediction["probes"])
    return reports


# Generate a single independent input, refusing overwrites and infeasible sampled prefixes.
def generate(bgp, vrp, output, prefix_count=100, seed=0, reference_rov="on", dut_asn=65000,
             as_sets="reject", operation_type=None):
    bgp, vrp = Path(bgp).resolve(strict=True), Path(vrp).resolve(strict=True)
    output = Path(output).absolute()
    if output.exists() or output.is_symlink():
        raise ValueError(f"Output already exists: {output}")
    output = output.resolve()
    if bgp in output.parents or output == vrp or output in vrp.parents:
        raise ValueError("Output must be outside input data")
    if not 1 <= dut_asn <= 4294967295:
        raise ValueError("DUT ASN is out of range")
    if operation_type is not None and operation_type not in TYPES:
        raise ValueError(f"Unknown operation type: {operation_type}")
    rng = random.Random(seed)
    metadata = load_bgp(bgp)
    peers = {peer["name"]: peer for peer in metadata["peers"]}
    print("Reading baseline prefix population and fixed VRPs.", flush=True)
    population, templates = scan_baseline(bgp, peers)
    authorizations = VRPs(load_vrp_text(vrp))
    policy = dict(dut_asn=dut_asn, as_sets=as_sets)
    selected, unavailable, failed = {}, {}, {}
    if operation_type is None:
        prefixes, quotas, population_counts = sample_prefixes(population, prefix_count, rng)
        groups, raws = read_selected(bgp, peers, prefixes, authorizations)
        for prefix in prefixes:
            options, reasons = options_for(prefix, groups[prefix], raws[prefix], templates, peers,
                                           policy, authorizations, reference_rov, rng)
            unavailable[prefix] = reasons
            if options:
                selected[prefix] = rng.choice(options)
            else:
                failed[prefix] = reasons
        if failed:
            raise ValueError("Sampled prefixes have no feasible change; no resampling or output: " + json.dumps(failed))
    else:
        population_counts = Counter(ip.ip_network(prefix).version for prefix in population)
        if prefix_count > len(population):
            raise ValueError(f"Need at most {len(population)} distinct prefixes; requested {prefix_count}")
        remaining = sorted(population)
        rng.shuffle(remaining)
        for offset in range(0, len(remaining), 20000):
            chunk = remaining[offset:offset + 20000]
            groups, raws = read_selected(bgp, peers, chunk, authorizations)
            for prefix in chunk:
                options, reasons = options_for(prefix, groups[prefix], raws[prefix], templates, peers,
                                               policy, authorizations, reference_rov, rng, operation_type)
                unavailable[prefix] = reasons
                if options:
                    selected[prefix] = options[0]
                    if len(selected) == prefix_count:
                        break
            print(f"{operation_type}: {len(selected)}/{prefix_count} feasible prefixes", flush=True)
            if len(selected) == prefix_count:
                break
        if len(selected) != prefix_count:
            raise ValueError(f"Only {len(selected)} feasible {operation_type} prefixes; requested {prefix_count}")
        quotas = Counter(ip.ip_network(prefix).version for prefix in selected)
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=output.parent, prefix=".exp63b-") as temporary:
        stage = Path(temporary)
        batch = write_sequence(stage / "update", bgp, metadata, selected)
        predictions = predict_saved(bgp, authorizations, peers, policy, stage, selected, seed)
        transitions = [dict(prefix=prefix, afi=ip.ip_network(prefix).version, type=choice["type"],
                            peer=choice["peer"], operation="withdraw" if choice["raw"] is None else "announce",
                            unavailable_types=unavailable[prefix], results=choice["results"])
                       for prefix, choice in selected.items()]
        report = dict(format="exp63b-prefix-set-v1", bgp=str(bgp), vrp=str(vrp), vrp_modified=False,
                      vrp_count=len(authorizations.rows), prefix_count=prefix_count, seed=seed,
                      reference_rov=reference_rov, policy=policy, baseline_prefixes=population_counts,
                      afi_counts=quotas, operations=batch["counts"],
                      type_counts=dict(Counter(choice["type"] for choice in selected.values())),
                      unavailable_type_reasons=dict(Counter(f"{kind}:{reason}" for reasons in unavailable.values() for kind, reason in reasons.items())),
                      predictions=predictions,
                      selection=("Largest-remainder AFI quotas; uniform prefixes without replacement; uniform feasible type; no resampling"
                                 if operation_type is None else "Random prefix order without replacement; first feasible prefixes for requested type"),
                      requested_type=operation_type,
                      scope="One update set shared by on/off; fixed VRPs; offline predictions only")
        (stage / "summary.json").write_text(json.dumps(report, indent=2) + "\n")
        (stage / "transitions.jsonl").write_text("".join(json.dumps(row) + "\n" for row in transitions))
        (stage / "scenario.json").write_text(json.dumps(dict(
            format="exp63b-scenario-v1", bgp=str(bgp), vrp=str(vrp), seed=seed,
            sequence="update", predictions={mode: f"predictions-{mode}.jsonl" for mode in ("on", "off")},
        ), indent=2) + "\n")
        output.mkdir()
        for child in stage.iterdir():
            child.rename(output / child.name)
    print(json.dumps(report, indent=2), flush=True)
    print(f"Prepared input: {output}", flush=True)
    return report


# Expose one prefix-set experiment, with no user-facing batch or repetition controls.
def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bgp", type=Path, default=BASELINE / "bgp")
    parser.add_argument("--vrp", type=Path, default=BASELINE / "balanced.vrps")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--prefix-count", type=int, default=1000)
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--reference-rov", choices=("on", "off"), default="on")
    parser.add_argument("--dut-asn", type=int, default=65000)
    parser.add_argument("--as-sets", choices=("accept", "reject"), default="reject")
    parser.add_argument("--operation-type", choices=TYPES)
    args = parser.parse_args()
    generate(**vars(args))


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError) as error:
        raise SystemExit(str(error)) from None
