"""Summarize the prepared shared inputs without changing their contents.

VRP operation counts include additions and withdrawals, with a withdrawal followed
by an addition counted as two operations. ROV and RIB changes are predicted net
changes within each batch; a prefix changed in two batches counts twice. FIB
counts include only prefixes with a predicted best-path change, classified by
whether their prepared probe is ready. A duplicate VRP identity counts once for
each occurrence after its first occurrence in the same batch. BGP inputs have
separate ROV-on and ROV-off rows because their path predictions differ; their
identical BGP operation counts must not be summed across those rows.
The six ROV transition columns partition the candidate changes by before and
after state, using the same per-batch net-change predictions.
"""

from collections import Counter, defaultdict
import csv
import json
from pathlib import Path
import tempfile

from rov.experiments.exp63b.generate import TYPES


FIELDS = (
    "dataset", "kind", "policy", "initial_bgp_candidates", "initial_vrp_count", "batch_count",
    "target_vrp_operations_per_batch", "actual_vrp_operations",
    "vrp_additions", "vrp_withdrawals", "bgp_operations", "selected_prefixes",
    "rov_changed_candidates", "rov_changed_prefixes", "rov_valid_to_invalid", "rov_valid_to_not_found",
    "rov_invalid_to_valid", "rov_invalid_to_not_found",
    "rov_not_found_to_valid", "rov_not_found_to_invalid",
    "rib_changed_prefixes", "fib_ready",
    "fib_unavailable", "repeated_vrp_identities_within_batch", *TYPES,
)
ROV_TRANSITIONS = {
    ("Valid", "Invalid"): "rov_valid_to_invalid",
    ("Valid", "NotFound"): "rov_valid_to_not_found",
    ("Invalid", "Valid"): "rov_invalid_to_valid",
    ("Invalid", "NotFound"): "rov_invalid_to_not_found",
    ("NotFound", "Valid"): "rov_not_found_to_valid",
    ("NotFound", "Invalid"): "rov_not_found_to_invalid",
}
VRP_SCALES = ("50k", "100k", "500k", "1m")
PREFIX_COUNTS = (100, 500, 1000)
BGP_WORKLOADS = ("mixed", *TYPES)


# Read one required JSON document from a prepared input.
def _read_json(path):
    with Path(path).open() as stream:
        return json.load(stream)


# Yield the required JSONL records without loading the full file into memory.
def _read_jsonl(path):
    with Path(path).open() as stream:
        for line in stream:
            if line.strip():
                yield json.loads(line)


# Count each predicted net candidate transition, including repeats across batches.
def _rov_change_counts(path):
    counts = Counter()
    changed_prefixes = 0
    for row in _read_jsonl(path):
        changed_prefixes += len({change["prefix"] for change in row["changes"]})
        for change in row["changes"]:
            transition = change["before"], change["after"]
            if transition not in ROV_TRANSITIONS:
                raise ValueError(f"Unknown ROV state transition: {transition}")
            counts[ROV_TRANSITIONS[transition]] += 1
    return {name: counts[name] for name in ROV_TRANSITIONS.values()}, changed_prefixes


# Count predicted RIB changes and probe availability at changed prefixes.
def _path_change_counts(path):
    rib = ready = unavailable = 0
    for row in _read_jsonl(path):
        for change in row["changes"]:
            rib += 1
            if change["probe"]["status"] == "ready":
                ready += 1
            else:
                unavailable += 1
    return rib, ready, unavailable


# Count extra appearances of an identical VRP within the same notification batch.
def _repeated_vrp_identities(path):
    seen = defaultdict(set)
    repeated = 0
    for row in _read_jsonl(path):
        batch = row["batch"]
        identity = tuple(row["vrp"])
        if identity in seen[batch]:
            repeated += 1
        seen[batch].add(identity)
    return repeated


# Count the distinct BGP target prefixes selected by a prefix-based workload.
def _selected_prefixes(path):
    return len({row["selected"]["prefix"] for row in _read_jsonl(path)})


# Resolve a sequence directory or sequence.json file from its scenario reference.
def _sequence_paths(directory, scenario):
    sequence = directory / scenario["sequence"]
    if sequence.is_dir():
        return sequence / "sequence.json", sequence
    return sequence, sequence.parent


# Summarize one prepared VRP workload and its offline predictions.
def _vrp_row(data_root, directory, kind, target_operations):
    scenario = _read_json(directory / "scenario.json")
    sequence_path, sequence_directory = _sequence_paths(directory, scenario)
    sequence = _read_json(sequence_path)
    batches = sequence["batches"]
    path_predictions = (
        directory / scenario.get("path_predictions", scenario["predictions"])
    )
    rov_predictions = (
        directory / "predictions.jsonl"
        if kind == "vrp-prefix" else directory / scenario["predictions"]
    )
    rov_changes, changed_prefixes = _rov_change_counts(rov_predictions)
    rib, ready, unavailable = _path_change_counts(path_predictions)
    row = dict.fromkeys(FIELDS, "")
    row.update(
        dataset=str(directory.relative_to(data_root)), kind=kind, policy="on",
        initial_vrp_count=sequence["initial_count"], batch_count=len(batches),
        target_vrp_operations_per_batch=target_operations,
        actual_vrp_operations=sum(batch["total"] for batch in batches),
        vrp_additions=sum(batch["added"] for batch in batches),
        vrp_withdrawals=sum(batch["withdrawn"] for batch in batches),
        selected_prefixes=(
            _selected_prefixes(sequence_directory / "transitions.jsonl")
            if kind == "vrp-prefix" else ""
        ),
        rov_changed_candidates=sum(rov_changes.values()), rov_changed_prefixes=changed_prefixes,
        rib_changed_prefixes=rib, fib_ready=ready, fib_unavailable=unavailable,
        repeated_vrp_identities_within_batch=_repeated_vrp_identities(
            sequence_directory / "events.jsonl"
        ),
    )
    row.update(rov_changes)
    return row


# Summarize one BGP workload separately under each of its two routing policies.
def _bgp_rows(data_root, directory):
    scenario = _read_json(directory / "scenario.json")
    summary = _read_json(directory / "summary.json")
    sequence_directory = directory / scenario["sequence"]
    sequence = _read_json(sequence_directory / "sequence.json")
    operation_count = sum(1 for _ in _read_jsonl(sequence_directory / "events.jsonl"))
    rows = []
    for policy in ("on", "off"):
        rib, ready, unavailable = _path_change_counts(
            directory / scenario["predictions"][policy]
        )
        row = dict.fromkeys(FIELDS, "")
        row.update(
            dataset=str(directory.relative_to(data_root)), kind="bgp",
            policy=policy, initial_vrp_count=summary["vrp_count"],
            batch_count=len(sequence["batches"]), bgp_operations=operation_count,
            selected_prefixes=summary["prefix_count"],
            rib_changed_prefixes=rib, fib_ready=ready,
            fib_unavailable=unavailable,
        )
        row.update({name: summary["type_counts"].get(name, 0) for name in TYPES})
        rows.append(row)
    return rows


# Write one CSV containing all prepared common input groups.
def write_statistics(data_root):
    data_root = Path(data_root).resolve(strict=True)
    inputs = data_root / "inputs"
    bgp_metadata = _read_json(data_root / "baseline" / "bgp" / "bgp.json")
    bgp_candidates = sum(bgp_metadata["counts"].values())
    rows = [
        _vrp_row(data_root, inputs / "vrp-single" / scale, "vrp-single", 1000)
        for scale in VRP_SCALES
    ]
    for name in ("500k-300-batches", "batch500", "batch1000", "500k-first60"):
        directory = inputs / "vrp-batch" / name
        scenario = _read_json(directory / "scenario.json")
        rows.append(_vrp_row(data_root, directory, "vrp-batch", scenario["batch_size"]))
    for count in PREFIX_COUNTS:
        for locality in ("localized", "distributed"):
            directory = inputs / "vrp-prefix" / f"prefixes-{count:06d}" / locality
            rows.append(_vrp_row(data_root, directory, "vrp-prefix", ""))
    for name in BGP_WORKLOADS:
        rows.extend(_bgp_rows(data_root, inputs / "bgp" / name))
    for row in rows:
        row["initial_bgp_candidates"] = bgp_candidates

    output = data_root / "statistics.csv"
    with tempfile.TemporaryDirectory(dir=data_root, prefix=".statistics-") as temporary:
        staged_output = Path(temporary) / output.name
        with staged_output.open("x", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=FIELDS)
            writer.writeheader()
            writer.writerows(rows)
        staged_output.replace(output)
    return output
