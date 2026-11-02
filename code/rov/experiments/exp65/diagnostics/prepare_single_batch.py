"""Reconstruct stable before-batch VRPs and retain the continuous run's targets."""

from bisect import bisect_left, insort
from collections import Counter
import json
from pathlib import Path

from rov.data.sequence import load_sequence
from rov.data.vrp import load_vrp_text
from rov.experiments.exp65.inputs.scenario import load_scenario


# Re-index BRIO withdrawals against a compact independent initial snapshot.
def reindex_operations(initial, operations):
    positions = {identity: index for index, identity in enumerate(initial)}
    removed = []
    next_position = len(positions)
    rewritten = []
    for operation in operations:
        identity = tuple(operation["vrp"])
        if operation["operation"] == "withdraw":
            position = positions.pop(identity)
            index = position - bisect_left(removed, position)
            command = f"remove {index + 1}"
            insort(removed, position)
            # BRIO compacts the removal and appends one empty cache slot.
            next_position += 1
        else:
            if identity in positions:
                raise ValueError(f"VRP addition already exists: {identity}")
            positions[identity] = next_position
            next_position += 1
            command = f"add {identity[0]} {identity[1]} {identity[2]}"
        rewritten.append(dict(operation, command=command))
    return rewritten


# Extract one retained batch's state definitions without copying measured observations.
def monitoring_plan(reference_path, reference, number, operations, occurrences):
    churn = reference["churn"]
    task = next(task for task in churn["batch_monitoring"]["tasks"] if task["batch"] == number)
    counts = next(row for row in churn["target_selection"]["by_batch"] if row["batch"] == number)
    targets = {
        component: [
            {key: target[key] for key in ("key", "prefix", "before", "after", "peers")}
            for target in task[component]["targets"]
        ]
        for component in ("rov", "rib", "fib")
    }
    totals = {component: counts[component] for component in ("rov", "rib", "fib")}
    return dict(
        reference_result=str(reference_path), batch=number, targets=targets,
        vrp_identities=[row["vrp"] for row in operations if occurrences[tuple(row["vrp"])] == 1],
        selection=dict(
            definition="Same retained targets as this batch in the referenced continuous experiment",
            totals=totals, by_batch=[{key: value for key, value in counts.items() if key != "vrp"}],
        ),
    )


# Write only the selected batch's existing prediction row into each control input.
def copy_predictions(source, destinations, filename):
    with Path(source).open() as stream:
        for line in stream:
            row = json.loads(line)
            directory = destinations.get(row["batch"])
            if directory is not None:
                (directory / filename).write_text(line.rstrip() + "\n")


# Materialize all selected before states without submitting their preceding updates.
def prepare(reference_path, output, *, routing_reference_path=None):
    reference_path = Path(reference_path).resolve(strict=True)
    reference = json.loads(reference_path.read_text())
    if reference["status"] != "success":
        raise ValueError("The continuous reference must be a successful run")
    if routing_reference_path is not None:
        routing_reference_path = Path(routing_reference_path).resolve(strict=True)
        routing_reference = json.loads(routing_reference_path.read_text())
        if routing_reference["status"] != "success":
            raise ValueError("The routing reference must be a successful run")
        if routing_reference["scenario"] != reference["scenario"]:
            raise ValueError("Split references must use the same scenario")
        routing_churn = routing_reference["churn"]
        churn = reference["churn"]
        if routing_churn["batch_monitoring"]["selected_batches"] != churn["batch_monitoring"]["selected_batches"]:
            raise ValueError("Split references must monitor the same batches")
        routing_tasks = {task["batch"]: task for task in routing_churn["batch_monitoring"]["tasks"]}
        routing_counts = {row["batch"]: row for row in routing_churn["target_selection"]["by_batch"]}
        for task in churn["batch_monitoring"]["tasks"]:
            for component in ("rib", "fib"):
                task[component] = routing_tasks[task["batch"]][component]
        for row in churn["target_selection"]["by_batch"]:
            for component in ("rib", "fib"):
                row[component] = routing_counts[row["batch"]][component]
    _, scenario = load_scenario(reference["scenario"])
    _, batches, operations = load_sequence(scenario["sequence"])
    selected = reference["churn"]["batch_monitoring"]["selected_batches"]
    occurrences = Counter(tuple(row["vrp"]) for rows in operations.values() for row in rows)
    active = dict.fromkeys(load_vrp_text(scenario["vrp"]))
    destinations = {}
    controls = []
    for batch in batches:
        number = batch["number"]
        if number in selected:
            directory = output / f"batch-{number:03d}"
            directory.mkdir(parents=True, exist_ok=False)
            sequence = directory / "sequence"
            sequence.mkdir()
            with (directory / "initial.vrps").open("x") as stream:
                for prefix, maximum, asn in active:
                    stream.write(f"{prefix} {maximum} {asn}\n")

            rewritten = reindex_operations(active, operations[number])
            metadata = dict(batch, script="update.script", through_operation=len(rewritten))
            (sequence / "sequence.json").write_text(json.dumps(dict(
                format="vrp-batches-v1", initial="../initial.vrps", initial_count=len(active),
                batches=[metadata],
            ), indent=2) + "\n")
            with (sequence / "events.jsonl").open("x") as stream:
                for row in rewritten:
                    stream.write(json.dumps(row, separators=(",", ":")) + "\n")
            (sequence / "update.script").write_text("\n".join(row["command"] for row in rewritten) + "\nnotify\n")
            control = dict(
                format="exp65-scenario-v1", bgp=scenario["bgp"], vrp="initial.vrps",
                sequence="sequence", predictions="sequence/predictions.jsonl",
                path_predictions="path-predictions.jsonl", size=len(active),
                operations=len(rewritten), batch_size=scenario["batch_size"],
                batch_count=1, duration_seconds=1, batch_interval_seconds=1,
                policy=scenario["policy"], generation_complete=True,
            )
            (directory / "scenario.json").write_text(json.dumps(control, indent=2) + "\n")
            plan = monitoring_plan(reference_path, reference, number, operations[number], occurrences)
            if routing_reference_path is not None:
                plan["routing_reference_result"] = str(routing_reference_path)
            (directory / "monitoring-plan.json").write_text(json.dumps(plan, indent=2) + "\n")
            destinations[number] = directory
            controls.append(dict(batch=number, initial_vrps=len(active), operations=len(rewritten),
                                 targets={name: len(rows) for name, rows in plan["targets"].items()}))
            print(f"Prepared batch {number}: {len(active)} initial VRPs; targets {controls[-1]['targets']}", flush=True)

        for row in operations[number]:
            identity = tuple(row["vrp"])
            if row["operation"] == "withdraw":
                del active[identity]
            else:
                if identity in active:
                    raise ValueError(f"Duplicate VRP in replayed state: {identity}")
                active[identity] = None

    copy_predictions(scenario["predictions"], destinations, "sequence/predictions.jsonl")
    copy_predictions(scenario["path_predictions"], destinations, "path-predictions.jsonl")
    return controls
