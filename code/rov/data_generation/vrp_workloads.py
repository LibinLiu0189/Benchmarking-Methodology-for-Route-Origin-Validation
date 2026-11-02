"""Prepare shared VRP workloads with complete candidate-state transitions."""

from collections import Counter
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
from rov.prepare.vrp.generate_rov_batches import (
    apply_edit,
    choose_edit,
    read_candidates,
    validate_inputs,
    write_event,
)
from rov.prepare.vrp.workload import describe_input


# Draw complete edits while keeping each VRP identity unique within its notification batch.
def _write_sequence(directory, initial_path, initial, candidates, groups, seed,
                    batch_count, batch_target):
    directory.mkdir()
    index = VRPs(initial)
    prefix_index = Prefixes(groups)
    networks = {prefix: ip.ip_network(prefix) for prefix in groups}
    states = [
        index.state(networks[prefix], origin)[0]
        for prefix, _, origin in candidates
    ]
    cache = list(initial)
    rng = random.Random(seed)
    batches = []
    operation_number = 0
    rejected_edits = 0

    with (directory / "events.jsonl").open("x") as events, (
        directory / "predictions.jsonl"
    ).open("x") as predictions:
        for batch_number in range(1, batch_count + 1):
            commands = []
            changed_candidates = {}
            counts = Counter()
            touched = set()

            while len(commands) < batch_target:
                for _ in range(10000):
                    candidate_id = rng.randrange(len(candidates))
                    candidate = candidates[candidate_id]
                    before = states[candidate_id]
                    _, edits = choose_edit(index, candidate, before, rng)
                    identities = [tuple(vrp) for _, vrp in edits]
                    if (len(set(identities)) == len(identities)
                            and not touched.intersection(identities)):
                        break
                    rejected_edits += 1
                else:
                    raise ValueError(
                        "Unable to select a complete edit with unique VRP identities"
                    )

                _, operation_changes = apply_edit(
                    index, prefix_index, candidates, groups, states, edits
                )
                if states[candidate_id] == before:
                    raise ValueError("Selected candidate did not change ROV state")

                for (operation, vrp), step_changes in zip(
                    edits, operation_changes, strict=True
                ):
                    operation_number += 1
                    commands.append(write_event(
                        events, cache, operation_number, batch_number,
                        operation, vrp, step_changes,
                    ))
                    counts[operation] += 1
                    touched.add(tuple(vrp))
                    for change in step_changes:
                        key = (change["prefix"], change["peer"])
                        if key not in changed_candidates:
                            changed_candidates[key] = dict(
                                prefix=change["prefix"],
                                peer=change["peer"],
                                origin_as=change["origin_as"],
                                before=change["before"],
                                after=change["after"],
                            )
                        else:
                            changed_candidates[key]["after"] = change["after"]

            script = f"{batch_number:04}.script"
            (directory / script).write_text("\n".join(commands + ["notify"]) + "\n")
            batches.append(dict(
                number=batch_number,
                script=script,
                total=len(commands),
                added=counts["add"],
                withdrawn=counts["withdraw"],
                through_operation=operation_number,
            ))
            net_changes = [
                change for change in changed_candidates.values()
                if change["before"] != change["after"]
            ]
            predictions.write(json.dumps(dict(
                batch=batch_number,
                vrp_operations=len(commands),
                changes=net_changes,
            )) + "\n")
            print(
                f"Batch {batch_number}: {len(commands)} VRP operations "
                f"({counts['add']} additions, {counts['withdraw']} withdrawals)",
                flush=True,
            )

    sequence = dict(
        format="vrp-batches-v1",
        initial=os.path.relpath(initial_path, directory),
        initial_count=len(initial),
        seed=seed,
        batches=batches,
        operation_count=operation_number,
        batch_size=batches[0]["total"] if batch_count == 1 else batch_target,
        unique_vrp_identity_per_batch=True,
        rejected_in_batch_repeat_edits=rejected_edits,
        modify_definition="Withdrawal plus addition count as two real operations",
        truncated_logical_edit=False,
        brio_assumption=(
            "Untouched cache in initial file order, ordered replay before "
            "3600-second withdrawal expiry"
        ),
    )
    (directory / "sequence.json").write_text(json.dumps(sequence, indent=2) + "\n")
    return sequence


# Generate one input with independent initial VRPs and a complete shared update sequence.
def _generate(bgp, source, output, size, seed, batch_count, batch_target,
              batch_interval_seconds):
    bgp = Path(bgp).resolve(strict=True)
    source = Path(source).resolve(strict=True)
    output = Path(output).absolute()
    if output.exists() or output.is_symlink():
        raise FileExistsError(f"Output already exists: {output}")
    if size <= 0 or batch_count <= 0 or batch_target <= 0 or batch_interval_seconds <= 0:
        raise ValueError("Size, batch count, batch target, and interval must be positive")

    source_rows = load_vrp_text(source)
    if size > len(source_rows):
        raise ValueError("Initial VRP size exceeds the source")
    positions = sorted(random.Random(seed).sample(range(len(source_rows)), size))
    initial = [source_rows[position] for position in positions]
    candidates, groups = read_candidates(bgp)
    validate_inputs(initial, candidates)
    description = describe_input(initial, candidates, groups, VRPs(initial))

    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=output.parent, prefix=".vrp-workload-") as temporary:
        stage = Path(temporary) / "input"
        stage.mkdir()
        initial_path = stage / "initial.vrps"
        initial_path.write_text(
            "".join(f"{prefix} {maximum} {asn}\n" for prefix, maximum, asn in initial)
        )
        sequence = _write_sequence(
            stage / "sequence", initial_path, initial, candidates, groups,
            seed, batch_count, batch_target,
        )
        predict_paths(
            bgp, initial_path, stage / "sequence", stage / "path-predictions.jsonl"
        )

        counts = Counter()
        for batch in sequence["batches"]:
            counts["add"] += batch["added"]
            counts["withdraw"] += batch["withdrawn"]
        scenario = dict(
            format="exp65-scenario-v1",
            bgp=os.path.relpath(bgp, output),
            vrp="initial.vrps",
            source_vrp=os.path.relpath(source, output),
            sequence="sequence",
            predictions="sequence/predictions.jsonl",
            path_predictions="path-predictions.jsonl",
            seed=seed,
            sampling="Independent uniform VRP sample without replacement; source order retained",
            update_sampling=(
                "Uniform random BGP candidate record; complete ROV transitions; "
                "unique VRP identities within each batch"
            ),
            update_mode="incremental",
            size=size,
            operations=sequence["operation_count"],
            batch_size=sequence["batch_size"],
            operation_counts=dict(counts),
            batch_count=batch_count,
            duration_seconds=batch_count * batch_interval_seconds,
            batch_interval_seconds=batch_interval_seconds,
            notification_batch_rule=(
                "Complete candidate transitions to the operation target; "
                "each VRP identity appears once per batch"
            ),
            policy=dict(rov="on", dut_asn=65000, as_sets="reject"),
            input_statistics=description,
            generation_complete=True,
        )
        (stage / "scenario.json").write_text(json.dumps(scenario, indent=2) + "\n")
        if output.exists() or output.is_symlink():
            raise FileExistsError(f"Output appeared during generation: {output}")
        stage.rename(output)

    return scenario


# Generate one batch and finish the edit that reaches its operation target.
def generate_single_batch(bgp, source, output, size, seed, operations=100):
    return _generate(bgp, source, output, size, seed, 1, operations, 1.0)


# Generate every batch to its minimum target, finishing the final ROV edit.
def generate_multi_batch(bgp, source, output, size, seed, batch_count=300,
                         batch_target=100, batch_interval_seconds=1.0):
    return _generate(
        bgp, source, output, size, seed, batch_count, batch_target,
        batch_interval_seconds,
    )


# Copy only the first requested batches and adjust their existing input metadata.
def slice_vrp_workload(source, output, batch_count):
    source = Path(source).resolve(strict=True)
    output = Path(output).absolute()
    if output.exists() or output.is_symlink():
        raise FileExistsError(f"Output already exists: {output}")
    scenario = json.loads((source / "scenario.json").read_text())
    sequence = json.loads((source / "sequence" / "sequence.json").read_text())
    if not 0 < batch_count <= len(sequence["batches"]):
        raise ValueError("Requested prefix of batches is outside the input sequence")

    selected = sequence["batches"][:batch_count]
    sequence["batches"] = selected
    sequence["operation_count"] = selected[-1]["through_operation"]
    sequence.pop("rejected_in_batch_repeat_edits", None)
    scenario["batch_count"] = batch_count
    scenario["duration_seconds"] = batch_count * scenario["batch_interval_seconds"]
    scenario["operations"] = sequence["operation_count"]
    scenario["operation_counts"] = dict(
        add=sum(batch["added"] for batch in selected),
        withdraw=sum(batch["withdrawn"] for batch in selected),
    )
    scenario.pop("rejected_in_batch_repeat_edits", None)
    for key in ("bgp", "source_vrp"):
        if key in scenario:
            scenario[key] = os.path.relpath((source / scenario[key]).resolve(), output)

    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=output.parent, prefix=".vrp-slice-") as temporary:
        stage = Path(temporary) / "input"
        stage.mkdir()
        shutil.copy2(source / "initial.vrps", stage / "initial.vrps")
        subset = stage / "sequence"
        subset.mkdir()
        for batch in selected:
            shutil.copy2(source / "sequence" / batch["script"], subset / batch["script"])

        for relative in (
            "sequence/events.jsonl",
            "sequence/predictions.jsonl",
            "path-predictions.jsonl",
        ):
            with (source / relative).open() as source_stream, (stage / relative).open("w") as output_stream:
                for line in source_stream:
                    if not line.strip():
                        continue
                    row = json.loads(line)
                    if row["batch"] > batch_count:
                        break
                    output_stream.write(line)

        (subset / "sequence.json").write_text(json.dumps(sequence, indent=2) + "\n")
        (stage / "scenario.json").write_text(json.dumps(scenario, indent=2) + "\n")
        stage.rename(output)

    return scenario
