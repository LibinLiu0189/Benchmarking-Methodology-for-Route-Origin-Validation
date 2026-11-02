"""Validate experiment 67's currently supported input contract."""

from rov.data.scenario import load_bound_scenario
from rov.data.predictions import load_rov_targets, load_rib_targets, load_fib_targets
from pathlib import Path


# Preserve the existing saved format and explicitly enforce this experiment's rules.
def load_scenario(path):
    path, scenario, batches, operations = load_bound_scenario(path, "exp65-scenario-v1")
    if scenario.get("policy") != dict(rov="on", dut_asn=65000, as_sets="reject"):
        raise ValueError("Scenario must bind the agreed ROV on, ASN 65000, AS-set reject policy")
    scenario["path_predictions"] = str((path.parent / scenario["path_predictions"]).resolve(strict=True))
    for batch in batches:
        if batch["total"] != scenario["batch_size"]:
            raise ValueError("Every workload batch must have the stated fixed operation count")
        rows = operations[batch["number"]]
        if len({tuple(row["vrp"]) for row in rows}) != len(rows):
            raise ValueError("Repeated VRP identity within notification batch can cancel before RTR delivery")
    if not scenario.get("generation_complete"):
        raise ValueError("Workload path predictions have not completed")
    directory = Path(scenario["sequence"])
    targets, _ = load_rov_targets(directory, batches, Path(scenario["predictions"]))
    if not targets:
        raise ValueError("Workload has no observable ROA query targets")
    load_rib_targets(scenario["path_predictions"], batches)
    load_fib_targets(scenario["path_predictions"], batches)
    return path, scenario
