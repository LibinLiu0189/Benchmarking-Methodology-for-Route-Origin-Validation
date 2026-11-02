"""Validate experiment 64's currently supported input contract."""

import json
from pathlib import Path

from rov.data.scenario import load_bound_scenario


# Accept the original input and a completed shared single-batch VRP input.
def load_scenario(path):
    path = Path(path).resolve(strict=True)
    format_name = json.loads(path.read_text()).get("format")
    if format_name not in ("exp64-scenario-v1", "exp65-scenario-v1"):
        raise ValueError("Expected an exp64 or shared VRP single-batch scenario")

    path, scenario, batches, operations = load_bound_scenario(path, format_name)
    if scenario.get("policy") != dict(rov="on", dut_asn=65000, as_sets="reject"):
        raise ValueError("Scenario must bind the agreed ROV on, ASN 65000, AS-set reject policy")
    if format_name == "exp65-scenario-v1":
        if (len(batches) != 1 or batches[0]["number"] != 1
                or scenario.get("batch_count") != 1):
            raise ValueError("Experiment 64 requires one shared VRP update batch")
        if scenario.get("operations") != batches[0]["total"]:
            raise ValueError("Shared VRP operation count differs from its batch")
        if not scenario.get("generation_complete"):
            raise ValueError("Shared VRP predictions are incomplete")
    return path, scenario
