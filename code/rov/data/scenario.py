"""Bind scenario paths and validate sequence records against their stored scripts."""

import json
from pathlib import Path
from rov.data.sequence import load_sequence

# Read a bound scenario before any service is started.
def load_bound_scenario(path, expected_format):
    path = Path(path).resolve(strict=True)
    scenario = json.loads(path.read_text())
    if scenario.get("format") != expected_format:
        raise ValueError(f"Expected {expected_format}")
    for key in ("bgp", "vrp", "sequence", "predictions"):
        scenario[key] = str((path.parent / scenario[key]).resolve(strict=True))
    if not (Path(scenario["bgp"]) / "bgp.json").is_file():
        raise ValueError("Missing bound BGP metadata")
    directory, batches, operations = load_sequence(scenario["sequence"])
    if not batches or len({row["number"] for row in batches}) != len(batches):
        raise ValueError("Expected nonempty unique batch identities")
    if (directory / json.loads((directory / "sequence.json").read_text())["initial"]).resolve() != Path(scenario["vrp"]):
        raise ValueError("Sequence and scenario bind different initial VRP files")
    for batch in batches:
        rows = operations[batch["number"]]
        if len(rows) != batch["total"] or (directory / batch["script"]).read_text().splitlines() != [row["command"] for row in rows] + ["notify"]:
            raise ValueError("Sequence scripts and operation records disagree")
    predictions = [json.loads(line) for line in Path(scenario["predictions"]).read_text().splitlines() if line.strip()]
    if [row["batch"] for row in predictions] != [row["number"] for row in batches]:
        raise ValueError("Prediction batch identities disagree with the sequence")
    return path, scenario, batches, operations
