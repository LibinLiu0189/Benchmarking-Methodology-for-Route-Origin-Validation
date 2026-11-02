"""Load VRP sequence metadata and per-batch operations."""

import json
from pathlib import Path


# Load ordered batch metadata and the expected VRP operations for each batch.
def load_sequence(directory):
    directory = Path(directory).resolve()
    manifest = json.loads((directory / "sequence.json").read_text())
    if manifest.get("format") != "vrp-batches-v1":
        raise ValueError("Expected a vrp-batches-v1 sequence")
    operations = {row["number"]: [] for row in manifest["batches"]}
    with (directory / "events.jsonl").open() as source:
        for line in source:
            event = json.loads(line)
            operations[event["batch"]].append(event)
    return directory, manifest["batches"], operations
