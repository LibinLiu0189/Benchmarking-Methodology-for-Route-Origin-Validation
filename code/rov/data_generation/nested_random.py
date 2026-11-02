"""Generate random VRP updates over an existing group of nested initial tables."""

import argparse
import csv
import json
from pathlib import Path

from rov.data_generation.statistics import FIELDS, _vrp_row
from rov.data_generation.vrp_workloads import generate_single_batch


SCALES = (("50k", 50_000), ("100k", 100_000),
          ("500k", 500_000), ("1m", 1_000_000))


# Reuse each nested baseline and generate unrestricted random candidate transitions.
def generate(bgp, initial_directory, output, operations, seed):
    bgp = Path(bgp).resolve(strict=True)
    initial_directory = Path(initial_directory).resolve(strict=True)
    output = Path(output).resolve()
    if output.exists():
        raise FileExistsError(f"Output already exists: {output}")
    if operations <= 0:
        raise ValueError("The VRP operation target must be positive")
    for label, _ in SCALES:
        initial = initial_directory / label / "initial.vrps"
        if not initial.is_file():
            raise FileNotFoundError(initial)

    rows = []
    for scale_number, (label, size) in enumerate(SCALES, 1):
        print(f"Generating {label}: target {operations} random VRP operations", flush=True)
        directory = output / label
        scenario = generate_single_batch(
            bgp, initial_directory / label / "initial.vrps", directory,
            size, seed + scale_number, operations=operations,
        )
        scenario["sampling"] = "Reuse supplied nested initial VRP tables; source order retained"
        (directory / "scenario.json").write_text(json.dumps(scenario, indent=2) + "\n")

        row = _vrp_row(output, directory, "vrp-single", operations)
        row["initial_bgp_candidates"] = sum(
            scenario["input_statistics"]["bgp_candidates_afi"].values()
        )
        rows.append(row)

    with (output / "statistics.csv").open("x", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    print(f"Statistics: {output / 'statistics.csv'}", flush=True)


# Accept existing nested baselines and the requested random single-batch workload.
def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bgp", type=Path, required=True, help="Prepared BGP directory")
    parser.add_argument(
        "--initial-directory", type=Path, required=True,
        help="Existing nested 50k, 100k, 500k, and 1m directories containing initial.vrps",
    )
    parser.add_argument("--output", type=Path, required=True, help="New input group directory")
    parser.add_argument(
        "--operations", type=int, default=1000,
        help="Minimum VRP operations per batch; finish the last candidate transition",
    )
    parser.add_argument("--seed", type=int, default=610001, help="Update sampling seed")
    args = parser.parse_args()
    generate(args.bgp, args.initial_directory, args.output, args.operations, args.seed)


if __name__ == "__main__":
    main()
