"""Prepare the four shared input groups under data/."""

import argparse

from rov.paths import ROOT
from rov.data_generation.bgp_workloads import generate_bgp_workloads
from rov.data_generation.prefix_workloads import generate as generate_prefix_workloads
from rov.data_generation.vrp_workloads import (
    generate_multi_batch,
    slice_vrp_workload,
)


DATA_ROOT = ROOT / "data"
# Generate the 300-batch input and the exact first-60-batch reuse input.
def generate_multi_group(bgp, vrp, inputs):
    for name, target in (("500k-300-batches", 100), ("batch500", 500), ("batch1000", 1000)):
        output = inputs / "vrp-batch" / name
        if output.exists():
            print(f"Input already exists: {output}", flush=True)
        else:
            generate_multi_batch(bgp, vrp, output, 500_000, 650001, batch_target=target)
    first_sixty = inputs / "vrp-batch" / "500k-first60"
    if first_sixty.exists():
        print(f"Input already exists: {first_sixty}", flush=True)
    else:
        slice_vrp_workload(inputs / "vrp-batch" / "500k-300-batches", first_sixty, 60)


# Generate localized and distributed inputs at each requested prefix count.
def generate_prefix_group(bgp, vrp, inputs):
    output = inputs / "vrp-prefix"
    if output.exists():
        print(f"Input already exists: {output}", flush=True)
    else:
        generate_prefix_workloads(bgp, vrp, output, (100, 500, 1000), 620001)


# Generate the mixed input and five single-operation-type inputs.
def generate_bgp_group(bgp, vrp, inputs, operations):
    output = inputs / "bgp"
    if output.exists():
        print(f"Input already exists: {output}", flush=True)
    else:
        generate_bgp_workloads(bgp, vrp, output, 630001, operations=operations)


# Run one current input preparation group without launching an experiment.
def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("group", choices=("multi", "prefix", "bgp", "statistics"))
    parser.add_argument(
        "--operations", type=int, default=1000,
        help="BGP updates per workload (default: 1000)",
    )
    args = parser.parse_args()
    if args.operations <= 0:
        parser.error("--operations must be positive")
    if args.group != "bgp" and args.operations != 1000:
        parser.error("--operations is only valid for the bgp group")

    bgp = DATA_ROOT / "baseline" / "bgp"
    vrp = DATA_ROOT / "baseline" / "balanced.vrps"
    if not (bgp / "bgp.json").is_file() or not vrp.is_file():
        raise SystemExit("The BGP and balanced VRP baseline files under data/baseline are missing")

    inputs = DATA_ROOT / "inputs"
    if args.group == "multi":
        generate_multi_group(bgp, vrp, inputs)
    elif args.group == "prefix":
        generate_prefix_group(bgp, vrp, inputs)
    elif args.group == "bgp":
        generate_bgp_group(bgp, vrp, inputs, args.operations)
    else:
        from rov.data_generation.statistics import write_statistics

        write_statistics(DATA_ROOT)


if __name__ == "__main__":
    main()
