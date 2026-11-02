"""Prepare the six shared BGP update inputs for Experiment 6.3B."""

from pathlib import Path
import tempfile

from rov.experiments.exp63b.generate import TYPES, generate


WORKLOADS = ("mixed", *TYPES)
BGP_OPERATIONS_PER_WORKLOAD = 1000


# Generate the mixed input and one input for each supported BGP operation type.
def generate_bgp_workloads(bgp, vrp, output, seed, operations=BGP_OPERATIONS_PER_WORKLOAD):
    bgp = Path(bgp).resolve(strict=True)
    vrp = Path(vrp).resolve(strict=True)
    output = Path(output).absolute()
    if output.exists() or output.is_symlink():
        raise FileExistsError(f"Output already exists: {output}")

    output.parent.mkdir(parents=True, exist_ok=True)
    reports = {}
    with tempfile.TemporaryDirectory(dir=output.parent, prefix=f".{output.name}-") as temporary:
        stage = Path(temporary)
        for name in WORKLOADS:
            reports[name] = generate(
                bgp=bgp,
                vrp=vrp,
                output=stage / name,
                prefix_count=operations,
                seed=seed,
                reference_rov="on",
                dut_asn=65000,
                as_sets="reject",
                operation_type=None if name == "mixed" else name,
            )
        if output.exists() or output.is_symlink():
            raise FileExistsError(f"Output appeared during generation: {output}")
        stage.rename(output)
    return reports
