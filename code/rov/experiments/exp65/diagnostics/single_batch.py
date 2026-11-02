"""Measure one isolated churn batch from its reconstructed stable before state."""

import argparse
import json
import os
from pathlib import Path
import signal
import sys

from rov.dut.managed import stop_requested
from rov.dut.output import MANAGED_OUTPUT, supervise
from rov.dut.virtualize import enter_namespace
from rov.experiments.exp65.__main__ import run_managed
from rov.paths import ROOT


# Enter the ordinary experiment lifecycle with one frozen monitoring plan.
def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("scenario", type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--monitoring-mode", nargs="+", choices=("vrp", "rov", "rib", "fib"),
                        default=["vrp", "rov"],
                        help="Components to monitor; VRP supplies the installation origin")
    parser.add_argument("--roa-settle", action=argparse.BooleanOptionalAction, default=None,
                        help="Use default ROA table settle timers; enabled for ROV monitoring unless explicitly disabled")
    parser.add_argument("--inside", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    args.scenario = args.scenario.resolve(strict=True)
    args.output = args.output.absolute()
    args.bin_dir = ROOT / "software/bin-brio-first-vrp"
    args.query_mode = "pipeline"
    args.query_group_size = 8
    args.timeout = 1000
    args.poll_interval = 0
    args.steady_seconds = 0
    args.pps = 10000
    args.completion_check_interval = 5
    args.monitor_every = 1
    args.packet_timeout = 1
    if not os.environ.get(MANAGED_OUTPUT):
        args.output.mkdir(parents=True, exist_ok=False)
        with (args.output / "run.log").open("xb") as log:
            return supervise(__spec__.name, sys.argv[1:], log=log)
    if not args.inside:
        enter_namespace(sys.argv[1:], module=__spec__.name)
    plan = json.loads((args.scenario.parent / "monitoring-plan.json").read_text())
    if "rov" not in args.monitoring_mode and "routing_reference_result" in plan:
        plan["reference_result"] = plan["routing_reference_result"]
    roa_settle = args.roa_settle
    if roa_settle is None:
        roa_settle = "rov" in args.monitoring_mode
    return run_managed(args, monitoring_plan=plan, roa_settle=roa_settle)


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, stop_requested)
    signal.signal(signal.SIGINT, stop_requested)
    raise SystemExit(main())
