"""Measure Section 6.6 resources using the existing managed churn and convergence observers."""

import argparse
import json
import os
from pathlib import Path
import platform
import queue
import signal
import subprocess
import sys
import threading
import time
from rov.paths import CONFIG, ROOT
from rov.experiments.exp61.brio_timing import read_control_records, read_records
from rov.experiments.exp66.runtime.measure import run as run_churn
from rov.dut.managed import check_initialization
from rov.dut.managed import wait_for_initialization
from rov.dut.managed import stop_initialization
from rov.dut.managed import stop_requested
from rov.measure.resource import ResourceRecorder
from rov.measure.resource import resource_summary
from rov.measure.resource import sync_boundary_summary
from rov.experiments.exp66.inputs.scenario import load_scenario
from rov.dut.managed import capture_environment
from rov.dut.output import MANAGED_OUTPUT
from rov.dut.output import supervise
from rov.dut.virtualize import enter_namespace
from rov.dut.managed import forward_prompt_sync_output as forward_output


# Collect one clean run, preserving files and cleanup status on every failure.
def run_managed(args):
    path, scenario = load_scenario(args.scenario)
    result = dict(format="exp66-result-v1", scenario=str(path), input=scenario, status="failed",
                  time_reference="FIFO write start to external observation; not RTR wire time",
                  full_sync_definition="BRIO first Prefix-PDU send to Established with exact initial IPv4/IPv6 VRP counts",
                  validation_definition="ROA query result observation; not internal BGP revalidation completion",
                  resource_definition="Linux/WSL system counters; BIRD CPU as all-CPU capacity share; memory bytes",
                  process_scope="Combined BIRD routing process; separate ROV/BGP threads unavailable",
                  hostname=platform.node(), platform=platform.platform(), cpu_count=os.cpu_count(),
                  resource_interval=args.resource_interval, quiet_seconds=args.quiet_seconds,
                  poll_interval=args.poll_interval, timeout=args.timeout, steady_seconds=args.steady_seconds,
                  errors=[], baseline_ready=False, cleanup_complete=None)
    result.update(rate=args.rate, pps=args.pps, packet_timeout=args.packet_timeout,
                  forwarding_scope="Sampled target UDP forwarding; not the entire kernel FIB")
    output = Path(args.output)
    trace = output / "bird-trace.log"
    brio_trace = output / "brio-timing.jsonl"
    with brio_trace.open("x"):
        pass
    with trace.open("xb"):
        pass
    resources = ResourceRecorder(args.resource_interval, CONFIG / "bird.pid")
    process = thread = None
    messages = queue.Queue()
    command = [sys.executable, "-u", "-m", "rov.dut.initialize", "--inside",
               "--bgp", scenario["bgp"], "--vrp", scenario["vrp"], "--rov", "on",
               "--dut-asn", "65000", "--as-sets", "reject", "--measure-full-sync",
               "--bird-trace-log", str(trace), "--bird-trace-info", "--timeout", str(args.timeout)]
    if args.bin_dir:
        command += ["--bin-dir", str(Path(args.bin_dir).resolve())]

    # Check only this runner's owned initialization process.
    def check_alive():
        check_initialization(process, messages)

    try:
        resources.start()
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                   env=dict(os.environ, **{MANAGED_OUTPUT: "1",
                                                         "BRIO_VRP_TIMING_LOG": str(brio_trace)}))
        thread = threading.Thread(target=forward_output, args=(process, messages, result))
        thread.start()
        wait_for_initialization(process, messages)
        result["baseline_ready"] = True
        if result.get("full_sync_ms") is None:
            raise RuntimeError("No initial Prefix-PDU synchronization timing was reported")
        capture_environment(output, args.timeout)
        result["steady_start_ns"] = time.monotonic_ns()
        until = time.monotonic() + args.steady_seconds
        while time.monotonic() < until:
            check_alive()
            time.sleep(min(0.1, max(0, until - time.monotonic())))
        result["steady_end_ns"] = time.monotonic_ns()
        result["churn"] = {}
        with (output / "events.jsonl").open("x") as events:
            run_churn(
                Path(scenario["sequence"]), Path(scenario["predictions"]),
                Path(scenario["path_predictions"]), CONFIG / "bird.ctl", CONFIG / "brio.in",
                scenario["batch_size"] / args.rate, args.poll_interval, args.timeout,
                CONFIG, args.pps, args.packet_timeout,
                args.quiet_seconds, log_path=trace,
                result=result["churn"], raw_output=events, check_alive=check_alive,
            )
        result["rate"] = args.rate
        result["rate_definition"] = "Target batch size divided by nominal submission interval; actual FIFO timestamps retained, not RTR wire rate"
        if result["churn"]["status"] != "success":
            raise RuntimeError("Churn measurement or final verification failed")
        check_alive()
        result["status"] = "success"
    except BaseException as error:
        result["errors"].append(f"{type(error).__name__}: {error}")
        result["status"] = "interrupted" if isinstance(error, KeyboardInterrupt) else "failed"
    finally:
        resources.close()
        result["resource_errors"] = resources.errors
        if process is not None:
            try:
                result["cleanup_complete"] = stop_initialization(process, thread) == 0
            except BaseException as error:
                result["cleanup_complete"] = False
                result["errors"].append(f"Cleanup: {error}")
        else:
            result["cleanup_complete"] = True
        if not result["cleanup_complete"] or resources.errors:
            result["status"] = "failed"
        try:
            result.setdefault("churn", {})["brio_timing"] = dict(
                source=str(brio_trace), clock="CLOCK_MONOTONIC",
                records=read_records(brio_trace), control_records=read_control_records(brio_trace),
                definition="Raw Notify sends and Prefix-PDU response starts; FIFO receipts remain in submissions; responses may span batches",
            )
        except (OSError, ValueError, KeyError, TypeError) as error:
            result["errors"].append(f"BRIO timing: {error}")
            result["status"] = "failed"
        result["resources"] = dict(samples=resources.samples, sync_boundary=sync_boundary_summary(result))
        churn = result.get("churn", {})
        submissions = churn.get("submissions", [])
        first = min((row["write_start_ns"] for row in submissions), default=None)
        last = churn.get("collection_end_ns")
        rows = [row for row in resources.samples if first is not None and last is not None
                and first <= row["observed_ns"] <= last]
        result["resources"]["churn_and_rov_convergence"] = resource_summary(rows)
        result["workload_scope"] = "Idle ROV, full sync, and mixed churn with ROV-triggered RIB/UDP convergence; churn and convergence overlap"
        result["threshold_scope"] = "Functional low-rate run; no saturation threshold search or capacity claim"
        result["observation_complete"] = (not churn.get("unobserved_vrp_events", 1)
                                          and not churn.get("unexpected_vrp_events", 1))
        for phase in ("sync", "steady"):
            first = result.get(f"{phase}_start_observed_ns" if phase == "sync" else "steady_start_ns")
            last = result.get(f"{phase}_end_observed_ns" if phase == "sync" else "steady_end_ns")
            rows = [row for row in resources.samples if first is not None and last is not None
                    and first <= row["observed_ns"] <= last]
            result["resources"][phase] = resource_summary(rows)
        result["unavailable"] = dict(internal_queue="Only submitted-minus-observed proxy is available",
                                     wire_rate="FIFO, Notify and response-start timestamps do not measure full RTR wire throughput",
                                     throttling="Scheduling lag is evidence, not proof of DUT throttling",
                                     internal_rov_latency="No internal instrumentation",
                                     separate_rov_process="ROV and BGP share BIRD",
                                     vrp_storage_bytes="No VRP-only memory counter")
        with (output / "result.json").open("x") as stream:
            json.dump(result, stream, indent=2)
            stream.write("\n")
    return 0 if result["status"] == "success" else 1


# Reserve a new output before entering an isolated namespace and owned services.
def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("scenario", type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--bin-dir", type=Path, default=ROOT / "software/bin-brio-first-vrp",
                        help="Executable directory with BRIO control and Prefix-PDU send timing")
    parser.add_argument("--quiet-seconds", type=float, default=10,
                        help="Seconds without new events before completion (default: 10)")
    parser.add_argument("--timeout", type=float, default=600)
    parser.add_argument("--poll-interval", type=float, default=0,
                        help="ROV pause after each complete query round in seconds (default: 0)")
    parser.add_argument("--resource-interval", type=float, default=0.1)
    parser.add_argument("--steady-seconds", type=float, default=30)
    parser.add_argument("--rate", type=float, default=100)
    parser.add_argument("--pps", type=float, default=10000,
                        help="Aggregate FIB probe rate in packets per second (default: 10000)")
    parser.add_argument("--packet-timeout", type=float, default=1)
    parser.add_argument("--inside", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    args.bin_dir = args.bin_dir.resolve()
    binary = args.bin_dir / "brio_rc"
    if (not binary.is_file() or any(marker not in binary.read_bytes() for marker in
                                   (b"brio-control-send-v1", b"brio-first-vrp-v1"))):
        parser.error("BRIO control and Prefix-PDU send timing is required; rebuild with software/build_brio_disconnect.sh")
    scenario_path, scenario = load_scenario(args.scenario)
    if not (0 < args.quiet_seconds < args.timeout < float("inf") and
            0.1 <= args.resource_interval <= 0.5 and 0 <= args.poll_interval < float("inf") and
            0 < args.steady_seconds < float("inf")):
        parser.error("Invalid finite measurement intervals")
    if not (0 < args.rate <= 100 and 0 < args.pps < float("inf") and 0 < args.packet_timeout < float("inf")):
        parser.error("Invalid rate or probe settings")
    args.output = args.output.absolute()
    if not os.environ.get(MANAGED_OUTPUT):
        args.output.mkdir(parents=True, exist_ok=False)
        with (args.output / "run.log").open("xb") as log:
            code = supervise("rov.experiments.exp66", sys.argv[1:], log=log)
        if not (args.output / "result.json").exists():
            (args.output / "result.json").write_text(json.dumps(dict(
                format="exp66-result-v1", status="failed", scenario=str(args.scenario),
                errors=["Managed child did not produce a result"], exit_code=code,
            ), indent=2) + "\n")
        return code
    if not args.inside:
        enter_namespace(sys.argv[1:], module="rov.experiments.exp66")
    return run_managed(args)


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, stop_requested)
    signal.signal(signal.SIGINT, stop_requested)
    raise SystemExit(main())
