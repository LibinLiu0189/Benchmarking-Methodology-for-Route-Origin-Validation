"""Run continuous Section 6.5 updates and monitor single-transition targets."""

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
from rov.experiments.exp65.runtime.measure import run as run_churn
from rov.dut.managed import check_initialization
from rov.dut.managed import wait_for_initialization
from rov.dut.managed import stop_initialization
from rov.dut.managed import stop_requested
from rov.experiments.exp65.inputs.scenario import load_scenario
from rov.experiments.exp65.runtime.brio_timing import attach as attach_brio_timing
from rov.data.sequence import load_sequence
from rov.dut.managed import forward_sync_output as forward_output
from rov.dut.managed import capture_environment
from rov.dut.output import MANAGED_OUTPUT
from rov.dut.output import supervise
from rov.dut.virtualize import enter_namespace


# Collect one clean run, preserving files and cleanup status on every failure.
def run_managed(args, *, monitoring_plan=None, roa_settle=False):
    path, scenario = load_scenario(args.scenario)
    result = dict(format="exp65-result-v1", scenario=str(path), input=scenario, status="failed",
                  time_reference="Performance metrics: FIFO write start to external observation; *_from_brio_ms: optional diagnostic intervals",
                  validation_definition="ROA query result observation; not internal BGP revalidation completion",
                  resource_definition="Not collected",
                  process_scope="Combined BIRD routing process; separate ROV/BGP threads unavailable",
                  hostname=platform.node(), platform=platform.platform(), cpu_count=os.cpu_count(),
                  poll_interval=args.poll_interval, timeout=args.timeout, steady_seconds=args.steady_seconds,
                  errors=[], baseline_ready=False, cleanup_complete=None)
    result.update(rate=scenario["batch_size"] / scenario["batch_interval_seconds"],
                  rate_definition="Minimum target operations per batch divided by fixed batch interval; actual per-batch counts and FIFO timestamps retained, not RTR wire rate",
                  pps=args.pps, packet_timeout=args.packet_timeout,
                  query_mode=args.query_mode, query_group_size=args.query_group_size,
                  brio_timing_method="fifo-control-prefix-send-monotonic-v1",
                  bin_dir=str(Path(args.bin_dir).resolve()),
                  completion_check_interval=args.completion_check_interval,
                  monitor_every=args.monitor_every,
                  monitoring_mode=args.monitoring_mode,
                  measurement_version="single-transition-continuous-v2",
                  pps_definition="Global FIB packet send rate cap across selected destinations",
                  forwarding_scope="Retained single-transition target UDP forwarding")
    if monitoring_plan is not None:
        result["measurement_version"] = monitoring_plan.get("measurement_version", "single-batch-control-v1")
        result["control_reference"] = monitoring_plan["reference_result"]
        result["control_batch"] = monitoring_plan["batch"]
        result["monitor_every"] = None
    if "rov" not in args.monitoring_mode:
        result["validation_definition"] = "ROV queries disabled"
    if "fib" not in args.monitoring_mode:
        result["forwarding_scope"] = "Not monitored"
    output = Path(args.output)
    trace = output / "bird-trace.log" if {"vrp", "rib"}.intersection(args.monitoring_mode) else None
    brio_trace = output / "brio-timing.jsonl"
    with brio_trace.open("x"):
        pass
    if trace is not None:
        with trace.open("xb"):
            pass
    process = thread = None
    messages = queue.Queue()
    command = [sys.executable, "-u", "-m", "rov.dut.initialize", "--inside",
               "--bgp", scenario["bgp"], "--vrp", scenario["vrp"], "--rov", "on",
               "--dut-asn", "65000", "--as-sets", "reject", "--timeout", str(args.timeout)]
    if not roa_settle:
        command.append("--no-roa-settle")
    if trace is not None:
        command += ["--bird-trace-log", str(trace)]
    if "rib" in args.monitoring_mode:
        command.append("--bird-trace-info")
    if args.bin_dir:
        command += ["--bin-dir", str(Path(args.bin_dir).resolve())]

    # Check only this runner's owned initialization process.
    def check_alive():
        check_initialization(process, messages)

    try:
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                   env=dict(os.environ, **{MANAGED_OUTPUT: "1", "BRIO_VRP_TIMING_LOG": str(brio_trace)}))
        thread = threading.Thread(target=forward_output, args=(process, messages, result))
        thread.start()
        wait_for_initialization(process, messages)
        result["baseline_ready"] = True
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
                scenario["batch_interval_seconds"], args.poll_interval, args.timeout,
                CONFIG, args.pps, args.packet_timeout,
                log_path=trace,
                result=result["churn"], raw_output=events, check_alive=check_alive,
                completion_check_interval=args.completion_check_interval,
                monitor_every=args.monitor_every,
                query_mode=args.query_mode, query_group_size=args.query_group_size,
                brio_timing_log=brio_trace, monitoring_mode=args.monitoring_mode,
                monitoring_plan=monitoring_plan,
            )
        if result["churn"]["status"] != "success":
            raise RuntimeError("Churn measurement or final verification failed")
        check_alive()
        result["status"] = "success"
    except BaseException as error:
        result["errors"].append(f"{type(error).__name__}: {error}")
        result["status"] = "interrupted" if isinstance(error, KeyboardInterrupt) else "failed"
    finally:
        if process is not None:
            try:
                result["cleanup_complete"] = stop_initialization(process, thread) == 0
            except BaseException as error:
                result["cleanup_complete"] = False
                result["errors"].append(f"Cleanup: {error}")
        else:
            result["cleanup_complete"] = True
        if not result["cleanup_complete"]:
            result["status"] = "failed"
        # Read completed source records after cleanup; never delay batch submission.
        if "churn" in result:
            try:
                _, _, operations = load_sequence(scenario["sequence"])
                attach_brio_timing(result["churn"], operations, brio_trace, args.monitoring_mode)
            except (OSError, ValueError, KeyError, TypeError) as error:
                result["churn"]["brio_timing"] = {
                    "issue": f"BRIO timing unavailable: {error}"
                }
                result["errors"].append(f"BRIO timing: {error}")
                result["status"] = "failed"
        result["resources"] = dict(enabled=False)
        result["unavailable"] = dict(internal_queue="Only submitted-minus-observed proxy is available",
                                     wire_rate="FIFO cadence and eligible first Prefix PDU starts; not full RTR wire throughput",
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
    parser.add_argument("--monitoring-mode", nargs="+", choices=("vrp", "rov", "rib", "fib"),
                        default=["vrp", "rov"],
                        help="Measured components: vrp rov, or vrp rib fib")
    parser.add_argument("--roa-settle", action=argparse.BooleanOptionalAction, default=None,
                        help="Use default ROA table settle timers; enabled for ROV monitoring unless explicitly disabled")
    parser.add_argument("--timeout", type=float, default=1000,
                        help="Total update/monitoring deadline from workload start; also bounds initialization and CLI operations")
    parser.add_argument("--poll-interval", type=float, default=0,
                        help="ROV pause after each complete query round in seconds (default: 0)")
    parser.add_argument("--steady-seconds", type=float, default=0,
                        help="Wait this many seconds after initialization before monitoring updates (default: 0)")
    parser.add_argument("--pps", type=float, default=10000,
                        help="Global FIB UDP packet send rate cap (packets/s; default: 10000)")
    parser.add_argument("--completion-check-interval", type=float, default=5,
                        help="Inspect saved observations and retire completed targets every N seconds (default: 5)")
    parser.add_argument("--monitor-every", type=int, default=10,
                        help="Monitor ROV/RIB/FIB for multiples of N; VRP logs remain continuous (default: 10)")
    parser.add_argument("--packet-timeout", type=float, default=1,
                        help="Receive cutoff per probe, evaluated from saved records; never blocks sending")
    parser.add_argument("--inside", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    args.query_mode = "pipeline"
    args.query_group_size = 8
    args.bin_dir = args.bin_dir.resolve()
    binary = args.bin_dir / "brio_rc"
    if (not binary.is_file() or any(marker not in binary.read_bytes() for marker in
                                   (b"brio-control-send-v1", b"brio-first-vrp-v1"))):
        parser.error("BRIO control and Prefix-PDU send timing is required; rebuild with software/build_brio_disconnect.sh")
    args.monitoring_mode = [name for name in ("vrp", "rov", "rib", "fib")
                            if name in args.monitoring_mode]
    if args.monitor_every < 1:
        parser.error("Monitoring batch interval must be positive")
    load_scenario(args.scenario)
    if not (0 < args.timeout < float("inf") and
            0 <= args.poll_interval < float("inf") and
            0 <= args.steady_seconds < float("inf")):
        parser.error("Invalid finite measurement intervals")
    if not (1e-9 <= args.completion_check_interval < float("inf")
            and 0 < args.pps < float("inf")
            and 0 < args.packet_timeout < float("inf")):
        parser.error("Invalid rate or probe settings")
    args.output = args.output.absolute()
    if not os.environ.get(MANAGED_OUTPUT):
        args.output.mkdir(parents=True, exist_ok=False)
        with (args.output / "run.log").open("xb") as log:
            code = supervise("rov.experiments.exp65", sys.argv[1:], log=log)
        if not (args.output / "result.json").exists():
            (args.output / "result.json").write_text(json.dumps(dict(
                format="exp65-result-v1", status="failed", scenario=str(args.scenario),
                errors=["Managed child did not produce a result"], exit_code=code,
            ), indent=2) + "\n")
        return code
    if not args.inside:
        enter_namespace(sys.argv[1:], module="rov.experiments.exp65")
    roa_settle = args.roa_settle
    if roa_settle is None:
        roa_settle = "rov" in args.monitoring_mode
    return run_managed(args, roa_settle=roa_settle)


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, stop_requested)
    signal.signal(signal.SIGINT, stop_requested)
    raise SystemExit(main())
