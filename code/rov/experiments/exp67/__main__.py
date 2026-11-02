"""Measure direct RTR session recovery or full reload on an owned baseline."""

import ipaddress as ip
import json
from pathlib import Path
import re
from rov.experiments.exp67.scenarios.full_reset import run as run_full_reset
from rov.experiments.exp67.faults.lab import FaultLab, FAULT_MODES
from rov.experiments.exp67.faults.recovery import fault_recovery
from rov.experiments.exp67.faults.transport import bird_rtr_sockets
from rov.experiments.exp61.brio_timing import (
    read_records, correlate_control_records,
)
from rov.data.sequence import load_sequence
from rov.data.vrp import load_vrp_text
from rov.measure.bird_connection import BirdConnection
from rov.paths import CONFIG, ROOT
import argparse
import os
import platform
import queue
import signal
import subprocess
import sys
import threading
import time
from rov.experiments.exp67.runtime.measure import run as run_churn
from rov.dut.managed import check_initialization
from rov.dut.managed import wait_for_initialization
from rov.dut.managed import stop_initialization
from rov.dut.managed import stop_requested
from rov.measure.resource import ResourceRecorder
from rov.measure.resource import resource_summary
from rov.measure.resource import sync_boundary_summary
from rov.experiments.exp67.inputs.scenario import load_scenario
from rov.dut.managed import capture_environment
from rov.dut.output import MANAGED_OUTPUT
from rov.dut.output import supervise
from rov.dut.virtualize import enter_namespace
from rov.dut.managed import forward_prompt_sync_output as forward_output


# Retain source-side sends independently of the scenario's fault or relay origin.
def source_timing(path, submission, response=None):
    responses = ([response] if response is not None else
                 [row for row in read_records(path)
                  if row['first_vrp_send_ns'] >= submission['write_start_ns']])
    response = responses[0] if len(responses) == 1 else None
    timing = dict(fifo_write_start_ns=submission['write_start_ns'],
                  fifo_submitted_ns=submission['submitted_ns'],
                  brio_first_vrp_send_ns=(response['first_vrp_send_ns']
                                          if response is not None else None),
                  brio_response_records=responses)
    timing.update(correlate_control_records(path, submission, response))
    return timing


# Compare the complete installed VRP identity set after timed collection has ended.
def verify_reload_table(scenario, timeout, protocol="rpki_source"):
    expected = set(load_vrp_text(scenario["vrp"]))
    _, batches, operations = load_sequence(scenario["sequence"])
    for batch in batches:
        for row in operations[batch["number"]]:
            key = tuple(row["vrp"])
            if row["operation"] == "add":
                expected.add(key)
            else:
                expected.remove(key)
    actual = set()
    connection = BirdConnection(CONFIG / "bird.ctl", timeout)
    try:
        connection.command("restrict")
        status = connection.command(f"show protocols all {protocol}")
        for afi in (4, 6):
            text = connection.command(f"show route table r{afi}")
            for line in text.splitlines():
                match = re.match(r"^(\S+/\d+)-(\d+) AS(\d+)\s", line)
                if match:
                    prefix, maximum, asn = match.groups()
                    actual.add((str(ip.ip_network(prefix)), int(maximum), int(asn)))
    finally:
        connection.close()
    missing, extra = expected-actual, actual-expected
    return dict(correct=not missing and not extra and "Established" in status,
                expected=len(expected), actual=len(actual), missing=len(missing), extra=len(extra),
                missing_examples=sorted(missing)[:10], extra_examples=sorted(extra)[:10],
                scope="All installed VRP identities after collection; selected ROA/RIB checks remain separate",
                rtr_status=status)


# Collect one clean run, preserving files and cleanup status on every failure.
def run_managed(args):
    path, scenario = load_scenario(args.scenario)
    result = dict(format="exp67-result-v1", scenario=str(path), input=scenario, status="failed",
                  time_reference="FIFO write start to external observation; not RTR wire time",
                  validation_definition="ROA query result observation; not internal BGP revalidation completion",
                  resource_definition="Linux/WSL system counters; BIRD CPU as all-CPU capacity share; memory bytes",
                  process_scope="Combined BIRD routing process; separate ROV/BGP threads unavailable",
                  hostname=platform.node(), platform=platform.platform(), cpu_count=os.cpu_count(),
                  resource_interval=args.resource_interval, quiet_seconds=args.quiet_seconds,
                  poll_interval=args.poll_interval, timeout=args.timeout, steady_seconds=args.steady_seconds,
                  errors=[], baseline_ready=False, cleanup_complete=None)
    result["mode"] = args.mode
    if args.mode in FAULT_MODES:
        result["time_reference"] = "Fault injection/socket shutdown start to external observation; FIFO timestamps retained separately"
    if args.mode == "disconnect":
        result['time_reference'] = 'BRIO monotonic timestamp after successful socket shutdown to external observation; command submission retained separately'
    result.update(rate=args.rate, pps=args.pps, packet_timeout=args.packet_timeout,
                  forwarding_scope="Sampled target UDP forwarding; not the entire kernel FIB")
    output = Path(args.output)
    trace = output / "bird-trace.log"
    with trace.open("xb"):
        pass
    resources = ResourceRecorder(args.resource_interval, CONFIG / "bird.pid")
    process = thread = None
    lab = FaultLab(args.mode, scenario, output, args.timeout, args.bin_dir) if args.mode in FAULT_MODES else None
    messages = queue.Queue()
    command = [sys.executable, "-u", "-m", "rov.dut.initialize", "--inside",
               "--bgp", scenario["bgp"], "--vrp", scenario["vrp"], "--rov", "on",
               "--dut-asn", "65000", "--as-sets", "reject",
               "--bird-trace-log", str(trace), "--bird-trace-info", "--timeout", str(args.timeout)]
    brio_trace = output / "brio-timing.jsonl"
    brio_trace.touch(exist_ok=False)
    result['brio_timing_log'] = str(brio_trace)
    environment = dict(os.environ, **{MANAGED_OUTPUT: "1",
                                      "BRIO_VRP_TIMING_LOG": str(brio_trace)})
    if args.mode == "full-reset":
        result["measurement"] = "cache-reset-send-v2"
        result["time_reference"] = "BRIO Cache Reset send start to external observations; FIFO and first Prefix-PDU sends retained"
        result["initial_sync_timing"] = "Not measured; baseline initialization uses the existing 6.3A file-only setup"
    else:
        command.append("--measure-full-sync")
    if lab is not None:
        command += ["--rtr-port", "3324"]
    if args.mode in ("disconnect",) or args.bin_dir:
        selected_bin = args.bin_dir or ROOT / 'software/bin-brio-disconnect'
        command += ["--bin-dir", str(Path(selected_bin).resolve())]
    else:
        selected_bin = ROOT / 'software/bin-brio-first-vrp'
        command += ['--bin-dir', str(selected_bin)]

    # Check only this runner's owned initialization process.
    def check_alive():
        check_initialization(process, messages)

    try:
        binary = Path(selected_bin) / 'brio_rc'
        if (not binary.is_file() or any(marker not in binary.read_bytes() for marker in
                (b'brio-first-vrp-v1', b'brio-control-send-v1'))):
            raise RuntimeError('The selected BRIO lacks Prefix-PDU and control-PDU timing support')
        if args.mode in ('disconnect',):
            binary = lab.binaries / 'brio_rc'
            if not binary.is_file() or b'BRIO_DISCONNECT client=' not in binary.read_bytes():
                raise RuntimeError('Native RTR fault mode requires software/build_brio_disconnect.sh output')
        resources.start()
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                   env=environment)
        observer = lab.observe_output if args.mode in ("disconnect",) else None
        thread = threading.Thread(target=forward_output, args=(process, messages, result, observer))
        thread.start()
        wait_for_initialization(process, messages)
        result["baseline_ready"] = True
        if lab is not None:
            lab.prepare(check_alive)
        capture_environment(output, args.timeout)
        result["steady_start_ns"] = time.monotonic_ns()
        until = time.monotonic() + args.steady_seconds
        while time.monotonic() < until:
            check_alive()
            time.sleep(min(0.1, max(0, until - time.monotonic())))
        result["steady_end_ns"] = time.monotonic_ns()
        result["tcp_before"] = bird_rtr_sockets(CONFIG / "bird.pid")
        result["churn"] = {}
        sequence = Path(scenario["sequence"])
        with (output / "events.jsonl").open("x") as events:
            if args.mode == "full-reset":
                run_full_reset(scenario, args, trace, brio_trace, result["churn"], events, check_alive)
            else:
                run_churn(
                    sequence, Path(scenario["predictions"]),
                    Path(scenario["path_predictions"]), CONFIG / "bird.ctl", CONFIG / "brio.in",
                    scenario["batch_size"] / args.rate, args.poll_interval, args.timeout,
                    config_dir=CONFIG, pps=args.pps, packet_timeout=args.packet_timeout,
                    quiet_seconds=args.quiet_seconds, log_path=trace,
                    result=result["churn"], raw_output=events, check_alive=check_alive, fault_lab=lab,
                )
                submission = result['churn']['submissions'][0]
                timing_path = brio_trace
                result['churn']['send_timing'] = source_timing(timing_path, submission)
        result["tcp_after"] = bird_rtr_sockets(CONFIG / "bird.pid")
        if args.mode == "full-reset":
            result["full_vrp_verification"] = result["churn"]["full_sync_verification"]["vrp_table"]
        else:
            result["full_vrp_verification"] = verify_reload_table(scenario, args.timeout,
                                                                   "rpki_source")
        if not result["full_vrp_verification"]["correct"]:
            raise RuntimeError("Full VRP identity verification failed after collection")
        if args.mode == "full-reset":
            timing = result["churn"]["timing"]
            result["recovery"] = dict(
                reference=timing['reference'],
                cache_reset_send_ns=timing['cache_reset_send_ns'],
                full_reload_observed=result["churn"]["full_sync_verification"]["protocol"]["complete"],
                full_reload_ms=timing["full_reload_ms"],
                reset_to_established_ms=timing["reset_to_established_ms"],
                affected_query_from_send_ms=timing['affected_query_from_send_ms'],
                rib_from_send_ms=timing['rib_from_send_ms'],
                sampled_fib_from_send_ms=timing['sampled_fib_from_send_ms'],
                rib_sampled_fib_from_send_ms=timing['rib_sampled_fib_from_send_ms'],
                post_install_rib_sampled_fib_ms=timing["post_install_rib_sampled_fib_ms"],
                definition=timing["definition"],
            )
        else:
            result["recovery"] = fault_recovery(result["churn"], lab)
        if lab is not None and not result["recovery"]["evidence_complete"]:
            raise RuntimeError("Missing required fault and recovery evidence")
        if args.mode == "full-reset":
            result["recovery"]["same_tcp_session"] = (len(result["tcp_before"]) == 1 and result["tcp_before"] == result["tcp_after"])
            if not result["recovery"]["same_tcp_session"]:
                raise RuntimeError("Cache Reset did not retain the same BIRD TCP socket")
        if args.mode == "full-reset" and not result["recovery"]["full_reload_observed"]:
            raise RuntimeError("No reset and subsequent Established transition observed")
        result["rate"] = args.rate
        result["rate_definition"] = "Fixed batch size divided by nominal submission interval; actual FIFO timestamps retained, not RTR wire rate"
        if result["churn"]["status"] != "success":
            raise RuntimeError("Churn measurement or final verification failed")
        check_alive()
        result["status"] = "success"
    except BaseException as error:
        result["errors"].append(f"{type(error).__name__}: {error}")
        result["status"] = "interrupted" if isinstance(error, KeyboardInterrupt) else "failed"
    finally:
        resources.close()
        if lab is not None:
            try:
                result["session_fault"] = lab.close()
                if result["session_fault"]["errors"]:
                    result["errors"].extend(result["session_fault"]["errors"])
                    result["status"] = "failed"
            except BaseException as error:
                result["session_fault"] = lab.result
                result["errors"].append(f"Fault resource cleanup: {error}")
                result["status"] = "failed"
        result["resource_errors"] = resources.errors
        if process is not None:
            try:
                result["cleanup_complete"] = stop_initialization(process, thread) == 0
            except BaseException as error:
                result["cleanup_complete"] = False
                result["errors"].append(f"Cleanup: {error}")
        else:
            result["cleanup_complete"] = True
        if lab is not None and result.get("session_fault", {}).get("errors"):
            result["cleanup_complete"] = False
        if lab is not None and "recovery" not in result:
            result["recovery"] = fault_recovery(result.get("churn", {}), lab)
        if not result["cleanup_complete"] or resources.errors:
            result["status"] = "failed"
        result["resources"] = dict(samples=resources.samples, sync_boundary=sync_boundary_summary(result))
        churn = result.get("churn", {})
        submissions = churn.get("submissions", [])
        first = min((row["write_start_ns"] for row in submissions), default=None)
        last = churn.get("collection_end_ns")
        rows = [row for row in resources.samples if first is not None and last is not None
                and first <= row["observed_ns"] <= last]
        result["resources"]["churn_and_rov_convergence"] = resource_summary(rows)
        result["workload_scope"] = "Controlled incremental/full reset, or fault with changed VRPs and selected RIB/forwarding recovery"
        result["not_covered"] = ["Internal ROV rebuild instrumentation", "Whole kernel FIB enumeration",
                                  "Priority-based failover (BIRD merges both cache authorizations)"]
        result["threshold_scope"] = "Functional low-rate run; no saturation threshold search or capacity claim"
        result["observation_complete"] = (not churn.get("unobserved_vrp_events", 1)
                                          and not churn.get("unexpected_vrp_events", 1)
                                          and churn.get("transition_observation_complete", False))
        for phase in ("sync", "steady"):
            first = result.get(f"{phase}_start_observed_ns" if phase == "sync" else "steady_start_ns")
            last = result.get(f"{phase}_end_observed_ns" if phase == "sync" else "steady_end_ns")
            rows = [row for row in resources.samples if first is not None and last is not None
                    and first <= row["observed_ns"] <= last]
            result["resources"][phase] = resource_summary(rows)
        result["unavailable"] = dict(internal_queue="Only submitted-minus-observed proxy is available",
                                     wire_rate="Only FIFO submission schedule is recorded",
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
    parser.add_argument("--bin-dir", type=Path)
    parser.add_argument("--quiet-seconds", type=float, default=10,
                        help="Seconds without new state-change events before quiet completion (default: 10)")
    parser.add_argument("--timeout", type=float, default=600)
    parser.add_argument("--poll-interval", type=float, default=0,
                        help="ROV pause after each complete query round in seconds (default: 0)")
    parser.add_argument("--resource-interval", type=float, default=0.1)
    parser.add_argument("--steady-seconds", type=float, default=30)
    parser.add_argument("--rate", type=float, default=100)
    parser.add_argument("--mode", choices=("full-reset", "disconnect"), default="full-reset")
    parser.add_argument("--pps", type=float, default=10000,
                        help="Aggregate FIB probe rate in packets per second (default: 10000)")
    parser.add_argument("--packet-timeout", type=float, default=1)
    parser.add_argument("--inside", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.mode == "full-reset":
        args.bin_dir = Path(args.bin_dir or ROOT / "software/bin-brio-first-vrp").resolve()
        brio = args.bin_dir / "brio_rc"
        if not brio.is_file():
            parser.error("Full reset requires a BRIO build with first-Prefix timing support")
        binary = brio.read_bytes()
        if b"BRIO_VRP_TIMING_LOG" not in binary or b"first_vrp_send_ns" not in binary:
            parser.error("Selected BRIO build lacks full-reset first-Prefix timing support")
    scenario_path, scenario = load_scenario(args.scenario)
    if scenario["operations"] != scenario["batch_size"]:
        parser.error("Reset and fault modes require exactly one notification batch")
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
            code = supervise("rov.experiments.exp67", sys.argv[1:], log=log)
        if not (args.output / "result.json").exists():
            (args.output / "result.json").write_text(json.dumps(dict(
                format="exp67-result-v1", status="failed", scenario=str(args.scenario),
                errors=["Managed child did not produce a result"], exit_code=code,
            ), indent=2) + "\n")
        return code
    if not args.inside:
        enter_namespace(sys.argv[1:], module="rov.experiments.exp67")
    return run_managed(args)


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, stop_requested)
    signal.signal(signal.SIGINT, stop_requested)
    raise SystemExit(main())
