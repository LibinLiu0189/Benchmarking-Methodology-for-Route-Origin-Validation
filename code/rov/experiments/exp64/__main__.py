"""Own one Section 6.4 baseline, collect resources, and preserve every outcome."""

from collections import Counter
import gc
import json
import os
from pathlib import Path
import platform
import queue
import subprocess
import sys
import threading
import time
from rov.paths import CONFIG, ROOT
from rov.data.sequence import load_sequence
from rov.data.vrp import load_vrp_text
from rov.measure.read_log import start_log_observer
from rov.measure.read_log import stop_observer
from rov.send.send_vrp import send_batch
from rov.experiments.exp61.brio_timing import (
    correlate, correlate_control_records, read_records,
)
from rov.experiments.exp62.observation.query_observer import start_candidate_observer
from rov.measure.vrp_state import check_initial_states
from rov.measure.vrp_state import build_vrp_expectation
from rov.dut.managed import check_initialization
from rov.dut.managed import wait_for_initialization
from rov.dut.managed import stop_initialization
from rov.dut.output import MANAGED_OUTPUT
from rov.measure.resource import resource_summary
from rov.measure.resource import ResourceRecorder
from rov.measure.resource import sync_boundary_summary
from rov.dut.managed import forward_prompt_sync_output as forward_output
from rov.dut.managed import capture_environment
from rov.measure.vrp_state import verify_final
from rov.experiments.exp64.inputs.scenario import load_scenario
from rov.measure.full_sync import verify_vrp_table
import argparse
import signal
from rov.dut.managed import stop_requested
from rov.dut.output import supervise
from rov.dut.virtualize import enter_namespace


# Receive one increment until actual events stay quiet, then verify independently.
def measure_batch(directory, info, operations, prediction, initial_vrps, trace, args, check_alive):
    result = dict(batch=info["number"], vrp_operations=len(operations), vrp_events=[],
                  rov_events=[], rtr_events=[], errors=[], metrics={})
    expected = build_vrp_expectation(initial_vrps, operations)
    events, errors, stop, thread = start_log_observer(
        CONFIG / "bird.ctl", ("vrp", "rtr"), args.timeout, log_path=trace,
    )
    rov = None
    baseline = {}
    latest_ns = 0
    file_drained_ns = 0
    pending_events = []
    gc_was_enabled = None

    # Retain actual state changes and reader progress without inspecting predictions.
    def collect(event):
        nonlocal latest_ns, file_drained_ns
        kind = event["event"]
        if kind == "drained":
            file_drained_ns = event["observed_ns"] if event["drained"] else 0
            return
        if kind not in ("vrp", "rov", "rtr"):
            raise RuntimeError(f"Unexpected observation event: {event}")
        if kind in ("vrp", "rtr"):
            file_drained_ns = 0
        result[kind + "_events"].append(event)
        latest_ns = max(latest_ns, event["observed_ns"])

    try:
        changes = prediction["changes"]
        if changes:
            baseline, _, _, rov_stop, rov_thread = start_candidate_observer(
                changes, CONFIG / "bird.ctl", args.poll_interval, args.timeout, check_alive,
                event_queue=events, error_queue=errors,
                mode=args.query_mode, group_size=args.query_group_size,
            )
            rov = (rov_stop, rov_thread)
        before_brio = read_records(args.brio_timing_log)
        gc_was_enabled = gc.isenabled()
        gc.disable()
        result["submission"] = send_batch(directory / info["script"], CONFIG / "brio.in")
        latest_ns = result["submission"]["submitted_ns"]
        deadline = time.monotonic() + args.timeout
        while True:
            check_alive()
            if not errors.empty():
                raise errors.get()
            for _ in range(len(pending_events) + events.qsize()):
                collect(pending_events.pop(0) if pending_events else events.get_nowait())
            if (file_drained_ns - latest_ns >= args.quiet_seconds * 1e9
                    and events.empty()):
                result["quiet_confirmed_ns"] = time.monotonic_ns()
                result["termination"] = "quiet"
                break
            if time.monotonic() >= deadline:
                result["termination"] = "timeout"
                raise TimeoutError("Actual changes did not become quiet")
            # Wake on a queued event while retaining the existing error-check interval.
            try:
                pending_events.append(events.get(timeout=0.001))
            except queue.Empty:
                pass
    except BaseException as error:
        result["errors"].append(f"{type(error).__name__}: {error}")
        if isinstance(error, KeyboardInterrupt):
            result["interrupted"] = True
    finally:
        try:
            stop.set()
            if rov is not None:
                rov[0].set()
            for state in ((stop, thread), rov):
                if state is not None:
                    try:
                        stop_observer(*state, args.timeout)
                    except BaseException as error:
                        result["errors"].append(f"Observer shutdown: {error}")
            for event in pending_events:
                collect(event)
            while not events.empty():
                collect(events.get_nowait())
            while not errors.empty():
                result["errors"].append(f"Observer: {errors.get()}")
            result["collection_end_ns"] = time.monotonic_ns()
        finally:
            if gc_was_enabled:
                gc.enable()

    # Check initial and final expectations only after all observers have stopped.
    if result.get("termination") == "quiet":
        if result["quiet_confirmed_ns"] - latest_ns < args.quiet_seconds * 1e9:
            result["errors"].append(
                "An in-flight observation invalidated the quiet interval during observer shutdown"
            )
    try:
        check_initial_states(prediction["changes"], baseline)
    except (KeyError, RuntimeError) as error:
        result["errors"].append(f"Initial verification: {error}")
    final_rovs = {(row["prefix"], row["origin_as"]): row["after"] for row in prediction["changes"]}
    result["final_verification"] = verify_final(CONFIG / "bird.ctl", args.timeout, expected, final_rovs)
    observed = Counter((row["action"], row["prefix"], row["max_length"], row["asn"])
                       for row in result["vrp_events"])
    wanted = Counter(("added" if row["operation"] == "add" else "removed", *row["vrp"])
                     for row in operations)
    result["unobserved_operations"] = sum((wanted - observed).values())
    result["unexpected_events"] = sum((observed - wanted).values())
    result["log_complete"] = observed == wanted and not result["errors"]
    result["unobserved_meaning"] = "Missing observations, not proof of dropped RTR updates"
    if result.get("submission"):
        fifo_start = result["submission"]["write_start_ns"]
        vrp_times = [row["observed_ns"] for row in result["vrp_events"]]
        start = None
        try:
            timing = correlate(
                before_brio, read_records(args.brio_timing_log),
                dict(**result["submission"],
                     first_observed_ns=min(vrp_times) if vrp_times else None,
                     last_observed_ns=max(vrp_times) if vrp_times else None,
                     full_completed_ns=None),
                "incremental", info["number"],
            )
            timing.update(correlate_control_records(
                args.brio_timing_log, result["submission"], timing.get("brio_response"),
            ))
            result["send_timing"] = timing
            if timing["brio_timing_status"] != "complete":
                raise RuntimeError("Missing first Prefix-PDU send timestamp")
            if (timing['control_timing_status'] != 'complete'
                    or timing['brio_notify_send_ns'] is None):
                raise RuntimeError('Missing verified Notify send timestamp')
            start = timing["brio_first_vrp_send_ns"]
        except (OSError, KeyError, ValueError, RuntimeError) as error:
            result["errors"].append(f"BRIO timing: {error}")
        if vrp_times and result["log_complete"] and start is not None:
            result["metrics"].update(first_vrp_ms=(min(vrp_times) - start) / 1e6,
                                      last_vrp_ms=(max(vrp_times) - start) / 1e6,
                                      vrp_span_ms=(max(vrp_times) - min(vrp_times)) / 1e6,
                                      fifo_first_vrp_ms=(min(vrp_times) - fifo_start) / 1e6,
                                      fifo_last_vrp_ms=(max(vrp_times) - fifo_start) / 1e6)
        observed_after = {(row["prefix"], row["peer"]): row for row in result["rov_events"]}
        complete = all(observed_after.get((row["prefix"], row["peer"]), {}).get("after") == row["after"]
                       for row in prediction["changes"])
        if prediction["changes"] and complete and start is not None:
            last = max(row["observed_ns"] for row in observed_after.values())
            result["metrics"]["last_roa_query_ms"] = (last - start) / 1e6
            result["metrics"]["fifo_last_roa_query_ms"] = (last - fifo_start) / 1e6
            if vrp_times and result["log_complete"]:
                delta = (last - max(vrp_times)) / 1e6
                result["metrics"]["post_install_query_ms"] = delta if delta >= 0 else None
                if delta < 0:
                    result["post_install_unavailable"] = "Cross-stream observation order; negative delta is not latency"
    result["status"] = "success" if (not result["errors"] and result["final_verification"]["correct"]
                                          and result.get("termination") == "quiet") else "failed"
    return result


# Collect one clean run, preserving files and cleanup status on every failure.
def run_managed(args):
    path, scenario = load_scenario(args.scenario)
    result = dict(format="exp64-result-v1", measurement_version="prefix-send-v5",
                  scenario=str(path), input=scenario, status="failed",
                  time_reference="BRIO first Prefix-PDU send start to external observation; FIFO and Notify times retained",
                  full_sync_definition=(
                      "BRIO first initial Prefix-PDU send start to Established with exact IPv4/IPv6 VRP counts"
                  ),
                  validation_definition="ROA query result observation; not internal BGP revalidation completion",
                  resource_definition="Linux/WSL system counters; BIRD CPU as all-CPU capacity share; memory bytes",
                  process_scope="Combined BIRD routing process; separate ROV/BGP threads unavailable",
                  hostname=platform.node(), platform=platform.platform(), cpu_count=os.cpu_count(),
                  resource_interval=args.resource_interval, quiet_seconds=args.quiet_seconds,
                  poll_interval=args.poll_interval, timeout=args.timeout, steady_seconds=args.steady_seconds,
                  query_mode=args.query_mode, query_group_size=args.query_group_size,
                  batches=[], errors=[], baseline_ready=False, cleanup_complete=None)
    output = Path(args.output)
    trace = output / "bird-trace.log"
    with trace.open("xb"):
        pass
    args.brio_timing_log = output / "brio-timing.jsonl"
    args.brio_timing_log.touch(exist_ok=False)
    resources = ResourceRecorder(args.resource_interval, CONFIG / "bird.pid")
    process = thread = None
    messages = queue.Queue()
    command = [sys.executable, "-u", "-m", "rov.dut.initialize", "--inside",
               "--bgp", scenario["bgp"], "--vrp", scenario["vrp"], "--rov", "on",
               "--dut-asn", "65000", "--as-sets", "reject", "--measure-full-sync",
               "--bird-trace-log", str(trace), "--bird-trace-info", "--timeout", str(args.timeout)]
    binaries = Path(args.bin_dir or ROOT / "software/bin-brio-first-vrp").resolve()
    brio = binaries / "brio_rc"
    if (not brio.is_file() or any(marker not in brio.read_bytes() for marker in
            (b"brio-first-vrp-v1", b"brio-control-send-v1"))):
        raise RuntimeError("The selected BRIO lacks Prefix-PDU and control-PDU timing support")
    command += ["--bin-dir", str(binaries)]

    # Check only this runner's owned initialization process.
    def check_alive():
        check_initialization(process, messages)

    try:
        resources.start()
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                   env=dict(os.environ, **{MANAGED_OUTPUT: "1",
                                       "BRIO_VRP_TIMING_LOG": str(args.brio_timing_log)}))
        thread = threading.Thread(target=forward_output, args=(process, messages, result))
        thread.start()
        wait_for_initialization(process, messages)
        result["baseline_ready"] = True
        if (result.get("full_sync_ms") is None
                or result.get("initial_sync_timing", {}).get("first_vrp_send_ns") is None):
            raise RuntimeError("No initial full synchronization measurement was reported")
        capture_environment(output, args.timeout)
        initial = set(load_vrp_text(scenario["vrp"]))
        result["steady_start_ns"] = time.monotonic_ns()
        until = time.monotonic() + args.steady_seconds
        while time.monotonic() < until:
            check_alive()
            time.sleep(min(0.1, max(0, until - time.monotonic())))
        result["steady_end_ns"] = time.monotonic_ns()
        directory, batches, operations = load_sequence(scenario["sequence"])
        predictions = {row["batch"]: row for row in map(json.loads, Path(scenario["predictions"]).read_text().splitlines())}
        for batch in batches:
            row = measure_batch(directory, batch, operations[batch["number"]], predictions[batch["number"]],
                                initial, trace, args, check_alive)
            result["batches"].append(row)
            if row["status"] != "success":
                raise RuntimeError("Batch measurement or final verification failed; raw result retained")
            for operation in operations[batch["number"]]:
                key = tuple(operation["vrp"])
                initial.add(key) if operation["operation"] == "add" else initial.remove(key)
        result["final_vrp_verification"] = verify_vrp_table(CONFIG / "bird.ctl", args.timeout, initial)
        if not result["final_vrp_verification"]["correct"]:
            raise RuntimeError("Final complete VRP identity verification failed")
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
        result["resources"] = dict(samples=resources.samples, sync_boundary=sync_boundary_summary(result))
        for phase in ("sync", "steady"):
            first = result.get(f"{phase}_start_observed_ns" if phase == "sync" else "steady_start_ns")
            last = result.get(f"{phase}_end_observed_ns" if phase == "sync" else "steady_end_ns")
            rows = [row for row in resources.samples if first is not None and last is not None
                    and first <= row["observed_ns"] <= last]
            result["resources"][phase] = resource_summary(rows)
        result["unavailable"] = dict(internal_rov_latency="No internal instrumentation",
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
                        help="Seconds without new events before completion (default: 10)")
    parser.add_argument("--timeout", type=float, default=600)
    parser.add_argument("--poll-interval", type=float, default=0,
                        help="ROV pause after each complete query round in seconds (default: 0)")
    parser.add_argument("--resource-interval", type=float, default=0.1)
    parser.add_argument("--steady-seconds", type=float, default=30)
    parser.add_argument("--inside", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    args.query_mode = "pipeline"
    args.query_group_size = 8
    load_scenario(args.scenario)
    if not (0 < args.quiet_seconds < args.timeout < float("inf") and
            0 < args.resource_interval < args.timeout and 0 <= args.poll_interval < float("inf") and
            0 < args.steady_seconds < float("inf")):
        parser.error("Invalid finite measurement intervals")
    args.output = args.output.absolute()
    if not os.environ.get(MANAGED_OUTPUT):
        args.output.mkdir(parents=True, exist_ok=False)
        with (args.output / "run.log").open("xb") as log:
            code = supervise("rov.experiments.exp64", sys.argv[1:], log=log)
        if not (args.output / "result.json").exists():
            (args.output / "result.json").write_text(json.dumps(dict(
                format="exp64-result-v1", status="failed", scenario=str(args.scenario),
                errors=["Managed child did not produce a result"], exit_code=code,
            ), indent=2) + "\n")
        return code
    if not args.inside:
        enter_namespace(sys.argv[1:], module="rov.experiments.exp64")
    return run_managed(args)


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, stop_requested)
    signal.signal(signal.SIGINT, stop_requested)
    raise SystemExit(main())
