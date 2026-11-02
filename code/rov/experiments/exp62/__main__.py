"""Run one prepared experiment 62 scenario and retain its result without overwrite."""

import argparse
from contextlib import contextmanager
import json
import os
from pathlib import Path
import queue
import signal
import subprocess
import sys
import tempfile
import threading
import time
from rov.data.sequence import load_sequence
from rov.data.vrp import load_vrp_text
from rov.paths import CONFIG, ROOT
from rov.measure.read_log import start_log_observer
from rov.measure.read_log import stop_observer
from rov.send.send_vrp import send_batch
from rov.experiments.exp62.observation.timing import validation_timing
from rov.experiments.exp61.brio_timing import read_records, correlate, correlate_control_records
from rov.dut.output import MANAGED_OUTPUT
from rov.dut.output import supervise
from rov.dut.virtualize import enter_namespace
from rov.dut.managed import forward_initialization_output
from rov.dut.managed import check_initialization
from rov.dut.managed import wait_for_initialization
from rov.dut.managed import stop_initialization
from rov.dut.managed import stop_requested
from rov.measure.vrp_state import accept_vrp_event
from rov.measure.vrp_state import build_vrp_expectation
from rov.experiments.exp62.observation.observe import monitoring_record
from rov.experiments.exp62.observation.observe import pending_net_changes
from rov.measure.vrp_state import check_repeated_vrp_states
from rov.experiments.exp62.observation.observe import net_observation_summary
from rov.measure.vrp_state import check_initial_states
from rov.experiments.exp62.observation.query_observer import start_candidate_observer as start_batch_observer


# Validate and record one actual candidate ROV transition for the current batch.
def accept_rov_event(event, expected, observed, batch):
    key = (event["prefix"], event["peer"])
    if key not in expected:
        raise RuntimeError(f"Batch {batch} produced an unexpected ROV event: {event}")
    change = expected[key]
    if event["before"] != change["before"] or event["after"] != change["after"]:
        raise RuntimeError(
            f"Batch {batch} produced the wrong ROV transition for {key}: "
            f"expected {change['before']} -> {change['after']}, "
            f"observed {event['before']} -> {event['after']}"
        )
    if key in observed:
        raise RuntimeError(f"Batch {batch} produced a duplicate ROV event: {event}")
    observed[key] = event


# Load one ordered ROV prediction record for every sequence batch.
def load_predictions(path, metadata):
    with Path(path).open() as source:
        records = [json.loads(line) for line in source if line.strip()]
    numbers = [row["number"] for row in metadata]
    if [row.get("batch") for row in records] != numbers:
        raise ValueError("ROV prediction batches do not match the VRP sequence")
    return {row["batch"]: row for row in records}


# Collect one batch until quiet, stop observers, then check its recorded observations.
def measure_batch(
    directory,
    info,
    operations,
    prediction,
    vrp_events,
    vrp_errors,
    socket_path,
    fifo,
    poll_interval,
    quiet_seconds,
    timeout,
    check_alive,
    expectation,
    query_mode="pipeline", query_group_size=8,
    *, log_stop, log_thread, brio_trace,
):
    print("VRP expectation: " + json.dumps(expectation["counts"], sort_keys=True), flush=True)
    monitoring = monitoring_record(expectation)
    print("VRP monitoring exclusions: " + json.dumps({
        field: monitoring[field] for field in (
            "repeated_operation_prefix_count", "repeated_operation_prefixes",
            "excluded_prefix_count", "excluded_prefixes", "excluded_vrps",
        )
    }, sort_keys=True), flush=True)
    check_repeated_vrp_states(expectation, socket_path, timeout, "initial")
    changes = prediction["changes"]
    expected_rovs = {(row["prefix"], row["peer"]): row for row in changes}
    if len(expected_rovs) != len(changes):
        raise ValueError(f"Batch {info['number']} contains duplicate ROV predictions")

    rov_state = None
    initial = {}
    if changes:
        initial, rov_events, rov_errors, rov_stop, rov_thread = start_batch_observer(
            changes, socket_path, poll_interval, timeout, check_alive,
            event_queue=vrp_events, error_queue=vrp_errors,
            mode=query_mode, group_size=query_group_size,
        )
        rov_state = (rov_events, rov_errors, rov_stop, rov_thread)

    collected = []
    try:
        check_alive()
        print("VRP and ROA query observers ready; submitting one update.", flush=True)
        before_brio = read_records(brio_trace)
        submission = send_batch(directory / info["script"], fifo)
    except BaseException:
        if rov_state is not None:
            stop_observer(rov_state[2], rov_state[3], timeout)
        raise
    submitted_ns = submission["submitted_ns"]
    latest_ns = submitted_ns
    file_drained_ns = 0
    deadline = time.monotonic() + timeout
    try:
        while True:
            check_alive()
            if not vrp_errors.empty():
                raise vrp_errors.get()
            if rov_state is not None and not rov_state[1].empty():
                raise rov_state[1].get()

            # Consume only observations and reader progress; do not inspect predictions here.
            for _ in range(vrp_events.qsize()):
                event = vrp_events.get_nowait()
                if event.get("event") == "drained":
                    file_drained_ns = event["observed_ns"] if event["drained"] else 0
                    continue
                if event["event"] == "vrp":
                    file_drained_ns = 0
                collected.append(event)
                latest_ns = max(latest_ns, event["observed_ns"])

            if (file_drained_ns - latest_ns >= quiet_seconds * 1e9
                    and vrp_events.empty()):
                quiet_confirmed_ns = time.monotonic_ns()
                break
            if time.monotonic() >= deadline:
                raise TimeoutError(f"Batch {info['number']} did not become quiet")
            # A queued event wakes this wait without adding prediction checks.
            try:
                event = vrp_events.get(timeout=0.001)
            except queue.Empty:
                continue
            if event.get("event") == "drained":
                file_drained_ns = event["observed_ns"] if event["drained"] else 0
            else:
                if event["event"] == "vrp":
                    file_drained_ns = 0
                collected.append(event)
                latest_ns = max(latest_ns, event["observed_ns"])
    finally:
        log_stop.set()
        if rov_state is not None:
            rov_state[2].set()
        try:
            if rov_state is not None:
                stop_observer(rov_state[2], rov_state[3], timeout)
        finally:
            stop_observer(log_stop, log_thread, timeout)

    if not vrp_errors.empty():
        raise vrp_errors.get()
    if rov_state is not None and not rov_state[1].empty():
        raise rov_state[1].get()

    # Retain in-flight observations queued during shutdown before checking correctness.
    while not vrp_events.empty():
        event = vrp_events.get_nowait()
        if event.get("event") != "drained":
            collected.append(event)
    installation_events = [event for event in collected if event["event"] == "vrp"]
    ordered = sorted(
        (event for event in collected if event["event"] == "rov"),
        key=lambda row: row["observed_ns"],
    )
    verification_errors = []
    last_observed_ns = max((event["observed_ns"] for event in collected), default=submitted_ns)
    if quiet_confirmed_ns - last_observed_ns < quiet_seconds * 1e9:
        verification_errors.append(
            "An in-flight observation invalidated the quiet interval during observer shutdown"
        )
    initial_verified = 0
    try:
        check_initial_states(changes, initial)
        initial_verified = len(expected_rovs)
    except RuntimeError as error:
        verification_errors.append(str(error))
    observed_vrps = {}
    observed_rovs = {}
    for event in collected:
        if event["observed_ns"] < submission["write_start_ns"]:
            verification_errors.append(
                f"Batch {info['number']} received a {event['event']} event before submission"
            )
            continue
        try:
            if event["event"] == "rov":
                accept_rov_event(event, expected_rovs, observed_rovs, info["number"])
            else:
                accept_vrp_event(event, expectation, observed_vrps, info["number"])
        except RuntimeError as error:
            verification_errors.append(str(error))
    missing_rovs = len(expected_rovs) - len(observed_rovs)
    if missing_rovs:
        verification_errors.append(f"Missing {missing_rovs} expected ROV transitions")
    missing_vrps = len(pending_net_changes(expectation, observed_vrps))
    if missing_vrps:
        verification_errors.append(f"Missing {missing_vrps} net VRP changes")
    final_vrp_verified = True
    try:
        check_repeated_vrp_states(expectation, socket_path, timeout, "final")
    except (OSError, ValueError, RuntimeError) as error:
        final_vrp_verified = False
        verification_errors.append(f"Final VRP verification: {error}")

    result = dict(
        batch=info["number"], vrp_operations=len(operations),
        vrp_monitoring=monitoring, vrp_events=installation_events,
        query_events=ordered,
        predicted_prefixes=len({row["prefix"] for row in changes}),
        verified_prefixes=len({row["prefix"] for row in observed_rovs.values()}),
        predicted_candidates=len(expected_rovs), verified_candidates=len(observed_rovs),
        initial_query_states_verified=initial_verified,
        verification=dict(correct=not verification_errors, errors=verification_errors),
    )
    if not missing_vrps:
        result.update(net_observation_summary(expectation, observed_vrps))
        if not final_vrp_verified:
            result["repeated_vrp_final_states_verified"] = 0
    print("Post-collection verification: " + json.dumps(result["verification"]), flush=True)
    for row in result.get("repeated_vrp_observations", []):
        print("Repeated VRP observation: " + json.dumps(row), flush=True)
    vrp_times = [event["observed_ns"] for event in installation_events]
    brio_timing = correlate(
        before_brio, read_records(brio_trace), dict(
            **submission, first_observed_ns=min(vrp_times, default=None),
            last_observed_ns=max(vrp_times, default=None), full_completed_ns=None,
        ), "incremental", info["number"],
    )
    brio_timing.update(correlate_control_records(
        brio_trace, submission, response=brio_timing.get("brio_response"), pdu_type=0,
    ))
    if brio_timing["brio_timing_status"] not in ("complete", "no_vrp"):
        raise RuntimeError("No verified BRIO Prefix PDU send origin is available")
    if brio_timing["control_timing_status"] != "complete":
        raise RuntimeError("No verified BRIO Serial Notify send timestamp is available")
    result["timing"] = validation_timing(
        submission, installation_events, ordered, monitoring, brio_timing,
    )
    print("Validation timing: " + json.dumps(result["timing"], sort_keys=True), flush=True)
    print(
        "Primary ROV latency from BRIO first Prefix PDU send: "
        f"{result['timing']['rtr_send_to_query_complete_ms']} ms",
        flush=True,
    )
    if not ordered:
        print(
            f"Batch {info['number']}: {len(operations)} VRP operations; "
            "no observed ROV changes",
            flush=True,
        )
        return result
    first_ms = (ordered[0]["observed_ns"] - submitted_ns) / 1e6
    last_ms = (ordered[-1]["observed_ns"] - submitted_ns) / 1e6
    result.update(first_query_change_ms=first_ms, last_query_change_ms=last_ms)
    print(
        f"Batch {info['number']}: {len(operations)} VRP operations; "
        f"{len(ordered)} ROA query changes; first observed {first_ms:.3f} ms; "
        f"last observed {last_ms:.3f} ms",
        flush=True,
    )
    for event in ordered:
        delay_ms = (event["observed_ns"] - submitted_ns) / 1e6
        print(
            f"  {event['prefix']} {event['peer']}: {event['before']} -> "
            f"{event['after']}; observed after {delay_ms:.3f} ms",
            flush=True,
        )

    return result


# Validate one explicit prepared scenario without directory discovery.
def load_scenario(sequence):
    sequence = Path(sequence).resolve(strict=True)
    if sequence.name != "sequence.json" or not sequence.is_file():
        raise ValueError("Expected one scenario sequence.json file")
    directory, batches, operations = load_sequence(sequence.parent)
    if len(batches) != 1 or batches[0]["number"] != 1:
        raise ValueError("Experiment 62 requires exactly one batch numbered 1")
    metadata = json.loads(sequence.read_text())
    manifest_path = directory.parent.parent / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    if manifest.get("format") != "exp62-prefix-sets-v1":
        raise ValueError("Expected the generated experiment 62 manifest")
    relative = directory.relative_to(manifest_path.parent).as_posix()
    cases = [case for case in manifest["cases"] if case["path"] == relative]
    if len(cases) != 1:
        raise ValueError("The selected scenario is not uniquely present in its manifest")
    case = cases[0]
    if any(key not in case or case[key] != value
           for key, value in metadata["counts"].items()):
        raise ValueError("Scenario counts disagree with the manifest")
    vrp = (directory / metadata["initial"]).resolve(strict=True)
    if vrp != (manifest_path.parent / manifest["vrp"]).resolve(strict=True):
        raise ValueError("Scenario initial VRPs disagree with the manifest")
    bgp = (manifest_path.parent / manifest["bgp"]).resolve(strict=True)
    if not (bgp / "bgp.json").is_file():
        raise ValueError("Missing baseline BGP metadata")
    predictions = load_predictions(directory / "predictions.jsonl", batches)
    info = batches[0]
    edits = operations[1]
    commands = (directory / info["script"]).read_text().splitlines()
    if commands != [event["command"] for event in edits] + ["notify"]:
        raise ValueError("The update script disagrees with the prepared operations")
    if len(edits) != case["operations"] or len(edits) != predictions[1]["vrp_operations"]:
        raise ValueError("The operation counts disagree")
    expectation = build_vrp_expectation(load_vrp_text(vrp), edits)
    changes = predictions[1]["changes"]
    if len({(row["prefix"], row["peer"]) for row in changes}) != len(changes):
        raise ValueError("Duplicate candidate predictions")
    if len(changes) != case["affected_bgp_candidates"]:
        raise ValueError("Candidate prediction count disagrees with the manifest")
    if len({row["prefix"] for row in changes}) != case["affected_bgp_prefixes"]:
        raise ValueError("Prefix prediction count disagrees with the manifest")
    return directory, info, edits, predictions[1], bgp, vrp, case, expectation


# Reserve one fresh trace and retain explicit result files and all failure evidence.
@contextmanager
def measurement_log(path):
    temporary = path is None
    if temporary:
        with tempfile.NamedTemporaryFile(prefix="exp62-bird-", suffix=".log", delete=False) as trace:
            path = Path(trace.name)
    else:
        path = Path(path).absolute()
        with path.open("xb"):
            pass
    print(f"BIRD trace log: {path}", flush=True)
    try:
        yield path
    except BaseException:
        print(f"BIRD trace retained after failure: {path}", file=sys.stderr, flush=True)
        raise
    else:
        if temporary:
            path.unlink()


# Initialize a fresh isolated baseline and measure exactly the selected scenario.
def run(sequence, socket_path, fifo, poll_interval, quiet_seconds, timeout, bin_dir=None,
        bird_trace_log=None, query_mode="pipeline", query_group_size=8,
        *, roa_trie=False):
    if socket_path.resolve() != (CONFIG / "bird.ctl").resolve():
        raise ValueError("The BIRD socket must belong to this initialization")
    if fifo.resolve() != (CONFIG / "brio.in").resolve():
        raise ValueError("The BRIO FIFO must belong to this initialization")
    directory, info, operations, prediction, bgp, vrp, case, expectation = load_scenario(sequence)
    command = [
        sys.executable, "-u", "-m", "rov.dut.initialize", "--inside",
        "--bgp", str(bgp), "--vrp", str(vrp),
    ]
    binaries = Path(bin_dir or ROOT / "software/bin-brio-first-vrp").resolve()
    brio_binary = (binaries / "brio_rc").read_bytes()
    if any(marker not in brio_binary for marker in (b"brio-first-vrp-v1", b"brio-control-send-v1")):
        raise RuntimeError("BRIO must record Prefix PDU and control PDU send timestamps")
    command.extend(["--bin-dir", str(binaries)])
    if roa_trie:
        command.append("--roa-trie")
    with measurement_log(bird_trace_log) as trace_path:
        command.extend(["--bird-trace-log", str(trace_path)])
        brio_trace = trace_path.with_suffix(".brio.jsonl")
        with brio_trace.open("x"):
            pass
        print(
            "Experiment 62: initializing a fresh baseline. Measurement: BIRD ROA "
            "query changes, not BGP refilter completion.",
            flush=True,
        )
        initialization = subprocess.Popen(
            command, stdout=subprocess.PIPE,
            env=dict(os.environ, **{MANAGED_OUTPUT: "1", "BRIO_VRP_TIMING_LOG": str(brio_trace)}),
        )
        messages = queue.Queue()
        output_thread = threading.Thread(
            target=forward_initialization_output, args=(initialization, messages),
            name="exp62-initialization-output",
        )

        # Keep the initializer alive throughout observer setup and measurement.
        def check_alive():
            check_initialization(initialization, messages)

        try:
            output_thread.start()
            wait_for_initialization(initialization, messages)
            check_alive()
            print(f"VRP log transport: dedicated BIRD trace file ({trace_path}).", flush=True)
            vrp_events, vrp_errors, vrp_stop, vrp_thread = start_log_observer(
                socket_path, ("vrp",), timeout, log_path=trace_path,
            )
            try:
                result = measure_batch(
                    directory, info, operations, prediction, vrp_events, vrp_errors,
                    socket_path, fifo, poll_interval, quiet_seconds, timeout, check_alive, expectation,
                    query_mode, query_group_size,
                    log_stop=vrp_stop, log_thread=vrp_thread, brio_trace=brio_trace,
                )
                check_alive()
                if not vrp_errors.empty():
                    raise vrp_errors.get()
            finally:
                if vrp_thread.is_alive():
                    stop_observer(vrp_stop, vrp_thread, timeout)
        finally:
            initialization_exit = stop_initialization(initialization, output_thread)
        if initialization_exit != 0:
            raise RuntimeError(f"Initialization cleanup failed ({initialization_exit})")

        # Derive the selected-prefix count from saved targets and net predictions.
        with (directory / "transitions.jsonl").open() as stream:
            selected_prefixes = {
                json.loads(line)["selected"]["prefix"] for line in stream if line.strip()
            }
        changed_prefixes = {change["prefix"] for change in prediction["changes"]}

        return dict(
            sequence=str(directory / "sequence.json"),
            measurement="bird_roa_query_changes", vrp_log_transport="file",
            time_reference="BRIO first Prefix PDU send; FIFO and post-installation intervals retained separately",
            brio_timing_log=str(brio_trace),
            baseline_ready=True, cleanup_complete=True,
            selected_prefixes=case["selected_prefixes"],
            selected_prefixes_net_changed=len(selected_prefixes & changed_prefixes),
            poll_interval=poll_interval, quiet_seconds=quiet_seconds, timeout=timeout,
            query_mode=query_mode, query_group_size=query_group_size,
            roa_trie=roa_trie,
            batch=result,
        )


# Execute only the internally supervised managed worker.
def _worker_main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sequence", required=True, type=Path, help="One scenario sequence.json file")
    parser.add_argument("--result", type=Path, help="New JSON measurement result file")
    parser.add_argument("--roa-trie", action="store_true",
                        help="Enable a prefix trie for both BIRD ROA tables (default: disabled)")
    parser.add_argument("--bin-dir", type=Path)
    parser.add_argument("--socket", type=Path, default=CONFIG / "bird.ctl")
    parser.add_argument("--fifo", type=Path, default=CONFIG / "brio.in")
    parser.add_argument("--poll-interval", type=float, default=0,
                        help="ROV pause after each complete query round in seconds (default: 0)")
    parser.add_argument("--quiet-seconds", type=float,
                        help="Seconds without new events before completion (default: 10)")
    parser.add_argument("--timeout", type=float, default=300)
    parser.add_argument("--inside", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    args.query_mode = "pipeline"
    args.query_group_size = 8
    if args.quiet_seconds is None:
        args.quiet_seconds = 10
    if not 0 <= args.poll_interval < float("inf"):
        parser.error("Poll interval must be finite and nonnegative")
    if not 0 < args.quiet_seconds < args.timeout < float("inf"):
        parser.error("Quiet seconds and timeout must be finite, positive, and quiet < timeout")
    load_scenario(args.sequence)
    if args.result is not None and (args.result.exists() or args.result.is_symlink()):
        raise FileExistsError(f"Result already exists: {args.result}")
    if not os.environ.get(MANAGED_OUTPUT):
        return supervise("rov.experiments.exp62", sys.argv[1:])
    if not args.inside:
        enter_namespace(sys.argv[1:], module="rov.experiments.exp62")
    result = run(
        args.sequence, args.socket, args.fifo, args.poll_interval,
        args.quiet_seconds, args.timeout, args.bin_dir,
        args.result.with_name("bird-trace.log") if args.result is not None else None,
        args.query_mode, args.query_group_size,
        roa_trie=args.roa_trie,
    )
    if args.result is not None:
        with args.result.open("x") as stream:
            json.dump(result, stream, indent=2)
            stream.write("\n")
    return 0 if result["batch"]["verification"]["correct"] else 1


# Validate one input file and reserve a new directory for this run only.
def run_one(sequence, output=None, *, poll_interval=0, quiet_seconds=None, timeout=None, bin_dir=None,
            query_mode="pipeline", query_group_size=8, roa_trie=False):
    sequence = Path(sequence).resolve(strict=True)
    load_scenario(sequence)
    output = Path(output).absolute() if output is not None else sequence.parent / "results"
    if output.exists() or output.is_symlink():
        raise FileExistsError(f"Result directory already exists: {output}")
    if not output.parent.is_dir():
        raise ValueError(f"Result parent directory does not exist: {output.parent}")
    output.mkdir()
    result_path = output / "result.json"
    options = []
    if roa_trie:
        options.append("--roa-trie")
    for name, value in (("poll-interval", poll_interval), ("quiet-seconds", quiet_seconds),
                        ("timeout", timeout), ("bin-dir", bin_dir)):
        if value is not None:
            options.extend(["--" + name, str(value)])
    with (output / "run.log").open("xb") as log:
        code = supervise(
            "rov.experiments.exp62",
            ["--worker", "--sequence", str(sequence), "--result", str(result_path), *options],
            log=log,
        )
    if result_path.exists():
        result = json.loads(result_path.read_text())
    else:
        result = dict(
            sequence=str(sequence), measurement="bird_roa_query_changes",
            vrp_log_transport="file", cleanup_complete=None, roa_trie=roa_trie,
        )
    result["exit_code"] = code
    result["status"] = "success" if code == 0 else "failed"
    result_path.write_text(json.dumps(result, indent=2) + "\n")
    print(f"Experiment 62 result: {result_path}; exit code: {code}", flush=True)
    return code


# Accept one explicit sequence file, with no directory or batch discovery.
def main():
    if "--worker" in sys.argv[1:]:
        return _worker_main()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("sequence", type=Path, help="One scenario sequence.json file")
    parser.add_argument("--output", type=Path, help="New result directory; default: scenario/results")
    parser.add_argument("--poll-interval", type=float, default=0,
                        help="ROV pause after each complete query round in seconds (default: 0)")
    parser.add_argument("--quiet-seconds", type=float,
                        help="Seconds without new events before completion (default: 10)")
    parser.add_argument("--timeout", type=float, help="Override the existing worker timeout")
    parser.add_argument("--roa-trie", action="store_true",
                        help="Enable a prefix trie for both BIRD ROA tables (default: disabled)")
    parser.add_argument("--bin-dir", type=Path)
    args = parser.parse_args()
    args.query_mode = "pipeline"
    args.query_group_size = 8
    try:
        options = {
            name: getattr(args, name)
            for name in (
                "poll_interval", "quiet_seconds", "timeout", "bin_dir",
                "roa_trie",
            )
            if getattr(args, name) is not None
        }
        return run_one(args.sequence, args.output, **options)
    except (OSError, ValueError, RuntimeError, KeyError) as error:
        parser.exit(1, f"{error}\n")


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, stop_requested)
    signal.signal(signal.SIGINT, stop_requested)
    raise SystemExit(main())
