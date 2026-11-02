"""Run prepared experiment 61 datasets, each incremental then full, with complete logs."""

from rov.paths import ROOT
import argparse
from collections import Counter
from contextlib import contextmanager
import gc
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
from rov.paths import CONFIG
from rov.data.sequence import load_sequence
from rov.data.vrp import normalize
from rov.data.vrp import load_vrp_text
from rov.measure.full_sync import begin_full_sync, finish_full_sync, verify_vrp_table
from rov.experiments.exp61.brio_timing import (
    read_records, correlate, correlate_control_records,
)
from rov.measure.read_log import observe as observe_log
from rov.send.send_vrp import send_batch
from rov.dut.output import MANAGED_OUTPUT
from rov.dut.output import supervise
from rov.dut.virtualize import enter_namespace
from rov.dut.managed import forward_initialization_output
from rov.dut.managed import check_initialization
from rov.dut.managed import wait_for_initialization
from rov.dut.managed import stop_initialization
from rov.dut.managed import stop_requested
import re
from rov.dut.output import copy_output


BGP_DIR = ROOT / "data/baseline/bgp"


# Start the BIRD log observer and wait until its trace subscription is ready.
def start_log_observer(socket_path, timeout, initialization, output_messages, mode="incremental", log_path=None):
    events = queue.Queue()
    errors = queue.Queue()
    stop = threading.Event()

    file_drained = False

    # Invalidate a previous EOF when more data or a partial record arrives.
    def file_poll(drained):
        nonlocal file_drained
        if drained or file_drained:
            events.put(dict(event="drained", drained=drained, observed_ns=time.monotonic_ns()))
        file_drained = drained

    # Forward observer results to the experiment thread.
    def worker():
        try:
            planes = ("vrp", "rtr") if mode == "full" else ("vrp",)
            if log_path is None:
                raise ValueError("Experiment 61 requires a dedicated BIRD trace file")
            observe_log(socket_path, planes, timeout, events.put, stop,
                        log_path=log_path, on_poll=file_poll)
        except BaseException as error:
            errors.put(error)

    thread = threading.Thread(target=worker, name="exp61-clean-log")
    thread.start()
    deadline = time.monotonic() + timeout
    try:
        while True:
            check_initialization(initialization, output_messages)
            if not errors.empty():
                raise errors.get()
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("Timed out waiting for the BIRD log observer")
            try:
                event = events.get(timeout=min(0.1, remaining))
            except queue.Empty:
                continue
            if event.get("event") != "ready":
                raise RuntimeError("Received a log event before the observer was ready")
            check_initialization(initialization, output_messages)
            return events, errors, stop, thread
    except BaseException:
        stop_observer(stop, thread, timeout)
        raise


# Stop one observer thread after its batch or run has finished.
def stop_observer(stop, thread, timeout):
    stop.set()
    thread.join(timeout + 1)
    if thread.is_alive():
        raise TimeoutError(f"Timed out stopping observer {thread.name}")


# Validate the prepared operations before staging a full-refresh batch.
def full_batch_commands(directory, info):
    commands = [line.split("#", 1)[0].strip()
                for line in (directory / info["script"]).read_text().splitlines()]
    commands = [line for line in commands if line]
    if not commands or commands[-1] != "notify":
        raise ValueError("A full-mode source batch must end with notify")
    for line in commands[:-1]:
        parts = line.split()
        if parts[0] == "add" and len(parts) == 4:
            normalize(*parts[1:])
        elif parts[0] == "remove" and len(parts) in (2, 3):
            indices = [int(value) for value in parts[1:]]
            if indices[0] < 1 or indices[-1] < indices[0]:
                raise ValueError(f"Invalid batch removal: {line}")
        else:
            raise ValueError(f"Unsupported full-mode batch command: {line}")
    commands[-1] = "reset"
    return commands


# Stage the same prepared operations with a final reset, leaving inputs untouched.
@contextmanager
def batch_for_mode(directory, info, mode):
    if mode == "incremental":
        yield info
        return
    commands = full_batch_commands(directory, info)
    with tempfile.NamedTemporaryFile(
        mode="w", prefix="exp61-full-", suffix=".script", dir=CONFIG,
    ) as script:
        script.write("\n".join(commands) + "\n")
        script.flush()
        yield dict(info, script=script.name)


# Collapse this atomic batch's operations into the expected net VRP changes.
def expected_full_changes(operations):
    delta = Counter()
    for operation in operations:
        if operation["operation"] not in ("add", "withdraw"):
            raise ValueError("Expected an add or withdraw VRP operation")
        vrp = normalize(*operation["vrp"])
        delta[vrp] += 1 if operation["operation"] == "add" else -1
    if any(abs(count) > 1 for count in delta.values()):
        raise ValueError("Full batch has inconsistent repeated VRP operations")
    return Counter({("added" if count > 0 else "removed", *vrp): 1
                    for vrp, count in delta.items() if count})


# Collect a drained quiet stream, then validate full-refresh events and report timing.
def measure_batch(
    directory, info, events, errors, fifo, quiet_seconds, timeout,
    initialization, output_messages, mode="incremental", expected=None,
):
    check_initialization(initialization, output_messages)
    gc_was_enabled = gc.isenabled()
    gc.disable()
    try:
        if mode == "full":
            if expected is None:
                raise ValueError("Full mode requires expected net VRP changes")
            submission = send_batch(directory / info["script"], fifo, atomic=True)
        else:
            submission = send_batch(directory / info["script"], fifo)
        submitted_ns = submission["submitted_ns"]
        latest_ns = submitted_ns
        deadline = time.monotonic() + timeout
        collected = []
        drained_ns = 0

        while True:
            check_initialization(initialization, output_messages)
            if not errors.empty():
                raise errors.get()
            for _ in range(events.qsize()):
                try:
                    event = events.get_nowait()
                except queue.Empty:
                    break
                if event.get("event") == "drained":
                    drained_ns = event["observed_ns"] if event.get("drained", True) else 0
                    continue
                if event.get("event") not in ("vrp", "rtr"):
                    continue
                if event["observed_ns"] < submission["write_start_ns"]:
                    continue
                collected.append(event)
                latest_ns = max(latest_ns, event["observed_ns"])

            # Wait for the first incremental update before accepting a quiet stream.
            if (
                (mode == "full" or collected)
                and events.empty()
                and drained_ns - latest_ns >= quiet_seconds * 1e9
            ):
                break
            if time.monotonic() >= deadline:
                raise TimeoutError(f"Batch {info['number']} did not become quiet with a drained stream")
            time.sleep(0.001)
    finally:
        if gc_was_enabled:
            gc.enable()

    check_initialization(initialization, output_messages)
    if not errors.empty():
        raise errors.get()

    if mode == "full":
        vrp_count = sum(event["event"] == "vrp" for event in collected)
        rtr_count = len(collected) - vrp_count
        print(
            f"Batch {info['number']}: collected {vrp_count} VRP events and "
            f"{rtr_count} RTR state changes before verification",
            flush=True,
        )

    observed = []
    reset_seen = False
    full_completed_ns = None
    received_changes = Counter()
    for event in collected:
        if event["event"] == "rtr":
            if mode == "full":
                if event["after"] in ("Transport-Error", "Fatal-Protocol-Error", "Down"):
                    raise RuntimeError(f"RTR failed during full refresh: {event['after']}")
                if event["after"] == "Resetting":
                    reset_seen = True
                    full_completed_ns = None
                if reset_seen and event["before"] == "Sync-Running" and event["after"] == "Established":
                    full_completed_ns = event["observed_ns"]
            continue
        if mode == "full":
            key = (event["action"], event["prefix"], event["max_length"], event["asn"])
            received_changes[key] += 1
            if received_changes[key] > expected[key]:
                raise RuntimeError(f"Unexpected or duplicate full-refresh VRP change: {key}")
        observed.append(event)

    if mode == "full":
        if full_completed_ns is None:
            raise RuntimeError(f"Batch {info['number']} did not complete an observed RTR reset")
        if received_changes != expected:
            missing = expected - received_changes
            raise RuntimeError(f"Batch {info['number']} is missing {sum(missing.values())} expected VRP changes")
        elapsed_ms = (full_completed_ns - submitted_ns) / 1e6
        print(
            f"Batch {info['number']}: full RTR synchronization observed after {elapsed_ms:.3f} ms; "
            "VRP event counts below describe route changes, not transmitted prefixes",
            flush=True,
        )
    observed.sort(key=lambda event: event["observed_ns"])
    measurement = dict(
        write_start_ns=submission["write_start_ns"], submitted_ns=submitted_ns,
        first_observed_ns=observed[0]["observed_ns"] if observed else None,
        last_observed_ns=observed[-1]["observed_ns"] if observed else None,
        full_completed_ns=full_completed_ns,
    )
    if not observed:
        print(f"Batch {info['number']}: no VRP changes observed", flush=True)
        return measurement

    first_ms = (observed[0]["observed_ns"] - submitted_ns) / 1e6
    last_ms = (observed[-1]["observed_ns"] - submitted_ns) / 1e6
    print(
        f"Batch {info['number']}: {len(observed)} VRP events; "
        f"first observed {first_ms:.3f} ms; last observed {last_ms:.3f} ms",
        flush=True,
    )
    for event in observed:
        delay_ms = (event["observed_ns"] - submitted_ns) / 1e6
        print(
            f"  {event['action']} {event['prefix']} maxLength={event['max_length']} "
            f"AS{event['asn']}; observed after {delay_ms:.3f} ms",
            flush=True,
        )

    return measurement


# Use the caller's measurement file or retain a temporary trace only on failure.
@contextmanager
def measurement_log(mode, path):
    temporary = path is None
    if temporary:
        with tempfile.NamedTemporaryFile(prefix=f"exp61-bird-{mode}-", suffix=".log", delete=False) as trace:
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


# Initialize one isolated baseline, then replay its selected VRP batches.
def run(
    sequence, batches, socket_path, fifo, quiet_seconds, timeout,
    mode="incremental", bin_dir=None, verbose_shutdown=False, bird_trace_log=None, bgp=None,
):
    if mode not in ("incremental", "full"):
        raise ValueError("Mode must be incremental or full")
    if socket_path.resolve() != (CONFIG / "bird.ctl").resolve():
        raise ValueError("The BIRD socket must belong to this initialization")
    if fifo.resolve() != (CONFIG / "brio.in").resolve():
        raise ValueError("The BRIO FIFO must belong to this initialization")

    directory, metadata, operations = load_sequence(sequence)
    if batches is not None:
        if not 1 <= batches <= len(metadata):
            raise ValueError(f"Batches must be between 1 and {len(metadata)}")
        metadata = metadata[:batches]

    manifest = json.loads((directory / "sequence.json").read_text())
    initial_vrp = (directory / manifest["initial"]).resolve()
    final_vrps = set(load_vrp_text(initial_vrp))
    command = [
        sys.executable, "-u", "-m", "rov.dut.initialize", "--inside",
        "--bgp", str(Path(bgp or BGP_DIR).resolve()), "--vrp", str(initial_vrp),
    ]
    binaries = Path(bin_dir or ROOT / "software/bin-brio-first-vrp").resolve()
    brio_binary = (binaries / "brio_rc").read_bytes()
    if any(marker not in brio_binary for marker in (b"brio-first-vrp-v1", b"brio-control-send-v1")):
        raise RuntimeError("BRIO must record Prefix PDU and control PDU send timestamps")
    command.extend(["--bin-dir", str(binaries)])
    if verbose_shutdown:
        command.append("--verbose-shutdown")
    with measurement_log(mode, bird_trace_log) as trace_path:
        if trace_path is not None:
            command.extend(["--bird-trace-log", str(trace_path)])
        print(f"Experiment 61 mode: {mode}; initializing a fresh baseline.", flush=True)
        brio_trace = Path(str(trace_path).removesuffix(".bird.log") + ".brio.jsonl")
        with brio_trace.open("x"):
            pass
        print(f"BRIO timing log: {brio_trace}", flush=True)
        initialization = subprocess.Popen(
            command, stdout=subprocess.PIPE,
            env=dict(os.environ, **{MANAGED_OUTPUT: "1", "BRIO_VRP_TIMING_LOG": str(brio_trace)}),
        )
        output_messages = queue.Queue()
        output_thread = threading.Thread(
            target=forward_initialization_output,
            args=(initialization, output_messages),
            name="exp61-clean-initialization-output",
        )
        try:
            output_thread.start()
            wait_for_initialization(initialization, output_messages)
            events, errors, stop, thread = start_log_observer(
                socket_path, timeout, initialization, output_messages, mode, trace_path,
            )
            print("Observer ready.", flush=True)
            try:
                for info in metadata:
                    with batch_for_mode(directory, info, mode) as selected:
                        if mode == "full":
                            evidence = begin_full_sync(trace_path, socket_path, CONFIG / "bird.pid", timeout)
                        for operation in operations[info["number"]]:
                            key = normalize(*operation["vrp"])
                            if operation["operation"] == "add":
                                final_vrps.add(key)
                            else:
                                final_vrps.remove(key)
                        before = read_records(brio_trace)
                        measurement = measure_batch(
                            directory, selected, events, errors, fifo, quiet_seconds, timeout,
                            initialization, output_messages, mode,
                            expected_full_changes(operations[info["number"]]) if mode == "full" else None,
                        )
                        timing = correlate(
                            before, read_records(brio_trace), measurement, mode, info["number"]
                        )
                        timing.update(correlate_control_records(
                            brio_trace, measurement, response=timing.get("brio_response"),
                            pdu_type=8 if mode == "full" else 0,
                        ))
                        print("BRIO timing: " + json.dumps(timing), flush=True)
                        if timing["brio_timing_status"] not in ("complete", "no_vrp"):
                            raise RuntimeError("No verified BRIO Prefix PDU send origin is available")
                        if timing["control_timing_status"] != "complete":
                            raise RuntimeError("No verified BRIO update control PDU send timestamp is available")
                        print("Primary VRP timing: " + json.dumps(dict(
                            reference="brio_first_prefix_pdu_send",
                            first_vrp_ms=timing["first_from_brio_ms"],
                            last_vrp_ms=timing["last_from_brio_ms"],
                            full_sync_ms=timing["full_sync_from_brio_ms"],
                        )), flush=True)
                        if mode == "full":
                            verification = finish_full_sync(
                                trace_path, socket_path, CONFIG / "bird.pid", timeout, evidence, final_vrps,
                            )
                            print("Full synchronization verification: " + json.dumps(verification), flush=True)
                        else:
                            table = verify_vrp_table(socket_path, timeout, final_vrps)
                            print("VRP table verification: " + json.dumps(table), flush=True)
                            if not table["correct"]:
                                raise RuntimeError("Complete installed VRP set differs after incremental update")
            finally:
                stop_observer(stop, thread, timeout)
            check_initialization(initialization, output_messages)
            if not errors.empty():
                raise errors.get()
        finally:
            initialization_exit = stop_initialization(initialization, output_thread)
        if initialization_exit != 0:
            raise RuntimeError(f"Initialization cleanup failed ({initialization_exit})")


# Execute only the internally supervised managed worker.
def _worker_main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sequence", required=True, type=Path)
    parser.add_argument("--bgp", type=Path)
    parser.add_argument("--batches", type=int)
    parser.add_argument("--managed", action="store_true",
                        help="Initialize owned services and use the managed incremental/full workflow")
    parser.add_argument("--mode", choices=("incremental", "full"), default="incremental")
    parser.add_argument("--bin-dir", type=Path, help="Executable directory passed to initialization")
    parser.add_argument("--socket", type=Path, default=CONFIG / "bird.ctl")
    parser.add_argument("--fifo", type=Path, default=CONFIG / "brio.in")
    parser.add_argument("--quiet-seconds", type=float,
                        help="Seconds without new events before completion (default: 10)")
    parser.add_argument("--timeout", type=float, default=300)
    parser.add_argument(
        "--bird-trace-log", type=Path,
        help="New BIRD measurement file; otherwise use a temporary file, retained on failure",
    )
    parser.add_argument(
        "--verbose-shutdown", action="store_true",
        help="Display normal software shutdown output as well as errors",
    )
    parser.add_argument("--inside", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.quiet_seconds is None:
        args.quiet_seconds = 10
    if not 0 < args.quiet_seconds < args.timeout < float("inf"):
        parser.error("Quiet seconds and timeout must be finite, positive, and quiet < timeout")
    if not os.environ.get(MANAGED_OUTPUT):
        return supervise(
            "rov.experiments.exp61", sys.argv[1:],
            verbose_shutdown=args.verbose_shutdown,
        )
    if not args.inside:
        enter_namespace(sys.argv[1:], module="rov.experiments.exp61")
    run(
        args.sequence, args.batches, args.socket, args.fifo, args.quiet_seconds,
        args.timeout, args.mode, args.bin_dir, args.verbose_shutdown, args.bird_trace_log,
        **({"bgp": args.bgp} if args.bgp is not None else {}),
    )


RESULTS_MARKER = ".exp61-results"


# Validate each prepared unit before starting any experiment in the selection.
def check_sequence(sequence):
    try:
        if sequence.is_symlink():
            raise ValueError("The sequence manifest must not be a symbolic link")
        manifest = json.loads(sequence.read_text())
        directory, batches, operations = load_sequence(sequence.parent)
        if len(batches) != 1 or batches[0]["number"] != 1:
            raise ValueError("Expected exactly one batch numbered 1")
        if not (directory / manifest["initial"]).is_file():
            raise ValueError("Missing initial VRP file")
        full_batch_commands(directory, batches[0])
        expected_full_changes(operations[1])
    except (OSError, ValueError, KeyError, TypeError, IndexError, AttributeError) as error:
        raise ValueError(f"Invalid experiment input {sequence}: {error}") from error


# Discover manifest units in stable relative-path order, excluding archived output.
def discover_sequences(directory):
    directory = Path(directory).resolve(strict=True)
    for parent in (directory, *directory.parents):
        if parent.name in ("results", "logs"):
            raise ValueError(f"Experiment results cannot be used as input: {directory}")
        marker = parent / RESULTS_MARKER
        if marker.exists() or marker.is_symlink():
            if marker.is_symlink() or marker.read_text() != "exp61-results-v1\n":
                raise ValueError(f"Invalid experiment results marker: {marker}")
            raise ValueError(f"Experiment results cannot be used as input: {directory}")
    if directory.is_file():
        if directory.name != "sequence.json":
            raise ValueError(f"Expected a sequence.json file: {directory}")
        check_sequence(directory)
        return [directory]
    if not directory.is_dir():
        raise ValueError(f"Expected an experiment directory: {directory}")
    sequences = []

    # Do not hide permission or filesystem errors in an apparently partial selection.
    def walk_error(error):
        raise error

    for current, children, files in os.walk(directory, onerror=walk_error):
        current = Path(current)
        if RESULTS_MARKER in files:
            marker = current / RESULTS_MARKER
            if marker.is_symlink() or marker.read_text() != "exp61-results-v1\n":
                raise ValueError(f"Invalid experiment results marker: {marker}")
            if current == directory or "sequence.json" in files:
                raise ValueError(f"Experiment results cannot be used as input: {current}")
            children[:] = []
            continue
        if "sequence.json" in children:
            raise ValueError(f"Expected a sequence.json file: {current / 'sequence.json'}")
        retained = []
        for name in sorted(children):
            child = current / name
            if name in ("results", "logs"):
                continue
            if child.is_symlink():
                raise ValueError(f"Symbolic-link directory in experiment inputs: {child}")
            retained.append(name)
        children[:] = retained
        sequence = current / "sequence.json"
        if "sequence.json" in files:
            check_sequence(sequence)
            sequences.append(sequence)
        elif (
            re.fullmatch(r"run-\d+", current.name)
            or "events.jsonl" in files
            or any(name.endswith(".script") for name in files)
        ):
            raise ValueError(f"Missing sequence.json in experiment input: {current}")
    if not sequences:
        raise ValueError(f"No experiment sequence.json files found under {directory}")
    return sorted(sequences, key=lambda path: path.relative_to(directory).as_posix())


# Ask the isolated experiment process group to stop.
def stop_child(process):
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass


# Route external termination through the owned child's cleanup and output drain.
def stop_requested(signum, frame):
    raise KeyboardInterrupt


# Resolve the scale directory without requiring an input run directory.
def scale_directory(sequence):
    directory = sequence.parent
    if directory.name == "sequence" or re.fullmatch(r"run-\d+", directory.name):
        return directory.parent
    return directory


# Run one clean child, draining its output and cleanup before returning its status.
def run_experiment(
    sequence, mode, log_path, status_path, trace_path=None, *, bin_dir=None,
    timeout=None, verbose_shutdown=False, bgp=None,
):
    command = [
        sys.executable, "-u", "-m", "rov.experiments.exp61", "--worker", "--managed",
        "--sequence", str(sequence.parent), "--mode", mode,
    ]
    if bgp is not None:
        command.extend(["--bgp", str(Path(bgp).resolve())])
    if trace_path is not None:
        command.extend(["--bird-trace-log", str(trace_path)])
    if bin_dir is not None:
        command.extend(["--bin-dir", str(Path(bin_dir).resolve())])
    if timeout is not None:
        command.extend(["--timeout", str(timeout)])
    if verbose_shutdown:
        command.append("--verbose-shutdown")
    capture_options = {"verbose_shutdown": True} if verbose_shutdown else {}
    interrupted = False
    capture_failed = False
    exit_code = None
    with log_path.open("xb") as log:
        print(f"Complete run log: {log_path.resolve()}", flush=True)
        try:
            process = subprocess.Popen(
                command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                env=dict(os.environ, **{MANAGED_OUTPUT: "1"}),
                cwd=ROOT, start_new_session=True,
            )
            try:
                capture_failed = copy_output(process, log, **capture_options)
                exit_code = process.wait()
            except KeyboardInterrupt:
                interrupted = True
                previous_int = signal.signal(signal.SIGINT, signal.SIG_IGN)
                previous_term = signal.signal(signal.SIGTERM, signal.SIG_IGN)
                try:
                    stop_child(process)
                    capture_failed = copy_output(process, log, **capture_options)
                    exit_code = process.wait()
                finally:
                    signal.signal(signal.SIGINT, previous_int)
                    signal.signal(signal.SIGTERM, previous_term)
            except OSError as error:
                capture_failed = True
                print(f"Output capture failed: {error}", file=sys.stderr, flush=True)
                stop_child(process)
                process.stdout.close()
                process.stderr.close()
                exit_code = process.wait()
            finally:
                if process.poll() is None:
                    stop_child(process)
                    process.stdout.close()
                    process.stderr.close()
                    process.wait()
                else:
                    process.stdout.close()
                process.stderr.close()
        except OSError as error:
            message = f"Could not capture experiment output: {error}\n"
            log.write(message.encode())
            log.flush()
            print(message, end="", file=sys.stderr)

    print(f"Complete run log: {log_path.resolve()}", flush=True)
    if interrupted:
        status = "interrupted"
    elif capture_failed:
        status = "capture_error"
    elif exit_code is None:
        status = "launch_error"
    elif exit_code == 0:
        status = "success"
    else:
        status = "nonzero"
    with status_path.open("x") as output:
        json.dump({
            "scale": scale_directory(sequence).name,
            "run": status_path.stem,
            "mode": mode,
            **({"full_sync_method": "cache-reset-query-v1"} if mode == "full" else {}),
            "sequence": str(sequence.resolve()),
            **({"bgp": str(Path(bgp).resolve())} if bgp is not None else {}),
            "status": status,
            "exit_code": exit_code,
            "bin_dir": str(Path(bin_dir or ROOT / "software/bin-brio-first-vrp").resolve()),
            "brio_timing_method": "prefix-and-control-send-monotonic-v1",
            "time_reference": "BRIO first Prefix PDU send; FIFO timestamps retained separately",
        }, output)
        output.write("\n")

    if status != "success":
        print(f"Experiment result: {status}; exit code: {exit_code}", file=sys.stderr)
    if interrupted:
        return 130
    if status == "capture_error" or exit_code is None:
        return 1
    return exit_code if exit_code >= 0 else 128 - exit_code


# Reject invalid options before creating output or starting a child.
def check_options(timeout):
    if timeout is not None and not 10 < timeout < float("inf"):
        raise ValueError("Timeout must be finite and greater than the 10-second quiet window")


# Write each prepared run directly into the selected result directory.
def result_paths(sequence, mode, results_dir=None, run_name=None):
    scale = scale_directory(sequence)
    prepared_name = sequence.parent.name
    run = run_name or (prepared_name if re.fullmatch(r"run-\d+", prepared_name) else "run-01")
    if re.fullmatch(r"run-\d+", run) is None:
        raise ValueError(f"Invalid result run name: {run}")
    output = scale / "results"
    if results_dir is not None:
        output = Path(results_dir).resolve()
    if mode == "full":
        output /= "full"
    return (
        output / f"{run}.log",
        output / f"{run}.json",
        output / f"{run}.bird.log",
    )


# Reject existing artifacts and directory aliases without changing earlier evidence.
def check_result_paths(sequence, log, status, trace):
    results = log.parent.parent if log.parent.name == "full" else log.parent
    for directory in dict.fromkeys((results, log.parent)):
        if directory.is_symlink():
            raise ValueError(f"The fixed results directory must not be a symbolic link: {directory}")
        if directory.exists() and not directory.is_dir():
            raise NotADirectoryError(f"Expected a results directory: {directory}")
    brio_trace = Path(str(trace).removesuffix(".bird.log") + ".brio.jsonl") if trace else None
    for path in (log, status, trace, brio_trace):
        if path is not None and (path.exists() or path.is_symlink()):
            raise FileExistsError(f"Result already exists: {path}")


# Preflight every output before starting any child, then finish each unit in order.
def run_sequences(
    sequences, modes, *, bin_dir=None, results_dir=None, timeout=None,
    verbose_shutdown=False, bgp=None, run_name=None,
):
    if bgp is None and any(
        re.fullmatch(r"run-\d+", sequence.parent.name) is None
        for sequence in sequences
    ):
        raise ValueError("Shared sequence inputs require an explicit --bgp baseline")
    outputs = [
        (sequence, mode, *result_paths(sequence, mode, results_dir, run_name))
        for sequence in sequences for mode in modes
    ]
    if len({log for _, _, log, _, _ in outputs}) != len(outputs):
        raise ValueError(
            "Selected inputs would write to the same result files; "
            "run each scale with its own --results-dir"
        )
    for sequence, mode, log, status, trace in outputs:
        check_result_paths(sequence, log, status, trace)
    print(f"Experiment units: {len(sequences)}; results: {results_dir or '<scale>/results'}", flush=True)
    for sequence, mode, log, status, trace in outputs:
        check_result_paths(sequence, log, status, trace)
        log.parent.mkdir(parents=True, exist_ok=True)
        print(f"Running {sequence.parent}: {mode}", flush=True)
        code = run_experiment(
            sequence, mode, log, status, trace, bin_dir=bin_dir,
            timeout=timeout, verbose_shutdown=verbose_shutdown, bgp=bgp,
        )
        if code != 0:
            return code
    return 0


# Finish both independent modes of every discovered unit before moving to the next.
def run_directory(directory, *, bin_dir=None, results_dir=None, timeout=None, verbose_shutdown=False, bgp=None):
    check_options(timeout)
    sequences = discover_sequences(directory)
    return run_sequences(
        sequences, ("incremental", "full"), bin_dir=bin_dir,
        results_dir=results_dir, timeout=timeout, verbose_shutdown=verbose_shutdown, bgp=bgp,
    )


# Run one prepared input under the requested result repetition number.
def run_one(
    scale, run, *, mode=None, bin_dir=None,
    results_dir=None, timeout=None, verbose_shutdown=False, bgp=None,
):
    check_options(timeout)
    if run < 1:
        raise ValueError("Select a positive run number")
    if mode not in (None, "incremental", "full"):
        raise ValueError("Mode must be incremental or full")
    selected = Path(scale)
    if selected.exists():
        sequences = discover_sequences(selected)
        if len(sequences) != 1:
            raise ValueError("Select one prepared sequence for an explicit run number")
        sequence = sequences[0]
    else:
        raise ValueError(f"Prepared input does not exist: {scale}")
    modes = (mode,) if mode is not None else ("incremental", "full")
    return run_sequences(
        [sequence], modes, bin_dir=bin_dir, results_dir=results_dir,
        timeout=timeout, verbose_shutdown=verbose_shutdown, bgp=bgp,
        run_name=f"run-{run:02d}",
    )


# Parse an explicit prepared input and its optional repetition number.
def main():
    if "--worker" in sys.argv[1:]:
        return _worker_main()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", help="Prepared sequence file or directory, with an optional repetition number")
    parser.add_argument("--bgp", type=Path, help="Converted baseline BGP directory; default: data/baseline/bgp")
    parser.add_argument(
        "run", nargs="?", type=int, metavar="RUN",
        help="Result repetition number for one prepared input; does not select new data",
    )
    parser.add_argument(
        "--mode", choices=("incremental", "full"),
        help="With RUN: execute one mode; otherwise incremental then full",
    )
    parser.add_argument("--bin-dir", type=Path, help="Executable directory passed to initialization")
    parser.add_argument(
        "--results-dir", type=Path,
        help="Exact result directory; only full mode adds a full/ subdirectory; default: <scale>/results",
    )
    parser.add_argument("--timeout", type=float, help="Batch timeout; preserves the existing quiet window")
    parser.add_argument(
        "--verbose-shutdown", action="store_true",
        help="Display normal software shutdown output; complete output is always logged",
    )
    args = parser.parse_args()
    if args.run is None and args.mode is not None:
        parser.error("Runs without an explicit RUN always execute incremental then full")
    try:
        options = dict(
            bin_dir=args.bin_dir, results_dir=args.results_dir,
            timeout=args.timeout, verbose_shutdown=args.verbose_shutdown,
            **({"bgp": args.bgp} if args.bgp is not None else {}),
        )
        if args.run is not None:
            return run_one(args.directory, args.run, mode=args.mode, **options)
        return run_directory(args.directory, **options)
    except (OSError, ValueError, KeyError, json.JSONDecodeError) as error:
        parser.exit(1, f"{error}\n")


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, stop_requested)
    raise SystemExit(main())
