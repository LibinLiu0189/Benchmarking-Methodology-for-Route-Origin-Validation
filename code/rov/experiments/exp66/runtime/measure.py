"""Own experiment 66's update schedule, quiet termination and final checks."""

import json
import math
import queue
import time

from rov.paths import CONFIG
from rov.data.sequence import load_sequence
from rov.data.predictions import load_rov_targets, load_rib_targets, load_fib_targets, rib_path
from rov.measure.cli import read_primary_rib
from rov.measure.forwarding import ForwardingObserver
from rov.measure.vrp_streams import ObservationState, stop_observer
from rov.experiments.exp66.runtime.observe import (
    start_log_observer, start_rov_observer, wait_observers_ready, receive_event,
    process_observations,
)
from rov.send.send_vrp import submit_with_backpressure


# Compare every prepared candidate against the observed initial ROA-query state.
def check_initial_rov(initial, targets):
    for target in targets.values():
        actual = initial[target["prefix"]][target["peer"]]["rov"]
        if actual != target["initial"]:
            raise RuntimeError(
                f"Initial ROV mismatch for {target['prefix']} {target['peer']}: "
                f"expected {target['initial']}, observed {actual}"
            )


# Compare the selected-path baseline after timed collection has stopped.
def check_initial_rib(initial, actual):
    mismatches = [
        (prefix, path, actual[prefix])
        for prefix, path in initial.items()
        if actual[prefix] != path
    ]
    if mismatches:
        prefix, expected, observed = mismatches[0]
        raise RuntimeError(
            f"Initial RIB mismatch for {prefix}: expected {expected}, observed {observed}; "
            f"{len(mismatches)} mismatched prefixes"
        )


# Verify final touched states after quiet; this never extends the collection timer.
def final_verification(state, directory, socket_path, timeout):
    from rov.data.vrp import load_vrp_text
    from rov.measure.vrp_state import build_vrp_expectation
    from rov.measure.vrp_state import verify_final
    if not state.submitted:
        return dict(correct=False, mismatches=[], errors=["No submitted workload"], scope="Not verified")
    metadata = json.loads((directory / "sequence.json").read_text())
    initial = load_vrp_text(directory / metadata["initial"])
    submitted = {number for number, _ in state.submitted}
    operations = [row for number, rows in state.vrp.operations.items() if number in submitted for row in rows]
    expected = build_vrp_expectation(initial, operations)
    rovs = {}
    for number, prefix, _, origin, _, after in state.rov.transitions:
        if number in submitted:
            rovs[(prefix, origin)] = after
    result = verify_final(socket_path, timeout, expected, rovs)
    expected_rib = dict(state.rib.initial)
    for number, rows in state.rib.changes.items():
        if number in submitted:
            expected_rib.update((prefix, after) for prefix, _, after in rows)
    try:
        actual = read_primary_rib(socket_path, timeout)
        result["rib_checked"] = len(expected_rib)
        for prefix, wanted in expected_rib.items():
            observed = rib_path(actual.get(prefix))
            if observed != wanted:
                result["mismatches"].append(dict(kind="rib", prefix=prefix, expected=wanted, actual=observed))
    except (OSError, ValueError, RuntimeError) as error:
        result["errors"].append(f"RIB verification: {error}")
    result["correct"] = not result["mismatches"] and not result["errors"]
    result["fib_final_scope"] = "Forwarding observations determine quiet termination; no final FIB verification"
    return result


# Submit every batch on schedule, then await all four observation streams.
def run(
    sequence, predictions, path_predictions, socket_path, fifo,
    interval, poll_interval, timeout, config_dir=CONFIG,
    pps=10000, packet_timeout=1, quiet_seconds=10,
    *, log_path, result, raw_output, check_alive,
):
    if not math.isfinite(interval) or interval <= 0:
        raise ValueError("Interval must be a finite positive number")
    if not math.isfinite(poll_interval) or poll_interval < 0:
        raise ValueError("Poll interval must be finite and nonnegative")
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("Timeout must be a finite positive number")
    if not math.isfinite(pps) or pps <= 0 or not math.isfinite(packet_timeout) or packet_timeout <= 0:
        raise ValueError("Packet rate and timeout must be finite and positive")
    if not math.isfinite(quiet_seconds) or quiet_seconds <= 0:
        raise ValueError("Quiet period must be finite and positive")

    directory, batches, operations = load_sequence(sequence)
    targets, transitions = load_rov_targets(directory, batches, predictions)
    if not targets:
        raise ValueError("Section 6.6 requires at least one ROV candidate")
    rib_changes, rib_initial = load_rib_targets(path_predictions, batches)
    fib_changes, fib_unavailable = load_fib_targets(path_predictions, batches)
    state = ObservationState(
        operations, targets, transitions, rib_changes, rib_initial,
        fib_changes, fib_unavailable,
    )

    state.raw_output = raw_output
    state.collected = []
    state.log_drained = False
    stop, thread = start_log_observer(state, socket_path, timeout, log_path)
    result.update(time_reference="FIFO write start to external observation", status="failed")
    rov_observer = None
    fib_observer = None
    initial_rov = None
    initial_rib = None
    cutoff_ns = None
    quiet_change_ns = None
    failure = None
    try:
        rov_observer = start_rov_observer(
            state.rov.targets, socket_path, poll_interval, timeout,
            state.events, state.errors,
        )
        if state.fib.destinations:
            fib_observer = ForwardingObserver(
                state.fib.destinations, config_dir, pps, packet_timeout,
                state.events, state.errors,
            )
            fib_observer.start(timeout)
        initial_rov = wait_observers_ready(state, timeout)

        actual_rib = read_primary_rib(socket_path, timeout)
        initial_rib = {
            prefix: rib_path(actual_rib.get(prefix)) for prefix in state.rib.initial
        }
        del actual_rib
        state.rib.current = initial_rib.copy()
        while True:
            try:
                state.pending.append(state.events.get_nowait())
            except queue.Empty:
                break
        if any(event["event"] == "rib" for event in state.pending):
            raise RuntimeError("RIB changed while the initial selected table was being read")

        state.started_ns = time.monotonic_ns()
        started = state.started_ns / 1e9
        print("VRP, ROV, RIB, and FIB observers ready.", flush=True)
        for index, batch in enumerate(batches):
            if check_alive is not None:
                check_alive()
            target = started + index * interval
            while True:
                remaining = target - time.monotonic()
                if remaining <= 0:
                    break
                receive_event(state, min(0.1, remaining))

            if not state.errors.empty():
                raise state.errors.get()
            try:
                submission = submit_with_backpressure(
                    directory / batch["script"], fifo, timeout,
                    lambda seconds: receive_event(state, seconds), check_alive,
                )
            except (OSError, ValueError, RuntimeError) as error:
                raise RuntimeError(
                    f"Batch {batch['number']} submission failed: {error}"
                ) from error

            state.submitted.append((batch["number"], submission["write_start_ns"]))
            state.submission_records.append(dict(batch=batch["number"], operations=batch["total"],
                                                 scheduled_ns=int(target * 1e9), **submission))
            lag_ms = max(0.0, (submission["write_start_ns"] / 1e9 - target) * 1000)
            print(
                f"Batch {batch['number']}: submitted; schedule lag {lag_ms:.1f} ms",
                flush=True,
            )

        result["last_submission_ns"] = time.monotonic_ns()
        state.last_change_ns = time.monotonic_ns()
        deadline = time.monotonic() + timeout
        while True:
            if check_alive is not None:
                check_alive()
            quiet_remaining = quiet_seconds - (
                time.monotonic_ns() - state.last_change_ns
            ) / 1e9
            if quiet_remaining <= 0 and state.log_drained:
                if state.pending or not state.events.empty():
                    receive_event(state, 0)
                    continue
                quiet_change_ns = state.last_change_ns
                cutoff_ns = time.monotonic_ns()
                break
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("Timed out waiting for observation changes to settle")
            wait = min(0.1, remaining)
            if quiet_remaining > 0:
                wait = min(wait, quiet_remaining)
            receive_event(state, wait)
    except BaseException as error:
        failure = error
    finally:
        if cutoff_ns is None:
            cutoff_ns = time.monotonic_ns()
        stop.set()
        if rov_observer is not None:
            rov_observer[0].set()
        if fib_observer is not None:
            fib_observer.stop.set()
        try:
            stop_observer(stop, thread, timeout)
        except BaseException as error:
            if failure is None:
                failure = error
        if rov_observer is not None:
            try:
                stop_observer(rov_observer[0], rov_observer[1], timeout)
            except BaseException as error:
                if failure is None:
                    failure = error
        if fib_observer is not None:
            try:
                fib_observer.close(timeout)
            except BaseException as error:
                if failure is None:
                    failure = error

        if not thread.is_alive() and (
            rov_observer is None or not rov_observer[1].is_alive()
        ) and (fib_observer is None or not fib_observer.thread.is_alive()):
            try:
                while receive_event(state, 0, check_errors=False, cutoff_ns=cutoff_ns):
                    pass
            except BaseException as error:
                if failure is None:
                    failure = error

        # A retained in-flight change must not invalidate the declared silence.
        if failure is None and quiet_change_ns is not None and (
            state.last_change_ns != quiet_change_ns
        ):
            failure = RuntimeError(
                "Observation changes arrived while stopping after the quiet interval"
            )

        result["collection_end_ns"] = cutoff_ns
        if failure is None and not state.errors.empty():
            failure = state.errors.get()
        result["termination"] = (
            "quiet" if failure is None
            else "timeout" if isinstance(failure, TimeoutError)
            else "error"
        )

        process_observations(state, initial_rib or {})
        try:
            if initial_rov is not None:
                check_initial_rov(initial_rov, state.rov.targets)
            if initial_rib is not None:
                check_initial_rib(state.rib.initial, initial_rib)
        except BaseException as error:
            if failure is None:
                failure = error

        result["rtr_failures"] = [event for event in state.rtr_events if event.get("after") in ("Down", "Transport-Error", "Fatal-Protocol-Error")]
        result["unobservable"] = dict(internal_queue="No DUT queue counter", dropped_wire_updates="Cannot infer from missing external log events",
                                     process_crash="Owned initialization exit and raw diagnostics retained; no independent crash detector",
                                     stall="No progress alone does not distinguish idle, observer delay, or DUT stall",
                                     throttle="Submission schedule lag is recorded; no internal throttle counter")
        result.update(submissions=state.submission_records, backlog_proxy=state.backlog,
                      backlog_definition="Submitted operations minus observed VRP events; includes cache, wire, DUT and observer lag; not an internal queue",
                      anomalies=state.anomalies, rtr_events=state.rtr_events,
                      status="success" if failure is None else "failed",
                      error=None if failure is None else f"{type(failure).__name__}: {failure}")
        try:
            result["final_verification"] = final_verification(state, directory, socket_path, timeout)
        except BaseException as error:
            result["final_verification"] = dict(correct=False, errors=[f"{type(error).__name__}: {error}"])
        result["unobserved_vrp_events"] = sum((state.vrp.expected - state.vrp.observed).values())
        result["unexpected_vrp_events"] = sum((state.vrp.observed - state.vrp.expected).values())
        result["unobserved_meaning"] = "Missing observations are not proof of dropped RTR updates; inspect file integrity and final verification"
        if state.anomalies or not result["final_verification"]["correct"]:
            result["status"] = "failed"
        raw_output.flush()

    if failure is not None:
        raise failure
    return result
