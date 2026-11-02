"""Own experiment 67's update schedule, readiness checks, quiet rule and metric attribution."""

from collections import defaultdict
import gc
import json
import math
from pathlib import Path
import queue
import time

from rov.paths import CONFIG
from rov.data.sequence import load_sequence
from rov.data.predictions import load_rov_targets, load_rib_targets, load_fib_targets, rib_path
from rov.measure.cli import read_primary_rib
from rov.experiments.exp67.runtime.observe import (
    query_observations, start_observer, start_reset_rov_observer,
)
from rov.experiments.exp67.faults.recovery import match_recovery_endpoints
from rov.measure.forwarding import ForwardingObserver
from rov.measure.read_log import stop_observer
from rov.measure.vrp_streams import ObservationState
from rov.send.send_vrp import submit_with_backpressure


# Store actual events and reader progress without comparing expected outcomes.
def receive_event(state, wait, check_errors=True, cutoff_ns=None):
    if check_errors and not state.errors.empty():
        raise state.errors.get()
    if state.pending:
        event = state.pending.pop(0)
    else:
        try:
            event = state.events.get(timeout=wait)
        except queue.Empty:
            return False

    kind = event["event"]
    if cutoff_ns is not None and event["observed_ns"] > cutoff_ns:
        return True
    if kind == "drained":
        state.log_drained_ns = event["observed_ns"] if event["drained"] else 0
        return True
    if kind not in ("vrp", "rtr", "rov", "rib", "received", "not_received"):
        raise RuntimeError(f"Unexpected observation event: {event}")

    state.collected.append(event)
    state.raw_output.write(json.dumps(event, separators=(",", ":")) + "\n")
    changed = True
    if kind == "rtr":
        state.rtr_events.append(event)
    elif kind == "rib" and event["prefix"] in state.rib.current:
        prefix = event["prefix"]
        after = rib_path(event["after"])
        changed = state.rib.current[prefix] != after
        state.rib.current[prefix] = after
    if changed:
        state.last_change_ns = max(state.last_change_ns or 0, event["observed_ns"])
    return True


# Wait for actual initial query states and both producer readiness messages.
def wait_observers_ready(state, timeout):
    ready, initial = set(), {}
    deadline = time.monotonic() + timeout
    while ready != {"read_log", "cli"}:
        if not state.errors.empty():
            raise state.errors.get()
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("Timed out waiting for initial log and query observations")
        try:
            event = state.events.get(timeout=min(.1, remaining))
        except queue.Empty:
            continue
        if event["event"] == "initial":
            initial[event["prefix"]] = event["candidates"]
        elif event["event"] == "ready":
            ready.add(event["component"])
        else:
            state.pending.append(event)
    return initial


# Compare every prepared candidate against the observed initial ROA-query state.
def check_initial_rov(initial, targets):
    for target in targets.values():
        actual = initial[target["prefix"]][target["peer"]]["rov"]
        if actual != target["initial"]:
            raise RuntimeError(
                f"Initial ROV mismatch for {target['prefix']} {target['peer']}: "
                f"expected {target['initial']}, observed {actual}"
            )

# Compare the saved selected-path baseline after collection has finished.
def check_initial_rib(initial, actual):
    mismatches = [
        (prefix, path, actual.get(prefix))
        for prefix, path in initial.items()
        if actual.get(prefix) != path
    ]
    if mismatches:
        prefix, expected, observed = mismatches[0]
        raise RuntimeError(
            f"Initial RIB mismatch for {prefix}: expected {expected}, observed {observed}; "
            f"{len(mismatches)} mismatched prefixes"
        )


# Preserve conservative per-batch metrics only when attribution is complete.
def batch_metrics(state):
    slots = defaultdict(list)
    observed = defaultdict(list)
    for number, submitted_ns in state.submitted:
        for operation in state.vrp.operations[number]:
            action = "added" if operation["operation"] == "add" else "removed"
            slots[(action, *operation["vrp"])].append((number, submitted_ns))
    for key, observed_ns in state.vrp.recorded:
        observed[key].append(observed_ns)
    times = defaultdict(list)
    uncertain = set()
    for key, expected in slots.items():
        actual = observed[key]
        if len(actual) != len(expected) or any(t < start for (_, start), t in zip(expected, actual)):
            uncertain.update(number for number, _ in expected)
        else:
            for (number, _), t in zip(expected, actual):
                times[number].append(t)
    result = []
    for number, start in state.submitted:
        row = dict(batch=number, vrp_operations=len(state.vrp.operations[number]), metrics={})
        vrp = times[number]
        if number not in uncertain and vrp:
            if getattr(state, 'disconnect', False):
                row['observed_endpoints_ns'] = dict(last_vrp=max(vrp))
            row["metrics"].update(first_vrp_ms=(min(vrp) - start) / 1e6,
                                  last_vrp_ms=(max(vrp) - start) / 1e6,
                                  vrp_span_ms=(max(vrp) - min(vrp)) / 1e6)
        for label, matches, count in (
            ("roa_query", state.rov.matched, state.rov.counts[number]),
            ("rib", state.rib.matched, len(state.rib.changes[number])),
            ("fib_probe", state.fib.matched,
             sum(entry[0] == number for rows in state.fib.changes.values() for entry in rows)),
        ):
            matched = [t for batch, t in matches if batch == number]
            row[label + "_observed"] = len(matched)
            row[label + "_expected"] = count
            if count and len(matched) == count:
                row["metrics"][label + "_complete_ms"] = (max(matched) - start) / 1e6
                if label == 'roa_query' and getattr(state, 'disconnect', False):
                    row.setdefault('observed_endpoints_ns', {})['roa_query'] = max(matched)
                if label == "roa_query" and number not in uncertain and vrp:
                    delta = (max(matched) - max(vrp)) / 1e6
                    row["metrics"]["post_install_query_ms"] = delta if delta >= 0 else None
        if "rib_complete_ms" in row["metrics"] and "fib_probe_complete_ms" in row["metrics"]:
            row["metrics"]["rib_and_sampled_fib_ms"] = max(
                row["metrics"]["rib_complete_ms"], row["metrics"]["fib_probe_complete_ms"])
        result.append(row)
    return result


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
    expected_fib = {}
    for changes in state.fib.changes.values():
        for number, destination, _, after in changes:
            if number in submitted:
                expected_fib[destination] = ("not_received", None) if after is None else ("received", after)
    for destination, wanted in expected_fib.items():
        observed = state.fib.current.get(destination)
        if observed != wanted:
            result["mismatches"].append(dict(
                kind="fib", destination=destination, expected=wanted, actual=observed,
            ))
    result["correct"] = not result["mismatches"] and not result["errors"]
    result["fib_final_scope"] = "Previously collected sampled forwarding only; no mandatory new probe round"
    return result


# Interpret retained events and reconstruct diagnostics only after every producer stops.
def process_observations(state, initial_rib):
    state.rib.current = initial_rib.copy()
    for event in sorted(state.collected, key=lambda row: row["observed_ns"]):
        kind, observed_ns = event["event"], event["observed_ns"]
        if observed_ns - state.last_backlog_ns >= 100_000_000:
            submitted = sum(len(state.vrp.operations[number])
                            for number, started_ns in state.submitted if started_ns <= observed_ns)
            state.backlog.append(dict(
                observed_ns=observed_ns, submitted_operations=submitted,
                observed_events=len(state.vrp.recorded),
                unobserved_operations=max(0, submitted - len(state.vrp.recorded)),
            ))
            state.last_backlog_ns = observed_ns
        if kind == "vrp":
            key = (event["action"], event["prefix"], event["max_length"], event["asn"])
            state.vrp.observed[key] += 1
            state.vrp.recorded.append((key, observed_ns))
            if state.vrp.observed[key] > state.vrp.expected[key]:
                state.anomalies.append(f"Unexpected VRP event: {key}")
        elif kind == "rov":
            state.rov.observed[(event["prefix"], event["origin_as"])].append(event)
        elif kind == "rib" and event["prefix"] in state.rib.current:
            prefix = event["prefix"]
            before, after = state.rib.current[prefix], rib_path(event["after"])
            state.rib.observed[prefix].append((before, after, observed_ns))
            if before == after:
                continue
            action = event["action"]
            if (action == "added" and before is not None) or (
                action in ("removed", "replaced") and before is None
            ) or (action == "removed" and after is not None):
                state.anomalies.append(f"Invalid RIB {action} for {prefix}")
            state.rib.current[prefix] = after
        elif kind in ("received", "not_received"):
            state.fib.record(event)
    state.rtr_events.sort(key=lambda row: row["observed_ns"])


# Match forwarding state transitions at their actual observation times.
def match_forwarding(state, submitted):
    submitted_ns = dict(submitted)
    state.matched = []
    for predictions in state.changes.values():
        previous_ns = -1
        positions = defaultdict(int)
        for number, destination, before, after in predictions:
            if number not in submitted_ns:
                break
            samples = state.observed[destination]
            before_outcome = ("not_received", None) if before is None else ("received", before)
            after_outcome = ("not_received", None) if after is None else ("received", after)
            start = None
            for index in range(positions[destination], len(samples)):
                if samples[index][2] <= previous_ns:
                    start = index if samples[index][0] == before_outcome else None
                elif start is None and samples[index][0] == before_outcome:
                    start = index
                    break
            if start is None:
                break
            end = next((index for index in range(start + 1, len(samples))
                        if samples[index][0] == after_outcome
                        and samples[index][2] >= submitted_ns[number]
                        and samples[index][2] >= previous_ns), None)
            if end is None:
                break
            state.matched.append((number, samples[end][2]))
            previous_ns = samples[end][2]
            positions[destination] = end


# Require this run's protocol completion independently of prediction correctness.
def protocol_recovered(state, fault_lab):
    if fault_lab is not None:
        return fault_lab.recovered(state)
    if not state.submission_records:
        return False
    boundary = state.submission_records[-1]["write_start_ns"]
    return any(row.get("protocol", "rpki_source") == "rpki_source"
               and row.get("after") == "Established" and row["observed_ns"] >= boundary
               for row in state.rtr_events)


# Submit every batch on schedule, then await all four observation streams.
def run(
    sequence, predictions, path_predictions, socket_path, fifo,
    interval, poll_interval, timeout, config_dir=CONFIG,
    pps=10000, packet_timeout=1, quiet_seconds=10,
    *, log_path, result, raw_output, check_alive, fault_lab=None, suspend_gc=False,
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
        raise ValueError("Section 6.7 requires at least one ROV candidate")
    rib_changes, rib_initial = load_rib_targets(path_predictions, batches)
    fib_changes, fib_unavailable = load_fib_targets(path_predictions, batches)
    unchanged_fib = sum(row[2] == row[3] for rows in fib_changes.values() for row in rows)
    fib_changes = {prefix: [row for row in rows if row[2] != row[3]]
                   for prefix, rows in fib_changes.items()}
    fib_changes = {prefix: rows for prefix, rows in fib_changes.items() if rows}
    result["fib_unchanged_forwarding_excluded"] = unchanged_fib
    state = ObservationState(
        operations, targets, transitions, rib_changes, rib_initial,
        fib_changes, fib_unavailable,
    )

    state.raw_output = raw_output
    state.disconnect = fault_lab is not None and fault_lab.mode == "disconnect"
    state.collected = []
    state.log_drained_ns = 0
    state.collection_end_ns = None
    stop, thread = start_observer(socket_path, timeout, state.events, state.errors, log_path)
    result.update(time_reference=("BRIO timestamp after successful socket shutdown to external observation" if state.disconnect
                                 else "Fault start to external observation" if fault_lab
                                 else "FIFO write start to external observation"), status="failed")
    rov_observer = None
    fib_observer = None
    initial_rov, initial_rib = None, None
    cutoff_ns = quiet_candidate_ns = None
    failure = None
    gc_was_enabled = None
    try:
        rov_observer = start_reset_rov_observer(
            state.rov.targets, socket_path, poll_interval, timeout,
            state.events, state.errors,
        )
        if state.fib.destinations:
            fib_observer = ForwardingObserver(
                state.fib.destinations, config_dir, pps, packet_timeout,
                state.events, state.errors,
            )
            for event in fib_observer.start(timeout).values():
                state.fib.record(event)
        initial_rov = wait_observers_ready(state, timeout)
        initial_table = read_primary_rib(socket_path, timeout)
        initial_rib = {prefix: rib_path(initial_table.get(prefix)) for prefix in state.rib.initial}
        del initial_table
        state.rib.current = initial_rib.copy()
        while True:
            try:
                state.pending.append(state.events.get_nowait())
            except queue.Empty:
                break
        if suspend_gc:
            gc_was_enabled = gc.isenabled()
            gc.disable()
        state.started_ns = time.monotonic_ns()
        state.last_change_ns = state.started_ns
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
                if fault_lab is None:
                    submission = submit_with_backpressure(
                        directory / batch["script"], fifo, timeout,
                        lambda seconds: receive_event(state, seconds), check_alive,
                    )
                else:
                    submission = fault_lab.submit(
                        directory / batch["script"], fifo,
                        lambda seconds: receive_event(state, seconds), check_alive,
                    )
            except (OSError, ValueError, RuntimeError) as error:
                raise RuntimeError(
                    f"Batch {batch['number']} submission failed: {error}"
                ) from error

            state.submitted.append((batch["number"], submission.get("fault_started_ns", submission["write_start_ns"])))
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
            for _ in range(len(state.pending) + state.events.qsize()):
                receive_event(state, 0)
            quiet_remaining = quiet_seconds - (
                time.monotonic_ns() - state.last_change_ns
            ) / 1e9
            if (quiet_remaining <= 0 and protocol_recovered(state, fault_lab)
                    and state.log_drained_ns - state.last_change_ns >= quiet_seconds * 1e9
                    and not state.pending and state.events.empty()):
                quiet_candidate_ns = state.last_change_ns
                cutoff_ns = time.monotonic_ns()
                result["quiet_confirmed_ns"] = cutoff_ns
                break
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("Timed out waiting for observation changes to settle")
            receive_event(state, min(0.1, remaining, max(0.01, quiet_remaining)))
    except BaseException as error:
        failure = error
    finally:
        try:
            if cutoff_ns is None:
                cutoff_ns = time.monotonic_ns()
            result["collection_end_ns"] = cutoff_ns
            state.collection_end_ns = cutoff_ns
            stop.set()
            if rov_observer is not None:
                rov_observer[0].set()
            if fib_observer is not None:
                fib_observer.stop.set()
            observers = [(stop, thread)] + ([rov_observer[:2]] if rov_observer is not None else [])
            if fib_observer is not None:
                observers.append((fib_observer.stop, fib_observer.thread))
            for observer_stop, observer_thread in observers:
                try:
                    stop_observer(observer_stop, observer_thread, timeout)
                except BaseException as error:
                    if failure is None:
                        failure = error

            if state.started_ns is not None and all(not worker.is_alive() for _, worker in observers):
                try:
                    while receive_event(state, 0, check_errors=False, cutoff_ns=cutoff_ns):
                        pass
                    if quiet_candidate_ns is not None and (
                        cutoff_ns - state.last_change_ns < quiet_seconds * 1e9
                        or state.log_drained_ns - state.last_change_ns < quiet_seconds * 1e9
                        or not protocol_recovered(state, fault_lab)
                    ):
                        raise RuntimeError("An in-flight observation invalidated the quiet collection boundary")
                except BaseException as error:
                    if failure is None:
                        failure = error
        finally:
            if gc_was_enabled:
                gc.enable()

        try:
            if initial_rib is not None:
                process_observations(state, initial_rib)
            if len(state.submitted) == len(batches):
                state.rov.match_offline(state.submitted)
                state.rib.match_offline(state.submitted)
                match_forwarding(state.fib, state.submitted)
                if fault_lab is not None:
                    match_recovery_endpoints(state)
        except BaseException as error:
            state.anomalies.append(f"Offline observation analysis: {type(error).__name__}: {error}")
            if failure is None:
                failure = error

        if failure is None and not state.errors.empty():
            failure = state.errors.get()
        if rov_observer is not None:
            result["query_observations"] = query_observations(state.rov.targets, rov_observer[2], cutoff_ns)
        result["termination"] = "quiet" if failure is None else "timeout" if isinstance(failure, TimeoutError) else "error"
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
        result["batch_metrics"] = batch_metrics(state)
        result["transition_observation_complete"] = all(
            row[label + "_observed"] == row[label + "_expected"]
            for row in result["batch_metrics"] for label in ("roa_query", "rib", "fib_probe")
        )
        try:
            result["final_verification"] = final_verification(state, directory, socket_path, timeout)
        except BaseException as error:
            result["final_verification"] = dict(correct=False, errors=[f"{type(error).__name__}: {error}"])
        try:
            if initial_rov is not None:
                check_initial_rov(initial_rov, state.rov.targets)
            if initial_rib is not None:
                check_initial_rib(state.rib.initial, initial_rib)
            for predictions in state.fib.changes.values():
                for _, destination, before, _ in predictions[:1]:
                    wanted = ("not_received", None) if before is None else ("received", before)
                    if state.fib.initial.get(destination) != wanted:
                        raise RuntimeError(f"Initial sampled forwarding mismatch for {destination}")
        except (KeyError, RuntimeError) as error:
            result["final_verification"].setdefault("errors", []).append(f"Initial verification: {error}")
            result["final_verification"]["correct"] = False
        result["unobserved_vrp_events"] = sum((state.vrp.expected - state.vrp.observed).values())
        result["unexpected_vrp_events"] = sum((state.vrp.observed - state.vrp.expected).values())
        result["unobserved_meaning"] = "Missing observations are not proof of dropped RTR updates; inspect file integrity and final verification"
        if "vrp_rebuild" in result:
            result["backlog_proxy"] = []
            result["backlog_unavailable"] = "Full purge/reload event counts cannot be subtracted from input edit counts"
        if (state.anomalies or result["unobserved_vrp_events"] or result["unexpected_vrp_events"]
                or not result["final_verification"]["correct"] or not result["transition_observation_complete"]):
            result["status"] = "failed"
        raw_output.flush()

    if failure is not None:
        raise failure
    return result
