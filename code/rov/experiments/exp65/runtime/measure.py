"""Own experiment 65's update schedule, completion deadline and metric attribution."""

from collections import Counter, defaultdict
import json
import math
from pathlib import Path
import queue
import threading
import time

from rov.paths import CONFIG
from rov.data.sequence import load_sequence
from rov.data.predictions import load_rov_targets, load_rib_targets, load_fib_targets, rib_path
from rov.measure.cli import read_primary_rib
from rov.measure.vrp_streams import ObservationState, stop_observer
from rov.send.send_vrp import submit_with_backpressure
from rov.experiments.exp65.runtime.batch_monitor import BatchMonitor
from rov.experiments.exp65.runtime.brio_timing import snapshot
from rov.experiments.exp65.runtime.log_capture import start_capture, receive_raw, replay_capture
from rov.experiments.exp65.runtime.vrp_selection import select_vrp_operations


# Save incoming capture records while the absolute update schedule continues.
def receive_event(state, wait, check_errors=True):
    return receive_raw(state, wait, check_errors)


# Check the selected-path baseline before the first batch is submitted.
def check_initial_rib(initial, socket_path, timeout):
    actual = read_primary_rib(socket_path, timeout)
    mismatches = [
        (prefix, path, rib_path(actual.get(prefix)))
        for prefix, path in initial.items()
        if rib_path(actual.get(prefix)) != path
    ]
    if mismatches:
        prefix, expected, observed = mismatches[0]
        raise RuntimeError(
            f"Initial RIB mismatch for {prefix}: expected {expected}, observed {observed}; "
            f"{len(mismatches)} mismatched prefixes"
        )
    return initial.copy()


# Preserve conservative per-batch metrics only when attribution is complete.
def batch_metrics(state):
    slots = defaultdict(list)
    observed = defaultdict(list)
    if "vrp" in state.monitoring_mode:
        for number, submitted_ns in state.submitted:
            for operation in state.vrp.monitored_operations[number]:
                action = "added" if operation["operation"] == "add" else "removed"
                slots[(action, *operation["vrp"])].append((number, submitted_ns))
    for key, observed_ns in state.vrp.recorded:
        if key in slots:
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
        specs = state.batch_monitor.specs[number]
        row = dict(batch=number, vrp_operations=len(state.vrp.operations[number]),
                   metrics={}, observed_endpoints_ns={})
        if "vrp" in state.monitoring_mode:
            operations = state.vrp.monitored_operations[number]
            row["vrp_expected"] = len(operations)
            row["vrp_observed"] = sum(
                len(observed[("added" if operation["operation"] == "add" else "removed",
                              *operation["vrp"])])
                for operation in operations
            )
        if {"rov", "fib"}.intersection(state.monitoring_mode):
            row["rov_fib_monitored"] = number in state.monitored_batches
        vrp = times[number]
        if number not in uncertain and vrp:
            row["observed_endpoints_ns"].update(first_vrp=min(vrp), last_vrp=max(vrp))
            row["metrics"].update(first_vrp_ms=(min(vrp) - start) / 1e6,
                                  last_vrp_ms=(max(vrp) - start) / 1e6,
                                  vrp_span_ms=(max(vrp) - min(vrp)) / 1e6)
        for component, label, matches, count in (
            ("rov", "roa_query", state.rov.matched,
             sum(len(spec["peers"]) for spec in specs.get("rov", {}).values())),
            ("rib", "rib", state.rib.matched, len(specs.get("rib", {}))),
            ("fib", "fib_probe", state.fib.matched, len(specs.get("fib", {}))),
        ):
            if component not in state.monitoring_mode:
                continue
            matched = [t for batch, t in matches if batch == number]
            row[label + "_observed"] = len(matched)
            row[label + "_expected"] = count
            if count and len(matched) == count:
                row["observed_endpoints_ns"][label + "_complete"] = max(matched)
                row["metrics"][label + "_complete_ms"] = (max(matched) - start) / 1e6
                if label == "roa_query" and number not in uncertain and vrp:
                    delta = (max(matched) - max(vrp)) / 1e6
                    row["metrics"]["post_install_query_ms"] = delta if delta >= 0 else None
        if "rib_complete_ms" in row["metrics"] and "fib_probe_complete_ms" in row["metrics"]:
            row["observed_endpoints_ns"]["rib_and_sampled_fib"] = max(
                row["observed_endpoints_ns"]["rib_complete"], row["observed_endpoints_ns"]["fib_probe_complete"])
            row["metrics"]["rib_and_sampled_fib_ms"] = max(
                row["metrics"]["rib_complete_ms"], row["metrics"]["fib_probe_complete_ms"])
        result.append(row)
    return result


# Summarize missing observations for retained targets in each submitted batch.
def missing_summary(state, batches, monitoring):
    components = ("vrp", "rov", "rib", "fib")
    by_batch = {
        row["batch"]: dict(batch=row["batch"], vrp=None, rov=None, rib=None, fib=None)
        for row in batches
    }
    for row in batches:
        missing = by_batch[row["batch"]]
        if "vrp" in state.monitoring_mode and state.log_replayed:
            missing["vrp"] = 0
        if ("rib" in state.monitoring_mode and state.log_replayed
                and row["batch"] in state.monitored_batches):
            missing["rib"] = row["rib_expected"] - row["rib_observed"]
    for task in monitoring.get("tasks", []):
        for component in ("rov", "fib"):
            if component in state.monitoring_mode:
                by_batch[task["batch"]][component] = sum(
                    target["exit_reason"] != "completed"
                    for target in task[component]["targets"]
                )

    totals = dict.fromkeys(components)
    for component in ("rov", "rib", "fib"):
        if component not in state.monitoring_mode:
            continue
        if component == "rib" and not state.log_replayed:
            continue
        values = [
            row[component] for number, row in by_batch.items()
            if number in state.monitored_batches
        ]
        if all(value is not None for value in values):
            totals[component] = sum(values)

    unexpected_vrp = None
    if "vrp" in state.monitoring_mode and state.log_replayed:
        slots = {}
        for number, _ in state.submitted:
            for operation in state.vrp.monitored_operations[number]:
                action = "added" if operation["operation"] == "add" else "removed"
                slots[(action, *operation["vrp"])] = number
        expected = Counter({key: 1 for key in slots})
        observed = Counter({
            key: count for key, count in state.vrp.observed.items()
            if key[1:] not in state.vrp.excluded_identities
        })
        deficits = expected - observed
        totals["vrp"] = sum(deficits.values())
        unexpected_vrp = sum((observed - expected).values())
        for key, count in deficits.items():
            by_batch[slots[key]]["vrp"] += count

    result = dict(
        monitoring_missing=totals,
        monitoring_missing_by_batch=[
            row for row in by_batch.values() if any(row[component] for component in components)
        ],
        unobserved_vrp_events=totals["vrp"],
        unexpected_vrp_events=unexpected_vrp,
    )
    return result


# Verify only enabled VRP/ROV/RIB measurements after collection has stopped.
def final_verification(state, directory, socket_path, timeout):
    from rov.data.vrp import load_vrp_text
    from rov.measure.vrp_state import build_vrp_expectation
    from rov.measure.vrp_state import verify_final
    if not state.submitted:
        return dict(correct=False, mismatches=[], errors=["No submitted workload"], scope="Not verified")
    submitted = {number for number, _ in state.submitted}
    expected = {"entries": {}}
    if "vrp" in state.monitoring_mode:
        metadata = json.loads((directory / "sequence.json").read_text())
        initial = load_vrp_text(directory / metadata["initial"])
        operations = [row for number, rows in state.vrp.operations.items()
                      if number in submitted for row in rows]
        expected = build_vrp_expectation(initial, operations)
    rovs = {}
    for number, prefix, _, origin, _, after in state.rov.transitions:
        if number in submitted:
            rovs[(prefix, origin)] = after
    result = dict(mismatches=[], errors=[])
    if {"vrp", "rov"}.intersection(state.monitoring_mode):
        result = verify_final(socket_path, timeout, expected, rovs)
        for component in ("vrp", "rov"):
            if component not in state.monitoring_mode:
                result.pop(component + "_checked", None)
    if "rib" in state.monitoring_mode:
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
    checked = [name.upper() for name in ("vrp", "rov", "rib") if name in state.monitoring_mode]
    result["scope"] = ("Final checks: " + ", ".join(checked)) if checked else "No final CLI checks requested"
    result["fib_final_scope"] = (
        "Previously collected sampled forwarding only; no mandatory new probe round"
        if "fib" in state.monitoring_mode else "Not monitored"
    )
    return result


# Consume all observation streams while waiting for an absolute schedule boundary.
def collect_until(state, target_ns, check_alive):
    while True:
        if check_alive is not None:
            check_alive()
        remaining = (target_ns - time.monotonic_ns()) / 1e9
        if remaining <= 0:
            return
        receive_event(state, min(0.1, remaining))


# Submit fixed-cadence batches without waiting for previous batches to converge.
def submit_batches(state, directory, batches, fifo, interval, timeout, result, check_alive):
    interval_ns = round(interval * 1e9)
    if interval_ns < 1:
        raise ValueError("Batch interval must be at least one nanosecond")
    result["workload_start_ns"] = state.started_ns
    result["scheduled_window_end_ns"] = state.started_ns + len(batches) * interval_ns
    for index, batch in enumerate(batches):
        target_ns = state.started_ns + index * interval_ns
        collect_until(state, target_ns, check_alive)
        if not state.errors.empty():
            raise state.errors.get()
        state.batch_monitor.start_batch(batch["number"])
        try:
            submission = submit_with_backpressure(
                directory / batch["script"], fifo, timeout,
                lambda seconds: receive_event(state, seconds), check_alive,
            )
        except (OSError, ValueError, RuntimeError) as error:
            raise RuntimeError(f"Batch {batch['number']} submission failed: {error}") from error

        lag_ms = max(0, submission["write_start_ns"] - target_ns) / 1e6
        state.submitted.append((batch["number"], submission["write_start_ns"]))
        state.batch_monitor.submitted(batch["number"], submission["write_start_ns"])
        state.submission_records.append(dict(
            batch=batch["number"], operations=batch["total"], scheduled_ns=target_ns,
            schedule_lag_ms=lag_ms, **submission,
        ))
        print(f"Batch {batch['number']}: submitted; schedule lag {lag_ms:.1f} ms", flush=True)

    result["last_submission_ns"] = time.monotonic_ns()
    # The final slot is part of the load window (300 batches: t0 ... t299, end t300).
    collect_until(state, result["scheduled_window_end_ns"], check_alive)
    result["load_window_end_ns"] = time.monotonic_ns()


# Wait for the selected VRP/RIB log subscriptions before submitting updates.
def wait_log_ready(state, timeout):
    planes = {"vrp", "rib"}.intersection(state.monitoring_mode)
    deadline = time.monotonic() + timeout
    while True:
        if not state.errors.empty():
            raise state.errors.get()
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("Timed out waiting for the VRP/RIB log observer")
        try:
            event = state.events.get(timeout=min(0.1, remaining))
        except queue.Empty:
            continue
        if (event.get("event") != "ready" or event.get("component") != "read_log"
                or not planes.issubset(event.get("planes", ()))):
            raise RuntimeError(f"Unexpected VRP/RIB readiness event: {event}")
        return []


# Submit every batch on schedule, then await the enabled observation streams.
def run(
    sequence, predictions, path_predictions, socket_path, fifo,
    interval, poll_interval, timeout, config_dir=CONFIG,
    pps=10000, packet_timeout=1,
    *, log_path, result, raw_output, check_alive, completion_check_interval=5,
    monitor_every=1,
    query_mode="pipeline", query_group_size=8, brio_timing_log=None,
    monitoring_mode=("vrp", "rov", "rib", "fib"),
    monitoring_plan=None,
):
    if not math.isfinite(interval) or interval <= 0:
        raise ValueError("Interval must be a finite positive number")
    if not math.isfinite(poll_interval) or poll_interval < 0:
        raise ValueError("Poll interval must be finite and nonnegative")
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("Timeout must be a finite positive number")
    if not math.isfinite(pps) or pps <= 0 or not math.isfinite(packet_timeout) or packet_timeout <= 0:
        raise ValueError("Packet rate and timeout must be finite and positive")
    if not math.isfinite(completion_check_interval) or completion_check_interval < 1e-9:
        raise ValueError("Completion check interval must be finite and at least one nanosecond")
    if not isinstance(monitor_every, int) or monitor_every < 1:
        raise ValueError("Monitoring batch interval must be a positive integer")
    if not monitoring_mode or isinstance(monitoring_mode, str) or not set(monitoring_mode) <= {"vrp", "rov", "rib", "fib"}:
        raise ValueError("Select one or more of vrp, rov, rib, fib")
    directory, batches, operations = load_sequence(sequence)
    targets, transitions = {}, []
    rib_changes, rib_initial = {}, {}
    fib_changes, fib_unavailable = {}, []
    if "rov" in monitoring_mode:
        targets, transitions = load_rov_targets(directory, batches, predictions)
        if not targets:
            raise ValueError("Section 6.5 requires at least one ROV candidate")
    if "fib" in monitoring_mode:
        fib_changes, fib_unavailable = load_fib_targets(path_predictions, batches)
    if {"rib", "fib"}.intersection(monitoring_mode):
        rib_changes, rib_initial = load_rib_targets(path_predictions, batches)
    state = ObservationState(
        operations, targets, transitions, rib_changes, rib_initial,
        fib_changes, fib_unavailable,
    )

    if "vrp" in monitoring_mode:
        selected_operations, excluded_identities, vrp_selection = select_vrp_operations(operations)
        if monitoring_plan is not None and len(operations) == 1:
            if set(operations) != {monitoring_plan["batch"]}:
                raise ValueError("A single-batch control plan must match its only workload batch")
            retained = {tuple(identity) for identity in monitoring_plan["vrp_identities"]}
            excluded_identities = {
                tuple(row["vrp"]) for rows in operations.values() for row in rows
                if tuple(row["vrp"]) not in retained
            }
            selected_operations = {
                number: [row for row in rows if tuple(row["vrp"]) in retained]
                for number, rows in operations.items()
            }
            vrp_selection = dict(
                totals=dict(original=sum(map(len, operations.values())), retained=len(retained),
                            repeated=len(excluded_identities)),
                by_batch={number: dict(original=len(rows), retained=len(selected_operations[number]),
                                       repeated=len(rows) - len(selected_operations[number]))
                          for number, rows in operations.items()},
            )
        state.vrp.monitored_operations = selected_operations
        state.vrp.excluded_identities = excluded_identities
        state.vrp.monitored_expected = Counter({
            key: count for key, count in state.vrp.expected.items()
            if key[1:] not in excluded_identities
        })
    state.fib.predictions_path = path_predictions

    state.monitoring_mode = monitoring_mode
    state.monitored_batches = {
        number for number in operations if number % monitor_every == 0
    }
    if monitoring_plan is not None:
        if monitoring_plan["batch"] not in operations:
            raise ValueError("The monitored batch is absent from the input sequence")
        state.monitored_batches = {monitoring_plan["batch"]}
    state.deadline_ns = math.inf
    state.capture_lock = threading.Lock()
    manager = BatchMonitor(
        state, socket_path, config_dir, pps, poll_interval, timeout,
        packet_timeout, completion_check_interval,
        query_mode=query_mode, query_group_size=query_group_size,
        monitoring_plan=monitoring_plan,
    )
    state.batch_monitor = manager
    if "vrp" in monitoring_mode:
        manager.selection["totals"]["vrp"] = vrp_selection["totals"]
        for row in manager.selection["by_batch"]:
            row["vrp"] = vrp_selection["by_batch"][row["batch"]]
        manager.selection["definition"] = (
            "VRP: retain operations on identities appearing exactly once over the full input; "
            "exclude every operation on repeated identities, irrespective of action. "
            + manager.selection["definition"]
        )
    result["target_selection"] = manager.selection
    if "fib" in monitoring_mode:
        result["fib_unchanged_forwarding_excluded"] = manager.selection["totals"]["fib"]["unchanged"]
    state.log_replayed = False
    state.raw_output = raw_output
    log_planes = [name for name in ("vrp", "rib") if name in monitoring_mode]
    sampled_planes = [name for name in ("rov", "fib") if name in monitoring_mode]
    state.raw_capture = None
    if log_planes:
        state.raw_capture_path = Path(raw_output.name).with_name("log-records.jsonl")
        state.raw_capture = state.raw_capture_path.open("x")
    state.monitor_capture = None
    if sampled_planes:
        state.monitor_capture_path = Path(raw_output.name).with_name("monitor-records.jsonl")
        state.monitor_capture = state.monitor_capture_path.open("x")
    stop = thread = None
    result.update(time_reference="Performance metrics: FIFO write start to external observation; *_from_brio_ms: optional diagnostic intervals",
                  status="failed")
    failure = None

    # Bound submission and collection together while leaving initialization outside this timer.
    def check_measurement():
        if check_alive is not None:
            check_alive()
        if time.monotonic_ns() >= state.deadline_ns:
            raise TimeoutError("Total update and monitoring timeout expired")

    try:
        if log_planes:
            stop, thread = start_capture(socket_path, timeout, state.events, state.errors, log_path, log_planes)
            state.pending = wait_log_ready(state, timeout)
        if "rib" in monitoring_mode:
            state.rib.current = check_initial_rib(state.rib.initial, socket_path, timeout)
        while True:
            try:
                state.pending.append(state.events.get_nowait())
            except queue.Empty:
                break
        if any(event["event"] == "rib" or (
            event["event"] == "bird_log" and event["text"].startswith(("ROVLOG RIB|", "fib4.", "fib6."))
        ) for event in state.pending):
            raise RuntimeError("RIB changed while the initial selected table was being read")

        manager.start()
        if brio_timing_log is not None:
            result["brio_timing_baseline"] = snapshot(brio_timing_log)
        state.started_ns = time.monotonic_ns()
        state.deadline_ns = state.started_ns + round(timeout * 1e9)
        result["monitoring_deadline_ns"] = state.deadline_ns
        manager.begin()
        print(f"Observers ready: {', '.join(name.upper() for name in monitoring_mode)}.", flush=True)
        submit_batches(
            state, directory, batches, fifo, interval, timeout, result, check_measurement,
        )
        while not manager.completed.is_set():
            check_measurement()
            receive_event(state, 0.1)
        check_measurement()
    except BaseException as error:
        failure = error
    finally:
        try:
            manager.close()
        except BaseException as error:
            if failure is None:
                failure = error
        if thread is not None:
            try:
                stop_observer(stop, thread, timeout)
            except BaseException as error:
                if failure is None:
                    failure = error
        result["collection_end_ns"] = time.monotonic_ns()
        if state.started_ns is not None and (thread is None or not thread.is_alive()):
            try:
                while receive_event(state, 0, check_errors=False):
                    pass
            except BaseException as error:
                if failure is None:
                    failure = error

        if state.raw_capture is not None:
            state.raw_capture.close()
            state.raw_capture = None
            try:
                result["log_collection"] = replay_capture(state)
                state.log_replayed = True
            except BaseException as error:
                if failure is None:
                    failure = error

        if state.monitor_capture is not None:
            state.monitor_capture.close()
            state.monitor_capture = None
        if state.started_ns is not None:
            try:
                result["batch_monitoring"] = manager.finish(result["collection_end_ns"])
            except BaseException as error:
                if failure is None:
                    failure = error

        if failure is None and not state.errors.empty():
            failure = state.errors.get()
        if failure is None:
            result["termination"] = "retained_targets_complete"
        else:
            result["termination"] = "timeout" if isinstance(failure, TimeoutError) else "error"
        if "vrp" in monitoring_mode:
            result["rtr_events"] = state.rtr_events
            result["rtr_failures"] = [event for event in state.rtr_events if event.get("after") in ("Down", "Transport-Error", "Fatal-Protocol-Error")]
        result["unobservable"] = dict(internal_queue="No DUT queue counter", dropped_wire_updates="Cannot infer from missing external log events",
                                     process_crash="Owned initialization exit and raw diagnostics retained; no independent crash detector",
                                     stall="No progress alone does not distinguish idle, observer delay, or DUT stall",
                                     throttle="Submission schedule lag is recorded; no internal throttle counter")
        result.update(submissions=state.submission_records, backlog_proxy=state.backlog,
                      backlog_definition="Submitted operations minus observed VRP events; includes cache, wire, DUT and observer lag; not an internal queue",
                      anomalies=state.anomalies,
                      status="success" if failure is None else "failed",
                      error=None if failure is None else f"{type(failure).__name__}: {failure}")
        result["batch_metrics"] = batch_metrics(state)
        result.update(missing_summary(
            state, result["batch_metrics"], result.get("batch_monitoring", {}),
        ))
        result["backlog_definition"] = "Unavailable: raw capture does not count expected or parsed VRPs online"
        result["measurement_definition"] = (
            "Batch 1 and every Nth batch add retained ROV/FIB targets before submission without waiting for samples. "
            "All workload batches are submitted at the original cadence. "
            "VRP retains only identities operated on once over the full input. "
            "ROV/RIB retain only prefixes with one predicted batch transition over the full input. "
            "FIB also requires the probe destination's predicted forwarding next hop to change once. "
            "Periodic checks inspect saved records and retire completed targets; unfinished targets persist. "
            "Latency uses the first expected after observed after that batch's FIFO origin, not the check time. "
            "These endpoints cover retained targets, not all changes in a batch. "
            "Enabled VRP/RIB logs remain continuous; final correctness checks run after collection. "
            "Collection ends after every batch is submitted and retained targets complete, or at total timeout.")
        if monitoring_plan is not None:
            result["measurement_definition"] = (
                "Submit the supplied sequence unchanged. Activate only the fixed batch's retained "
                "ROV/RIB/FIB targets immediately before that batch is submitted. VRP observations "
                "retain the continuous reference's identity selection. Completion checks retire "
                "matched targets; all input batches are still submitted before collection ends. "
                "First expected-after observation timestamps are the latency endpoints."
            )
        print(f"Batch monitoring finished; missing targets: {json.dumps(result['monitoring_missing'])}", flush=True)
        for row in result["monitoring_missing_by_batch"]:
            print(f"Missing by batch: {json.dumps(row)}", flush=True)
        try:
            result["final_verification"] = final_verification(state, directory, socket_path, timeout)
        except BaseException as error:
            result["final_verification"] = dict(correct=False, errors=[f"{type(error).__name__}: {error}"])
        result["unobserved_meaning"] = "Missing observations are not proof of dropped RTR updates; inspect file integrity and final verification"
        if state.anomalies or not result["final_verification"]["correct"]:
            result["status"] = "failed"
        raw_output.flush()

    if failure is not None:
        raise failure
    return result
