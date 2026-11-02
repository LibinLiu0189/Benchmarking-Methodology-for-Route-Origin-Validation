"""Collect experiment 66 events and defer prediction checks until collection ends."""

import json
import queue
import threading
import time
from collections import defaultdict

from rov.data.predictions import rib_path
from rov.experiments.exp62.observation.query_observer import query_group
from rov.measure.bird_connection import BirdConnection
from rov.measure.read_log import observe as observe_log
from rov.measure.roa_batch import groups


# Observe file logs and report EOF independently of actual protocol changes.
def start_log_observer(state, socket_path, timeout, log_path):
    stop = threading.Event()
    file_drained = False

    # Publish EOF changes without treating a poll as a new measurement event.
    def on_poll(drained):
        nonlocal file_drained
        if drained or file_drained:
            state.events.put(dict(event="drained", drained=drained))
        file_drained = drained

    # Keep the log reader's errors in the same channel as the other observers.
    def worker():
        try:
            observe_log(
                socket_path, ("vrp", "rib", "rtr"), timeout,
                state.events.put, stop, log_path=log_path, on_poll=on_poll,
            )
        except BaseException as error:
            state.errors.put(error)

    thread = threading.Thread(target=worker, name="exp66-log-observer")
    thread.start()
    return stop, thread


# Query fixed groups without collecting round or throughput diagnostics.
def start_rov_observer(targets, socket_path, poll_interval, timeout, events, errors):
    stop = threading.Event()
    candidates = defaultdict(list)
    for target in targets.values():
        candidates[(target["prefix"], target["origin_as"])].append(target["peer"])
    queries = list(candidates)
    plan = groups(queries, 8, "pipeline")

    # Keep actual prior values and publish only the first round and later changes.
    def worker():
        connection = None
        previous = {}
        initial = defaultdict(dict)

        # Associate each ordered response with the candidates sharing that query.
        def receive(index, value, started_ns, completed_ns):
            key = queries[index]
            if key not in previous:
                for peer in candidates[key]:
                    initial[key[0]][peer] = dict(
                        rov=value, origin_as=key[1], observed_ns=completed_ns,
                    )
                if len(previous) + 1 == len(queries):
                    for prefix, states in initial.items():
                        events.put(dict(event="initial", prefix=prefix, candidates=states))
                    events.put(dict(event="ready", component="cli", prefixes=len(initial)))
            elif previous[key] != value:
                events.put(dict(
                    event="rov", prefix=key[0], origin_as=key[1],
                    before=previous[key], after=value,
                    query_start_ns=started_ns, query_end_ns=completed_ns,
                    observed_ns=completed_ns,
                ))
            previous[key] = value

        try:
            connection = BirdConnection(socket_path, min(timeout, 10))
            connection.command("restrict")
            while not stop.is_set():
                for indices in plan:
                    if stop.is_set():
                        break
                    states, record = query_group(connection, queries, indices)
                    for index, value, completed_ns in zip(
                        indices, states, record["response_end_ns"], strict=True,
                    ):
                        receive(index, value, record["start_ns"], completed_ns)
                if poll_interval > 0 and stop.wait(poll_interval):
                    break
        except BaseException as error:
            errors.put(error)
        finally:
            if connection is not None:
                connection.close()

    thread = threading.Thread(target=worker, name="exp66-rov-observer")
    thread.start()
    return stop, thread


# Gather actual initial query states while retaining any concurrent changes.
def wait_observers_ready(state, timeout):
    required = {"read_log", "cli"}
    ready = set()
    initial = {}
    deadline = time.monotonic() + timeout
    while ready != required:
        if not state.errors.empty():
            raise state.errors.get()
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("Timed out waiting for the log and ROV observers")
        try:
            event = state.events.get(timeout=min(0.1, remaining))
        except queue.Empty:
            continue

        kind = event["event"]
        if kind == "initial":
            initial[event["prefix"]] = event["candidates"]
        elif kind == "ready":
            component = event["component"]
            if component not in required or component in ready:
                raise RuntimeError(f"Unexpected observer readiness: {event}")
            ready.add(component)
        else:
            state.pending.append(event)
    return initial


# Store actual events and update silence without comparing them with predictions.
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
    if kind == "drained":
        state.log_drained = event["drained"]
        return True
    if kind not in ("vrp", "rtr", "rov", "rib", "received", "not_received"):
        raise RuntimeError(f"Unexpected observer event: {event}")
    if cutoff_ns is not None and event["observed_ns"] > cutoff_ns:
        return True

    state.collected.append(event)
    state.raw_output.write(json.dumps(event, separators=(",", ":")) + "\n")
    changed = True
    if kind == "rib" and event["prefix"] in state.rib.current:
        prefix = event["prefix"]
        after = rib_path(event["after"])
        changed = state.rib.current[prefix] != after
        state.rib.current[prefix] = after
    if changed:
        state.last_change_ns = time.monotonic_ns()
    return True


# Check log consistency and summarize backlog after measurement producers stop.
def process_observations(state, initial_rib):
    state.rib.current = initial_rib.copy()
    for event in state.collected:
        kind = event["event"]
        observed_ns = event["observed_ns"]
        if observed_ns - state.last_backlog_ns >= 100_000_000:
            submitted = sum(
                len(state.vrp.operations[number])
                for number, started_ns in state.submitted if started_ns <= observed_ns
            )
            observed = state.vrp.observed.total()
            state.backlog.append(dict(
                observed_ns=observed_ns, submitted_operations=submitted,
                observed_events=observed,
                unobserved_operations=max(0, submitted - observed),
            ))
            state.last_backlog_ns = observed_ns

        if kind == "rtr":
            state.rtr_events.append(event)
        elif kind == "vrp":
            key = (event["action"], event["prefix"], event["max_length"], event["asn"])
            state.vrp.observed[key] += 1
            if state.vrp.observed[key] > state.vrp.expected[key]:
                state.anomalies.append(f"Unexpected VRP event: {key}")
        elif kind == "rib" and event["prefix"] in state.rib.current:
            prefix = event["prefix"]
            before = state.rib.current[prefix]
            after = rib_path(event["after"])
            action = event["action"]
            if (action == "added" and before is not None) or (
                action in ("removed", "replaced") and before is None
            ) or (action == "removed" and after is not None):
                state.anomalies.append(f"Invalid RIB {action} for {prefix}")
            state.rib.current[prefix] = after
