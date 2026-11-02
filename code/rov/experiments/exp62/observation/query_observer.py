"""Observe candidate ROV changes through one CLI connection in groups of eight."""

import ipaddress as ip
import queue
import select
import threading
import time

from rov.measure.bird_connection import BirdConnection
from rov.measure.read_log import stop_observer
from rov.measure.roa_batch import groups, labelled, parse


# Reject residual replies before another bounded group can be sent.
def assert_idle(connection):
    if connection.buffer or select.select([connection.socket], [], [], 0)[0]:
        connection.close()
        raise RuntimeError("Unexpected residual BIRD response; connection closed")


# Timestamp each ordered response without adding transport diagnostics.
def query_group(connection, targets, indices):
    commands = ["eval " + labelled(index, targets[index]) for index in indices]
    payload = ("\n".join(commands) + "\n").encode()
    if any(len(command.encode()) >= 1000 for command in commands) or len(payload) > 1023:
        raise ValueError("Query group exceeds a verified input byte limit")
    assert_idle(connection)
    started = time.monotonic_ns()
    states = []
    completed = []
    try:
        deadline = time.monotonic() + connection.timeout
        connection.socket.settimeout(connection.timeout)
        connection.socket.sendall(payload)
        for index in indices:
            text = connection.reply(deadline)
            completed.append(time.monotonic_ns())
            states.append(parse(text, [index])[0])
        assert_idle(connection)
    except BaseException:
        connection.close()
        raise
    return states, dict(start_ns=started, end_ns=completed[-1], response_end_ns=completed)


# Finish each query group before proceeding through the current target list.
def observe(targets, socket_path, poll_interval, timeout, on_sample, stop_event,
            mode, group_size):
    plan = groups(targets, group_size, mode)
    if not targets:
        return
    connection = BirdConnection(socket_path, timeout)
    try:
        connection.command("restrict")
        while not stop_event.is_set():
            for indices in plan:
                if stop_event.is_set():
                    break
                states, record = query_group(connection, targets, indices)
                for index, state, end in zip(indices, states, record["response_end_ns"], strict=True):
                    on_sample(dict(
                        prefix=targets[index][0], origin_as=targets[index][1], rov=state,
                        query_start_ns=record["start_ns"], query_end_ns=end,
                        group_end_ns=record["end_ns"],
                        query_interval_kind="group_send_to_ordered_reply",
                    ))
            if poll_interval > 0 and stop_event.wait(poll_interval):
                break
    finally:
        connection.close()


# Observe each candidate and emit initial states, readiness, and ROV changes.
def observe_candidates(targets, socket_path, poll_interval, timeout, on_event, stop_event, mode, group_size):
    unique = {}
    for target in targets:
        prefix = str(ip.ip_network(target["prefix"]))
        peer = target["peer"]
        key = (prefix, peer)
        candidate = dict(prefix=prefix, peer=peer, origin_as=int(target["origin_as"]))
        if key in unique and unique[key] != candidate:
            raise ValueError(f"Conflicting ROV target: {prefix} {peer}")
        unique[key] = candidate

    candidates = list(unique.values())
    if not candidates:
        raise ValueError("At least one ROV candidate is required")
    queries = [(row["prefix"], row["origin_as"]) for row in candidates]
    previous = {}
    initial = {}
    position = 0

    # Associate each sample with the candidate at the same position in the round.
    def receive_sample(sample):
        nonlocal position
        candidate = candidates[position]
        prefix = candidate["prefix"]
        peer = candidate["peer"]
        key = (prefix, peer)
        state = sample["rov"]
        completed = sample["query_end_ns"]
        current = dict(
            rov=state,
            origin_as=candidate["origin_as"],
            observed_ns=completed,
        )

        if key not in previous:
            initial.setdefault(prefix, {})[peer] = current
        elif previous[key]["rov"] != state:
            on_event(dict(
                event="rov",
                prefix=prefix,
                peer=peer,
                origin_as=candidate["origin_as"],
                before=previous[key]["rov"],
                after=state,
                observed_ns=completed,
                query_start_ns=sample["query_start_ns"],
                query_end_ns=completed,
                query_interval_kind=sample["query_interval_kind"],
                group_end_ns=sample["group_end_ns"],
            ))
        previous[key] = current

        position += 1
        if position == len(candidates):
            position = 0
            if initial:
                for initial_prefix, states in initial.items():
                    on_event(dict(event="initial", prefix=initial_prefix, candidates=states))
                on_event(dict(event="ready", component="cli", prefixes=len(initial)))
                initial.clear()

    observe(queries, socket_path, poll_interval, timeout, receive_sample, stop_event,
            mode, group_size)


# Start one persistent CLI connection for a batch and return its initial states.
def start_candidate_observer(changes, socket_path, poll_interval, timeout, check_alive,
                             *, event_queue=None, error_queue=None, mode="pipeline", group_size=8):
    events = event_queue if event_queue is not None else queue.Queue()
    errors = error_queue if error_queue is not None else queue.Queue()
    startup = queue.Queue()
    stop = threading.Event()
    observer_ready = False

    # Keep readiness private so a shared pool's BIRD records cannot be consumed here.
    def receive(event):
        nonlocal observer_ready
        if observer_ready:
            events.put(event)
        else:
            startup.put(event)
            if event.get("event") == "ready":
                observer_ready = True

    # Forward CLI observations to the experiment thread.
    def worker():
        try:
            observe_candidates(
                changes,
                socket_path,
                poll_interval,
                timeout,
                receive,
                stop, mode, group_size,
            )
        except BaseException as error:
            errors.put(error)

    thread = threading.Thread(target=worker, name="exp62-rov-cli")
    thread.start()
    initial = {}
    deadline = time.monotonic() + timeout
    while True:
        try:
            check_alive()
        except BaseException:
            stop_observer(stop, thread, timeout)
            raise
        if not errors.empty():
            error = errors.get()
            stop_observer(stop, thread, timeout)
            raise error
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            stop_observer(stop, thread, timeout)
            raise TimeoutError("Timed out waiting for the ROV observer")
        try:
            event = startup.get(timeout=min(0.1, remaining))
        except queue.Empty:
            continue
        if event.get("event") == "initial":
            initial[event["prefix"]] = event["candidates"]
            continue
        if event.get("event") == "ready":
            break
        stop_observer(stop, thread, timeout)
        raise RuntimeError(f"Received an ROV change before the observer was ready: {event}")
    prefixes = {row["prefix"] for row in changes}
    if set(initial) != prefixes:
        stop_observer(stop, thread, timeout)
        raise RuntimeError("The ROV observer did not return every requested initial prefix")
    return initial, events, errors, stop, thread
