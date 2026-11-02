"""Read primary RIB snapshots and observe ROA states through BIRD's CLI."""

import ipaddress as ip
import json
import re
import time

from rov.paths import CONFIG
from .bird_connection import BirdConnection
from .read_log import parse_as_path, parse_origin
import queue
import threading

from rov.measure.read_log import stop_observer

ROUTE_HEADER = re.compile(
    r"^\s*(?:(\S+/\d+)\s+)?"
    r"(unicast|blackhole|unreachable|prohibit)\s+"
    r"\[([^\s\]]+)\s+[^]]+\]\s*([*!]?)\s*\(\d+\).*$"
)

STATES = {"0": "NotFound", "1": "Valid", "2": "Invalid"}
ROA_RESULT = re.compile(r"\(enum 34\)([012])")


# Print events without creating result files.
def emit(event):
    print(json.dumps(event, separators=(",", ":")), flush=True)


# Evaluate one prefix and origin ASN directly against BIRD's current ROA table.
def query_roa(connection, prefix, origin_as):
    network = ip.ip_network(prefix)
    started = time.monotonic_ns()
    text = connection.command(
        f"eval roa_check(r{network.version}, {network}, {origin_as})"
    )
    completed = time.monotonic_ns()
    match = ROA_RESULT.fullmatch(text.strip())
    if match is None:
        raise ValueError(f"Unexpected roa_check result: {text}")
    return STATES[match[1]], started, completed


# Query each supplied pair in order, optionally pausing after a complete round.
def observe(
    targets,
    socket_path=CONFIG / "bird.ctl",
    poll_interval=0,
    timeout=10,
    on_sample=emit,
    stop_event=None,
):
    targets = list(targets)
    if not targets:
        return
    connection = BirdConnection(socket_path, timeout)
    try:
        connection.command("restrict")
        while stop_event is None or not stop_event.is_set():
            for prefix, origin_as in targets:
                if stop_event is not None and stop_event.is_set():
                    break
                state, started, completed = query_roa(connection, prefix, origin_as)
                on_sample(dict(
                    prefix=prefix,
                    origin_as=origin_as,
                    rov=state,
                    query_start_ns=started,
                    query_end_ns=completed,
                ))
            if stop_event is not None and stop_event.is_set():
                break
            if poll_interval > 0:
                if stop_event is None:
                    time.sleep(poll_interval)
                elif stop_event.wait(poll_interval):
                    break
    finally:
        connection.close()


# Observe each candidate and emit initial states, readiness, and ROV changes.
def observe_candidates(targets, socket_path, poll_interval, timeout, on_event, stop_event):
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

    observe(queries, socket_path, poll_interval, timeout, receive_sample, stop_event)


# Parse selected routes, requiring BGP attributes only for consumers that request them.
def parse_primary_rib(text, *, include_attributes=False):
    rib = {}
    prefix = None
    route = None

    # Finish one route after all of its indented attributes have been read.
    def finish():
        if route is None:
            return
        if route["type"] != "unicast" or not route["selected"]:
            raise ValueError(f"Unsupported primary route for {route['prefix']}")
        if route["path"] is None:
            raise ValueError(f"Primary route is missing its next hop: {route['prefix']}")
        if include_attributes:
            for attribute in ("as_path", "origin"):
                if attribute not in route["attributes"]:
                    raise ValueError(f"Primary route is missing {attribute}: {route['prefix']}")
            route["path"].update(route["attributes"])
        if route["prefix"] in rib:
            raise ValueError(f"Multiple primary routes for {route['prefix']}")
        rib[route["prefix"]] = route["path"]

    for line in text.splitlines():
        match = ROUTE_HEADER.match(line)
        if match:
            finish()
            supplied, route_type, peer, mark = match.groups()
            if supplied is not None:
                prefix = str(ip.ip_network(supplied))
            if prefix is None:
                raise ValueError("Primary route is missing its prefix")
            route = dict(
                prefix=prefix,
                type=route_type,
                selected=mark == "*",
                path=None,
                attributes=dict(med=None),
            )
            route["peer"] = peer
            continue
        if route is not None:
            next_hop = re.match(r"via (\S+) on (\S+)", line.strip())
            if next_hop:
                if route["path"] is not None:
                    raise ValueError(f"Multiple next hops for {route['prefix']}")
                gateway, interface = next_hop.groups()
                route["path"] = dict(
                    peer=route["peer"],
                    gateway=str(ip.ip_address(gateway)),
                    interface=interface,
                )

            attribute = re.fullmatch(r"BGP\.(as_path|origin|med):\s*(.*)", line.strip())
            if include_attributes and attribute:
                name, value = attribute.groups()
                if name == "as_path":
                    route["attributes"][name] = parse_as_path(value)
                elif name == "origin":
                    route["attributes"][name] = parse_origin(value)
                else:
                    route["attributes"][name] = int(value)
    finish()
    return rib


# Read both primary RIB tables through the existing restricted control connection.
def read_primary_rib(socket_path, timeout, *, include_attributes=False):
    connection = BirdConnection(socket_path, timeout)
    try:
        connection.command("restrict")
        rib = {}
        for table in ("master4", "master6"):
            output = connection.command(f"show route table {table} primary all")
            rib.update(parse_primary_rib(output, include_attributes=include_attributes))
    finally:
        connection.close()
    return rib


# Format a primary RIB path for the existing experiment reports.
def describe_path(path):
    if path is None:
        return "none"
    return f"{path['peer']} via {path['gateway']} ({path['interface']})"


# Select and normalize the BGP path fields used by every RIB equality check.
def rib_path_fields(path):
    if path is None:
        return None

    segments = []
    for segment in path["as_path"]:
        kind = segment["type"]
        asns = list(segment["asns"])
        if kind in ("AS_SET", "AS_CONFED_SET"):
            asns.sort()
        if kind == "AS_SEQUENCE" and segments and segments[-1]["type"] == kind:
            segments[-1]["asns"].extend(asns)
        else:
            segments.append(dict(type=kind, asns=asns))

    origin = path["origin"]
    if isinstance(origin, int):
        origin = {0: "IGP", 1: "EGP", 2: "INCOMPLETE"}[origin]

    return dict(
        peer=path["peer"],
        gateway=path["gateway"],
        as_path=segments,
        origin=origin,
        # Treat an absent MED as zero only in the comparison representation.
        med=0 if path["med"] is None else path["med"],
    )


# Start one persistent CLI connection for a batch and return its initial states.
def start_candidate_observer(changes, socket_path, poll_interval, timeout, check_alive,
                             *, event_queue=None, error_queue=None):
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
                stop,
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
