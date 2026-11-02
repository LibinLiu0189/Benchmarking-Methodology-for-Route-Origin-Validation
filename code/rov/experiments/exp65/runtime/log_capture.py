"""Capture unfiltered timed BIRD lines and interpret them only after collection."""

from collections import Counter
import json
import queue
import threading
import time

from rov.measure.bird_connection import BirdConnection
from rov.measure.read_log import FileLog, LogParser
from rov.measure.vrp_streams import record_next_event


# Subscribe to native traces without parsing routes or inspecting expected targets.
def start_capture(socket_path, timeout, events, errors, log_path, planes):
    if log_path is None:
        raise ValueError("Raw VRP/RIB capture requires the dedicated BIRD trace")
    stop = threading.Event()

    # Read every complete trace line with its original external receipt timestamp.
    def worker():
        connection = None
        stream = None
        enabled = []
        try:
            connection = BirdConnection(socket_path, timeout)
            stream = FileLog(log_path)
            if "vrp" in planes:
                connection.command("debug rpki_source { routes, events }")
                enabled.append("rpki_source")
            if "rib" in planes:
                protocols = {
                    line.split()[0]
                    for line in connection.command("show protocols").splitlines()
                    if line.split()
                }
                if not protocols.intersection(("fib4", "fib6")):
                    raise ValueError("No fib4 or fib6 protocol is available")
                for protocol in ("fib4", "fib6"):
                    if protocol in protocols:
                        connection.command(f"debug {protocol} {{ routes }}")
                        enabled.append(protocol)
            events.put(dict(event="ready", component="read_log", planes=list(planes)))
            while not stop.is_set():
                try:
                    line = stream.line(time.monotonic() + 0.5)
                except TimeoutError:
                    continue
                observed_ns = time.monotonic_ns()
                if not line.startswith("+"):
                    raise RuntimeError(f"Unexpected raw trace record: {line}")
                events.put(dict(event="bird_log", text=line[1:], observed_ns=observed_ns))
            stream.check_complete()
        except BaseException as error:
            errors.put(error)
        finally:
            if stream is not None:
                stream.close()
            if connection is not None:
                connection.close()
            if enabled:
                cleanup = None
                try:
                    cleanup = BirdConnection(socket_path, timeout)
                    for protocol in enabled:
                        cleanup.command(f"debug {protocol} off")
                except BaseException as error:
                    errors.put(error)
                finally:
                    if cleanup is not None:
                        cleanup.close()

    thread = threading.Thread(target=worker, name="exp65-raw-log-observer")
    thread.start()
    return stop, thread


# Save every captured line; only raw activity influences the quiet timer.
def receive_raw(state, wait, check_errors=True):
    if check_errors and not state.errors.empty():
        raise state.errors.get()
    if state.pending:
        event = state.pending.pop(0)
    else:
        try:
            event = state.events.get(timeout=wait)
        except queue.Empty:
            if check_errors and not state.errors.empty():
                raise state.errors.get()
            return False
    destination = state.raw_capture if event["event"] == "bird_log" else getattr(state, "monitor_capture", None)
    if destination is None:
        raise RuntimeError(f"Unexpected raw capture event: {event}")
    record = json.dumps(event, separators=(",", ":")) + "\n"
    with state.capture_lock:
        destination.write(record)
    state.last_change_ns = time.monotonic_ns()
    return True


# Decode saved lines with unchanged receipt times, then build the original matching pools.
def replay_capture(state):
    parser = LogParser()
    counts = Counter()
    # The online backlog proxy has no meaning during offline interpretation.
    state.last_backlog_ns = float("inf")
    with state.raw_capture_path.open() as source:
        for line in source:
            raw = json.loads(line)
            counts["raw_lines"] += 1
            event = parser.parse(raw["text"], raw["observed_ns"])
            if event is None:
                continue
            counts[event["event"]] += 1
            if event["event"] == "rib" and event["prefix"] not in state.rib.current:
                counts["rib_outside_targets"] += 1
            state.pending.append(event)
            record_next_event(state, 0, check_errors=False)
    if parser.pending_rib:
        raise RuntimeError("Raw capture ended with unpaired selected-path details")
    return dict(counts)


