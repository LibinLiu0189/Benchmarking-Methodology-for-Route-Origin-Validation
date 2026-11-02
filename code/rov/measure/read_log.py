"""Observe native BIRD events through log echo or a dedicated trace file."""

import ipaddress as ip
import json
import os
import stat
from pathlib import Path
import re
import queue
import threading
import time

from rov.paths import CONFIG
from .bird_connection import BirdConnection
from .birdc_log import BirdcLog

VRP = re.compile(
    r"^rpki_source\.\S+ > (added|removed|ignored)(?: \[[^]]+\])? "
    r"(\S+/\d+)-(\d+) AS(\d+) .*"
)
RTR_STATE = re.compile(r"^rpki_source: Changing from (\S+) to (\S+) state$")
RIB = re.compile(r"^fib[46]\.\S+ < (added|replaced|removed) (\S+/\d+) (.*)")


# Print events without creating result files.
def emit(event):
    print(json.dumps(event, separators=(",", ":")), flush=True)


# Parse BIRD's AS path notation while preserving sequence and set segment types.
def parse_as_path(text):
    text = text.strip()
    if text.startswith("(path ") and text.endswith(")"):
        text = text[6:-1].strip()

    segments = []
    position = 0
    pattern = r"\s*(\(\{[^{}()]*\}\)|\{[^{}()]*\}|\([^{}()]*\)|[0-9]+)"
    while position < len(text):
        match = re.match(pattern, text[position:])
        if match is None:
            raise ValueError(f"Unrecognized or truncated AS path: {text}")
        token = match.group(1)
        position += match.end()

        if token.startswith("({"):
            kind, members = "AS_CONFED_SET", token[2:-2]
        elif token.startswith("{"):
            kind, members = "AS_SET", token[1:-1]
        elif token.startswith("("):
            kind, members = "AS_CONFED_SEQUENCE", token[1:-1]
        else:
            kind, members = "AS_SEQUENCE", token

        asns = [int(value) for value in members.split()]
        if kind in ("AS_SET", "AS_CONFED_SET"):
            asns.sort()
        if kind == "AS_SEQUENCE" and segments and segments[-1]["type"] == kind:
            segments[-1]["asns"].extend(asns)
        else:
            segments.append(dict(type=kind, asns=asns))

    return segments


# Normalize CLI origin names and the BIRD filter's BGP origin enum notation.
def parse_origin(text):
    origin = text.strip().upper()
    if origin in ("IGP", "EGP", "INCOMPLETE"):
        return origin

    match = re.fullmatch(r"\(enum 31\)([012])", text.strip())
    if match:
        return ("IGP", "EGP", "INCOMPLETE")[int(match.group(1))]
    raise ValueError(f"Unrecognized BGP origin: {text}")


class FileLog:
    # Open the dedicated append-only trace file at the pre-subscription boundary.
    def __init__(self, path):
        self.path = Path(path)
        self.source = self.path.open("rb")
        self.buffer = b""
        try:
            info = os.fstat(self.source.fileno())
            if not stat.S_ISREG(info.st_mode):
                raise ValueError("BIRD trace must be a regular file")
            self.identity = (info.st_dev, info.st_ino)
            if info.st_size:
                self.source.seek(-1, os.SEEK_END)
                if self.source.read(1) != b"\n":
                    raise ValueError("BIRD trace subscription boundary ends in a partial line")
            self.source.seek(0, os.SEEK_END)
        except BaseException:
            self.source.close()
            raise

    # Tail complete lines; the diagnostic wall-clock prefix is never used for timing.
    def line(self, deadline):
        while True:
            block = self.source.readline()
            if block:
                self.buffer += block
                if not self.buffer.endswith(b"\n"):
                    continue
                text = self.buffer.decode("utf-8", errors="strict")
                self.buffer = b""
                match = re.fullmatch(
                    r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{6} <(?:TRACE|INFO)> (.*)\n",
                    text,
                )
                if match is None:
                    raise ValueError(f"Unrecognized BIRD trace file line: {text.rstrip()}")
                return "+" + match.group(1)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                info = self.path.stat()
                if (info.st_dev, info.st_ino) != self.identity or info.st_size < self.source.tell():
                    raise RuntimeError("BIRD trace file was replaced or truncated")
                raise TimeoutError("BIRD trace file is idle")
            time.sleep(min(0.001, remaining))

    # Validate the stopped reader without draining or extending collection.
    def check_complete(self):
        info = self.path.stat()
        if (info.st_dev, info.st_ino) != self.identity or info.st_size < self.source.tell():
            raise RuntimeError("BIRD trace file was replaced or truncated")
        if self.buffer:
            raise RuntimeError("BIRD trace stopped with an incomplete line")
        if info.st_size != self.source.tell():
            raise RuntimeError("BIRD trace stopped with unread records")

    # Close the reader while retaining the trace as evidence on success or failure.
    def close(self):
        self.source.close()


class LogParser:
    # Keep the selected path printed by the export filter until its native trace arrives.
    def __init__(self):
        self.pending_rib = {}

    # Decode one log line; timestamps identify external receipt, not application.
    def parse(self, text, observed_ns):
        if text.startswith("ROVLOG RIB|"):
            fields = text.split("|")
            if len(fields) != 8:
                raise ValueError(f"Unrecognized selected-path log: {text}")
            _, prefix, peer, gateway, interface, as_path, origin, med = fields
            prefix = str(ip.ip_network(prefix))
            if prefix in self.pending_rib:
                raise RuntimeError(f"Selected-path log was not paired for {prefix}")
            self.pending_rib[prefix] = dict(
                peer=peer,
                gateway=str(ip.ip_address(gateway)),
                interface=interface,
                as_path=parse_as_path(as_path),
                origin=parse_origin(origin),
                med=None if med.strip() == "(void)" else int(med),
            )
            return None

        match = RTR_STATE.fullmatch(text)
        if match:
            before, after = match.groups()
            return dict(event="rtr", before=before, after=after, observed_ns=observed_ns)

        match = VRP.fullmatch(text)
        if match:
            action, prefix, maximum, asn = match.groups()
            if action == "ignored":
                return None
            return dict(
                event="vrp",
                action=action,
                prefix=prefix,
                max_length=int(maximum),
                asn=int(asn),
                observed_ns=observed_ns,
            )

        match = RIB.fullmatch(text)
        if match:
            action, prefix, detail = match.groups()
            prefix = str(ip.ip_network(prefix))
            if action == "removed":
                if prefix in self.pending_rib:
                    raise RuntimeError(
                        f"Removed route unexpectedly has a selected path for {prefix}"
                    )
                after = None
            else:
                if prefix not in self.pending_rib:
                    raise RuntimeError(
                        f"RIB {action} is missing its selected-path log for {prefix}"
                    )
                after = self.pending_rib.pop(prefix)
            return dict(
                event="rib",
                action=action,
                prefix=prefix,
                after=after,
                detail=detail,
                observed_ns=observed_ns,
            )

        if text.startswith("rpki_source.") and " > " in text:
            raise ValueError(f"Unrecognized VRP trace: {text}")
        if text.startswith(("fib4.", "fib6.")) and " < " in text:
            raise ValueError(f"Unrecognized RIB trace: {text}")
        return None


# Subscribe before enabling tracing and disable the owned tracing on exit.
def observe(
    socket_path=CONFIG / "bird.ctl",
    planes=("vrp", "rib"),
    timeout=10,
    on_event=emit,
    stop_event=None,
    *, on_poll=None, log_path=None, birdc=None,
):
    if log_path is not None and birdc is not None:
        raise ValueError("Choose one log transport")
    connection = BirdConnection(socket_path, timeout)
    stream = connection
    enabled = []
    parser = LogParser()

    # Reuse the same parser and external receipt timestamps for both transports.
    def receive(text, observed_ns):
        if text.startswith("ROVLOG RIB|") and "rib" not in planes:
            return
        event = parser.parse(text, observed_ns)
        if event is not None and event["event"] in planes:
            on_event(event)

    connection.on_log = receive
    try:
        protocols = set()
        if "rib" in planes:
            protocols = {
                line.split()[0]
                for line in connection.command("show protocols").splitlines()
                if line.split()
            }
            if not protocols.intersection(("fib4", "fib6")):
                raise ValueError("No fib4 or fib6 protocol is available for RIB observation")

        if log_path is None:
            echo_classes = "trace, info" if "rib" in planes else "trace"
            if birdc is None:
                connection.command(f"echo {{ {echo_classes} }} 65536")
            else:
                stream = BirdcLog(birdc, socket_path, timeout, echo_classes)
        else:
            stream = FileLog(log_path)
        if "vrp" in planes or "rtr" in planes:
            classes = ["routes"] if "vrp" in planes else []
            if "rtr" in planes:
                classes.append("events")
            connection.command("debug rpki_source { " + ", ".join(classes) + " }")
            enabled.append("rpki_source")
        if "rib" in planes:
            for protocol in ("fib4", "fib6"):
                if protocol in protocols:
                    connection.command(f"debug {protocol} {{ routes }}")
                    enabled.append(protocol)
        on_event(dict(event="ready", component="read_log", planes=list(planes)))
        while stop_event is None or not stop_event.is_set():
            if on_poll is not None and log_path is None:
                on_poll(False)
            try:
                line = stream.line(time.monotonic() + 0.5)
            except TimeoutError:
                # Only a drained stream with no unfinished RIB record is idle.
                if on_poll is not None:
                    on_poll(not stream.buffer and not parser.pending_rib)
                continue
            if on_poll is not None and log_path is not None:
                on_poll(False)
            if not line.startswith("+"):
                raise RuntimeError(f"Unexpected log response: {line}")
            receive(line[1:], time.monotonic_ns())
        if log_path is not None:
            stream.check_complete()
            if parser.pending_rib:
                raise RuntimeError("BIRD trace stopped with unpaired selected-path details")
    finally:
        if stream is not connection:
            stream.close()
        connection.close()
        # Use a separate connection because the log stream may have overflowed.
        if enabled:
            cleanup = BirdConnection(socket_path, timeout)
            try:
                for protocol in enabled:
                    cleanup.command(f"debug {protocol} off")
            finally:
                cleanup.close()


# Start a log observer and wait for its existing ready event.
def start_log_observer(socket_path, planes, timeout, *, birdc=None, log_path=None):
    events = queue.Queue()
    errors = queue.Queue()
    stop = threading.Event()

    file_drained = False

    # Report EOF and invalidate it when a new record or partial line arrives.
    def file_poll(drained):
        nonlocal file_drained
        if drained or file_drained:
            events.put(dict(event="drained", drained=drained, observed_ns=time.monotonic_ns()))
        file_drained = drained

    # Forward observer results to thread-safe queues for the experiment loop.
    def worker():
        try:
            if log_path is not None:
                observe(socket_path, planes, timeout, events.put, stop,
                        log_path=log_path, birdc=birdc, on_poll=file_poll)
            elif birdc is None:
                observe(socket_path, planes, timeout, events.put, stop)
            else:
                observe(socket_path, planes, timeout, events.put, stop, birdc=birdc)
        except BaseException as error:
            errors.put(error)

    thread = threading.Thread(target=worker, name="bird-log-observer")
    thread.start()
    deadline = time.monotonic() + timeout
    while True:
        if not errors.empty():
            thread.join()
            raise errors.get()
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            stop_observer(stop, thread, timeout)
            raise TimeoutError("Timed out waiting for the BIRD log observer")
        try:
            ready = events.get(timeout=min(0.1, remaining))
            break
        except queue.Empty:
            continue
    if ready.get("event") != "ready":
        stop_observer(stop, thread, timeout)
        raise RuntimeError("Received a log event before the observer was ready")
    if not errors.empty():
        raise errors.get()
    return events, errors, stop, thread


# Stop and join an observation thread without changing experiment completion rules.
def stop_observer(stop, thread, timeout):
    stop.set()
    thread.join(timeout + 1)
    if thread.is_alive():
        raise TimeoutError(f"Timed out stopping observer {thread.name}")
