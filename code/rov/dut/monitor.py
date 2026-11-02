"""Observe initial loading of existing services without owning their lifecycle."""

import json
from pathlib import Path
import re
import socket
import subprocess
import sys
import threading
import time

from rov.measure.read_log import observe as observe_log


# Query the existing Bngblaster control socket for initial sending progress.
def generator_sessions(config_dir):
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
        connection.settimeout(5)
        connection.connect(str(config_dir / "bng.sock"))
        connection.sendall(b'{"command":"bgp-sessions"}')
        response = bytearray()
        while chunk := connection.recv(65536):
            response.extend(chunk)
    result = json.loads(response)
    if result.get("code") != 200:
        raise RuntimeError(f"Bngblaster command failed: {result}")
    return result["bgp-sessions"]


# Read a BIRD route count without treating the number as a convergence measurement.
def route_count(binary, config_dir, command):
    result = subprocess.run(
        [str(binary), "-r", "-s", str(config_dir / "bird.ctl"), command],
        capture_output=True,
        text=True,
        check=True,
        timeout=10,
    )
    match = re.search(r"(?m)^(\d+) of \d+ routes", result.stdout)
    if match is None:
        raise RuntimeError(f"Missing route count in BIRD response: {result.stdout}")
    return int(match[1])


# Check RTR establishment and both VRP table counts.
def vrps_loaded(binary, config_dir, vrp_counts):
    status = subprocess.run(
        [str(binary), "-r", "-s", str(config_dir / "bird.ctl"),
         "show protocols all rpki_source"],
        capture_output=True, text=True, check=True, timeout=10,
    )
    if "Established" not in status.stdout:
        return False
    return all(
        route_count(binary, config_dir, f"show route table r{afi} count")
        == vrp_counts[str(afi)]
        for afi in (4, 6)
    )


# Require every configured session to establish and finish its RAW transmission.
def routes_sent(config_dir, peer_count):
    sessions = generator_sessions(config_dir)
    return len(sessions) == peer_count and all(
        session["state"] == "established" and session["raw-update-state"] == "done"
        for session in sessions
    )


# Compare prefilter import counts for each peer and announced address family.
def routes_received(binary, config_dir, peers):
    return all(
        route_count(
            binary, config_dir,
            f"show route import table {peer['name']}.ipv{afi} count",
        ) == peer["import_counts"][afi]
        for peer in peers
        for afi in peer["families"]
    )


# Check the current network namespace for a listening TCP socket on one port.
def tcp_listener_ready(port):
    for table in (Path("/proc/net/tcp"), Path("/proc/net/tcp6")):
        try:
            lines = table.read_text().splitlines()[1:]
        except FileNotFoundError:
            continue
        for line in lines:
            fields = line.split()
            local_port = int(fields[1].rsplit(":", 1)[1], 16)
            state = fields[3]
            if local_port == port and state == "0A":
                return True
    return False


# Require a drained RIB stream to stay quiet after the observer is ready.
def wait_for_rib_stability(config_dir, timeout, alive):
    if not 0 < timeout < float("inf"):
        raise ValueError("RIB stability timeout must be finite and positive")
    deadline = time.monotonic() + timeout
    last_change = None
    stop = threading.Event()

    # Start the window at readiness and reset it on selected-route changes.
    def receive(event):
        nonlocal last_change
        if event["event"] == "ready":
            last_change = time.monotonic()
        elif (
            last_change is not None and event["event"] == "rib"
            and event["action"] in ("added", "replaced", "removed")
        ):
            last_change = time.monotonic()

    # Check liveness and timeout even while unrelated log lines keep arriving.
    def check_progress(idle):
        alive()
        now = time.monotonic()
        if now >= deadline:
            raise TimeoutError("Timed out waiting for 10 seconds of RIB stability")
        if idle and last_change is not None and now - last_change >= 10:
            stop.set()

    print("Waiting for 10 seconds of RIB stability.", flush=True)
    observe_log(
        Path(config_dir) / "bird.ctl", ("rib",), min(5.0, timeout),
        receive, stop, on_poll=check_progress,
    )
    if not stop.is_set():
        raise RuntimeError("RIB observer stopped before stability was confirmed")
    check_progress(False)
    print("RIB stable for 10 seconds.", flush=True)


# Forward BIRD's stderr and record the RTR transport connection event.
def read_bird_log(stream, on_connected):
    for line in stream:
        observed_ns = time.monotonic_ns()
        sys.stderr.write(line)
        sys.stderr.flush()
        if re.search(r"(?:^|\s)rpki_source: Connected\s*$", line):
            on_connected(observed_ns)


# Wait for an initialization condition while checking process liveness.
def wait_until(check, description, timeout, alive):
    deadline = time.monotonic() + timeout
    last_error = ""
    while True:
        alive()
        try:
            if check():
                return time.monotonic_ns()
        except (OSError, RuntimeError, subprocess.SubprocessError) as error:
            last_error = str(error)
        if time.monotonic() >= deadline:
            raise RuntimeError(f"Timed out waiting for {description}. {last_error}")
        time.sleep(0.001)
