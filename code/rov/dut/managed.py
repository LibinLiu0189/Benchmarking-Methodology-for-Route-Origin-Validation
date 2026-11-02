"""Own initialization subprocess output, readiness, and orderly shutdown."""

import json
import os
import queue
import re
import signal
import sys
import time

from rov.paths import CONFIG
from rov.measure.bird_connection import BirdConnection
from rov.measure.resource import sync_snapshot

INITIALIZATION_READY = b"Initial VRP and BGP loading complete."
INITIALIZATION_READY_TIMEOUT = 3600

# Forward all initialization output and recognize its complete readiness line.
def forward_initialization_output(process, messages):
    pending = b""
    discard_line = False
    forward_error = None
    try:
        while chunk := os.read(process.stdout.fileno(), 65536):
            if forward_error is None:
                try:
                    offset = 0
                    while offset < len(chunk):
                        offset += os.write(sys.stdout.fileno(), chunk[offset:])
                except OSError as error:
                    forward_error = error
                    messages.put(error)

            parts = chunk.split(b"\n")
            for segment in parts[:-1]:
                if not discard_line and pending + segment == INITIALIZATION_READY:
                    messages.put("ready")
                pending = b""
                discard_line = False
            if not discard_line:
                if len(pending) + len(parts[-1]) > len(INITIALIZATION_READY) + 1:
                    pending = b""
                    discard_line = True
                else:
                    pending += parts[-1]
    except OSError as error:
        messages.put(error)
    finally:
        process.stdout.close()
        messages.put("closed")


# Reject an exited initialization or failed output stream during measurement.
def check_initialization(process, messages):
    if process.poll() is not None:
        raise RuntimeError(f"Initialization exited unexpectedly ({process.returncode})")
    try:
        message = messages.get_nowait()
    except queue.Empty:
        return
    if isinstance(message, BaseException):
        raise RuntimeError(f"Initialization output failed: {message}") from message
    if message == "closed":
        raise RuntimeError("Initialization output closed unexpectedly")


# Wait for this initialization process to print readiness while it remains alive.
def wait_for_initialization(process, messages):
    deadline = time.monotonic() + INITIALIZATION_READY_TIMEOUT
    while True:
        if process.poll() is not None:
            raise RuntimeError(f"Initialization exited before readiness ({process.returncode})")
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("Timed out waiting for initial VRP and BGP loading")
        try:
            message = messages.get(timeout=min(0.1, remaining))
        except queue.Empty:
            continue
        if message == "ready":
            if process.poll() is not None:
                raise RuntimeError(f"Initialization exited before readiness ({process.returncode})")
            return
        if isinstance(message, BaseException):
            raise RuntimeError(f"Initialization output failed: {message}") from message
        if message == "closed":
            raise RuntimeError("Initialization output closed before readiness")


# Request the owned initialization to stop and wait for its service cleanup.
def stop_initialization(process, output_thread):
    previous_term = signal.signal(signal.SIGTERM, signal.SIG_IGN)
    previous_int = signal.signal(signal.SIGINT, signal.SIG_IGN)
    try:
        if process.poll() is None:
            process.terminate()
        return_code = process.wait()
        if output_thread.is_alive():
            output_thread.join()
        return return_code
    finally:
        signal.signal(signal.SIGINT, previous_int)
        signal.signal(signal.SIGTERM, previous_term)


# Convert an external stop request into the experiment's cleanup path.
def stop_requested(signum, frame):
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    raise KeyboardInterrupt


# Retain initialization output and mark observed sync and readiness boundaries.
def forward_sync_output(process, messages, result):
    try:
        for raw in iter(process.stdout.readline, b""):
            sys.stdout.buffer.write(raw)
            sys.stdout.buffer.flush()
            text = raw.decode(errors="replace").rstrip()
            now = time.monotonic_ns()
            if re.search(r"(?:^|\s)rpki_source: Connected\s*$", text):
                result["sync_start_observed_ns"] = now
                try:
                    result["sync_start_counters"] = sync_snapshot()
                except (OSError, ValueError, RuntimeError) as error:
                    result["sync_boundary_error"] = str(error)
            match = re.fullmatch(r"Initial RTR connection-to-installation: ([0-9.]+) s", text)
            if match:
                result["connection_to_initial_install_ms"] = float(match[1]) * 1000
                result["sync_end_observed_ns"] = now
                try:
                    result["sync_end_counters"] = sync_snapshot()
                except (OSError, ValueError, RuntimeError) as error:
                    result["sync_boundary_error"] = str(error)
            timing_match = re.search(r"(?:^|>> )Initial RTR timing: (\{.*\})$", text)
            if timing_match:
                timing = json.loads(timing_match[1])
                result["initial_sync_timing"] = timing
                result["full_sync_ms"] = timing["full_sync_ms"]
            if text == "Initial VRP and BGP loading complete.":
                messages.put("ready")
    except BaseException as error:
        messages.put(error)
    finally:
        process.stdout.close()
        messages.put("closed")


# Preserve raw output while accepting BRIO console prompts before readiness markers.
def forward_prompt_sync_output(process, messages, result, output_observer=None):
    try:
        for raw in iter(process.stdout.readline, b""):
            sys.stdout.buffer.write(raw)
            sys.stdout.buffer.flush()
            text = raw.decode(errors="replace").rstrip()
            now = time.monotonic_ns()
            if output_observer is not None:
                output_observer(text, now)
            boundary = None
            if re.search(r"(?:^|\s)rpki_source: Connected\s*$", text):
                result["sync_start_observed_ns"] = now
                boundary = "sync_start_counters"
            match = re.search(r"(?:^|>+\s*)Initial RTR connection-to-installation: ([0-9.]+) s$", text)
            if match:
                result["connection_to_initial_install_ms"] = float(match[1]) * 1000
                result["sync_end_observed_ns"] = now
                boundary = "sync_end_counters"
            if boundary is not None:
                try:
                    result[boundary] = sync_snapshot()
                except (OSError, ValueError, RuntimeError) as error:
                    result["sync_boundary_error"] = str(error)
            timing_match = re.search(r"(?:^|>+\s*)Initial RTR timing: (\{.*\})$", text)
            if timing_match:
                timing = json.loads(timing_match[1])
                result["initial_sync_timing"] = timing
                result["full_sync_ms"] = timing["full_sync_ms"]
            if re.search(r"(?:^|>+\s*)Initial VRP and BGP loading complete\.$", text):
                messages.put("ready")
    except BaseException as error:
        messages.put(error)
    finally:
        process.stdout.close()
        messages.put("closed")


# Save only the owned run's configuration and BIRD status as reproducibility evidence.
def capture_environment(output, timeout):
    for name in ("bird.conf", "bng.json"):
        with (output / name).open("xb") as target:
            target.write((CONFIG / name).read_bytes())
    connection = BirdConnection(CONFIG / "bird.ctl", timeout)
    try:
        connection.command("restrict")
        status = connection.command("show status")
        rtr = connection.command("show protocols all rpki_source")
        (output / "bird-status.txt").write_text(status)
        (output / "rtr-status.txt").write_text(rtr)
    finally:
        connection.close()
