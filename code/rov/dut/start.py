"""Generate software configurations, load initial inputs, and own their processes."""

import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import threading
import time

from . import monitor
from rov.dut.configuration import bird_config
from rov.dut.configuration import generator_config


# Convert process termination into the same cleanup path as Ctrl+C.
def stop_requested(signum, frame):
    raise KeyboardInterrupt


# Read the initial response after installation without changing its observed endpoint.
def initial_rtr_timing(path, started_ns, connected_ns, installed_ns, timeout, alive):
    response = None
    if path is not None:
        deadline = time.monotonic() + timeout
        while response is None:
            alive()
            text = Path(path).read_text()
            if not text or text.endswith("\n"):
                records = [json.loads(line) for line in text.splitlines()]
                response = next((row for row in records
                                 if row.get("format") == "brio-first-vrp-v1"
                                 and row.get("query") == "reset"
                                 and row.get("status") == "complete"
                                 and row.get("first_vrp_send_ns", 0) >= started_ns), None)
            if response is None:
                if time.monotonic() >= deadline:
                    raise TimeoutError("No completed BRIO initial Prefix-PDU timing record")
                time.sleep(0.01)
    first = response["first_vrp_send_ns"] if response is not None else None
    if first is not None and first > installed_ns:
        raise RuntimeError("Initial VRP installation observation precedes its first Prefix PDU")
    return dict(
        clock="CLOCK_MONOTONIC", reference="first_prefix_pdu_send",
        fifo_write_start_ns=None, fifo_submitted_ns=None, brio_notify_send_ns=None,
        connected_observed_ns=connected_ns, first_vrp_send_ns=first,
        vrp_complete_observed_ns=installed_ns,
        full_sync_ms=(installed_ns - first) / 1e6 if first is not None else None,
        connection_to_complete_ms=(installed_ns - connected_ns) / 1e6,
        brio_response=response,
    )


# Start the three foreground processes, confirm initial loading, and clean up on exit.
def serve(settings, peers, config_dir, vrp_file, vrp_counts):
    config_dir = Path(config_dir)
    binaries = Path(settings["bin_dir"])
    processes = []
    owned_paths = []
    fifo_fd = None
    bird_log_thread = None
    connection_observed_ns = None
    signal.signal(signal.SIGTERM, stop_requested)

    # Track only children created by this run and report unexpected exits.
    def alive():
        for name, process in processes:
            if process.poll() is not None:
                raise RuntimeError(
                    f"{name} exited ({process.returncode}); see the terminal output"
                )

    # Start a foreground child in its own group to allow complete owned-child cleanup.
    def launch(name, command, stdin=subprocess.DEVNULL, stderr=None):
        process = subprocess.Popen(
            list(map(str, command)),
            stdin=stdin,
            stderr=stderr,
            text=stderr == subprocess.PIPE,
            cwd=config_dir,
            start_new_session=True,
        )
        processes.append((name, process))
        return process

    # Retain the connection timestamp reported by the monitoring reader.
    def record_connection(observed_ns):
        nonlocal connection_observed_ns
        connection_observed_ns = observed_ns

    try:
        (config_dir / "bird.conf").write_text(bird_config(settings, peers))
        (config_dir / "bng.json").write_text(
            json.dumps(generator_config(settings, peers), indent=2) + "\n"
        )
        subprocess.run(
            [str(binaries / "bird"), "-p", "-c", str(config_dir / "bird.conf")], check=True
        )
        fifo = config_dir / "brio.in"
        os.mkfifo(fifo, 0o600)
        owned_paths.append(fifo)
        fifo_fd = os.open(fifo, os.O_RDWR)
        command = f"append {vrp_file}\n"
        if (
            any(c.isspace() for c in str(vrp_file))
            or "%" in str(vrp_file)
            or len(command.splitlines()[0].encode()) >= 254
        ):
            raise ValueError(
                "BRIO requires an initial path without whitespace or %, shorter than its console limit"
            )
        (config_dir / "brio-init.script").write_text(command)
        brio_started_ns = time.monotonic_ns()
        launch(
            "brio",
            [
                binaries / "brio_rc",
                settings["rtr_port"],
                "-f",
                config_dir / "brio-init.script",
            ],
            stdin=fifo_fd,
        )
        monitor.wait_until(
            lambda: monitor.tcp_listener_ready(settings["rtr_port"]),
            "BRIO RTR listener", settings["timeout"], alive,
        )
        bird_process = launch(
            "bird",
            [
                binaries / "bird",
                "-f",
                "-c",
                config_dir / "bird.conf",
                "-s",
                config_dir / "bird.ctl",
                "-P",
                config_dir / "bird.pid",
            ],
            stderr=subprocess.PIPE if settings["measure_full_sync"] else None,
        )
        owned_paths.extend((config_dir / "bird.ctl", config_dir / "bird.pid"))
        if settings["measure_full_sync"]:
            bird_log_thread = threading.Thread(
                target=monitor.read_bird_log, args=(bird_process.stderr, record_connection),
                name="bird-initial-log"
            )
            bird_log_thread.start()
            monitor.wait_until(
                lambda: connection_observed_ns is not None,
                "BIRD RTR connection log", settings["timeout"], alive,
            )
        loaded_ns = monitor.wait_until(
            lambda: monitor.vrps_loaded(binaries / "birdc", config_dir, vrp_counts),
            "VRP installation", settings["timeout"], alive,
        )
        if settings["measure_full_sync"]:
            elapsed_seconds = (loaded_ns - connection_observed_ns) / 1_000_000_000
            print(f"Initial RTR connection-to-installation: {elapsed_seconds:.3f} s", flush=True)
            timing = initial_rtr_timing(
                os.environ.get("BRIO_VRP_TIMING_LOG"), brio_started_ns,
                connection_observed_ns, loaded_ns, settings["timeout"], alive,
            )
            print("Initial RTR timing: " + json.dumps(timing, separators=(",", ":")), flush=True)
        print("Initial VRPs installed; starting BGP sessions.", flush=True)
        launch(
            "bngblaster",
            [
                binaries / "bngblaster",
                "-C",
                config_dir / "bng.json",
                "-S",
                config_dir / "bng.sock",
                "-b",
            ],
        )
        owned_paths.append(config_dir / "bng.sock")
        monitor.wait_until(
            lambda: monitor.routes_sent(config_dir, len(peers)),
            "initial BGP transmission", settings["timeout"], alive,
        )
        monitor.wait_until(
            lambda: monitor.routes_received(binaries / "birdc", config_dir, peers),
            "initial BGP import counts", settings["timeout"], alive,
        )
        monitor.wait_for_rib_stability(config_dir, settings["timeout"], alive)
        print(
            "Initial VRP and BGP loading complete.", flush=True
        )
        print(
            "Services remain attached to this command. Press Ctrl+C to stop them.",
            flush=True,
        )
        while True:
            alive()
            time.sleep(1)
    finally:
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        print("Stopping owned services.", flush=True)
        requested_stops = set()
        cleanup_failures = []
        for name, process in reversed(processes):
            if process.poll() is None:
                requested_stops.add(name)
                try:
                    os.killpg(process.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
        for name, process in reversed(processes):
            # Bngblaster joins workers and writes final reports before returning.
            stop_timeout = 30 if name == "bngblaster" else 5
            timeout_expired = False
            try:
                process.wait(timeout=stop_timeout)
            except subprocess.TimeoutExpired:
                timeout_expired = True
                if process.poll() is None:
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                process.wait()
            expected_term = name in requested_stops and process.returncode == -signal.SIGTERM
            if timeout_expired and process.returncode == -signal.SIGKILL:
                cleanup_failures.append(f"{name} did not stop within {stop_timeout} seconds; killed")
            elif process.returncode != 0 and not expected_term:
                cleanup_failures.append(f"{name} exited with code {process.returncode}")
        if bird_log_thread is not None:
            bird_log_thread.join()
            bird_process.stderr.close()
        if fifo_fd is not None:
            os.close(fifo_fd)
        for path in owned_paths:
            path.unlink(missing_ok=True)
        if cleanup_failures:
            message = "Service cleanup failed: " + "; ".join(cleanup_failures)
            if sys.exc_info()[0] in (None, KeyboardInterrupt):
                raise RuntimeError(message)
            print(message, file=sys.stderr, flush=True)
        else:
            print(
                "Owned services stopped; namespace resources are released on exit.",
                flush=True,
            )
