"""Control the direct RTR disconnect used by Section 6.7 scenario 1."""

import math
import os
from pathlib import Path
import queue
import re
import subprocess
import time

from rov.measure.bird_connection import BirdConnection
from rov.paths import CONFIG, ROOT
from rov.send.send_vrp import submit_with_backpressure
from rov.experiments.exp67.faults.transport import bird_rtr_sockets

FAULT_MODES = ("disconnect",)


class FaultLab:
    # Retain direct-session control state without creating another RTR source.
    def __init__(self, mode, scenario, output, timeout, bin_dir=None):
        self.mode = mode
        self.scenario = scenario
        self.output = Path(output)
        self.timeout = timeout
        self.binaries = Path(bin_dir or ROOT / "software/bin-brio-disconnect").resolve()
        self.fault = None
        self.console = queue.Queue()
        self.connections = []
        self.reconnected_ns = None
        self.result = dict(
            mode=mode, refresh_seconds=60, retry_seconds=1, expire_seconds=600,
            cache_semantics="Single direct BIRD to BRIO cache session; listener and cache state persist",
            errors=[],
        )

    # Apply the original direct-session timers before measured collection starts.
    def prepare(self, check_alive):
        config = CONFIG / "bird.conf"
        text = config.read_text()
        original = "  remote 127.0.0.1 port 3324;"
        if text.count(original) != 1:
            raise ValueError("Expected one owned primary RTR configuration on port 3324")
        text = text.replace(
            original,
            original + "\n  refresh keep 60;\n  retry keep 1;\n  expire keep 600;",
        )
        config.write_text(text)
        subprocess.run([str(self.binaries / "bird"), "-p", "-c", str(config)], check=True, timeout=10)
        connection = BirdConnection(CONFIG / "bird.ctl", self.timeout)
        try:
            connection.command("configure")
        finally:
            connection.close()
        deadline = time.monotonic() + self.timeout
        while not all("Established" in text for text in self.status().values()):
            check_alive()
            if time.monotonic() >= deadline:
                raise TimeoutError("Timed out preparing experiment RTR session")
            time.sleep(0.1)
        self.result["baseline_sessions"] = self.status()
        sockets = bird_rtr_sockets(CONFIG / "bird.pid")
        ports = sorted(int(row["remote"].split(":")[1], 16) for row in sockets)
        if ports != [3324]:
            raise RuntimeError(f"Expected BIRD RTR socket on port 3324, got {ports}")
        self.result["baseline_connections"] = sockets

    # Read the sole configured cache session independently of BGP protocols.
    def status(self):
        connection = BirdConnection(CONFIG / "bird.ctl", self.timeout)
        try:
            return {"rpki_source": connection.command("show protocols all rpki_source")}
        finally:
            connection.close()

    # Pass BRIO control replies from the existing output forwarder to this experiment.
    def observe_output(self, line, observed_ns):
        accepted = re.search(r'BRIO_CONNECT client=(\d+) accepted_ns=(\d+)', line)
        if accepted is not None:
            self.connections.append((int(accepted.group(1)), int(accepted.group(2))))
        if ('BRIO_DISCONNECT client=' in line or 'fd=' in line
                or 'No clients' in line or 'EXP67_CONTROL_' in line):
            self.console.put((line, observed_ns))


    # Write one short BRIO control command through its owned FIFO.
    def control(self, command):
        descriptor = os.open(CONFIG / 'brio.in', os.O_WRONLY | os.O_NONBLOCK)
        try:
            write_start_ns = time.monotonic_ns()
            payload = (command + '\n').encode()
            if os.write(descriptor, payload) != len(payload):
                raise RuntimeError('Incomplete BRIO control command')
            return write_start_ns
        finally:
            os.close(descriptor)


    # Collect a command reply up to a unique echo boundary from BRIO.
    def command_reply(self, command, check_alive):
        marker = f'EXP67_CONTROL_{time.monotonic_ns()}'
        write_start_ns = self.control(f'{command}\necho {marker}')
        lines = []
        deadline = time.monotonic() + self.timeout
        while True:
            check_alive()
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f'No BRIO reply to {command}')
            try:
                line, observed_ns = self.console.get(timeout=min(.1, remaining))
            except queue.Empty:
                continue
            if marker in line:
                if 'BRIO_DISCONNECT client=' in line or 'fd=' in line or 'No clients' in line:
                    lines.append(line)
                return write_start_ns, lines
            if observed_ns >= write_start_ns:
                lines.append(line)


    # Disconnect the sole direct BRIO client without submitting any VRP edits.
    def disconnect_client(self, check_alive):
        _, listing = self.command_reply('clients', check_alive)
        identifiers = [int(match.group(1)) for line in listing
                       if (match := re.search(r'(?:^|\s)(\d+): fd=\d+', line))]
        if len(identifiers) != 1:
            raise RuntimeError(f'Expected one BRIO client, found {len(identifiers)}')
        write_start_ns, reply = self.command_reply(f'disconnect {identifiers[0]}', check_alive)
        shutdowns = [match for line in reply
                     if (match := re.search(r'BRIO_DISCONNECT client=(\d+) shutdown_ns=(\d+)', line))]
        if len(shutdowns) != 1 or int(shutdowns[0].group(1)) != identifiers[0]:
            raise RuntimeError('BRIO did not confirm the selected client shutdown')
        shutdown_ns = int(shutdowns[0].group(2))
        if shutdown_ns < write_start_ns:
            raise RuntimeError('BRIO shutdown timestamp precedes command submission')
        self.fault = dict(client_id=identifiers[0], command_write_start_ns=write_start_ns,
                          shutdown_ns=shutdown_ns, observed_ns=shutdown_ns)
        self.result['fault'] = self.fault
        return self.fault


    # Disconnect the direct client, then submit this scenario's prepared VRP edits.
    def submit(self, script, fifo, wait, check_alive):
        if self.fault is not None:
            raise ValueError("Fault scenarios require one notification batch")
        self.disconnect_client(check_alive)
        self.result["fault"] = self.fault
        submission = submit_with_backpressure(script, fifo, self.timeout, wait, check_alive)
        submission["fault_started_ns"] = self.fault["observed_ns"]
        self.result["submission"] = submission
        return submission

    # Require both a new TCP client and an observed Established transition.
    def recovered(self, state):
        if self.fault is None:
            return False
        start = self.fault["observed_ns"]
        end = state.collection_end_ns or math.inf
        if self.reconnected_ns is None:
            matches = [accepted_ns for client_id, accepted_ns in self.connections
                       if client_id != self.fault["client_id"] and start <= accepted_ns <= end]
            if matches:
                self.reconnected_ns = min(matches)
                self.result["reconnect_accepted_ns"] = self.reconnected_ns
        established = any(
            start <= row["observed_ns"] <= end
            and row.get("protocol", "rpki_source") == "rpki_source"
            and row.get("after") == "Established"
            for row in state.rtr_events
        )
        return self.reconnected_ns is not None and established

    # Return retained fault evidence; service cleanup belongs to the initializer.
    def close(self):
        return self.result
