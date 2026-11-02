"""Observe forwarding through a dedicated ingress and publish state changes."""

from collections import deque
import ipaddress as ip
import threading
import time

from rov.measure.probe_transport import ProbeTransport
from rov.measure.read_log import stop_observer


# Identify the forwarding outcome independently of packet sequence and timestamps.
def forwarding_state(event):
    return (event["event"], event.get("next_hop"), event.get("interface"))


class ForwardingObserver:
    # Keep baseline samples and live state in the same collector thread.
    def __init__(self, destinations, config_dir, pps, packet_timeout, events, errors):
        self.destinations = list(dict.fromkeys(
            str(ip.ip_address(destination)) for destination in destinations
        ))
        self.config_dir = config_dir
        self.pps = pps
        self.packet_timeout_ns = round(packet_timeout * 1e9)
        self.events = events
        self.errors = errors
        self.initial = {}
        self.latest = {}
        self.lock = threading.Lock()
        self.initialized = threading.Event()
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self.worker, name="forwarding-observer")

    # Wait for one accepted packet outcome per target before returning the baseline.
    def start(self, timeout):
        self.thread.start()
        deadline = time.monotonic() + timeout
        try:
            while not self.initialized.is_set():
                if not self.errors.empty():
                    raise self.errors.get()
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("Timed out waiting for the packet observer")
                self.initialized.wait(min(0.1, remaining))
            if not self.errors.empty():
                raise self.errors.get()
        except BaseException:
            self.close(timeout)
            raise
        return self.initial

    # Preserve the latest samples for result output without publishing repeated states.
    def snapshot(self):
        with self.lock:
            return {destination: dict(event) for destination, event in self.latest.items()}

    # Compare and update state before a change becomes visible in the event queue.
    def record(self, event):
        destination = event["destination"]
        with self.lock:
            previous = self.latest.get(destination)
            if previous is not None and event["sent_ns"] < previous["sent_ns"]:
                return
            self.latest[destination] = event

            if not self.initialized.is_set():
                if len(self.latest) == len(self.destinations):
                    self.initial = dict(self.latest)
                    self.initialized.set()
                return

            before = forwarding_state(previous)
            if before != forwarding_state(event):
                self.events.put(dict(event, before=before))

    # Send continuously and turn each timely reception or expiration into one outcome.
    def observe(self, transport):
        pending = {}
        deadlines = deque()
        index = 0
        interval_ns = max(1, round(1e9 / self.pps))
        next_send_ns = time.monotonic_ns()

        while not self.stop.is_set():
            now = time.monotonic_ns()
            if now >= next_send_ns:
                packet = transport.send(self.destinations[index % len(self.destinations)])
                index += 1
                sequence = packet["sequence"]
                pending[sequence] = packet
                deadlines.append((packet["sent_ns"] + self.packet_timeout_ns, sequence))
                next_send_ns = packet["sent_ns"] + interval_ns

            wake_ns = min(next_send_ns, deadlines[0][0]) if deadlines else next_send_ns
            wait = min(0.02, max(0, (wake_ns - time.monotonic_ns()) / 1e9))
            for received in transport.poll(wait):
                packet = pending.pop(received["sequence"], None)
                if packet is None:
                    continue
                if received["observed_ns"] - packet["sent_ns"] > self.packet_timeout_ns:
                    self.record(dict(
                        event="not_received", sequence=packet["sequence"],
                        destination=packet["destination"], sent_ns=packet["sent_ns"],
                        observed_ns=received["observed_ns"],
                    ))
                else:
                    self.record(dict(received, sent_ns=packet["sent_ns"]))

            now = time.monotonic_ns()
            while deadlines and deadlines[0][0] <= now:
                _, sequence = deadlines.popleft()
                packet = pending.pop(sequence, None)
                if packet is not None:
                    self.record(dict(
                        event="not_received", sequence=sequence,
                        destination=packet["destination"], sent_ns=packet["sent_ns"],
                        observed_ns=now,
                    ))

    # Release the owned ingress even if collection fails, and report errors upstream.
    def worker(self):
        try:
            transport = ProbeTransport(self.destinations, self.config_dir)
            try:
                self.observe(transport)
            finally:
                transport.close()
        except BaseException as error:
            self.errors.put(error)

    # Stop collection before destroying the owned ingress.
    def close(self, timeout):
        stop_observer(self.stop, self.thread, timeout)
