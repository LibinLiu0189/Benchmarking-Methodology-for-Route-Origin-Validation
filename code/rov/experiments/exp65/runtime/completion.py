"""Inspect saved observations at checkpoints without blocking the update sender."""

from array import array
from collections import Counter, deque
import json

from rov.data.predictions import rib_path
from rov.measure.read_log import LogParser


class CompletionTracker:
    # Keep first observations and compact packet bookkeeping between checkpoints.
    def __init__(self, state, specs, packet_timeout):
        self.state = state
        self.reports = {}
        self.rib_targets = {}
        for number, components in specs.items():
            for component, targets in components.items():
                for key, spec in targets.items():
                    identity = (component, number, key)
                    self.reports[identity] = dict(
                        **spec, submitted_ns=None, actual_baseline=None,
                        actual_changes=[], samples=0, before_confirmed=False,
                        before_observed_ns=None, before_observed_from_submit_ms=None,
                        completed_ns=None, exit_reason="after_not_observed",
                    )
                    if component == "rib":
                        self.rib_targets[key] = identity
        self.identities = [identity for identity in self.reports if identity[0] == "fib"]
        self.identity_indices = {identity: index for index, identity in enumerate(self.identities)}
        self.parser = LogParser()
        self.positions = {}
        self.deferred = []
        self.submissions = {}
        self.vrp_counts = Counter()
        self.cutoff_ns = round(packet_timeout * 1e9)
        self.packet_starts = array("q")
        self.packet_owners = array("I")
        self.packet_replies = bytearray()
        self.pending_packets = deque()
        self.unreceived = self.late = self.duplicates = 0
        self.collector_errors = []

    # Match samples only during a checkpoint, preserving their original timestamps.
    def observe(self, identity, sample):
        report = self.reports.get(identity)
        if report is None:
            return
        number = identity[1]
        if number not in self.submissions:
            self.deferred.append((identity, sample))
            return
        submitted_ns = self.submissions[number]
        report["submitted_ns"] = submitted_ns
        report["samples"] += 1
        if report["actual_baseline"] is None:
            report["actual_baseline"] = dict(sample)
        changes = report["actual_changes"]
        if not changes or changes[-1]["value"] != sample["value"]:
            changes.append(dict(sample))
        if sample["value"] == report["before"]:
            before_ns = report["before_observed_ns"]
            if before_ns is None or sample["observed_ns"] < before_ns:
                report["before_confirmed"] = True
                report["before_observed_ns"] = sample["observed_ns"]
                report["before_observed_from_submit_ms"] = (sample["observed_ns"] - submitted_ns) / 1e6
        if sample["started_ns"] < submitted_ns or sample["value"] != report["after"]:
            return
        previous = report["completed_ns"]
        if previous is None or sample["observed_ns"] < previous:
            report["completed_ns"] = sample["observed_ns"]
            report["exit_reason"] = "completed"

    # Resolve packet absence only after the captured FIB stream passes its deadline.
    def expire_packets(self, boundary_ns, inclusive=False):
        while self.pending_packets:
            index = self.pending_packets[0]
            deadline = self.packet_starts[index] + self.cutoff_ns
            if deadline > boundary_ns or (deadline == boundary_ns and not inclusive):
                break
            self.pending_packets.popleft()
            if self.packet_replies[index] & 2:
                continue
            self.unreceived += 1
            identity = self.identities[self.packet_owners[index]]
            self.observe(identity, dict(
                value=None, started_ns=self.packet_starts[index],
                observed_ns=deadline, sequence=index + 1,
            ))

    # Join raw sends and receptions without retaining a full Python object per packet.
    def receive_packet(self, event):
        packet = event["packet_event"]
        timestamp = packet.get("sent_ns", packet.get("observed_ns"))
        self.expire_packets(timestamp)
        index = packet["sequence"] - 1
        if packet["event"] == "sent":
            if index != len(self.packet_starts):
                raise ValueError("Probe send sequence is not contiguous")
            identity = ("fib", event["batch"], packet["destination"])
            self.packet_starts.append(packet["sent_ns"])
            self.packet_owners.append(self.identity_indices[identity])
            self.packet_replies.append(0)
            self.pending_packets.append(index)
            return
        if index < 0 or index >= len(self.packet_starts):
            return
        identity = self.identities[self.packet_owners[index]]
        start = self.packet_starts[index]
        if packet["destination"] != identity[2] or timestamp < start:
            return
        if self.packet_replies[index] & 1:
            self.duplicates += 1
        self.packet_replies[index] |= 1
        if timestamp > start + self.cutoff_ns:
            self.late += 1
            return
        if not self.packet_replies[index] & 2:
            self.packet_replies[index] |= 2
            self.observe(identity, dict(
                value=packet["next_hop"], started_ns=start,
                observed_ns=timestamp, sequence=packet["sequence"],
            ))

    # Interpret one already-saved record; collection itself performs no matching.
    def consume(self, raw, end_ns):
        if raw["event"] == "bird_log":
            event = self.parser.parse(raw["text"], raw["observed_ns"])
            if event is None or not self.state.started_ns <= event["observed_ns"] < end_ns:
                return
            if event["event"] == "vrp":
                key = (event["action"], event["prefix"], event["max_length"], event["asn"])
                self.vrp_counts[key] += 1
            elif event["event"] == "rib" and event["prefix"] in self.rib_targets:
                self.observe(self.rib_targets[event["prefix"]], dict(
                    value=rib_path(event["after"]),
                    started_ns=event["observed_ns"], observed_ns=event["observed_ns"],
                ))
        elif raw["event"] == "rov_sample":
            if raw["query_end_ns"] < end_ns:
                self.observe(("rov", raw["batch"], tuple(raw["key"])), dict(
                    value=raw["value"], started_ns=raw["query_start_ns"],
                    observed_ns=raw["query_end_ns"],
                ))
        elif raw["event"] == "probe_packet":
            packet = raw["packet_event"]
            if packet.get("sent_ns", packet.get("observed_ns")) < end_ns:
                self.receive_packet(raw)
        elif raw["event"] == "collector_error":
            self.collector_errors.append(raw)

    # Read only newly flushed complete records up to a captured file boundary.
    def advance(self, boundaries, submissions, end_ns, final=False):
        self.submissions = submissions
        deferred, self.deferred = self.deferred, []
        for identity, sample in deferred:
            self.observe(identity, sample)
        for path, boundary in boundaries:
            with path.open("rb") as source:
                source.seek(self.positions.get(path, 0))
                while source.tell() < boundary:
                    line = source.readline()
                    if not line.endswith(b"\n") or source.tell() > boundary:
                        raise ValueError("Incomplete checkpoint capture record")
                    self.consume(json.loads(line), end_ns)
                self.positions[path] = source.tell()
        if final:
            self.expire_packets(end_ns - 1, inclusive=True)

    # Count observations still required by the enabled, retained measurement targets.
    def remaining(self):
        counts = Counter()
        for identity, report in self.reports.items():
            if identity[1] in self.submissions and report["completed_ns"] is None:
                counts[identity[0]] += 1
        if "vrp" in self.state.monitoring_mode:
            counts["vrp"] = sum((self.state.vrp.monitored_expected - self.vrp_counts).values())
        return {component: counts[component] for component in self.state.monitoring_mode}
