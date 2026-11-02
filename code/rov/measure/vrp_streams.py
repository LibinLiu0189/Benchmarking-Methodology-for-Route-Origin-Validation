"""Collect shared protocol observations; experiment callers own schedule, quiet and metrics."""

from collections import Counter, defaultdict
import json
import queue
import threading
import time

from rov.data.predictions import rib_path


# Stop an observer and wait for its BIRD connection to close.
def stop_observer(stop, thread, timeout):
    stop.set()
    thread.join(timeout + 1)
    if thread.is_alive():
        raise TimeoutError(f"Timed out stopping observer {thread.name}")


# Find predicted endpoints in an ordered sequence of observed states.
def match_state_sequence(predictions, states):
    position = 0
    matched = []
    for number, before, after, submitted_ns in predictions:
        start = next(
            (index for index in range(position, len(states))
             if states[index][0] == before), None,
        )
        if start is None:
            break
        end = next(
            (index for index in range(start + 1, len(states))
             if states[index][0] == after and states[index][1] >= submitted_ns), None,
        )
        if end is None:
            break
        matched.append((number, states[end][1]))
        position = end
    return matched


# Match one prefix's batch probes without joining different destination streams.
def match_fib_prefix(predictions, observed, submitted):
    matched = []
    previous_ns = -1
    positions = defaultdict(int)
    for number, destination, before, after in predictions:
        if number not in submitted:
            break
        samples = observed[destination]
        before_outcome = ("not_received", None) if before is None else ("received", before)
        after_outcome = ("not_received", None) if after is None else ("received", after)
        start = None
        for index in range(positions[destination], len(samples)):
            if samples[index][2] <= previous_ns:
                start = index if samples[index][0] == before_outcome else None
            elif start is None and samples[index][0] == before_outcome:
                start = index
                break
        if start is None:
            break
        end = next(
            (index for index in range(start + 1, len(samples))
             if samples[index][0] == after_outcome
             and samples[index][1] >= submitted[number]
             and samples[index][2] >= previous_ns), None,
        )
        if end is None:
            break
        matched.append((number, samples[end][2]))
        previous_ns = samples[end][2]
        positions[destination] = end
    return matched


# Keep expected VRP operations and the raw observations used for later attribution.
class VrpState:
    # Count expected VRP log keys without assigning them to batches yet.
    def __init__(self, operations):
        self.operations = operations
        self.expected = Counter()
        for batch_operations in operations.values():
            for operation in batch_operations:
                prefix, maximum, asn = operation["vrp"]
                action = "added" if operation["operation"] == "add" else "removed"
                self.expected[(action, prefix, int(maximum), int(asn))] += 1
        self.observed = Counter()
        self.recorded = []


# Keep candidate ROV predictions and ordered query changes.
class RovState:
    # Retain candidate links while sharing each prefix and origin query sequence.
    def __init__(self, targets, transitions):
        self.targets = targets
        self.transitions = transitions
        self.counts = Counter()
        for number, _, _, _, _, _ in transitions:
            self.counts[number] += 1
        self.matched = []
        self.observed = defaultdict(list)

    # Attribute each query transition to its ordered candidate predictions.
    def match_offline(self, submitted):
        submitted_ns = dict(submitted)
        predictions = defaultdict(list)
        for number, prefix, peer, origin_as, before, after in self.transitions:
            if number not in submitted_ns:
                continue
            key = (prefix, origin_as)
            entry = (number, before, after, submitted_ns[number])
            if predictions[key] and predictions[key][-1][0] == entry:
                predictions[key][-1][1].append(peer)
            else:
                predictions[key].append((entry, [peer]))

        initial_states = {}
        for target in self.targets.values():
            initial_states.setdefault((target["prefix"], target["origin_as"]), target["initial"])
        self.matched = []
        for key, rows in predictions.items():
            initial = initial_states[key]
            states = [(initial, -1)] + [
                (event["after"], event["query_end_ns"])
                for event in self.observed[key]
            ]
            hits = match_state_sequence([entry for entry, _ in rows], states)
            for (number, observed_ns), (_, peers) in zip(hits, rows):
                self.matched.extend((number, observed_ns) for _ in peers)


# Keep selected-path predictions and each prefix's raw RIB log sequence.
class RibState:
    # Retain batch predictions and the initial paths needed for offline matching.
    def __init__(self, changes, initial):
        self.changes = changes
        self.initial = initial
        self.current = {}
        self.matched = []
        self.observed = defaultdict(list)

    # Attribute recorded path states only after the observer has stopped.
    def match_offline(self, submitted):
        submitted_ns = dict(submitted)
        predictions = defaultdict(list)
        for number, _ in submitted:
            for prefix, before, after in self.changes[number]:
                predictions[prefix].append((number, before, after, submitted_ns[number]))

        self.matched = []
        for prefix, rows in predictions.items():
            states = [(self.initial[prefix], -1)] + [
                (entry[1], entry[2]) for entry in self.observed[prefix]
            ]
            self.matched.extend(match_state_sequence(rows, states))


# Keep per-prefix predictions and per-destination packet observations.
class FibState:
    # Retain available probes and their ordered packet results.
    def __init__(self, changes, unavailable):
        self.changes = changes
        self.unavailable = unavailable
        self.destinations = list(dict.fromkeys(
            destination for rows in changes.values() for _, destination, _, _ in rows
        ))
        self.destination_set = set(self.destinations)
        self.initial = {}
        self.current = {}
        self.latest_sent_ns = {}
        self.observed = defaultdict(list)
        self.matched = []

    # Retain new results using the original probe send and result timestamps.
    def record(self, event):
        if event["event"] not in ("received", "not_received"):
            return False
        destination = event["destination"]
        sent_ns = event["sent_ns"]
        if sent_ns is None or destination not in self.destination_set:
            return False
        if sent_ns <= self.latest_sent_ns.get(destination, -1):
            return False
        self.latest_sent_ns[destination] = sent_ns
        gateway = event.get("next_hop") if event["event"] == "received" else None
        outcome = (event["event"], gateway)
        changed = destination in self.current and self.current[destination] != outcome
        self.current[destination] = outcome
        if destination not in self.initial:
            self.initial[destination] = outcome
        self.observed[destination].append((outcome, sent_ns, event["observed_ns"]))
        return changed

    # Match each prefix's batches against their own probe destination sequences.
    def match_offline(self, submitted):
        submitted_ns = dict(submitted)
        self.matched = []
        for rows in self.changes.values():
            self.matched.extend(match_fib_prefix(rows, self.observed, submitted_ns))


# Own the four observation states and the queues shared by their observers.
class ObservationState:
    # Prepare one experiment's queues, batch submissions, and observation states.
    def __init__(
        self, operations, targets, transitions, rib_changes, rib_initial,
        fib_changes=None, fib_unavailable=None,
    ):
        self.vrp = VrpState(operations)
        self.rov = RovState(targets, transitions)
        self.rib = RibState(rib_changes, rib_initial)
        self.fib = FibState(fib_changes or {}, fib_unavailable or [])
        self.events = queue.Queue()
        self.errors = queue.Queue()
        self.pending = []
        self.submitted = []
        self.started_ns = None
        self.last_change_ns = None
        self.raw_output = None
        self.anomalies = []
        self.rtr_events = []
        self.submission_records = []
        self.backlog = []
        self.last_backlog_ns = 0


# Record VRP, ROV, and RIB observations from the shared event queue.
def record_next_event(state, wait, check_errors=True):

    changed = False
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
            return False, False

    state.raw_output.write(json.dumps(event, separators=(",", ":")) + "\n")
    if event["event"] == "rtr":
        state.rtr_events.append(event)
        changed = True
        return True, changed
    now = time.monotonic_ns()
    if now - state.last_backlog_ns >= 100_000_000:
        submitted = sum(len(state.vrp.operations[number]) for number, _ in state.submitted)
        state.backlog.append(dict(observed_ns=now, submitted_operations=submitted,
                                 observed_events=len(state.vrp.recorded),
                                 unobserved_operations=max(0, submitted - len(state.vrp.recorded))))
        state.last_backlog_ns = now
    if event["event"] in ("received", "not_received", "late", "late_or_duplicate"):
        destination = event["destination"]
        if state.fib.record(event):
            changed = True
        return True, changed
    if event["event"] == "vrp":
        key = (
            event["action"], event["prefix"], event["max_length"], event["asn"]
        )
        state.vrp.observed[key] += 1
        state.vrp.recorded.append((key, event["observed_ns"]))
        changed = True
        if state.vrp.observed[key] > state.vrp.expected[key]:
            state.anomalies.append(f"Unexpected VRP event: {key}")
    elif event["event"] == "rov":
        state.rov.observed[(event["prefix"], event["origin_as"])].append(event)
        changed = True
    elif event["event"] == "rib":
        prefix = event["prefix"]
        if prefix in state.rib.current:
            before = state.rib.current[prefix]
            after = rib_path(event["after"])
            action = event["action"]
            state.rib.observed[prefix].append((before, after, event["observed_ns"]))
            if before == after:
                return True, changed
            if (action == "added" and before is not None) or (
                action in ("removed", "replaced") and before is None
            ) or (action == "removed" and after is not None):
                state.anomalies.append(f"Invalid RIB {action} for {prefix}")
            state.rib.current[prefix] = after
            changed = True
    else:
        raise RuntimeError(f"Unexpected observer event: {event}")
    if check_errors and not state.errors.empty():
        raise state.errors.get()
    return True, changed
