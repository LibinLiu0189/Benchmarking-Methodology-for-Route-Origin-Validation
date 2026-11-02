"""Run one bound 6.3A scenario with retained logs, result JSON and owned cleanup."""

import argparse
import json
import os
import signal
import subprocess
import sys
import ipaddress as ip
from pathlib import Path
import queue
import threading
import time
from rov.data.sequence import load_sequence
from rov.data.vrp import load_vrp_text
from rov.paths import CONFIG, ROOT
from rov.measure.read_log import start_log_observer
from rov.measure.read_log import stop_observer
from rov.measure.cli import describe_path
from rov.measure.cli import read_primary_rib
from rov.measure.data_plane import PACKET_EVENTS
from rov.measure.forwarding import ForwardingObserver, forwarding_state
from rov.send.send_vrp import send_batch
from rov.experiments.exp61.brio_timing import read_records, correlate, correlate_control_records
from rov.measure.cli import rib_path_fields as normalize_bgp_path
from rov.dut.managed import forward_initialization_output
from rov.dut.managed import wait_for_initialization
from rov.dut.managed import check_initialization
from rov.dut.managed import stop_initialization
from rov.dut.managed import stop_requested
from rov.measure.vrp_state import build_vrp_expectation
from rov.measure.vrp_state import accept_vrp_event
from rov.measure.vrp_state import pending_vrp_changes
from rov.measure.vrp_state import check_repeated_vrp_states
from rov.measure.vrp_state import vrp_observation_summary
from rov.dut.output import MANAGED_OUTPUT
from rov.dut.output import supervise
from rov.dut.virtualize import enter_namespace


# Replay the baseline across batches with the same exact-identity rules as 6.2.
def load_vrp_expectations(directory, batches, operations):
    metadata = json.loads((directory / "sequence.json").read_text())
    initial = load_vrp_text(directory / metadata["initial"])
    if "initial_count" in metadata and metadata["initial_count"] != len(initial):
        raise ValueError("Initial VRP count disagrees with the bound baseline")
    active = set(initial)
    expectations = {}
    for batch in batches:
        expectation = build_vrp_expectation(active, operations[batch["number"]])
        expectations[batch["number"]] = expectation
        for key, entry in expectation["entries"].items():
            if entry["final"]:
                active.add(key)
            else:
                active.discard(key)
    return expectations


# Reuse 6.3B's BGP normalization while retaining 6.3A's interface identity.
def rib_path_fields(path):
    fields = normalize_bgp_path(path)
    if fields is not None:
        fields["interface"] = path["interface"]
    return fields


# Check that a RIB action agrees with the currently selected path's presence.
def check_rib_event(event, before):
    prefix = event["prefix"]
    action = event["action"]
    if action == "added" and before is not None:
        raise RuntimeError(f"RIB added an already present prefix {prefix}")
    if action in ("replaced", "removed") and before is None:
        raise RuntimeError(f"RIB {action} has no previous selected path for {prefix}")


# Compare predicted and observed final changes before checking their selected paths.
def check_rib_result(prediction, final_rib_changes, batch):
    expected = {change["prefix"] for change in prediction["changes"]}
    actual = {change["prefix"] for change in final_rib_changes}
    missing = expected - actual
    extra = actual - expected
    if missing or extra:
        details = [f"Batch {batch} RIB change set mismatch"]
        if missing:
            details.append("Missing expected RIB changes:")
            details.extend(
                f"  {prefix}" for prefix in sorted(
                    missing, key=lambda value: (ip.ip_network(value).version, ip.ip_network(value))
                )
            )
        if extra:
            details.append("Unexpected RIB changes:")
            details.extend(
                f"  {prefix}" for prefix in sorted(
                    extra, key=lambda value: (ip.ip_network(value).version, ip.ip_network(value))
                )
            )
        raise RuntimeError("\n".join(details))

    actual_after = {change["prefix"]: change["after"] for change in final_rib_changes}
    mismatches = []
    for change in prediction["changes"]:
        prefix = change["prefix"]
        expected_route = change["after"]
        actual_route = actual_after[prefix]
        expected_path = (
            None if expected_route is None
            else (expected_route["peer"], expected_route["gateway"])
        )
        actual_path = (
            None if actual_route is None
            else (actual_route["peer"], actual_route["gateway"])
        )
        if actual_path != expected_path:
            mismatches.append(
                f"{prefix}: expected {expected_path}, actual {actual_path}"
            )
    if mismatches:
        raise RuntimeError(f"Batch {batch} RIB mismatch: " + "; ".join(mismatches))
    print(
        f"  RIB matched predictions for {len(prediction['changes'])} changed prefixes",
        flush=True,
    )


# Check stable packet outcomes against the predicted next hop for each destination.
def check_forwarding(expected, observed, batch):
    mismatches = []
    for destination, gateway in expected.items():
        actual = observed.get(destination)
        kind = "not_received" if gateway is None else "received"
        if actual is None or actual[0] != kind or actual[1] != gateway:
            mismatches.append(f"{destination}: expected {kind} via {gateway}, observed {actual}")
    if mismatches:
        raise RuntimeError(
            f"Batch {batch} forwarding mismatch: " + "; ".join(mismatches)
        )


# Describe one observed packet outcome for terminal output.
def describe_forwarding(state):
    if state is None:
        return "not yet observed"
    kind, gateway, interface = state
    if kind == "not_received":
        return "not received within packet timeout"
    return f"{kind} via {gateway} ({interface})"


# Retain changes already compared and ordered by the forwarding collector.
def collect_forwarding(events, errors, current, changes, submitted_ns, samples):
    if not errors.empty():
        raise errors.get()
    for event in events:
        destination = event["destination"]
        state = forwarding_state(event)
        current[destination] = state
        samples[destination] = {key: value for key, value in event.items() if key != "before"}
        if event["observed_ns"] < submitted_ns:
            continue
        changes.append(dict(
            destination=destination, before=event["before"], after=state,
            observed_ns=event["observed_ns"], sent_ns=event["sent_ns"],
        ))


# Reject pending real events without treating file transport notifications as changes.
def check_log_boundary(events, errors, message):
    if not errors.empty():
        raise errors.get()
    packets = []
    for _ in range(events.qsize()):
        event = events.get_nowait()
        if event.get("event") in PACKET_EVENTS:
            packets.append(event)
        elif event.get("event") != "drained":
            raise RuntimeError(message)
    return packets


# Measure one batch until logs and the sampled forwarding outcomes stop changing.
def measure_batch(
    directory,
    info,
    operations,
    log_events,
    log_errors,
    fifo,
    current_rib,
    initial_forwarding,
    expected_forwarding,
    quiet_seconds,
    timeout,
    *, expectation, socket_path, packet_observer=None, result=None, check_alive=None,
):
    print("VRP expectation: " + json.dumps(expectation["counts"], sort_keys=True), flush=True)
    if result is not None:
        result["vrp_expectation"] = expectation["counts"]
    check_repeated_vrp_states(expectation, socket_path, timeout, "initial")

    current_forwarding = {
        destination: (event["event"], event.get("next_hop"), event.get("interface"))
        for destination, event in initial_forwarding.items()
    }
    forwarding_changes = []
    vrp_events = []
    vrp_actions = {}
    observed_ribs = []
    batch_before = {}
    final_forwarding_samples = {key: dict(value) for key, value in initial_forwarding.items()}
    if result is not None:
        result.update(
            number=info["number"], verified=False,
            vrp_operations_detail=[
                dict(number=row.get("number", number), operation=row["operation"], vrp=row["vrp"])
                for number, row in enumerate(operations, 1)
            ],
            initial_forwarding=initial_forwarding,
            final_forwarding_samples=final_forwarding_samples,
            vrp_events=vrp_events, rib_events=observed_ribs,
            forwarding_changes=forwarding_changes,
            expected_forwarding=expected_forwarding, final_forwarding=current_forwarding,
            collection_status="not_submitted",
        )
    pending_packets = check_log_boundary(
        log_events, log_errors, "BIRD produced a VRP or RIB event before batch submission",
    )
    submission = send_batch(directory / info["script"], fifo)
    submitted_ns = submission["submitted_ns"]
    if result is not None:
        result.update(collection_status="collecting", submission=submission,
                      timestamp_semantics="monotonic_external_observation")
    latest_ns = submitted_ns
    last_change_ns = submitted_ns
    pending_logs = []
    deadline = time.monotonic() + timeout
    while True:
        if check_alive is not None:
            check_alive()
        if not log_errors.empty():
            raise log_errors.get()

        # Process a bounded snapshot so repeated records cannot starve the quiet clock.
        for _ in range(len(pending_logs) + log_events.qsize()):
            try:
                event = pending_logs.pop(0) if pending_logs else log_events.get_nowait()
            except queue.Empty:
                break
            if event.get("event") in PACKET_EVENTS:
                pending_packets.append(event)
                continue
            if event.get("event") == "drained":
                continue
            if event["observed_ns"] < submission["write_start_ns"]:
                raise RuntimeError("BIRD event predates batch submission")
            if event["event"] == "vrp":
                vrp_events.append(event)
                key = (event["prefix"], event["max_length"], event["asn"])
                changed = vrp_actions.get(key) != event["action"]
                vrp_actions[key] = event["action"]
            elif event["event"] == "rib":
                prefix = event["prefix"]
                before = current_rib.get(prefix)
                check_rib_event(event, before)
                batch_before.setdefault(prefix, before)
                event["before"] = before
                after = event["after"]
                changed = rib_path_fields(before) != rib_path_fields(after)
                if after is None:
                    current_rib.pop(prefix, None)
                else:
                    current_rib[prefix] = after
                observed_ribs.append(event)
            else:
                raise RuntimeError(f"Unexpected BIRD log event: {event}")
            if changed:
                latest_ns = max(latest_ns, event["observed_ns"])
                last_change_ns = time.monotonic_ns()

        if pending_packets:
            previous_changes = len(forwarding_changes)
            collect_forwarding(
                pending_packets, log_errors, current_forwarding,
                forwarding_changes, submitted_ns, samples=final_forwarding_samples,
            )
            pending_packets = []
            if len(forwarding_changes) > previous_changes:
                latest_ns = max(latest_ns, forwarding_changes[-1]["observed_ns"])
                last_change_ns = time.monotonic_ns()

        if time.monotonic() >= deadline:
            raise TimeoutError(f"Batch {info['number']} did not become quiet")
        if time.monotonic_ns() - last_change_ns >= quiet_seconds * 1e9:
            confirmed_ns = time.monotonic_ns()
            if packet_observer is not None:
                # Keep samples aligned with the states consumed before this boundary.
                for destination, sample in packet_observer.snapshot().items():
                    if (sample["observed_ns"] <= confirmed_ns
                            and forwarding_state(sample) == current_forwarding[destination]):
                        final_forwarding_samples[destination] = sample
            break
        # Wake on a queued event while retaining the existing error-check interval.
        try:
            pending_logs.append(log_events.get(timeout=0.001))
        except queue.Empty:
            pass

    if result is not None:
        result.update(collection_status="quiet", quiet_confirmed_ns=confirmed_ns)
    # Collection ends on silence; expected-event validation happens only afterwards.
    if not log_errors.empty():
        raise log_errors.get()
    observed_vrps = {}
    for event in vrp_events:
        accept_vrp_event(event, expectation, observed_vrps, info["number"])
    missing = pending_vrp_changes(expectation, observed_vrps)
    if missing:
        raise RuntimeError(
            f"Batch {info['number']} unresolved {len(missing)} VRP tuples after quiet interval"
        )

    check_repeated_vrp_states(expectation, socket_path, timeout, "final")
    vrp_summary = vrp_observation_summary(expectation, observed_vrps)
    print("VRP observations: " + json.dumps(vrp_summary, sort_keys=True), flush=True)

    rib_events = observed_ribs
    final_rib_changes = [
        dict(
            prefix=prefix,
            before=before,
            after=current_rib.get(prefix),
        )
        for prefix, before in batch_before.items()
        if rib_path_fields(before) != rib_path_fields(current_rib.get(prefix))
    ]
    final_rib_changes.sort(
        key=lambda row: (ip.ip_network(row["prefix"]).version, ip.ip_network(row["prefix"]))
    )
    all_events = [*vrp_events, *rib_events]
    end_ns = max((event["observed_ns"] for event in all_events), default=None)
    installed_ns = (
        max(event["observed_ns"] for event in vrp_events)
        if expectation["counts"]["required_net_changes"] else None
    )
    installation_reference = (
        "last_observed_vrp_event" if installed_ns is not None
        else "unavailable_no_required_net_changes"
    )

    # Preserve the existing subtraction only when both observed references exist.
    def post_installation(event_ns):
        if event_ns is None or installed_ns is None:
            return None
        return (event_ns - installed_ns) / 1e6

    end_to_end_ms = None if end_ns is None else (end_ns - submitted_ns) / 1e6
    post_installation_ms = post_installation(end_ns)
    print(
        f"Batch {info['number']}: {len(operations)} VRP operations; "
        f"{len(rib_events)} RIB events; "
        f"{len(final_rib_changes)} final RIB changes; "
        f"VRP/RIB last event from FIFO submission {end_to_end_ms} ms; "
        f"post-install {post_installation_ms} ms ({installation_reference})",
        flush=True,
    )
    for event in sorted(all_events, key=lambda row: row["observed_ns"]):
        if event["event"] == "vrp":
            continue
        delay = (event["observed_ns"] - submitted_ns) / 1e6
        description = (
            f"{event['action']} {event['prefix']}: "
            f"{describe_path(event['before'])} -> "
            f"{describe_path(event['after'])}"
        )
        print(
            f"  [RIB] {description}; "
            f"observed after {delay:.3f} ms",
            flush=True,
        )
    for change in final_rib_changes:
        print(
            f"  [RIB FINAL] {change['prefix']}: "
            f"{describe_path(change['before'])} -> "
            f"{describe_path(change['after'])}",
            flush=True,
        )
    for event in forwarding_changes:
        delay_ms = (event["observed_ns"] - submitted_ns) / 1e6
        print(
            f"  [FORWARDING] {event['destination']}: "
            f"{describe_forwarding(event['before'])} -> "
            f"{describe_forwarding(event['after'])}; "
            f"observed after {delay_ms:.3f} ms",
            flush=True,
        )
    rib_changes = [
        event for event in rib_events
        if rib_path_fields(event["before"]) != rib_path_fields(event["after"])
    ]
    rib_end = max((event["observed_ns"] for event in rib_changes), default=None)
    forwarding_end = max(
        (event["observed_ns"] for event in forwarding_changes), default=None,
    )
    joint_end = latest_ns if vrp_events or rib_changes or forwarding_changes else None
    timing = dict(
        reference="fifo_command_submitted",
        first_rtr_pdu_sent_ns=None,
        first_rtr_pdu_evidence="not_measured",
        vrp_install_reference=installation_reference,
        write_start_ns=submission["write_start_ns"], submitted_ns=submitted_ns,
        vrp_install_observed_ns=installed_ns, quiet_confirmed_ns=confirmed_ns,
        joint_last_change_ns=joint_end,
        vrp_rib_last_event_from_submit_ms=end_to_end_ms,
        vrp_rib_last_event_post_install_ms=post_installation_ms,
        rib_last_change_from_submit_ms=None if rib_end is None else (rib_end - submitted_ns) / 1e6,
        rib_last_change_post_install_ms=post_installation(rib_end),
        forwarding_last_change_from_submit_ms=(
            None if forwarding_end is None else (forwarding_end - submitted_ns) / 1e6
        ),
        forwarding_last_change_post_install_ms=post_installation(forwarding_end),
        joint_last_change_from_submit_ms=(
            None if joint_end is None else (joint_end - submitted_ns) / 1e6
        ),
        joint_last_change_post_install_ms=post_installation(joint_end),
        joint_quiet_confirmed_from_submit_ms=(confirmed_ns - submitted_ns) / 1e6,
        joint_quiet_confirmed_post_install_ms=post_installation(confirmed_ns),
    )
    if result is not None:
        result.update(
            number=info["number"], vrp_operations=len(operations),
            **vrp_summary, vrp_events=vrp_events, rib_events=rib_events,
            final_rib_changes=final_rib_changes, forwarding_changes=forwarding_changes,
            expected_forwarding=expected_forwarding, final_forwarding=current_forwarding,
            timing=timing, quiet_seconds=quiet_seconds, verified=False,
        )
    print(
        f"  RIB last change from FIFO submission: {timing['rib_last_change_from_submit_ms']} ms; "
        f"target forwarding last change: {timing['forwarding_last_change_from_submit_ms']} ms",
        flush=True,
    )
    print(
        f"  Joint quiet confirmation: {timing['joint_quiet_confirmed_from_submit_ms']:.3f} ms "
        f"from FIFO submission; {timing['joint_quiet_confirmed_post_install_ms']} ms "
        f"post-install ({installation_reference}); target probes only, not full kernel FIB",
        flush=True,
    )

    if expected_forwarding:
        observed_end_ms = (latest_ns - submitted_ns) / 1e6
        confirmed_ms = (confirmed_ns - submitted_ns) / 1e6
        print(
            f"  RIB + sampled forwarding: last event {observed_end_ms:.3f} ms; "
            f"stable confirmed {confirmed_ms:.3f} ms "
            f"(includes {quiet_seconds:g}s quiet window)",
            flush=True,
        )
        check_forwarding(expected_forwarding, current_forwarding, info["number"])
        print(
            f"  Forwarding matched predictions for {len(expected_forwarding)} destinations",
            flush=True,
        )
    else:
        print("  No packet targets; log-only result", flush=True)
    return final_rib_changes


# Derive source-based intervals after collection while keeping FIFO and installation diagnostics.
def attach_send_timing(batch, before_brio, brio_trace):
    submission = batch["submission"]
    vrp_times = [event["observed_ns"] for event in batch["vrp_events"]]
    brio = correlate(before_brio, read_records(brio_trace), dict(
        **submission, first_observed_ns=min(vrp_times, default=None),
        last_observed_ns=max(vrp_times, default=None), full_completed_ns=None,
    ), "incremental", batch["number"])
    brio.update(correlate_control_records(
        brio_trace, submission, response=brio.get("brio_response"), pdu_type=0,
    ))
    batch["timing"].update(brio=brio, brio_notify_send_ns=brio.get("brio_notify_send_ns"))
    if brio["brio_timing_status"] not in ("complete", "no_vrp"):
        raise RuntimeError("No verified BRIO Prefix PDU send origin is available")
    if brio["control_timing_status"] != "complete":
        raise RuntimeError("No verified BRIO Serial Notify send timestamp is available")
    first_send = brio["brio_first_vrp_send_ns"]
    timing = batch["timing"]
    timing.update(reference="brio_first_prefix_pdu_send", clock="CLOCK_MONOTONIC",
                  first_rtr_pdu_sent_ns=first_send,
                  first_rtr_pdu_evidence=("verified_brio_response" if first_send is not None
                                          else "unavailable_no_prefix_pdu"))
    rib_end = max((event["observed_ns"] for event in batch["rib_events"]
                   if rib_path_fields(event["before"]) != rib_path_fields(event["after"])), default=None)
    fib_end = max((event["observed_ns"] for event in batch["forwarding_changes"]), default=None)
    endpoints = dict(
        vrp_rib_last_event=max((event["observed_ns"] for event in
                               [*batch["vrp_events"], *batch["rib_events"]]), default=None),
        first_vrp=min(vrp_times, default=None), last_vrp=max(vrp_times, default=None),
        rib_last_change=rib_end, forwarding_last_change=fib_end,
        joint_last_change=timing["joint_last_change_ns"],
        joint_quiet_confirmed=timing["quiet_confirmed_ns"],
    )
    for name, endpoint in endpoints.items():
        timing[name + "_from_send_ms"] = (
            (endpoint - first_send) / 1e6
            if first_send is not None and endpoint is not None and endpoint >= first_send else None
        )
    print("Primary convergence timing: " + json.dumps({
        name: timing[name] for name in (
            "reference", "first_rtr_pdu_sent_ns", "rib_last_change_from_send_ms",
            "forwarding_last_change_from_send_ms", "joint_last_change_from_send_ms",
        )
    }), flush=True)


# Replay selected batches against one already initialized software instance.
def run(
    sequence,
    predictions,
    config_dir,
    pps,
    packet_timeout,
    batches,
    socket_path,
    fifo,
    quiet_seconds,
    timeout,
    *, log_path=None, results=None, check_alive=None, brio_trace=None,
):
    directory, metadata, operations = load_sequence(sequence)
    if batches is not None:
        if not 1 <= batches <= len(metadata):
            raise ValueError(f"Batches must be between 1 and {len(metadata)}")
        metadata = metadata[:batches]
    nonempty = []
    for info in metadata:
        if not operations[info["number"]]:
            print(f"Batch {info['number']}: no VRP operations; skipped", flush=True)
            continue
        nonempty.append(info)
    metadata = nonempty
    if not metadata:
        return

    expectations = load_vrp_expectations(directory, metadata, operations)
    forwarding_predictions = {info["number"]: {} for info in metadata}
    prediction_rows = {}
    with Path(predictions).open() as source:
        for line in source:
            if line.strip():
                row = json.loads(line)
                prediction_rows[row["batch"]] = row
    for info in metadata:
        if info["number"] not in prediction_rows:
            raise ValueError(f"Missing prediction for batch {info['number']}")
    for info in metadata:
        expected = {}
        for change in prediction_rows[info["number"]]["changes"]:
            probe = change["probe"]
            if probe["status"] == "ready":
                destination = str(ip.ip_address(probe["destination"]))
                gateway = probe["expected_next_hop"]
                if destination in expected and expected[destination] != gateway:
                    raise ValueError(f"Conflicting next-hop predictions for {destination}")
                expected[destination] = gateway
            else:
                print(
                    f"Batch {info['number']}: no probe for {change['prefix']}; "
                    f"{probe['status']}: {probe.get('reason', 'unspecified')}",
                    flush=True,
                )
        forwarding_predictions[info["number"]] = expected
    log_options = {} if log_path is None else dict(log_path=log_path)
    log_events, log_errors, log_stop, log_thread = start_log_observer(
        socket_path,
        ("vrp", "rib"),
        timeout, **log_options,
    )
    try:
        current_rib = read_primary_rib(socket_path, timeout, include_attributes=True)
        check_log_boundary(
            log_events, log_errors, "BIRD changed its RIB while the initial table was being read",
        )
        print(
            f"Observers ready with {len(current_rib)} initial selected routes. "
            "The running BRIO cache must still contain only the initial VRP set.",
            flush=True,
        )
        for position, info in enumerate(metadata):
            if check_alive is not None:
                check_alive()
            batch_result = dict(
                number=info["number"], verified=False,
                unprobeable_prefixes=[
                    dict(prefix=change["prefix"], **change["probe"])
                    for change in prediction_rows[info["number"]]["changes"]
                    if change["probe"]["status"] != "ready"
                ],
            )
            if results is not None:
                results.append(batch_result)
            measure_options = dict(
                expectation=expectations[info["number"]], socket_path=socket_path,
            )
            if results is not None or brio_trace is not None:
                measure_options["result"] = batch_result
            if check_alive is not None:
                measure_options["check_alive"] = check_alive
            packet_observer = None
            initial_forwarding = {}
            expected_forwarding = forwarding_predictions[info["number"]]
            destinations = list(expected_forwarding)
            if destinations:
                check_log_boundary(log_events, log_errors, "BIRD changed before packet startup")
                packet_observer = ForwardingObserver(
                    destinations, config_dir, pps, packet_timeout, log_events, log_errors,
                )
                initial_forwarding = packet_observer.start(timeout)
            try:
                before_brio = read_records(brio_trace) if brio_trace is not None else None
                final_rib_changes = measure_batch(
                    directory,
                    info,
                    operations[info["number"]],
                    log_events,
                    log_errors,
                    fifo,
                    current_rib,
                    initial_forwarding,
                    expected_forwarding,
                    quiet_seconds,
                    timeout, packet_observer=packet_observer, **measure_options,
                )
            finally:
                if packet_observer is not None:
                    packet_observer.close(timeout)
            if not log_errors.empty():
                raise log_errors.get()
            if brio_trace is not None:
                attach_send_timing(batch_result, before_brio, brio_trace)
            check_rib_result(
                prediction_rows[info["number"]], final_rib_changes, info["number"]
            )
            batch_result["verified"] = True
            batch_result["forwarding_verification"] = (
                "verified_targets_only" if expected_forwarding else "not_observed_no_targets"
            )
            if position + 1 < len(metadata):
                print(flush=True)
    finally:
        stop_observer(log_stop, log_thread, timeout)
    if not log_errors.empty():
        raise log_errors.get()
    if log_path is not None:
        check_log_boundary(
            log_events, log_errors, "BIRD produced an event after quiet collection ended",
        )


# Bind existing files explicitly and reject incompatible inputs before starting services.
def load_scenario(path):
    path = Path(path).resolve(strict=True)
    manifest = json.loads(path.read_text())
    if manifest.get("format") != "exp63a-scenario-v1":
        raise ValueError("Expected an exp63a-scenario-v1 input file")
    bound = {}
    for name in ("bgp", "sequence", "predictions"):
        bound[name] = (path.parent / manifest[name]).resolve(strict=True)
    sequence = bound["sequence"]
    if sequence.name != "sequence.json" or not sequence.is_file():
        raise ValueError("Scenario sequence must identify one sequence.json file")
    if not (bound["bgp"] / "bgp.json").is_file():
        raise ValueError("Missing BGP baseline metadata")
    directory, batches, operations = load_sequence(sequence.parent)
    numbers = [batch["number"] for batch in batches]
    if not numbers or len(numbers) != len(set(numbers)):
        raise ValueError("Expected nonempty, unique batch numbers")
    metadata = json.loads(sequence.read_text())
    bound["vrp"] = (directory / metadata["initial"]).resolve(strict=True)
    if not bound["vrp"].is_file():
        raise ValueError("Missing initial VRP file")
    load_vrp_expectations(directory, batches, operations)
    predictions = {}
    with bound["predictions"].open() as source:
        for line in source:
            if not line.strip():
                continue
            row = json.loads(line)
            if row["batch"] in predictions:
                raise ValueError("Duplicate path-prediction batch")
            predictions[row["batch"]] = row
    if set(predictions) != set(numbers):
        raise ValueError("Prediction and sequence batch identities differ")
    policy = predictions[numbers[0]]["policy"]
    if (policy.get("rov") != "on" or policy.get("as_sets") not in ("accept", "reject")
            or not isinstance(policy.get("dut_asn"), int)
            or not 1 <= policy["dut_asn"] <= 4294967295):
        raise ValueError("Predictions must specify ROV on, AS-set policy and a valid DUT ASN")
    for info in batches:
        rows, prediction = operations[info["number"]], predictions[info["number"]]
        if not rows:
            raise ValueError("Managed 6.3A requires nonempty prepared batches")
        if prediction["policy"] != policy or prediction["vrp_operations"] != len(rows):
            raise ValueError("Prediction policy or operation count disagrees with the sequence")
        if prediction.get("unresolved"):
            raise ValueError("Unresolved path predictions cannot be formally verified")
        commands = (directory / info["script"]).read_text().splitlines()
        if commands != [row["command"] for row in rows] + ["notify"]:
            raise ValueError("Batch script disagrees with the recorded operations")
        prefixes = [change["prefix"] for change in prediction["changes"]]
        if len(prefixes) != len(set(prefixes)):
            raise ValueError("Duplicate predicted RIB prefixes")
        for change in prediction["changes"]:
            ip.ip_network(change["prefix"])
            for side in ("before", "after"):
                route = change[side]
                if route is not None:
                    ip.ip_address(route["gateway"])
                    if not route["peer"]:
                        raise ValueError("Predicted route is missing its peer")
            if change["probe"]["status"] == "ready":
                ip.ip_address(change["probe"]["destination"])
    return path, bound, policy


# Own one initialized baseline and always persist its outcome after owned-service cleanup.
def run_managed(scenario, result_path, *, pps=10000, packet_timeout=1, quiet_seconds=10,
                timeout=300, bin_dir=None):
    scenario, bound, policy = load_scenario(scenario)
    if not 0 < quiet_seconds < timeout < float("inf"):
        raise ValueError("Require finite 0 < quiet seconds < timeout")
    if not 0 < pps < float("inf") or not 0 < packet_timeout < float("inf"):
        raise ValueError("Packet rate and timeout must be finite and positive")
    result_path = Path(result_path).absolute()
    trace_path = result_path.with_name("bird-trace.log")
    if result_path.exists() or result_path.is_symlink():
        raise FileExistsError(f"Result already exists: {result_path}")
    if result_path == trace_path:
        raise ValueError("Result and trace paths must differ")
    with trace_path.open("xb"):
        pass
    brio_trace = result_path.with_name("brio-timing.jsonl")
    with brio_trace.open("x"):
        pass
    result = dict(
        scenario=str(scenario), inputs={name: str(value) for name, value in bound.items()},
        policy=policy, measurement="vrp_triggered_rib_and_target_forwarding",
        log_transport="file", bird_trace_log=str(trace_path),
        time_reference="BRIO first Prefix PDU send; FIFO and post-installation intervals retained separately",
        brio_timing_log=str(brio_trace),
        installation_reference="Last observed VRP event when mandatory net changes exist; otherwise unavailable",
        forwarding_scope="UDP probes to selected target addresses; not full kernel FIB",
        pps=pps, packet_timeout=packet_timeout, quiet_seconds=quiet_seconds, timeout=timeout,
        baseline_ready=False, cleanup_complete=None, status="failed", batches=[],
    )
    command = [
        sys.executable, "-u", "-m", "rov.dut.initialize", "--inside",
        "--bgp", str(bound["bgp"]), "--vrp", str(bound["vrp"]),
        "--rov", "on", "--dut-asn", str(policy["dut_asn"]), "--as-sets", policy["as_sets"],
        "--bird-trace-log", str(trace_path), "--bird-trace-info",
    ]
    binaries = Path(bin_dir or ROOT / "software/bin-brio-first-vrp").resolve()
    brio_binary = (binaries / "brio_rc").read_bytes()
    if any(marker not in brio_binary for marker in (b"brio-first-vrp-v1", b"brio-control-send-v1")):
        raise RuntimeError("BRIO must record Prefix PDU and control PDU send timestamps")
    command.extend(["--bin-dir", str(binaries)])
    initialization = None
    output_thread = None
    failure = None
    messages = queue.Queue()

    # Reuse the existing initialization liveness check throughout collection.
    def check_alive():
        check_initialization(initialization, messages)

    try:
        initialization = subprocess.Popen(
            command, stdout=subprocess.PIPE,
            env=dict(os.environ, **{MANAGED_OUTPUT: "1", "BRIO_VRP_TIMING_LOG": str(brio_trace)}),
        )
        output_thread = threading.Thread(
            target=forward_initialization_output, args=(initialization, messages),
            name="exp63a-initialization-output",
        )
        output_thread.start()
        wait_for_initialization(initialization, messages)
        check_alive()
        result["baseline_ready"] = True
        print(f"Experiment 63A: VRP and RIB file log {trace_path}; ROV enabled.", flush=True)
        run(
            bound["sequence"].parent, bound["predictions"], CONFIG,
            pps, packet_timeout, None, CONFIG / "bird.ctl", CONFIG / "brio.in",
            quiet_seconds, timeout, log_path=trace_path, results=result["batches"],
            check_alive=check_alive, brio_trace=brio_trace,
        )
        check_alive()
    except BaseException as error:
        failure = error
    finally:
        if initialization is not None:
            try:
                exit_code = stop_initialization(initialization, output_thread)
                result["initialization_exit_code"] = exit_code
                result["cleanup_complete"] = exit_code == 0
                if exit_code != 0:
                    raise RuntimeError(f"Initialization cleanup failed ({exit_code})")
            except BaseException as error:
                result["cleanup_complete"] = False
                result["cleanup_error"] = str(error)
                if failure is None:
                    failure = error
        else:
            result["cleanup_complete"] = True
        result["status"] = (
            "success" if failure is None else
            "interrupted" if isinstance(failure, KeyboardInterrupt) else "failed"
        )
        result["exit_code"] = 0 if failure is None else 130 if isinstance(failure, KeyboardInterrupt) else 1
        if failure is not None:
            result["error"] = f"{type(failure).__name__}: {failure}"
        with result_path.open("x") as target:
            json.dump(result, target, indent=2)
            target.write("\n")
    if failure is not None:
        raise failure
    return result


# Execute only the internally supervised managed worker.
def _worker_main():
    parser = argparse.ArgumentParser(description=__doc__)
    inputs = parser.add_mutually_exclusive_group(required=True)
    inputs.add_argument("--sequence", type=Path)
    inputs.add_argument("--scenario", type=Path, help="Bound input file for a fresh managed run")
    parser.add_argument("--predictions", type=Path)
    parser.add_argument("--result", type=Path, help="New managed JSON result file")
    parser.add_argument("--bin-dir", type=Path)
    parser.add_argument("--bird-trace-log", type=Path, help="Existing TRACE+INFO file for existing-service mode")
    parser.add_argument("--observe-only", action="store_true",
                        help="Use predictions only for probe targets; retain observation-only behavior")
    parser.add_argument("--config-dir", type=Path, default=CONFIG)
    parser.add_argument("--pps", type=float, default=10000,
                        help="Aggregate FIB probe rate in packets per second (default: 10000)")
    parser.add_argument("--packet-timeout", type=float, default=1)
    parser.add_argument("--inside", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--batches", type=int)
    parser.add_argument("--socket", type=Path, default=CONFIG / "bird.ctl")
    parser.add_argument("--fifo", type=Path, default=CONFIG / "brio.in")
    parser.add_argument("--quiet-seconds", type=float,
                        help="Seconds without new events before stopping collection (default: 10)")
    parser.add_argument("--timeout", type=float, default=300)
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.quiet_seconds is None:
        args.quiet_seconds = 10
    if not 0 < args.quiet_seconds < args.timeout < float("inf"):
        parser.error("Quiet seconds and timeout must be finite, positive, and quiet < timeout")
    if not 0 < args.pps < float("inf") or not 0 < args.packet_timeout < float("inf"):
        parser.error("Packet rate and timeout must be finite and positive")
    if args.scenario is not None:
        if args.result is None or args.predictions or args.observe_only or args.batches or args.bird_trace_log:
            parser.error("--scenario requires --result and cannot use existing-service input options")
        if (args.config_dir.resolve() != CONFIG.resolve()
                or args.socket.resolve() != (CONFIG / "bird.ctl").resolve()
                or args.fifo.resolve() != (CONFIG / "brio.in").resolve()):
            parser.error("Managed runs require the owned initialization endpoints")
        load_scenario(args.scenario)
        if args.result.exists() or args.result.is_symlink():
            raise FileExistsError(f"Result already exists: {args.result}")
        if not os.environ.get(MANAGED_OUTPUT):
            return supervise("rov.experiments.exp63a", sys.argv[1:])
        if not args.inside:
            enter_namespace(sys.argv[1:], module="rov.experiments.exp63a")
        run_managed(
            args.scenario, args.result, pps=args.pps, packet_timeout=args.packet_timeout,
            quiet_seconds=args.quiet_seconds, timeout=args.timeout,
            bin_dir=args.bin_dir,
        )
        return 0


# Reserve a new result directory and execute exactly the explicitly bound input.
def run_one(scenario, output=None, *, pps=10000, packet_timeout=1, quiet_seconds=10,
            timeout=300, bin_dir=None):
    scenario, _, _ = load_scenario(scenario)
    if not 0 < quiet_seconds < timeout < float("inf"):
        raise ValueError("Require finite 0 < quiet seconds < timeout")
    if not 0 < pps < float("inf") or not 0 < packet_timeout < float("inf"):
        raise ValueError("Packet rate and timeout must be finite and positive")
    output = Path(output).absolute() if output is not None else scenario.parent / "results"
    if output.exists() or output.is_symlink():
        raise FileExistsError(f"Result directory already exists: {output}")
    output.mkdir()
    result_path = output / "result.json"
    arguments = [
        "--worker", "--scenario", str(scenario), "--result", str(result_path),
        "--pps", str(pps), "--packet-timeout", str(packet_timeout),
        "--quiet-seconds", str(quiet_seconds), "--timeout", str(timeout),
    ]
    if bin_dir is not None:
        arguments.extend(["--bin-dir", str(Path(bin_dir).resolve())])
    failure = None
    try:
        with (output / "run.log").open("xb") as log:
            code = supervise("rov.experiments.exp63a", arguments, log=log)
    except BaseException as error:
        failure = error
        code = 130 if isinstance(error, KeyboardInterrupt) else 1
    if result_path.exists():
        result = json.loads(result_path.read_text())
    else:
        result = dict(
            scenario=str(scenario), measurement="vrp_triggered_rib_and_target_forwarding",
            log_transport="file", baseline_ready=False, cleanup_complete=None,
            error="Managed child did not produce a complete result",
        )
    result["exit_code"] = code
    if code != 0 or result.get("status") != "success":
        result["status"] = "interrupted" if code == 130 else "failed"
        if code == 0:
            code = result["exit_code"] = 1
    if failure is not None:
        result["runner_error"] = f"{type(failure).__name__}: {failure}"
    result_path.write_text(json.dumps(result, indent=2) + "\n")
    print(f"Experiment 63A result: {result_path}; exit code: {code}", flush=True)
    if failure is not None:
        raise failure
    return code


# Accept one scenario file and preserve the existing measurement controls.
def main():
    if "--worker" in sys.argv[1:]:
        return _worker_main()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("scenario", type=Path, help="An exp63a-scenario-v1 JSON input file")
    parser.add_argument("--output", type=Path, help="New result directory; default: scenario/results")
    parser.add_argument("--pps", type=float, default=10000,
                        help="Aggregate FIB probe rate in packets per second (default: 10000)")
    parser.add_argument("--packet-timeout", type=float, default=1)
    parser.add_argument("--quiet-seconds", type=float, default=10,
                        help="Seconds without new events before stopping collection (default: 10)")
    parser.add_argument("--timeout", type=float, default=300)
    parser.add_argument("--bin-dir", type=Path)
    args = parser.parse_args()
    try:
        return run_one(**vars(args))
    except (OSError, ValueError, RuntimeError, KeyError) as error:
        parser.exit(1, f"{error}\n")


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, stop_requested)
    signal.signal(signal.SIGINT, stop_requested)
    raise SystemExit(main())
