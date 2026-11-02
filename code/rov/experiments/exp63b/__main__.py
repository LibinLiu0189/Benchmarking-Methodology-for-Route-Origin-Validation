"""Run one 6.3B mode with a fresh owned baseline and complete retained output."""

from rov.paths import ROOT
import argparse
import ipaddress as ip
import json
import os
import signal
import subprocess
from pathlib import Path
import queue
import sys
import threading
import time
from rov.paths import CONFIG
from rov.measure.read_log import start_log_observer
from rov.measure.read_log import stop_observer
from rov.measure.cli import read_primary_rib as read_rib
from rov.data.bgp import load_bgp
from rov.data.bgp_updates import load_update_sequence
from rov.measure.data_plane import PACKET_EVENTS
from rov.measure.forwarding import ForwardingObserver, forwarding_state
from rov.send.send_bgp import send_file
from rov.send.send_bgp import session
from rov.dut.managed import forward_initialization_output
from rov.dut.managed import wait_for_initialization
from rov.dut.managed import check_initialization
from rov.dut.managed import stop_initialization
from rov.dut.managed import stop_requested
from rov.dut.output import MANAGED_OUTPUT
from rov.dut.output import supervise
from rov.dut.virtualize import enter_namespace
from rov.measure.cli import rib_path_fields


# Drain only the events already queued, allowing the measurement loop to progress.
def drain(events):
    for _ in range(events.qsize()):
        try:
            yield events.get_nowait()
        except queue.Empty:
            break


# Check that a RIB action agrees with the currently selected path's presence.
def check_rib_event(event, before):
    prefix = event["prefix"]
    action = event["action"]
    if action == "added" and before is not None:
        raise RuntimeError(f"RIB added an already present prefix {prefix}")
    if action in ("replaced", "removed") and before is None:
        raise RuntimeError(f"RIB {action} has no previous selected path for {prefix}")


# Report only differing selected-path fields, including route presence changes.
def rib_path_differences(expected, actual, side):
    expected_fields = rib_path_fields(expected)
    actual_fields = rib_path_fields(actual)
    if expected_fields == actual_fields:
        return {}
    if expected_fields is None or actual_fields is None:
        return {side: dict(expected=expected_fields, actual=actual_fields)}

    return {
        f"{side}.{field}": dict(expected=value, actual=actual_fields[field])
        for field, value in expected_fields.items()
        if value != actual_fields[field]
    }


# Save complete RIB mismatches and print paths plus their field-level differences.
def report_rib_mismatches(mismatches, batch, directory=None):
    exclusive = directory is not None
    directory = Path(directory) if exclusive else ROOT / "tmp"
    directory.mkdir(parents=True, exist_ok=True)
    destination = directory / f"exp63b-batch-{batch}-rib-mismatch.json"
    with destination.open("x" if exclusive else "w", encoding="utf-8") as stream:
        stream.write(json.dumps(mismatches, indent=2) + "\n")

    for mismatch in mismatches:
        print(f"Prefix: {mismatch['prefix']} ({mismatch['reason']})", flush=True)
        prediction = mismatch["prediction"]
        if prediction == "no predicted change":
            print("  Prediction: no predicted change", flush=True)
        else:
            print(f"  Prediction type: {prediction['type']}", flush=True)
            print("  Expected before: " + json.dumps(prediction["before"]), flush=True)
            print("  Expected after: " + json.dumps(prediction["after"]), flush=True)

        print("  Actual before: " + json.dumps(mismatch["actual"]["before"]), flush=True)
        print("  Actual after: " + json.dumps(mismatch["actual"]["after"]), flush=True)
        print("  Differences:", flush=True)
        for field, values in mismatch["differences"].items():
            presence = " route presence differs" if field in ("before", "after", "actual") else ""
            print(f"    {field}{presence}:", flush=True)
            for label, value in values.items():
                print(f"      {label}: {json.dumps(value)}", flush=True)

    print(f"RIB mismatch details saved to: {destination}", flush=True)


# Compare change sets first, then both endpoints using the same selected-path fields.
def check_rib_result(prediction, final_rib_changes, batch, batch_before, current_rib, diagnostic_dir=None):
    expected = {change["prefix"]: change for change in prediction["changes"]}
    actual = {change["prefix"]: change for change in final_rib_changes}
    missing = expected.keys() - actual.keys()
    extra = actual.keys() - expected.keys()
    mismatches = []

    for prefix in sorted(
        missing, key=lambda value: (ip.ip_network(value).version, ip.ip_network(value))
    ):
        # Untouched prefixes have the same path before and after the batch.
        after = current_rib.get(prefix)
        before = batch_before.get(prefix, after)
        change = expected[prefix]
        differences = rib_path_differences(change["before"], before, "before")
        differences.update(rib_path_differences(change["after"], after, "after"))
        mismatches.append(dict(
            prefix=prefix,
            reason="missing expected RIB change",
            prediction=change,
            actual=dict(before=before, after=after),
            differences=differences,
        ))

    for prefix in sorted(
        extra, key=lambda value: (ip.ip_network(value).version, ip.ip_network(value))
    ):
        change = actual[prefix]
        differences = {
            field: dict(before=values["expected"], after=values["actual"])
            for field, values in rib_path_differences(
                change["before"], change["after"], "actual",
            ).items()
        }
        mismatches.append(dict(
            prefix=prefix,
            reason="unexpected RIB change",
            prediction="no predicted change",
            actual=dict(before=change["before"], after=change["after"]),
            differences=differences,
        ))

    if missing or extra:
        report_rib_mismatches(mismatches, batch, diagnostic_dir)
        raise RuntimeError(
            f"Batch {batch} RIB change set mismatch: "
            f"{len(missing)} missing, {len(extra)} unexpected"
        )

    for prefix, change in expected.items():
        observed = actual[prefix]
        differences = rib_path_differences(change["before"], observed["before"], "before")
        differences.update(rib_path_differences(change["after"], observed["after"], "after"))
        if differences:
            mismatches.append(dict(
                prefix=prefix,
                reason="selected-path mismatch",
                prediction=change,
                actual=dict(before=observed["before"], after=observed["after"]),
                differences=differences,
            ))

    if mismatches:
        report_rib_mismatches(mismatches, batch, diagnostic_dir)
        raise RuntimeError(f"Batch {batch} RIB path mismatch for {len(mismatches)} prefixes")


# Read primary routes with the attributes required by BGP-update verification.
def read_primary_rib(socket_path, timeout):
    return read_rib(socket_path, timeout, include_attributes=True)


# Print batch timing and up to three changes each for RIB and forwarding.
def print_results(result):
    started = result["first_update_sent_ns"]
    last = result["last_change_ms"]
    last_text = "unavailable (no state change)" if last is None else f"{last:.3f} ms"
    print(
        f"Batch {result['batch']} ROV-{result['rov']}: "
        f"last change {last_text}; "
        f"stable confirmed {result['stable_confirmed_ms']:.3f} ms "
        f"(includes {result['quiet_seconds']:g}s quiet window)",
        flush=True,
    )

    changes = [("RIB", event) for event in result["rib_events"][:3]]
    changes.extend(("FORWARDING", event) for event in result["forwarding_changes"][:3])
    changes.sort(key=lambda item: item[1]["observed_ns"])

    for kind, event in changes:
        delay = (event["observed_ns"] - started) / 1e6

        if kind == "RIB":
            path = event["after"]
            selected = "none" if path is None else (
                f"{path['peer']} via {path['gateway']} ({path['interface']})"
            )
            print(
                f"  [RIB] {event['action']} {event['prefix']}: "
                f"after={selected}; observed after {delay:.3f} ms", flush=True,
            )
        else:
            previous = event["before"]
            before = "not yet observed after injection" if previous is None else (
                f"{previous[0]} via {previous[1]} ({previous[2]})"
            )
            print(
                f"  [FORWARDING] {event['destination']}: {before} -> "
                f"{event['event']} via {event.get('next_hop')} ({event.get('interface')}); "
                f"observed after {delay:.3f} ms", flush=True,
            )


# Collect only logs and forwarding changes until the shared event queue is quiet.
def measure_batch(
    args, batch, prediction, endpoints, logs, log_errors,
    *, result=None, check_alive=None, stop_logs,
):
    expected = {}
    skipped = []
    targets = prediction.get("probes")
    if targets is None:
        targets = [dict(prefix=change["prefix"], **change["probe"]) for change in prediction["changes"]]
    for probe in targets:
        if probe["status"] != "ready":
            skipped.append(dict(prefix=probe["prefix"], probe=probe))
            continue
        address = str(ip.ip_address(probe["destination"]))
        gateway = probe["expected_next_hop"]
        if address in expected and expected[address] != gateway:
            raise ValueError(f"Conflicting predictions for {address}")
        expected[address] = gateway

    print(
        f"\nBatch {batch['number']}: preparing {len(expected)} probe destinations; "
        f"{len(skipped)} skipped probes; {len(prediction['unresolved'])} unresolved predictions",
        flush=True,
    )

    if result is None:
        result = {}
    rib_events, packet_changes, receipts = [], [], []
    samples, current = {}, {}
    result.update(
        batch=batch["number"], counts=batch["counts"], rov=args.rov, verified=False,
        expected_forwarding=expected, skipped_probes=skipped,
        unresolved=prediction["unresolved"], submitted_ns=None,
        first_update_sent_ns=None, last_change_ms=None,
        stable_confirmed_ms=None, quiet_seconds=args.quiet_seconds,
        rib_events=rib_events, forwarding_changes=packet_changes,
        send_receipts=receipts, final_forwarding_samples=samples,
        initial_forwarding={}, final_forwarding=current,
    )
    observer = None
    started = None
    latest = None
    last_activity = None
    log_drained = getattr(args, "bird_trace_log", None) is None

    # Retain published changes without prediction checks or packet state comparisons.
    def collect(events):
        nonlocal latest, last_activity, log_drained
        for event in events:
            if event.get("event") == "drained":
                log_drained = event["drained"]
                continue

            if event.get("event") in PACKET_EVENTS:
                destination = event["destination"]
                current[destination] = forwarding_state(event)
                samples[destination] = {
                    key: value for key, value in event.items() if key != "before"
                }
                if event["observed_ns"] < started:
                    continue
                packet_changes.append(event)
            else:
                rib_events.append(event)

            if event["observed_ns"] >= started:
                latest = max(latest, event["observed_ns"])
                last_activity = time.monotonic_ns()

    try:
        if not log_errors.empty():
            raise log_errors.get()
        if any(event.get("event") != "drained" for event in drain(logs)):
            raise RuntimeError("RIB changed before batch preparation; restore the baseline")

        if expected:
            observer = ForwardingObserver(
                list(expected), args.config_dir, args.pps, args.packet_timeout,
                logs, log_errors,
            )
            initial = observer.start(args.timeout)
            result["initial_forwarding"] = dict(initial)
            samples.update(initial)
            current.update({
                destination: forwarding_state(event)
                for destination, event in initial.items()
            })

        if not log_errors.empty():
            raise log_errors.get()
        pending_events = list(drain(logs))
        print(
            f"Observers ready; submitting RAW updates for {len(batch['peers'])} peers.",
            flush=True,
        )
        deadline = time.monotonic() + args.timeout
        for item in batch["peers"]:
            local, remote = endpoints[item["peer"]]
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("Batch submission timed out")
            receipt = send_file(
                args.sequence / item["file"], local, remote,
                args.config_dir / "bng.sock", min(10, remaining),
            )
            receipts.append(dict(peer=item["peer"], **receipt))
            if started is None:
                started = receipt["write_start_ns"]
                result["submitted_ns"] = receipt["submitted_ns"]
        if not receipts:
            raise ValueError("Cannot measure a batch without BGP updates")

        latest = started
        last_activity = time.monotonic_ns()
        while True:
            if check_alive is not None:
                check_alive()
            if not log_errors.empty():
                raise log_errors.get()
            collect(pending_events)
            collect(drain(logs))
            pending_events = []

            now = time.monotonic_ns()
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"Batch {batch['number']} did not reach RIB and sampled forwarding silence"
                )
            if (log_drained and logs.empty()
                    and now - last_activity >= args.quiet_seconds * 1e9):
                break
            try:
                pending_events.append(logs.get(timeout=0.001))
            except queue.Empty:
                pass
    finally:
        try:
            if observer is not None:
                observer.close(args.timeout)
        finally:
            stop_logs()

    if not log_errors.empty():
        raise log_errors.get()
    collect(drain(logs))
    confirmed = time.monotonic_ns()
    result["quiet_confirmed_ns"] = confirmed
    if confirmed - latest < args.quiet_seconds * 1e9:
        raise RuntimeError("A state change arrived while quiet collection was stopping")

    if observer is not None:
        for destination, event in observer.snapshot().items():
            samples[destination] = event
            current[destination] = forwarding_state(event)

    # Read send evidence after collection, reusing the existing final session queries.
    raw_update_state_at_end = {}
    for receipt in receipts:
        selected = session(args.config_dir / "bng.sock", *endpoints[receipt["peer"]], 10)
        raw_update_state_at_end[receipt["peer"]] = selected.get("raw-update-state")
        first_sent_ns = selected.get("raw-update-first-tx-ns")
        if (selected.get("raw-update-file") != receipt["file"]
                or not isinstance(first_sent_ns, int)
                or not receipt["write_start_ns"] <= first_sent_ns <= confirmed):
            raise RuntimeError(f"Missing or invalid first UPDATE send timestamp for {receipt['peer']}")
        receipt["first_update_sent_ns"] = first_sent_ns

    first_sent_ns = min(receipt["first_update_sent_ns"] for receipt in receipts)
    result["first_update_sent_ns"] = first_sent_ns
    packet_changes[:] = [event for event in packet_changes if event["observed_ns"] >= first_sent_ns]
    measured_events = [event for event in [*rib_events, *packet_changes]
                       if event["observed_ns"] >= first_sent_ns]
    result.update(
        last_change_ms=(max(event["observed_ns"] for event in measured_events) - first_sent_ns) / 1e6
        if measured_events else None,
        stable_confirmed_ms=(confirmed - first_sent_ns) / 1e6,
        raw_update_state_at_end=raw_update_state_at_end,
    )
    return result


# Validate prepared batches and map their peer names to configured session endpoints.
def run(args, *, results=None, check_alive=None):
    batches = load_update_sequence(args.sequence)
    if args.batches is not None:
        if not 1 <= args.batches <= len(batches):
            raise ValueError(f"Batches must be between 1 and {len(batches)}")
        batches = batches[:args.batches]

    predictions = {}
    with args.predictions.open() as source:
        for line in source:
            if line.strip():
                row = json.loads(line)
                if row["batch"] in predictions:
                    raise ValueError("Duplicate prediction batch")
                predictions[row["batch"]] = row

    bgp = load_bgp(args.bgp)
    config = json.loads((args.config_dir / "bng.json").read_text())
    bird_config = (args.config_dir / "bird.conf").read_text()
    rov_enabled = any(
        f"roa_check(r{afi}, net, bgp_path.last)" in bird_config
        for afi in (4, 6)
    )
    if rov_enabled != (args.rov == "on"):
        raise ValueError("ROV mode does not match the generated BIRD configuration")
    if rov_enabled and "ROA_INVALID then reject;" not in bird_config:
        raise ValueError("ROV-on measurement requires the Invalid rejection policy")
    
    endpoints = {}
    for peer in bgp["peers"]:
        matches = [s for s in config["bgp"] if s["local-address"] == peer["source_address"]
                   and s["local-as"] == peer["asn"]]
        if len(matches) != 1:
            raise ValueError(f"Expected one configured session for {peer['name']}")
        endpoints[peer["name"]] = (matches[0]["local-address"], matches[0]["peer-address"])

    for batch in batches:
        prediction = predictions.get(batch["number"])
        if prediction is None or prediction["counts"] != batch["counts"] or prediction["source"] != batch["source"]:
            raise ValueError(f"Predictions do not match batch {batch['number']}")
        prediction_policy = prediction["policy"]
        if prediction_policy.get("rov") != args.rov:
            raise ValueError("Prediction policy does not match the requested ROV mode")
        as_sets = "on" if prediction["policy"]["as_sets"] == "accept" else "off"
        if f"allow as sets {as_sets};" not in bird_config or any(
            item["peer-as"] != prediction["policy"]["dut_asn"] for item in config["bgp"]
        ):
            raise ValueError("Prediction ASN or AS_SET policy differs from the baseline")
        for item in batch["peers"]:
            if item["peer"] not in endpoints or not (args.sequence / item["file"]).is_file():
                raise ValueError(f"Missing session or RAW file: {item}")
    
    # Require initial transmission to have finished before starting batch measurements.
    for local, remote in endpoints.values():
        selected = session(args.config_dir / "bng.sock", local, remote, 10)
        if selected.get("raw-update-state") != "done":
            raise RuntimeError("Initial RAW transmission is not complete")
    
    logs, errors, stop, thread = start_log_observer(
        args.config_dir / "bird.ctl", ("rib",), args.timeout,
        **({} if getattr(args, "bird_trace_log", None) is None else dict(log_path=args.bird_trace_log)),
    )
    logs_stopped = False

    # Stop the shared log producer before validation and retain idempotent cleanup.
    def stop_logs():
        nonlocal logs_stopped
        if not logs_stopped:
            stop_observer(stop, thread, args.timeout)
            logs_stopped = True

    try:
        current_rib = read_primary_rib(args.config_dir / "bird.ctl", args.timeout)

        for batch in batches:
            result = {}
            if results is not None:
                results.append(result)
            result = measure_batch(
                args, batch, predictions[batch["number"]], endpoints, logs, errors,
                result=result, check_alive=check_alive, stop_logs=stop_logs,
            )

            for target in predictions[batch["number"]].get("baseline_selected", []):
                if rib_path_fields(target["path"]) != rib_path_fields(current_rib.get(target["prefix"])):
                    raise RuntimeError(f"Initial RIB disagrees for {target['prefix']}")
            for probe in predictions[batch["number"]].get("probes", []):
                if probe["status"] != "ready" or "before_next_hop" not in probe:
                    continue
                address = str(ip.ip_address(probe["destination"]))
                gateway = probe["before_next_hop"]
                wanted = ("not_received" if gateway is None else "received", gateway)
                actual = forwarding_state(result["initial_forwarding"][address])
                if actual[:2] != wanted:
                    raise RuntimeError(f"Initial forwarding disagrees for {address}")

            batch_before = {}
            for event in result["rib_events"]:
                if event["observed_ns"] < result["first_update_sent_ns"]:
                    raise RuntimeError("RIB event predates batch injection")
                prefix = event["prefix"]
                before = current_rib.get(prefix)
                event["before"] = before
                try:
                    check_rib_event(event, before)
                except RuntimeError as error:
                    predicted = next(
                        (change for change in predictions[batch["number"]]["changes"]
                         if change["prefix"] == prefix),
                        "no predicted change",
                    )
                    report_rib_mismatches([dict(
                        prefix=prefix,
                        reason=f"invalid RIB event: {error}",
                        prediction=predicted,
                        actual=dict(before=before, after=event["after"]),
                        differences={"event.action": dict(
                            expected="added" if before is None else "replaced or removed",
                            actual=event["action"],
                        )},
                    )], batch["number"], args.result.parent if getattr(args, "result", None) else None)
                    raise

                batch_before.setdefault(prefix, before)

                after = event["after"]
                if after is None:
                    current_rib.pop(prefix, None)
                else:
                    current_rib[prefix] = after

            final_rib_changes = [
                dict(prefix=prefix, before=before, after=current_rib.get(prefix))
                for prefix, before in batch_before.items()
                if rib_path_fields(before) != rib_path_fields(current_rib.get(prefix))
            ]
            final_rib_changes.sort(
                key=lambda row: (
                    ip.ip_network(row["prefix"]).version,
                    ip.ip_network(row["prefix"]),
                )
            )

            result["final_rib_changes"] = final_rib_changes
            check_rib_result(
                predictions[batch["number"]], final_rib_changes, batch["number"],
                batch_before, current_rib,
                args.result.parent if getattr(args, "result", None) else None,
            )

            for address, gateway in result["expected_forwarding"].items():
                expected = ("not_received" if gateway is None else "received", gateway)
                actual = result["final_forwarding"][address]

                if actual[:2] != expected:
                    raise RuntimeError(
                        f"Batch {batch['number']} forwarding mismatch for {address}: "
                        f"expected={expected}; actual={actual}"
                    )

            result["verified"] = True
            print_results(result)

    finally:
        stop_logs()
    if not errors.empty():
        raise errors.get()


# Bind one generated input to a specific mode and check the paired probe identities.
def load_scenario(path, mode):
    path = Path(path).resolve(strict=True)
    manifest = json.loads(path.read_text())
    if manifest.get("format") != "exp63b-scenario-v1":
        raise ValueError("Expected exp63b-scenario-v1")
    bound = {key: (path.parent / manifest[key]).resolve(strict=True) for key in ("bgp", "vrp", "sequence")}
    predictions = {}
    for name in ("on", "off"):
        prediction_path = (path.parent / manifest["predictions"][name]).resolve(strict=True)
        prediction, = [json.loads(line) for line in prediction_path.read_text().splitlines() if line.strip()]
        predictions[name] = prediction
        if name == mode:
            bound["predictions"] = prediction_path
        if prediction["policy"]["rov"] != name or prediction["unresolved"]:
            raise ValueError("Mode mismatch or unresolved predictions")
    identities = {name: [(row["prefix"], row["status"], row["destination"])
                         for row in prediction["probes"]] for name, prediction in predictions.items()}
    if identities["on"] != identities["off"]:
        raise ValueError("ROV modes must use identical probe identities and destination mapping")
    if len(load_update_sequence(bound["sequence"])) != 1:
        raise ValueError("Managed 63B requires one update set")
    return path, bound, predictions[mode]["policy"], manifest.get("seed")


# Own one fresh isolated initialization, retaining all observations even on failure.
def run_managed(args):
    scenario, bound, policy, seed = load_scenario(args.scenario, args.rov)
    result_path = args.result.absolute()
    trace = result_path.with_name("bird-trace.log")
    if result_path.exists() or result_path.is_symlink():
        raise FileExistsError(result_path)
    with trace.open("xb"):
        pass
    result = dict(format="exp63b-result-v1", scenario=str(scenario), seed=seed, rov=args.rov,
                  inputs={key: str(value) for key, value in bound.items()}, policy=policy,
                  status="failed", baseline_ready=False, cleanup_complete=None, batches=[],
                  log_transport="file", bird_trace_log=str(trace),
                  time_reference="CLOCK_MONOTONIC before the first successful RAW socket send of this batch's first BGP UPDATE",
                  termination="pure quiet after last RIB or sampled forwarding change; no coverage completion gate",
                  forwarding_scope="paired fixed UDP targets, not full kernel FIB",
                  pps=args.pps, packet_timeout=args.packet_timeout, quiet_seconds=args.quiet_seconds,
                  timeout=args.timeout, frame_size_note="fixed packet builder per AFI, identical in both modes")
    command = [sys.executable, "-u", "-m", "rov.dut.initialize", "--inside",
               "--bgp", str(bound["bgp"]), "--vrp", str(bound["vrp"]),
               "--rov", args.rov, "--dut-asn", str(policy["dut_asn"]), "--as-sets", policy["as_sets"],
               "--bird-trace-log", str(trace), "--bird-trace-info"]
    if args.bin_dir is not None:
        command.extend(["--bin-dir", str(args.bin_dir.resolve())])
    initialization, output_thread, failure = None, None, None
    messages = queue.Queue()

    # Check only this run's owned initialization process during observation.
    def check_alive():
        check_initialization(initialization, messages)

    try:
        initialization = subprocess.Popen(command, stdout=subprocess.PIPE,
                                           env=dict(os.environ, **{MANAGED_OUTPUT: "1"}))
        output_thread = threading.Thread(target=forward_initialization_output,
                                          args=(initialization, messages), name="exp63b-initialization-output")
        output_thread.start()
        wait_for_initialization(initialization, messages)
        result["baseline_ready"] = True
        args.bgp, args.sequence, args.predictions = bound["bgp"], bound["sequence"], bound["predictions"]
        args.bird_trace_log = trace
        run(args, results=result["batches"], check_alive=check_alive)
        check_alive()
    except BaseException as error:
        failure = error
    finally:
        if initialization is not None:
            try:
                code = stop_initialization(initialization, output_thread)
                result["initialization_exit_code"] = code
                result["cleanup_complete"] = code == 0
                if code != 0:
                    raise RuntimeError(f"Initialization cleanup failed ({code})")
            except BaseException as error:
                result["cleanup_complete"] = False
                result["cleanup_error"] = str(error)
                if failure is None:
                    failure = error
        else:
            result["cleanup_complete"] = True
        result["status"] = "success" if failure is None else "interrupted" if isinstance(failure, KeyboardInterrupt) else "failed"
        result["exit_code"] = 0 if failure is None else 130 if isinstance(failure, KeyboardInterrupt) else 1
        if failure is not None:
            result["error"] = f"{type(failure).__name__}: {failure}"
        with result_path.open("x") as stream:
            json.dump(result, stream, indent=2)
            stream.write("\n")
    if failure is not None:
        raise failure
    return result


# Execute only the internally supervised managed worker.
def _worker_main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bgp", type=Path)
    parser.add_argument("--sequence", type=Path)
    parser.add_argument("--predictions", type=Path)
    parser.add_argument("--scenario", type=Path)
    parser.add_argument("--result", type=Path)
    parser.add_argument("--bin-dir", type=Path)
    parser.add_argument("--bird-trace-log", type=Path)
    parser.add_argument("--rov", choices=("on", "off"), required=True)
    parser.add_argument("--config-dir", type=Path, default=CONFIG)
    parser.add_argument("--pps", type=float, default=10000,
                        help="Aggregate FIB probe rate in packets per second (default: 10000)")
    parser.add_argument("--packet-timeout", type=float, default=1)
    parser.add_argument("--quiet-seconds", type=float, default=10,
                        help="Seconds without new events before stopping collection (default: 10)")
    parser.add_argument("--timeout", type=float, default=300)
    parser.add_argument("--batches", type=int)
    parser.add_argument("--inside", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if not 0 < args.quiet_seconds < args.timeout < float("inf"):
        parser.error("Require finite 0 < quiet-seconds < timeout")
    if not 0 < args.pps < float("inf") or not 0 < args.packet_timeout < float("inf"):
        parser.error("Packet rate and timeout must be finite and positive")
    if args.scenario is not None:
        if args.result is None or args.bgp or args.sequence or args.predictions or args.batches or args.bird_trace_log:
            parser.error("Managed mode requires --result and cannot use existing-service input options")
        if args.config_dir.resolve() != CONFIG.resolve():
            parser.error("Managed mode requires the owned configuration directory")
        load_scenario(args.scenario, args.rov)
        if not os.environ.get(MANAGED_OUTPUT):
            return supervise("rov.experiments.exp63b", sys.argv[1:])
        if not args.inside:
            enter_namespace(sys.argv[1:], module="rov.experiments.exp63b")
        run_managed(args)
        return 0


# Preserve each attempt in a new directory, including initialization or verification failures.
def run_one(scenario, rov, output, *, pps=10000, packet_timeout=1, quiet_seconds=10,
            timeout=300, bin_dir=None):
    scenario, _, _, _ = load_scenario(scenario, rov)
    if not 0 < quiet_seconds < timeout < float("inf"):
        raise ValueError("Require finite 0 < quiet-seconds < timeout")
    if not 0 < pps < float("inf") or not 0 < packet_timeout < float("inf"):
        raise ValueError("Packet rate and timeout must be finite and positive")
    output = Path(output).absolute()
    if output.exists() or output.is_symlink():
        raise FileExistsError(f"Result directory already exists: {output}")
    output.mkdir(parents=True)
    result_path = output / "result.json"
    arguments = ["--worker", "--scenario", str(scenario), "--rov", rov, "--result", str(result_path),
                 "--pps", str(pps), "--packet-timeout", str(packet_timeout),
                 "--quiet-seconds", str(quiet_seconds), "--timeout", str(timeout)]
    if bin_dir is not None:
        arguments.extend(["--bin-dir", str(Path(bin_dir).resolve())])
    failure = None
    try:
        with (output / "run.log").open("xb") as log:
            code = supervise("rov.experiments.exp63b", arguments, log=log)
    except BaseException as error:
        failure = error
        code = 130 if isinstance(error, KeyboardInterrupt) else 1
    result = json.loads(result_path.read_text()) if result_path.exists() else dict(
        format="exp63b-result-v1", scenario=str(scenario), rov=rov, baseline_ready=False,
        cleanup_complete=None, log_transport="file", batches=[],
        error="Managed child did not produce a result",
    )
    result["exit_code"] = code
    if code != 0 or result.get("status") != "success":
        result["status"] = "interrupted" if code == 130 else "failed"
        if code == 0:
            code = result["exit_code"] = 1
    if failure is not None:
        result["runner_error"] = f"{type(failure).__name__}: {failure}"
    result_path.write_text(json.dumps(result, indent=2) + "\n")
    print(f"Experiment 63B ROV-{rov}: {result_path}; exit code: {code}", flush=True)
    if failure is not None:
        raise failure
    return code


# Accept exactly one generated input and one ROV mode.
def main():
    if "--worker" in sys.argv[1:]:
        return _worker_main()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("scenario", type=Path)
    parser.add_argument("--rov", choices=("on", "off"), required=True)
    parser.add_argument("--output", type=Path, required=True)
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
