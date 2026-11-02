"""Replay VRP identities and check the states selected by each VRP experiment.

Callers retain target selection, completion rules, and timing boundaries.
"""

import ipaddress as ip
from rov.data.vrp import normalize
from rov.measure.bird_connection import BirdConnection
from rov.measure.bird_connection import BirdError
from rov.measure.cli import query_roa

# Replay legal operations by complete VRP identity and retain initial/final presence.
def build_vrp_expectation(initial, operations):
    initial_rows = {normalize(*row) for row in initial}
    entries = {}
    for operation in operations:
        key = normalize(*operation["vrp"])
        entry = entries.setdefault(key, dict(
            initial=key in initial_rows, final=key in initial_rows, operations=0,
        ))
        action = operation["operation"]
        if action not in ("add", "withdraw"):
            raise ValueError(f"Unknown VRP operation: {action}")
        present = action == "add"
        if entry["final"] == present:
            raise ValueError(f"Illegal VRP {action} for current presence: {key}")
        entry["final"] = present
        entry["operations"] += 1
    required = sum(row["initial"] != row["final"] for row in entries.values())
    repeated = [row for row in entries.values() if row["operations"] > 1]
    counts = dict(
        original_operations=len(operations), distinct_vrps=len(entries),
        repeated_vrps=len(repeated),
        operations_on_repeated_vrps=sum(row["operations"] for row in repeated),
        cancelled_operations=len(operations) - required,
        net_zero_vrps=sum(row["initial"] == row["final"] for row in entries.values()),
        required_net_changes=required,
    )
    return dict(entries=entries, counts=counts)


# Check each observed action against the tuple's legal alternating presence history.
def accept_vrp_event(event, expectation, observed, batch):
    key = normalize(event["prefix"], event["max_length"], event["asn"])
    entry = expectation["entries"].get(key)
    if entry is None:
        raise RuntimeError(f"Batch {batch} produced an unexpected VRP event: {event}")
    actions = observed.setdefault(key, [])
    if len(actions) >= entry["operations"]:
        raise RuntimeError(f"Batch {batch} produced an extra VRP event: {event}")
    present = entry["initial"] ^ bool(len(actions) % 2)
    wanted = "removed" if present else "added"
    if event["action"] != wanted:
        raise RuntimeError(
            f"Batch {batch} produced the wrong VRP action for {key}: "
            f"expected {wanted}, observed {event['action']}"
        )
    actions.append(event["action"])


# Require every observed tuple history to reach its final state, including net changes.
def pending_vrp_changes(expectation, observed):
    return [
        key for key, entry in expectation["entries"].items()
        if (entry["initial"] ^ bool(len(observed.get(key, ())) % 2)) != entry["final"]
    ]


# Query exact ROA identities for repeated tuples; only the absent-network error is absence.
def check_repeated_vrp_states(expectation, socket_path, timeout, stage):
    repeated = {
        key: row for key, row in expectation["entries"].items() if row["operations"] > 1
    }
    if not repeated:
        return
    connection = BirdConnection(socket_path, timeout)
    try:
        connection.command("restrict")
        for (prefix, maximum, asn), row in repeated.items():
            afi = ip.ip_network(prefix).version
            command = f"show route table r{afi} {prefix} max {maximum} as {asn} count"
            try:
                connection.command(command)
                present = True
            except BirdError as error:
                if error.code != 8001 or str(error) != "BIRD 8001: Network not found":
                    raise
                present = False
            if present != row[stage]:
                raise RuntimeError(
                    f"{stage.capitalize()} VRP presence mismatch for "
                    f"{prefix} max {maximum} AS{asn}: "
                    f"expected {row[stage]}, observed {present}"
                )
    finally:
        connection.close()


# Report actual events separately from required net changes and cancelled operations.
def vrp_observation_summary(expectation, observed):
    if pending_vrp_changes(expectation, observed):
        raise RuntimeError("Cannot summarize incomplete VRP observations")
    required = expectation["counts"]["required_net_changes"]
    total = sum(map(len, observed.values()))
    return dict(
        vrp_expectation=expectation["counts"],
        verified_vrp_events=total,
        verified_net_vrp_changes=required,
        observed_intermediate_vrp_events=total - required,
        repeated_vrp_final_states_verified=expectation["counts"]["repeated_vrps"],
        repeated_vrp_observations=[
            dict(vrp=list(key), initial_present=row["initial"], final_present=row["final"],
                 observed_actions=observed.get(key, []))
            for key, row in expectation["entries"].items() if row["operations"] > 1
        ],
    )


# Check that the live initial candidate states match the offline prediction.
def check_initial_states(changes, initial):
    for change in changes:
        actual = initial[change["prefix"]].get(change["peer"], {}).get("rov")
        if actual != change["before"]:
            raise RuntimeError(
                f"Initial ROV mismatch for {change['prefix']} {change['peer']}: "
                f"expected {change['before']}, observed {actual}"
            )


# Check exact touched VRP identities and final query states after collection ends.
def verify_final(socket_path, timeout, expectation, rov_expected):
    result = dict(vrp_checked=0, rov_checked=0, mismatches=[], errors=[])
    connection = None
    try:
        connection = BirdConnection(socket_path, timeout)
        connection.command("restrict")
        for (prefix, maximum, asn), row in expectation["entries"].items():
            afi = ip.ip_network(prefix).version
            try:
                connection.command(f"show route table r{afi} {prefix} max {maximum} as {asn} count")
                present = True
            except BirdError as error:
                if error.code != 8001 or str(error) != "BIRD 8001: Network not found":
                    raise
                present = False
            result["vrp_checked"] += 1
            if present != row["final"]:
                result["mismatches"].append(dict(kind="vrp", vrp=[prefix, maximum, asn],
                                                expected=row["final"], actual=present))
        for (prefix, origin), expected in rov_expected.items():
            actual, _, _ = query_roa(connection, prefix, origin)
            result["rov_checked"] += 1
            if actual != expected:
                result["mismatches"].append(dict(kind="roa_query", prefix=prefix, origin_as=origin,
                                                expected=expected, actual=actual))
    except (OSError, ValueError, RuntimeError) as error:
        result["errors"].append(f"{type(error).__name__}: {error}")
    finally:
        if connection is not None:
            connection.close()
    result["correct"] = not result["mismatches"] and not result["errors"]
    result["scope"] = "Touched VRP identities and selected prefix/origin queries; not every RIB entry"
    return result
