"""Describe and verify experiment 6.2 net VRP changes after collection."""

from collections import Counter

from rov.measure.vrp_state import pending_vrp_changes


# Describe input-derived net changes and cancelled operations in saved results.
def monitoring_record(expectation):
    entries = expectation["entries"]
    prefix_operations = Counter()
    required = []
    excluded = []
    for key, row in entries.items():
        prefix_operations[key[0]] += row["operations"]
        changed = row["initial"] != row["final"]
        action = ("added" if row["final"] else "removed") if changed else None
        if changed:
            required.append(dict(vrp=list(key), action=action))
        if row["operations"] > int(changed):
            excluded.append(dict(
                vrp=list(key), original_operations=row["operations"],
                excluded_operations=row["operations"] - int(changed),
                initial_present=row["initial"], final_present=row["final"],
                required_action=action,
            ))
    repeated_prefixes = sorted(p for p, count in prefix_operations.items() if count > 1)
    excluded_prefixes = sorted({row["vrp"][0] for row in excluded})
    return dict(
        format="vrp-net-monitoring-v1",
        entries=[dict(vrp=list(key), **row) for key, row in entries.items()],
        counts=expectation["counts"], required_changes=required,
        repeated_operation_prefixes=repeated_prefixes,
        repeated_operation_prefix_count=len(repeated_prefixes),
        excluded_prefixes=excluded_prefixes, excluded_prefix_count=len(excluded_prefixes),
        excluded_vrps=excluded,
    )


# Require only identities with a net change, retaining all observed intermediate actions.
def pending_net_changes(expectation, observed):
    required = {key: row for key, row in expectation["entries"].items()
                if row["initial"] != row["final"]}
    return pending_vrp_changes(dict(entries=required), observed)


# Report raw observations separately from required net changes and final-state checks.
def net_observation_summary(expectation, observed):
    if pending_net_changes(expectation, observed):
        raise RuntimeError("Cannot summarize incomplete net VRP observations")
    required = expectation["counts"]["required_net_changes"]
    total = sum(map(len, observed.values()))
    return dict(
        vrp_expectation=expectation["counts"],
        verified_vrp_events=total, verified_net_vrp_changes=required,
        observed_intermediate_vrp_events=total - required,
        repeated_vrp_final_states_verified=expectation["counts"]["repeated_vrps"],
        repeated_vrp_observations=[
            dict(vrp=list(key), initial_present=row["initial"], final_present=row["final"],
                 observed_actions=observed.get(key, []))
            for key, row in expectation["entries"].items() if row["operations"] > 1
        ],
    )
