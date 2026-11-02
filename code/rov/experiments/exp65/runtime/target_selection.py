"""Select prefixes with one predicted batch transition over the complete input."""

from collections import defaultdict

from rov.experiments.exp65.runtime.fib_selection import single_transition_batches


# Build experiment-specific targets without changing the submitted workload.
def select_targets(state):
    components = [name for name in ("rov", "rib", "fib") if name in state.monitoring_mode]
    specs = {number: {name: {} for name in components} for number in state.vrp.operations}
    counts = {
        number: {name: dict(original=0, retained=0, repeated=0, unavailable=0, unchanged=0)
                 for name in components}
        for number in specs
    }
    rov_batches = defaultdict(set)
    for number, prefix, _, _, _, _ in state.rov.transitions:
        rov_batches[prefix].add(number)
    rib_batches = defaultdict(set)
    for number, changes in state.rib.changes.items():
        for prefix, _, _ in changes:
            rib_batches[prefix].add(number)

    seen = set()
    for number, prefix, peer, origin, before, after in state.rov.transitions:
        if number not in state.monitored_batches:
            continue
        key = (prefix, origin)
        if (number, key) not in seen:
            seen.add((number, key))
            row = counts[number]["rov"]
            row["original"] += 1
            row["retained" if len(rov_batches[prefix]) == 1 else "repeated"] += 1
        if len(rov_batches[prefix]) != 1:
            continue
        spec = specs[number]["rov"].setdefault(key, dict(
            key=key, prefix=prefix, before=before, after=after, peers=[],
        ))
        if (spec["before"], spec["after"]) != (before, after):
            raise ValueError("Conflicting ROV predictions within one batch")
        spec["peers"].append(peer)

    if "rib" in components:
        for number, changes in state.rib.changes.items():
            if number not in state.monitored_batches:
                continue
            for prefix, before, after in changes:
                row = counts[number]["rib"]
                row["original"] += 1
                if len(rib_batches[prefix]) != 1:
                    row["repeated"] += 1
                    continue
                row["retained"] += 1
                specs[number]["rib"][prefix] = dict(
                    key=prefix, prefix=prefix, before=before, after=after, peers=[],
                )

    if "fib" in components:
        for number, _, _ in state.fib.unavailable:
            if number not in state.monitored_batches:
                continue
            counts[number]["fib"]["original"] += 1
            counts[number]["fib"]["unavailable"] += 1
        for prefix, changes in state.fib.changes.items():
            for number, destination, before, after in changes:
                if number not in state.monitored_batches:
                    continue
                row = counts[number]["fib"]
                row["original"] += 1
                if len(rib_batches[prefix]) != 1:
                    row["repeated"] += 1
                elif before == after:
                    row["unchanged"] += 1
                else:
                    if destination in specs[number]["fib"]:
                        raise ValueError(f"Duplicate FIB destination in batch {number}")
                    row["retained"] += 1
                    specs[number]["fib"][destination] = dict(
                        key=destination, prefix=prefix, before=before, after=after, peers=[],
                    )

        # A single exact-prefix change can still expose changing covering routes.
        forwarding_batches = single_transition_batches(
            (destination for targets in specs.values() for destination in targets["fib"]),
            state.rib.initial, state.rib.changes, state.fib.predictions_path,
        )
        for number, components_for_batch in specs.items():
            targets = components_for_batch["fib"]
            for destination in list(targets):
                if forwarding_batches.get(destination) != number:
                    del targets[destination]
                    counts[number]["fib"]["retained"] -= 1
                    counts[number]["fib"]["repeated"] += 1

    totals = {
        component: {
            field: sum(rows[component][field] for rows in counts.values())
            for field in ("original", "retained", "repeated", "unavailable", "unchanged")
        }
        for component in components
    }
    selection = dict(
        definition=(
            "ROV/RIB/FIB targets are selected only for monitored batches; "
            "single-transition eligibility is evaluated over every input batch. "
            "ROV: prefixes with net ROV changes in exactly one input batch, queried per origin ASN. "
            "RIB/FIB: prefixes with a selected-path change in exactly one input batch. "
            "FIB additionally requires an available probe, different before/after next hops, "
            "and exactly one destination longest-prefix-match next-hop change in its owning batch. "
            "FIB repeated also includes destinations without this single owning-batch change. "
            "ROV/RIB/FIB counts are monitored-batch target occurrences; exclusions are disjoint. "
            "Within-batch intermediate states are not counted as additional batch transitions."
        ),
        totals=totals,
        by_batch=[dict(batch=number, **rows) for number, rows in counts.items()],
    )
    return specs, selection
