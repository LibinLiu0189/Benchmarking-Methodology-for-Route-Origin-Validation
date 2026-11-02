"""Select VRP identities operated on once over the complete churn workload."""

from collections import Counter


# Exclude every operation on an identity repeated anywhere in the input sequence.
def select_vrp_operations(operations):
    occurrences = Counter(
        tuple(operation["vrp"])
        for rows in operations.values()
        for operation in rows
    )
    excluded = {identity for identity, count in occurrences.items() if count > 1}
    retained = {}
    counts = {}
    for number, rows in operations.items():
        retained[number] = [
            operation for operation in rows if tuple(operation["vrp"]) not in excluded
        ]
        counts[number] = dict(
            original=len(rows), retained=len(retained[number]),
            repeated=len(rows) - len(retained[number]),
        )
    selection = dict(
        totals={field: sum(row[field] for row in counts.values())
                for field in ("original", "retained", "repeated")},
        by_batch=counts,
    )
    return retained, excluded, selection
