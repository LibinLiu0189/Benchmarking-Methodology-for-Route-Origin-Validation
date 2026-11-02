"""Predict affected selected paths for the existing fixed ROV policy."""

from collections import ChainMap
import ipaddress as ip
import json
import random
from pathlib import Path

from rov.predict.preprocess import inputs
from rov.predict.path import choose
from rov.predict.path import path_change
from rov.predict.selection import path_info


# Recompute only prefixes covered by the changed VRPs, retaining the complete baseline.
def predict_paths(bgp, vrp, sequence, output):
    policy = dict(rov="on", dut_asn=65000, as_sets="reject")
    if sequence is not None:
        random.seed(json.loads((Path(sequence) / "sequence.json").read_text())["seed"])
    batches, groups, peers, index, prefixes = inputs(bgp, vrp, sequence, path_info, retain_all_candidates=True)
    current = {prefix: choose(prefix, routes, peers, index, policy) for prefix, routes in groups.items()}
    with Path(output).open("x") as stream:
        for info, operations in batches:
            affected = prefixes.covered(operations)
            previous = {prefix: current[prefix] for prefix in affected}
            index.apply(operations)
            current.update({prefix: choose(prefix, groups[prefix], peers, index, policy) for prefix in affected})
            before_table = ChainMap(previous, current)
            changes, unresolved = [], []
            for prefix in sorted(affected):
                before, after = previous[prefix], current[prefix]
                if before["status"] != "known" or after["status"] != "known":
                    if before != after:
                        unresolved.append(dict(prefix=prefix, before=before, after=after))
                elif before["path"] != after["path"]:
                    # Ancestors and descendants are the complete relevant set for this probe.
                    related = set(prefixes.covered([dict(vrp=(prefix, 0, 0))]))
                    ancestor = ip.ip_network(prefix)
                    while True:
                        if str(ancestor) in current:
                            related.add(str(ancestor))
                        if not ancestor.prefixlen:
                            break
                        ancestor = ancestor.supernet()
                    local_before = {key: before_table[key] for key in related}
                    local_after = {key: current[key] for key in related}
                    changes.append(path_change(prefix, before["path"], after["path"], local_before, local_after))
            row = dict(batch=info["number"], vrp_operations=len(operations), policy=policy,
                       prefixes_recalculated=len(affected), changes=changes, unresolved=unresolved)
            stream.write(json.dumps(row) + "\n")
            stream.flush()
            print(f"Path batch {info['number']}: {len(affected)} affected prefixes; {len(changes)} changes; {len(unresolved)} unresolved", flush=True)
