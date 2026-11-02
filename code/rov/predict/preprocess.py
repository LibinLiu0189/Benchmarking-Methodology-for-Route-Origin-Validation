"""Prepare candidate groups and indexes, and write offline prediction results."""

from pathlib import Path
from bisect import bisect_left
from bisect import bisect_right
from collections import defaultdict
import ipaddress as ip
import json
import os
import tempfile
import time

from rov.data.bgp import load_bgp
from rov.data.bgp import read_raw
from rov.data.sequence import load_sequence
from rov.data.vrp import load_vrp_text


class VRPs:
    # Build a VRP set and prefix index from the supplied initial authorizations.
    def __init__(self, rows, progress=None):
        self.rows = set()
        self.tables = {4: defaultdict(dict), 6: defaultdict(dict)}
        for count, row in enumerate(rows, 1):
            self.edit(row, True)
            if progress is not None and count % 10000 == 0:
                progress(f"Initial VRP index: {count:,}/{len(rows):,}")

    # Add or withdraw one VRP while keeping the set and prefix index consistent.
    def edit(self, row, add):
        row = tuple(row)
        net = ip.ip_network(row[0])
        key = int(net.network_address)
        if (row in self.rows) == add:
            raise ValueError(f"Update is inconsistent with the current VRP set: {row}")
        branch = self.tables[net.version][net.prefixlen]
        if add:
            self.rows.add(row)
            branch.setdefault(key, set()).add(row[1:])
        else:
            self.rows.remove(row)
            branch[key].remove(row[1:])
            if not branch[key]:
                del branch[key]

    # Evaluate a route prefix and origin AS against all covering VRPs,
    # returning its validation state and the reason for that state.
    def state(self, net, origin):
        cover = []
        value = int(net.network_address)
        bits = net.max_prefixlen
        for length, table in self.tables[net.version].items():
            if length <= net.prefixlen:
                cover.extend(table.get(value >> (bits - length) << (bits - length), ()))
        if not cover:
            return "NotFound", "no_covering_vrp"
        if origin and any(
            asn == origin and net.prefixlen <= maximum for maximum, asn in cover
        ):
            return "Valid", "authorized"
        return "Invalid", (
            "prefix_length"
            if origin and any(asn == origin for _, asn in cover)
            else "origin_as"
        )

    # Apply all additions and withdrawals in one VRP batch in their given order.
    def apply(self, batch):
        for event in batch:
            if event["operation"] not in ("add", "withdraw"):
                raise ValueError("Unknown VRP operation")
            self.edit(event["vrp"], event["operation"] == "add")


class Prefixes:
    # Index BGP prefixes by address family and network address for coverage searches.
    def __init__(self, prefixes):
        self.rows = {4: [], 6: []}
        for prefix in prefixes:
            n = ip.ip_network(prefix)
            self.rows[n.version].append((int(n.network_address), n.prefixlen, prefix))
        for rows in self.rows.values():
            rows.sort()
        self.starts = {a: [r[0] for r in rows] for a, rows in self.rows.items()}

    # Check whether this network contains at least one indexed BGP prefix.
    def has_covered(self, prefix):
        network = ip.ip_network(prefix)
        rows = self.rows[network.version]
        starts = self.starts[network.version]
        first = bisect_left(starts, int(network.network_address))
        last = bisect_right(starts, int(network.broadcast_address))
        return any(
            rows[position][1] >= network.prefixlen
            for position in range(first, last)
        )

    # Find BGP prefixes contained in the VRP prefixes changed by a batch;
    # do not exclude longer prefixes based on VRP maxLength.
    def covered(self, batch):
        result = set()
        for prefix in {e["vrp"][0] for e in batch}:
            n = ip.ip_network(prefix)
            a = n.version
            lo = bisect_left(self.starts[a], int(n.network_address))
            hi = bisect_right(self.starts[a], int(n.broadcast_address))
            result.update(
                p for _, length, p in self.rows[a][lo:hi] if length >= n.prefixlen
            )
        return sorted(
            result,
            key=lambda p: (
                ip.ip_network(p).version,
                int(ip.ip_network(p).network_address),
                ip.ip_network(p).prefixlen,
            ),
        )


# Load the VRP sequence and BGP candidates using the caller's attribute decoder,
# then build the VRP and prefix indexes.
def inputs(bgp, vrp, sequence, decode_route, retain_all_candidates=False, progress=None):
    meta = load_bgp(bgp)
    _, metadata, operations = load_sequence(sequence)
    batches = [(row, operations[row["number"]]) for row in metadata]
    covers = {4: defaultdict(set), 6: defaultdict(set)}
    for _, batch in batches:
        for e in batch:
            n = ip.ip_network(e["vrp"][0])
            covers[n.version][n.prefixlen].add(int(n.network_address))
    groups = defaultdict(list)
    peers = {p["name"]: p for p in meta["peers"]}
    if retain_all_candidates:
        message = "Reading all local BGP candidates."
    else:
        message = (
            "Reading local BGP data and collecting all candidates covered by "
            "updated VRPs."
        )
    print(message, flush=True)
    candidate_count = 0
    for peer_number, peer in enumerate(meta["peers"], 1):
        for prefix, afi, raw in read_raw(Path(bgp) / peer["file"]):
            candidate_count += 1
            if progress is not None and candidate_count % 10000 == 0:
                progress(
                    f"BGP candidates read: {candidate_count:,}; "
                    f"peers {peer_number}/{len(meta['peers'])}"
                )
            net = ip.ip_network(prefix)
            value = int(net.network_address)
            bits = net.max_prefixlen
            covered = any(
                length <= net.prefixlen
                and value >> (bits - length) << (bits - length) in addrs
                for length, addrs in covers[afi].items()
            )
            if retain_all_candidates or covered:
                groups[prefix].append(dict(peer=peer["name"], **decode_route(raw)))
        if progress is not None:
            progress(
                f"BGP candidates read: {candidate_count:,}; "
                f"peers {peer_number}/{len(meta['peers'])}"
            )

    if progress is not None:
        progress("Loading initial VRPs.", True)
    rows = load_vrp_text(vrp)
    if progress is not None:
        progress(f"Building initial VRP index for {len(rows):,} entries.", True)
    index = VRPs(rows, progress)
    if progress is not None:
        progress("Initial VRP index complete.", True)
        progress("Building BGP prefix index.", True)
    return batches, groups, peers, index, Prefixes(groups)


# Write prediction records as JSON Lines and replace the output only after
# generation succeeds, without overwriting input data.
def write_results(output, records, protected=(), progress=None):
    output = Path(output).resolve()
    for source in protected:
        source = Path(source).resolve()
        if output == source or (source.is_dir() and source in output.parents):
            raise ValueError(
                "Results must not overwrite inputs or be written inside an input data directory"
            )
    output.parent.mkdir(parents=True, exist_ok=True)
    fd, temp = tempfile.mkstemp(dir=output.parent, prefix=".prediction-")
    temp = Path(temp)
    try:
        with os.fdopen(fd, "w") as stream:
            if progress is not None:
                progress("opened", temp)
            for row in records:
                if progress is not None:
                    progress("writing", row)
                    write_started = time.monotonic()
                stream.write(
                    json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n"
                )
                if progress is not None:
                    progress("written", (row, time.monotonic() - write_started))
        os.replace(temp, output)
        if progress is not None:
            progress("published", output)
    finally:
        temp.unlink(missing_ok=True)
    print(f"Results saved: {output}")
