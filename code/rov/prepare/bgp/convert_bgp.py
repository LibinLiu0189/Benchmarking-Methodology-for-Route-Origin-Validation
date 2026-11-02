"""Convert MRT to per-peer BGP RAW with peer next hops and bounded BIRD AS_PATH text."""

from pathlib import Path

import argparse
from collections import Counter
from contextlib import ExitStack
import ipaddress as ip
import json
import struct
import tempfile

from rov.data.bgp import attributes
from rov.data.mrt import mrt_records
from rov.prepare.bgp.path_limit import MAX_AS_PATH_TEXT_BYTES
from rov.prepare.bgp.path_limit import filter_report
from rov.prepare.bgp.path_limit import path_text_length


# Encode one BGP attribute with its flags, type, length, and payload.
def attribute(kind, data, flags=0x80):
    return (
        bytes([flags | (16 if len(data) > 255 else 0), kind])
        + len(data).to_bytes(2 if len(data) > 255 else 1, "big")
        + data
    )


# Wrap a payload in a BGP UPDATE header and enforce the 4096-byte limit.
def message(body):
    if len(body) + 19 > 4096:
        raise ValueError(
            "Encoded UPDATE exceeds 4096 bytes; extended messages are not enabled"
        )
    return b"\xff" * 16 + struct.pack("!HB", len(body) + 19, 2) + body


# Encode one snapshot candidate as an UPDATE while preserving its next hop.
def encode(prefix, raw):
    net = ip.ip_network(prefix)
    nlri = (
        bytes([net.prefixlen]) + net.network_address.packed[: (net.prefixlen + 7) // 8]
    )
    if net.version == 4:
        attrs = raw
        tail = nlri
    else:
        parts = []
        found = False
        for kind, start, payload, end in attributes(raw):
            if kind != 14:
                parts.append(raw[start:end])
                continue
            # TABLE_DUMP_V2 stores only the next-hop length and address here.
            value = raw[payload:end]
            if not value or value[0] not in (16, 32) or len(value) != 1 + value[0]:
                raise ValueError("Invalid TABLE_DUMP_V2 IPv6 MP_REACH next hop")
            parts.append(
                attribute(14, b"\0\x02\x01" + value + b"\0" + nlri, raw[start] & ~16)
            )
            found = True
        if not found:
            raise ValueError("Missing IPv6 MP_REACH next hop")
        attrs = b"".join(parts)
        tail = b""
    return message(b"\0\0" + len(attrs).to_bytes(2, "big") + attrs + tail)


# Compare the snapshot next hop to the announcing peer without rewriting attributes.
def next_hop_matches_peer(raw, afi, peer_address):
    peer = ip.ip_address(peer_address)
    for kind, _, payload, end in attributes(raw):
        value = raw[payload:end]
        if afi == 4 and kind == 3:
            return ip.IPv4Address(value) == peer
        if afi == 6 and kind == 14:
            if not value or value[0] not in (16, 32) or len(value) != value[0] + 1:
                raise ValueError("Invalid TABLE_DUMP_V2 IPv6 MP_REACH next hop")
            return ip.IPv6Address(value[1:17]) == peer
    raise ValueError("Missing snapshot next hop")


# Filter non-peer next hops, encode retained candidates, and write peer metadata.
def convert(source, output):
    source = Path(source).resolve()
    output = Path(output).resolve()
    if output.exists():
        raise ValueError(
            f"Output directory already exists: {output}; specify a new directory"
        )
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=output.parent, prefix=".bgp-") as tmp:
        stage = Path(tmp) / "result"
        stage.mkdir()
        peers = []
        total = 0
        discarded = 0
        dropped = Counter()
        affected = {4: set(), 6: set()}
        retained = {4: set(), 6: set()}
        with ExitStack() as stack:
            files = {}
            for record in mrt_records(source):
                if record[0] == "peers":
                    _, timestamp, peers = record
                    for p in peers:
                        p.update(
                            counts={"4": 0, "6": 0},
                            file=f'{p["name"]}.bgp',
                        )
                    continue
                _, index, prefix, afi, raw = record
                p = peers[index]
                if not next_hop_matches_peer(raw, afi, p["source_address"]):
                    discarded += 1
                    continue
                if path_text_length(raw) > MAX_AS_PATH_TEXT_BYTES:
                    dropped[afi] += 1
                    affected[afi].add(prefix)
                    continue
                retained[afi].add(prefix)
                if index not in files:
                    files[index] = stack.enter_context((stage / p["file"]).open("wb"))
                files[index].write(encode(prefix, raw))
                p["counts"][str(afi)] += 1
                total += 1
                if total % 250000 == 0:
                    print(f"Converted {total:,} BGP candidates", flush=True)
        active = [p for p in peers if sum(p["counts"].values())]
        metadata = dict(
            format="bng-raw-v1",
            source=str(source),
            snapshot_timestamp=timestamp,
            peers=active,
            counts={str(a): sum(p["counts"][str(a)] for p in active) for a in (4, 6)},
            next_hop_rewritten=False,
            as_path_preserved=True,
            as_path_text_filter=filter_report(dropped, affected, retained),
            discarded_non_peer_next_hops=discarded,
        )
        (stage / "bgp.json").write_text(
            json.dumps(metadata, ensure_ascii=False, indent=2) + "\n"
        )
        stage.rename(output)
    print("AS_PATH filter: " + json.dumps(metadata["as_path_text_filter"]))
    print(f"Discarded {discarded:,} announcements with non-peer next hops")
    print(
        f'BGP conversion complete: {output}; IPv4={metadata["counts"]["4"]:,}, IPv6={metadata["counts"]["6"]:,}'
    )


# Parse conversion arguments and invoke the snapshot converter.
def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--input", required=True, type=Path)
    p.add_argument("--output", required=True, type=Path)
    a = p.parse_args()
    convert(a.input, a.output)


if __name__ == "__main__":
    main()
