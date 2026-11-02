"""Read MRT TABLE_DUMP_V2 blocks, peers, and candidate records."""

from pathlib import Path
import bz2
import gzip
import ipaddress as ip
import struct


# Read MRT blocks and locate candidate entries without decoding path attributes.
def mrt_blocks(source):
    source = Path(source)
    opener = (
        gzip.open
        if source.suffix == ".gz"
        else (bz2.open if source.suffix == ".bz2" else open)
    )
    peer_count = None
    with opener(source, "rb") as stream:
        while header := stream.read(12):
            if len(header) != 12:
                raise ValueError("Truncated MRT header")
            timestamp, kind, sub, length = struct.unpack("!IHHI", header)
            if kind != 13 or length > 64 * 1024 * 1024:
                raise ValueError(
                    "Only TABLE_DUMP_V2 RIB is supported, not UPDATE/ADD-PATH"
                )
            body = stream.read(length)
            if len(body) != length:
                raise ValueError("Truncated MRT record")
            if sub == 1:
                if peer_count is not None:
                    raise ValueError("Multiple peer index tables in one snapshot")
                # Read only the peer count here; full peer decoding is performed by read_peers.
                if len(body) < 8:
                    raise ValueError("Truncated peer index table")
                count_offset = 6 + int.from_bytes(body[4:6], "big")
                if count_offset + 2 > len(body):
                    raise ValueError("Truncated peer index table view name")
                peer_count = int.from_bytes(
                    body[count_offset : count_offset + 2], "big"
                )
                yield timestamp, sub, body, ()
                continue
            if peer_count is None or sub not in (2, 4) or len(body) < 7:
                raise ValueError(f"Unsupported MRT subtype {sub}")
            plen = body[4]
            width = (plen + 7) // 8
            if plen > (32 if sub == 2 else 128) or len(body) < 7 + width:
                raise ValueError("Invalid MRT prefix")
            count = int.from_bytes(body[5 + width : 7 + width], "big")
            pos = 7 + width
            entries = []
            seen = set()
            for _ in range(count):
                start = pos
                if pos + 8 > len(body):
                    raise ValueError("Truncated MRT candidate")
                index, originated, size = struct.unpack_from("!HIH", body, pos)
                pos += 8 + size
                if index >= peer_count or index in seen or pos > len(body):
                    raise ValueError("Duplicate or out-of-range MRT candidate index")
                seen.add(index)
                entries.append((index, start, pos))
            if pos != len(body):
                raise ValueError("Unparsed data in MRT record")
            yield timestamp, sub, body, entries
    if peer_count is None:
        raise ValueError("No peer index table in snapshot")


# Parse the MRT peer index table into peer identities, ASNs, and addresses.
def read_peers(body):
    if len(body) < 8:
        raise ValueError("Truncated peer index table")
    pos = 6 + int.from_bytes(body[4:6], "big")
    if pos + 2 > len(body):
        raise ValueError("Truncated peer index table view name")
    count = int.from_bytes(body[pos : pos + 2], "big")
    pos += 2
    peers = []
    for index in range(count):
        if pos + 5 > len(body):
            raise ValueError("Truncated peer entry")
        flags = body[pos]
        router = str(ip.IPv4Address(body[pos + 1 : pos + 5]))
        pos += 5
        if flags & ~3:
            raise ValueError("Unknown peer flags")
        width = 16 if flags & 1 else 4
        aswidth = 4 if flags & 2 else 2
        if pos + width + aswidth > len(body):
            raise ValueError("Truncated peer address/ASN")
        address = str(ip.ip_address(body[pos : pos + width]))
        pos += width
        asn = int.from_bytes(body[pos : pos + aswidth], "big")
        pos += aswidth
        peers.append(
            dict(
                name=f"peer_{index:03}",
                index=index,
                asn=asn,
                router_id=router,
                source_address=address,
            )
        )
    if pos != len(body):
        raise ValueError("Unparsed data in peer index table")
    return peers


# Decode snapshot prefixes and expose the original candidate attributes.
def mrt_records(source):
    for timestamp, sub, body, entries in mrt_blocks(source):
        if sub == 1:
            yield "peers", timestamp, read_peers(body)
            continue
        afi = 4 if sub == 2 else 6
        plen = body[4]
        width = (plen + 7) // 8
        address = (ip.IPv4Address if afi == 4 else ip.IPv6Address)(
            body[5 : 5 + width] + b"\0" * ((4 if afi == 4 else 16) - width)
        )
        prefix = str(ip.ip_network((address, plen), strict=True))
        for index, start, end in entries:
            yield "route", index, prefix, afi, body[start + 8 : end]
