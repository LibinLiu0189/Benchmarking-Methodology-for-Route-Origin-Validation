"""Decode incoming MRT BGP4MP updates for a non-ADD-PATH baseline."""

from contextlib import ExitStack
from contextlib import closing
import ipaddress as ip
import json
from pathlib import Path
import struct

from .bgp import attributes


# Load the ordered metadata for prepared BGP update batches.
def load_update_sequence(directory):
    directory = Path(directory)
    manifest = json.loads((directory / "sequence.json").read_text())
    if manifest.get("format") != "bgp-update-batches-v1" or manifest.get("afi") not in (
        4, 6, [4, 6]
    ):
        raise ValueError("Expected a bgp-update-batches-v1 sequence")
    return manifest["batches"]


# Read prepared RAW operations in wire order, applying withdrawals first.
def read_raw_updates(path):
    with Path(path).open("rb") as stream:
        while header := stream.read(19):
            if len(header) != 19 or header[:16] != b"\xff" * 16:
                raise ValueError(f"{path}: truncated or invalid BGP header")
            size, kind = struct.unpack("!HB", header[16:])
            if kind != 2 or not 23 <= size <= 4096:
                raise ValueError("Expected a standard-length BGP UPDATE")
            body = stream.read(size - 19)
            if len(body) != size - 19:
                raise ValueError("Truncated BGP UPDATE")
            withdrawn, announced = decode_update(body)
            for prefix in withdrawn:
                yield "withdraw", prefix, None
            for prefix, attributes in announced:
                yield "announce", prefix, attributes


# Interleave peer RAW streams in the original operation order recorded in events.jsonl.
def read_batch_updates(directory, batch, events):
    directory = Path(directory)
    with ExitStack() as stack:
        streams = {
            source["peer"]: stack.enter_context(
                closing(read_raw_updates(directory / source["file"]))
            )
            for source in batch["peers"]
        }
        total = batch["counts"]["announce"] + batch["counts"]["withdraw"]
        for _ in range(total):
            line = next(events, None)
            if line is None:
                raise ValueError(f"Missing BGP events for batch {batch['number']}")
            event = json.loads(line)
            peer = event["peer"]
            if event["batch"] != batch["number"] or peer not in streams:
                raise ValueError("BGP event does not match the current batch or peer")
            update = next(streams[peer], None)
            if update is None or update[:2] != (event["operation"], event["prefix"]):
                raise ValueError("BGP event does not match its RAW operation")
            operation, prefix, raw = update
            yield peer, operation, prefix, raw
        if any(next(stream, None) is not None for stream in streams.values()):
            raise ValueError(f"Unlisted RAW operations in batch {batch['number']}")


# Decode unicast NLRIs into canonical prefixes, ignoring unused trailing bits.
def prefixes(raw, afi=4):
    offset = 0
    address_type = ip.IPv4Address if afi == 4 else ip.IPv6Address
    address_size = 4 if afi == 4 else 16
    while offset < len(raw):
        length = raw[offset]
        width = (length + 7) // 8
        offset += 1
        if length > address_size * 8 or offset + width > len(raw):
            raise ValueError("Invalid or truncated NLRI")
        address = address_type(raw[offset:offset + width] + b"\0" * (address_size - width))
        yield str(ip.ip_network((address, length), strict=False))
        offset += width


# Encode a path attribute after changing its value while retaining its flags.
def attribute(flags, kind, value):
    width = 2 if len(value) > 255 else 1
    return (bytes([flags & ~16 | (16 if width == 2 else 0), kind])
            + len(value).to_bytes(width, "big") + value)


# Separate withdrawals and per-prefix announcements from both unicast families.
def decode_update(body):
    if len(body) < 4:
        raise ValueError("Truncated BGP UPDATE")
    withdrawn_size = int.from_bytes(body[:2], "big")
    offset = 2 + withdrawn_size
    if offset + 2 > len(body):
        raise ValueError("Truncated withdrawn routes")
    attr_size = int.from_bytes(body[offset:offset + 2], "big")
    attr_end = offset + 2 + attr_size
    if attr_end > len(body):
        raise ValueError("Truncated path attributes")
    raw = body[offset + 2:attr_end]
    withdrawn = list(prefixes(body[2:offset], 4))
    conventional = list(prefixes(body[attr_end:], 4))
    parts = []
    mp_next_hop = None
    has_next_hop = False
    mp_announcements = []
    for kind, start, payload, end in attributes(raw):
        value = raw[payload:end]
        if kind not in (14, 15):
            parts.append(raw[start:end])
            has_next_hop |= kind == 3
            continue
        if len(value) < 3:
            raise ValueError("Truncated multiprotocol attribute")
        afi = int.from_bytes(value[:2], "big")
        if value[2] != 1 or afi not in (1, 2):
            continue
        family = 4 if afi == 1 else 6
        if kind == 15:
            withdrawn.extend(prefixes(value[3:], family))
        else:
            if len(value) < 5:
                raise ValueError("Truncated MP_REACH next hop")
            hop_length = value[3]
            position = 4 + hop_length
            if position >= len(value):
                raise ValueError("Truncated MP_REACH next hop")
            snpa_count = value[position]
            position += 1
            for _ in range(snpa_count):
                if position >= len(value):
                    raise ValueError("Truncated MP_REACH SNPA")
                snpa_length = value[position]
                position += 1 + (snpa_length + 7) // 8
                if position > len(value):
                    raise ValueError("Truncated MP_REACH SNPA")
            if family == 4:
                if hop_length != 4:
                    raise ValueError("Only IPv4 next hops for IPv4 MP_REACH are supported")
                mp_next_hop = value[4:8]
                conventional.extend(prefixes(value[position:], 4))
            else:
                if hop_length not in (16, 32):
                    raise ValueError("Invalid IPv6 MP_REACH next hop length")
                for prefix in prefixes(value[position:], 6):
                    network = ip.ip_network(prefix)
                    nlri = (bytes([network.prefixlen]) + network.network_address.packed[
                        :(network.prefixlen + 7) // 8
                    ])
                    reach = attribute(raw[start], 14, value[:position] + nlri)
                    mp_announcements.append((prefix, reach))
    if mp_next_hop is not None:
        if has_next_hop:
            raise ValueError("Mixed conventional and MP IPv4 announcements are unsupported")
        parts.append(b"\x40\x03\x04" + mp_next_hop)
    base = b"".join(parts)
    announced = [(prefix, base) for prefix in conventional]
    announced.extend((prefix, base + reach) for prefix, reach in mp_announcements)
    return withdrawn, announced
