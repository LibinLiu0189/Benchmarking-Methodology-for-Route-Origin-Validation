"""Read converted BGP metadata, RAW announcements, and attribute boundaries."""

from pathlib import Path
import ipaddress as ip
import json
import struct


# Locate each attribute in a BGP attribute block and validate its boundaries.
def attributes(raw):
    pos, seen = 0, set()
    while pos < len(raw):
        start = pos
        if pos + 3 > len(raw):
            raise ValueError("Truncated BGP attribute header")
        flags, kind = raw[pos : pos + 2]
        pos += 2
        width = 2 if flags & 16 else 1
        if pos + width > len(raw):
            raise ValueError("Truncated BGP attribute length")
        size = int.from_bytes(raw[pos : pos + width], "big")
        pos += width
        end = pos + size
        if end > len(raw) or kind in seen:
            raise ValueError("Truncated or duplicate BGP attribute")
        seen.add(kind)
        yield kind, start, pos, end
        pos = end


# Stream individual route UPDATEs from a RAW file; skip end-of-RIB markers.
def read_raw(path):
    """Yield (prefix, AFI, attributes); do not yield EOR as a candidate."""
    with Path(path).open("rb") as stream:
        while header := stream.read(19):
            if len(header) != 19 or header[:16] != b"\xff" * 16:
                raise ValueError(f"{path}: Truncated BGP header")
            size, kind = struct.unpack("!HB", header[16:])
            if kind != 2 or not 23 <= size <= 4096:
                raise ValueError("Expected a standard-length UPDATE")
            body = stream.read(size - 19)
            if len(body) != size - 19:
                raise ValueError("Truncated UPDATE")
            withdrawn, alen = struct.unpack("!HH", body[:4])
            if withdrawn or 4 + alen > len(body):
                raise ValueError("Baseline RAW must not contain withdrawals")
            raw = body[4 : 4 + alen]
            nlri = body[4 + alen :]
            afi = 4
            for k, _, a, b in attributes(raw):
                if k == 14:
                    mp = raw[a:b]
                    if (
                        len(mp) < 5
                        or mp[:3] != b"\0\x02\x01"
                        or mp[3] not in (16, 32)
                        or len(mp) < 5 + mp[3]
                    ):
                        raise ValueError("Unsupported or truncated MP_REACH")
                    if nlri:
                        raise ValueError("Mixed address families in a single UPDATE")
                    afi = 6
                    nlri = mp[5 + mp[3] :]
            if not nlri:
                continue
            plen = nlri[0]
            width = (plen + 7) // 8
            if plen > (32 if afi == 4 else 128) or len(nlri) != 1 + width:
                raise ValueError("Expected one prefix per UPDATE")
            addr = (ip.IPv4Address if afi == 4 else ip.IPv6Address)(
                nlri[1:] + b"\0" * ((4 if afi == 4 else 16) - width)
            )
            yield str(ip.ip_network((addr, plen))), afi, raw


# Read the BGP directory metadata and check its declared format.
def load_bgp(folder):
    folder = Path(folder)
    metadata = json.loads((folder / "bgp.json").read_text())
    if metadata.get("format") != "bng-raw-v1":
        raise ValueError("Not a BGP RAW data directory produced by this project")
    return metadata
