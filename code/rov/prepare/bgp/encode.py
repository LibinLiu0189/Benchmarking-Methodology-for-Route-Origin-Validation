"""Encode the BGP changes prepared for the current experiment workflow."""

import ipaddress as ip
import struct


# Encode a single announcement or withdrawal in Bngblaster's RAW format.
def encode_update(operation, prefix, raw):
    network = ip.ip_network(prefix)
    nlri = bytes([network.prefixlen]) + network.network_address.packed[:(network.prefixlen + 7) // 8]
    if operation == "withdraw":
        if network.version == 4:
            body = len(nlri).to_bytes(2, "big") + nlri + b"\0\0"
        else:
            value = b"\0\x02\x01" + nlri
            attribute = b"\x80\x0f" + bytes([len(value)]) + value
            body = b"\0\0" + len(attribute).to_bytes(2, "big") + attribute
    else:
        tail = nlri if network.version == 4 else b""
        body = b"\0\0" + len(raw).to_bytes(2, "big") + raw + tail
    size = 19 + len(body)
    if size > 4096:
        raise ValueError("Encoded UPDATE exceeds the baseline's 4096-byte message limit")
    return b"\xff" * 16 + struct.pack("!HB", size, 2) + body
