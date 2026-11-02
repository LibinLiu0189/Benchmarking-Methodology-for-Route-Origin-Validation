"""Describe and create one isolated link per original BGP peer."""

import ipaddress as ip
import os
from pathlib import Path
import struct
import subprocess
import sys

from rov.data.bgp import attributes
from rov.data.bgp import load_bgp
from rov.data.bgp import read_raw


# Re-enter the selected module in private user, network, and mount namespaces.
def enter_namespace(arguments, module="rov.dut.initialize"):
    environment = dict(os.environ)
    environment["ROV_PARENT_NETNS"] = os.readlink("/proc/self/ns/net")
    environment["ROV_PARENT_MNTNS"] = os.readlink("/proc/self/ns/mnt")
    command = [
        "unshare", "--user", "--map-root-user", "--net", "--mount",
        "--fork", "--kill-child=SIGTERM", "--propagation", "private",
        sys.executable, "-u", "-m", module, "--inside",
        *arguments,
    ]
    os.execvpe(command[0], command, environment)


# Reject network mutations outside the namespaces created by this entry point.
def require_isolation():
    for kind, variable in (("net", "ROV_PARENT_NETNS"), ("mnt", "ROV_PARENT_MNTNS")):
        parent = os.environ.get(variable)
        if not parent or os.readlink(f"/proc/self/ns/{kind}") == parent:
            raise RuntimeError("Enter isolated namespaces before configuring the network")


# Match the configured eBGP AS-path admission rules for initial import counts.
def accepted_path(raw, settings):
    offset, accepted = 0, True
    while offset < len(raw):
        if offset + 2 > len(raw):
            raise ValueError("Truncated AS_PATH segment")
        kind, count = raw[offset:offset + 2]
        offset += 2
        if not count or kind not in (1, 2, 3, 4) or offset + count * 4 > len(raw):
            raise ValueError("Invalid AS_PATH segment")
        numbers = struct.unpack_from(f"!{count}I", raw, offset)
        offset += count * 4
        if settings["dut_asn"] in numbers or kind in (3, 4):
            accepted = False
        if kind == 1 and settings["as_sets"] == "reject":
            accepted = False
    return accepted


# Build per-peer Ethernet segments and receiving endpoints from original inputs.
def describe_network(folder, settings):
    folder = Path(folder).resolve()
    metadata = load_bgp(folder)
    peers = []
    occupied = {4: set(), 6: set()}
    for original in metadata["peers"]:
        address = ip.ip_address(original["source_address"])
        occupied[address.version].add(str(address))
    for number, original in enumerate(metadata["peers"]):
        if original["asn"] == settings["dut_asn"]:
            raise ValueError("DUT ASN must differ from snapshot peer ASNs for eBGP")
        source = str(ip.ip_address(original["source_address"]))
        transport_afi = ip.ip_address(source).version
        routes = {4: {}, 6: {}}
        raw_file = (folder / original["file"]).resolve()
        for prefix, afi, raw in read_raw(raw_file):
            values = {kind: raw[start:end] for kind, _, start, end in attributes(raw)}
            if afi == 4:
                next_hop = ip.IPv4Address(values[3])
            else:
                mp = values[14]
                next_hop = ip.IPv6Address(mp[4:20])
            if next_hop != ip.ip_address(source):
                raise ValueError(
                    f"{raw_file}: NEXT_HOP differs from peer {source}; "
                    "regenerate the input with convert_bgp"
                )
            path = values.get(2, b"")
            routes[afi][prefix] = accepted_path(path, settings)
        families = [afi for afi in (4, 6) if routes[afi]]
        if not families:
            continue
        peer = {
            "name": original["name"],
            "asn": original["asn"],
            "router_id": original["router_id"],
            "source_address": source,
            "transport_afi": transport_afi,
            "families": families,
            "raw_file": str(raw_file),
            "dut_interface": f"dut{number}",
            "import_counts": {afi: sum(routes[afi].values()) for afi in families},
            "endpoints": [],
        }
        peer["endpoints"].append({
            "address": source,
            "interface": f"rx{number}",
            "bridge_port": f"port{number}",
            "mac": "02:bb:" + ":".join(
                f"{byte:02x}" for byte in number.to_bytes(4, "big")
            ),
        })
        peers.append(peer)
    # Allocate DUT-side addresses while retaining original peer addresses.
    next_v4 = int(ip.IPv4Address("198.18.0.1"))
    next_v6 = int(ip.IPv6Address("2001:db8::1"))
    for number, peer in enumerate(peers):
        if peer["transport_afi"] == 4:
            while str(ip.IPv4Address(next_v4)) in occupied[4]:
                next_v4 += 1
            address = str(ip.IPv4Address(next_v4))
            next_v4 += 1
        else:
            while str(ip.IPv6Address(next_v6)) in occupied[6]:
                next_v6 += 1
            address = str(ip.IPv6Address(next_v6))
            next_v6 += 1
        occupied[peer["transport_afi"]].add(address)
        peer["dut_address"] = address
        peer["dut_mac"] = "02:aa:" + ":".join(f"{b:02x}" for b in number.to_bytes(4, "big"))
    return peers


# Create Ethernet segments; Bngblaster owns all receiving endpoint IP stacks.
def setup_network(peers):
    require_isolation()
    subprocess.run(
        ["mount", "-t", "tmpfs", "-o", "mode=1777", "tmpfs", "/run/lock"],
        check=True,
    )
    subprocess.run(["ip", "link", "set", "lo", "up"], check=True)
    subprocess.run(
        ["sysctl", "-qw", "net.ipv4.ip_forward=1", "net.ipv6.conf.all.forwarding=1"],
        check=True,
    )
    for peer in peers:
        bridge = peer["dut_interface"]
        subprocess.run(["ip", "link", "add", bridge, "type", "bridge", "stp_state", "0"], check=True)
        subprocess.run(["ip", "link", "set", bridge, "address", peer["dut_mac"]], check=True)
        subprocess.run(["ip", "link", "set", bridge, "up"], check=True)
        afi = peer["transport_afi"]
        if afi == 6:
            subprocess.run(
                ["sysctl", "-qw", f"net.ipv6.conf.{bridge}.disable_ipv6=0"],
                check=True,
            )
        prefix_length = 32 if afi == 4 else 128
        address_command = [
            "ip", f"-{afi}", "addr", "add", f"{peer['dut_address']}/{prefix_length}",
            "dev", bridge,
        ]
        if afi == 6:
            # Keep the DUT address usable as the route source immediately.
            address_command.append("nodad")
        subprocess.run(
            address_command,
            check=True,
        )
        for endpoint in peer["endpoints"]:
            port, receiver = endpoint["bridge_port"], endpoint["interface"]
            subprocess.run(
                ["ip", "link", "add", port, "type", "veth", "peer", "name", receiver],
                check=True,
            )
            subprocess.run(["ip", "link", "set", port, "master", bridge], check=True)
            subprocess.run(["ip", "link", "set", receiver, "address", endpoint["mac"]], check=True)
            for interface in (port, receiver):
                subprocess.run(
                    ["sysctl", "-qw", f"net.ipv6.conf.{interface}.disable_ipv6=1",
                     f"net.ipv4.conf.{interface}.forwarding=0"], check=True,
                )
                subprocess.run(["ip", "link", "set", interface, "up"], check=True)
                subprocess.run(
                    ["ethtool", "-K", interface, "tx", "off"],
                    check=True,
                    stdout=subprocess.DEVNULL,
                )
            address = endpoint["address"]
            # Resolve the original peer through its receiving interface.
            subprocess.run(
                ["ip", f"-{afi}", "neigh", "replace", address,
                 "lladdr", endpoint["mac"], "nud", "permanent", "dev", bridge],
                check=True,
            )
            subprocess.run(
                ["ip", f"-{afi}", "route", "replace", f"{address}/{prefix_length}",
                 "dev", bridge, "src", peer["dut_address"]],
                check=True,
            )
