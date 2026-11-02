"""Inject tagged UDP probes through a dedicated link and record raw receptions."""

import ipaddress as ip
import json
import os
import secrets
import select
import socket
import struct
import subprocess
import time

from rov.dut.virtualize import require_isolation
from rov.measure.data_plane import packet, decode


class ProbeTransport:
    # Create one injection link without a BGP session or routes to probe destinations.
    def __init__(self, destinations, config_dir):
        self.receiver = None
        self.senders = {}
        self.link_created = False
        self.token = secrets.token_bytes(8)
        self.sequence = 0
        self.dropped_packets = 0
        self.source_interface = "probe-tx"
        self.dut_interface = "probe-dut"
        self.source_mac = bytes.fromhex("02cc00000002")
        self.gateway_mac = bytes.fromhex("02cc00000001")
        self.sources = {4: "198.19.255.254", 6: "2001:db8:ffff::2"}

        require_isolation()
        bird_pid = int((config_dir / "bird.pid").read_text().strip())
        if os.readlink("/proc/self/ns/net") != os.readlink(f"/proc/{bird_pid}/ns/net"):
            raise RuntimeError("Probes must run inside BIRD's network namespace")
        config = json.loads((config_dir / "bng.json").read_text())
        self.endpoints = {
            row["interface"]: row for row in config["interfaces"]["network"]
        }
        families = {ip.ip_address(destination).version for destination in destinations}
        try:
            subprocess.run([
                "ip", "link", "add", self.dut_interface, "type", "veth",
                "peer", "name", self.source_interface,
            ], check=True)
            self.link_created = True
            for interface, mac in (
                (self.dut_interface, "02:cc:00:00:00:01"),
                (self.source_interface, "02:cc:00:00:00:02"),
            ):
                subprocess.run(["ip", "link", "set", interface, "address", mac], check=True)
            subprocess.run([
                "sysctl", "-qw", "net.ipv4.conf.all.rp_filter=0",
                f"net.ipv4.conf.{self.dut_interface}.rp_filter=0",
                f"net.ipv4.conf.{self.dut_interface}.forwarding=1",
                f"net.ipv6.conf.{self.dut_interface}.disable_ipv6=0",
                f"net.ipv6.conf.{self.dut_interface}.forwarding=1",
                f"net.ipv6.conf.{self.source_interface}.disable_ipv6=1",
                f"net.ipv4.conf.{self.source_interface}.forwarding=0",
            ], check=True)
            for interface in (self.dut_interface, self.source_interface):
                subprocess.run(["ip", "link", "set", interface, "up"], check=True)
            subprocess.run([
                "ethtool", "-K", self.source_interface, "tx", "off",
            ], check=True, stdout=subprocess.DEVNULL)

            protocol = 0x0003 if len(families) > 1 else (
                0x0800 if 4 in families else 0x86dd
            )
            self.receiver = socket.socket(socket.AF_PACKET, socket.SOCK_RAW, socket.htons(protocol))
            self.receiver.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4 * 1024 * 1024)
            self.receiver.setblocking(False)
            for family in families:
                protocol = 0x0800 if family == 4 else 0x86dd
                sender = socket.socket(socket.AF_PACKET, socket.SOCK_RAW, socket.htons(protocol))
                self.senders[family] = sender
                sender.bind((self.source_interface, 0))
        except BaseException:
            self.close()
            raise

    # Send the next numbered packet without waiting for any previous packet.
    def send(self, destination):
        family = ip.ip_address(destination).version
        self.sequence += 1
        frame = packet(
            self.sources[family], destination, self.source_mac,
            self.gateway_mac, self.token, self.sequence,
        )
        sent_ns = time.monotonic_ns()
        if self.senders[family].send(frame) != len(frame):
            raise RuntimeError("Incomplete probe transmission")
        return dict(
            event="sent", sequence=self.sequence, destination=destination,
            sent_ns=sent_ns, source_interface=self.source_interface,
        )

    # Record every tagged reception at a configured neighbor, including late copies.
    def poll(self, wait):
        events = []
        readable, _, _ = select.select([self.receiver], [], [], max(0, wait))
        if readable:
            for _ in range(256):
                try:
                    frame, address = self.receiver.recvfrom(65535)
                except BlockingIOError:
                    break
                observed_ns = time.monotonic_ns()
                interface = address[0]
                if interface not in self.endpoints or address[2] == socket.PACKET_OUTGOING:
                    continue
                decoded = decode(frame, self.token)
                if decoded is None:
                    continue
                sequence, destination, ttl = decoded
                if ttl != 63:
                    raise RuntimeError("Probe did not undergo exactly one forwarding hop")
                endpoint = self.endpoints[interface]
                next_hop = str(ip.ip_interface(
                    endpoint.get("address") or endpoint["address-ipv6"]
                ).ip)
                events.append(dict(
                    event="received", sequence=sequence, destination=destination,
                    observed_ns=observed_ns, next_hop=next_hop, interface=interface,
                ))
        _, dropped = struct.unpack("II", self.receiver.getsockopt(263, 6, 8))
        self.dropped_packets += dropped
        if dropped:
            raise RuntimeError(f"Probe receiver socket dropped {dropped} packets")
        return events

    # Close only owned sockets and remove the dedicated injection link.
    def close(self):
        if self.receiver is not None:
            self.receiver.close()
            self.receiver = None
        for sender in self.senders.values():
            sender.close()
        self.senders.clear()
        if self.link_created:
            subprocess.run(["ip", "link", "del", self.dut_interface], check=True)
            self.link_created = False
