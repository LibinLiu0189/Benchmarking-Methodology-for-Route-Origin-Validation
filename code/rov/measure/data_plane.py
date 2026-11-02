"""Send continuous UDP probes and observe actual forwarding to simulated peers."""

from collections import deque
import ipaddress as ip
import json
import os
import secrets
import threading
import select
import socket
import struct
import subprocess
import time

from rov.paths import CONFIG
from rov.measure.read_log import stop_observer

MAGIC = b"ROVPROBE"
PORT = 49000
PACKET_EVENTS = frozenset(("received", "not_received", "late", "late_or_duplicate"))


# Print events without creating result files.
def emit(event):
    print(json.dumps(event, separators=(",", ":")), flush=True)


# Compute an Internet checksum for IP and UDP headers.
def checksum(data):
    if len(data) % 2:
        data += b"\0"
    total = sum(struct.unpack(f"!{len(data) // 2}H", data))
    while total >> 16:
        total = (total & 65535) + (total >> 16)
    return (~total) & 65535


# Build a tagged Ethernet/IP/UDP probe with an initial hop count of 64.
def packet(source, destination, source_mac, destination_mac, token, number):
    source_address = ip.ip_address(source)
    destination_address = ip.ip_address(destination)
    if source_address.version != destination_address.version:
        raise ValueError("Probe source and destination must use the same IP family")

    source_bytes = source_address.packed
    destination_bytes = destination_address.packed

    # Tag the payload with this observer's token and the packet sequence.
    payload = MAGIC + token + struct.pack("!Q", number)
    udp = struct.pack("!HHHH", PORT, PORT, 8 + len(payload), 0) + payload
    if source_address.version == 4:
        pseudo_header = (
            source_bytes + destination_bytes + struct.pack("!BBH", 0, 17, len(udp))
        )
    else:
        pseudo_header = (
            source_bytes + destination_bytes + struct.pack("!I3xB", len(udp), 17)
        )
    udp_checksum = checksum(pseudo_header + udp) or 65535
    udp = udp[:6] + struct.pack("!H", udp_checksum) + payload

    if source_address.version == 6:
        ipv6_header = struct.pack(
            "!IHBB16s16s",
            6 << 28, len(udp), 17, 64, source_bytes, destination_bytes,
        )
        return destination_mac + source_mac + b"\x86\xdd" + ipv6_header + udp

    # Build a 20-byte IPv4 header with UDP protocol and an initial TTL of 64.
    ipv4_header = struct.pack(
        "!BBHHHBBH4s4s",
        0x45, 0, 20 + len(udp), number & 65535,
        0, 64, 17, 0, source_bytes, destination_bytes,
    )
    ipv4_header = (
        ipv4_header[:10]
        + struct.pack("!H", checksum(ipv4_header))
        + ipv4_header[12:]
    )
    return destination_mac + source_mac + b"\x08\x00" + ipv4_header + udp


# Recognize only this observer's UDP payload and return sequence, destination, and hop count.
def decode(frame, token):
    # Locate UDP in the IPv4 or IPv6 header made by packet().
    if len(frame) >= 34 and frame[12:14] == b"\x08\x00" and frame[14] >> 4 == 4:
        udp_offset = 14 + (frame[14] & 15) * 4
        if udp_offset < 34 or frame[23] != 17:
            return None
        destination = str(ip.IPv4Address(frame[30:34]))
        hop_count = frame[22]
    elif len(frame) >= 54 and frame[12:14] == b"\x86\xdd" and frame[14] >> 4 == 6:
        if frame[20] != 17:
            return None
        udp_offset = 54
        destination = str(ip.IPv6Address(frame[38:54]))
        hop_count = frame[21]
    else:
        return None

    if len(frame) < udp_offset + 32:
        return None
    if frame[udp_offset:udp_offset + 4] != struct.pack("!HH", PORT, PORT):
        return None
    if frame[udp_offset + 8:udp_offset + 24] != MAGIC + token:
        return None

    sequence = struct.unpack_from("!Q", frame, udp_offset + 24)[0]
    return sequence, destination, hop_count


# Select an injection peer with the same address family as each destination.
def select_sources(endpoints, destinations, source_interface):
    families = dict.fromkeys(ip.ip_address(destination).version for destination in destinations)
    if source_interface and len(families) > 1:
        raise ValueError(
            "Mixed IPv4/IPv6 probes cannot use one source interface; "
            "omit --source-interface or run separate batches"
        )
    if source_interface and source_interface not in endpoints:
        raise ValueError("Source interface must be one of the simulated peers")

    sources = {}
    for family in families:
        address_field = "address" if family == 4 else "address-ipv6"
        if source_interface:
            interface = source_interface
            if address_field not in endpoints[interface]:
                raise ValueError(f"Source interface has no IPv{family} address")
        else:
            interface = next(
                (name for name, row in endpoints.items() if address_field in row),
                None,
            )
            if interface is None:
                raise ValueError(f"No simulated IPv{family} peers are configured")

        endpoint = endpoints[interface]
        sources[family] = (
            interface,
            str(ip.ip_interface(endpoint[address_field]).ip),
            bytes.fromhex(endpoint["gateway-mac"].replace(":", "")),
        )
    return sources


# Probe destinations round-robin and report each receive or deadline expiration.
def observe(
    destinations,
    config_dir=CONFIG,
    source_interface=None,
    pps=10000,
    packet_timeout=1,
    on_event=emit,
    stop_event=None,
):
    # Normalize destinations without changing their order.
    destinations = list(dict.fromkeys(
        str(ip.ip_address(destination)) for destination in destinations
    ))

    # Require the observer to run in the existing DUT network namespace.
    bird_pid = int((config_dir / "bird.pid").read_text().strip())
    if os.readlink("/proc/self/ns/net") != os.readlink(f"/proc/{bird_pid}/ns/net"):
        raise RuntimeError("Packet observation must run inside BIRD's network namespace")

    # Select one injection interface per destination family and all reception peers.
    config = json.loads((config_dir / "bng.json").read_text())
    endpoints = {
        row["interface"]: row
        for row in config["interfaces"]["network"]
    }
    if not endpoints:
        raise ValueError("No simulated peers are configured")

    sources = select_sources(endpoints, destinations, source_interface)
    links = json.loads(subprocess.check_output(
        ["ip", "-j", "link", "show"], text=True,
    ))
    interface_macs = {
        row["ifname"]: bytes.fromhex(row["address"].replace(":", ""))
        for row in links
    }

    # Track outstanding sequences and their expiration times in send order.
    token = secrets.token_bytes(8)
    pending = {}  # sequence -> (destination, sent_ns)
    deadlines = deque()  # (monotonic deadline in seconds, sequence)

    receiver_protocol = 0x0003 if len(sources) > 1 else (
        0x0800 if 4 in sources else 0x86dd
    )
    receiver = socket.socket(
        socket.AF_PACKET, socket.SOCK_RAW, socket.htons(receiver_protocol),
    )
    senders = {}
    try:
        receiver.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4 * 1024 * 1024)
        receiver.setblocking(False)

        for family, (interface, _, _) in sources.items():
            protocol = 0x0800 if family == 4 else 0x86dd
            sender = socket.socket(
                socket.AF_PACKET, socket.SOCK_RAW, socket.htons(protocol),
            )
            senders[family] = sender
            sender.bind((interface, 0))

        # Keep the existing readiness field as the first probe's interface.
        on_event(dict(
            event="ready",
            component="data_plane",
            source_interface=sources[ip.ip_address(destinations[0]).version][0],
            destinations=destinations,
            pps=pps,
        ))

        sequence_number = 0
        next_send_at = time.monotonic()
        while stop_event is None or not stop_event.is_set():
            # Send one probe when due, rotating through the destinations.
            now = time.monotonic()
            if now >= next_send_at:
                destination = destinations[sequence_number % len(destinations)]
                family = ip.ip_address(destination).version
                interface, source_ip, destination_mac = sources[family]
                sequence_number += 1
                frame = packet(
                    source_ip, destination, interface_macs[interface],
                    destination_mac, token, sequence_number,
                )
                sent_ns = time.monotonic_ns()
                if senders[family].send(frame) != len(frame):
                    raise RuntimeError("Incomplete probe transmission")

                pending[sequence_number] = (destination, sent_ns)
                deadlines.append((sent_ns / 1e9 + packet_timeout, sequence_number))
                next_send_at = time.monotonic() + 1 / pps

            # Wait for a received frame, the next send, or the earliest timeout.
            wake_at = min(next_send_at, deadlines[0][0]) if deadlines else next_send_at
            if stop_event is not None:
                wake_at = min(wake_at, time.monotonic() + 0.1)
            readable, _, _ = select.select(
                [receiver], [], [], max(0, wake_at - time.monotonic()),
            )

            # Limit each receive pass so sending and timeout handling can continue.
            if readable:
                for _ in range(256):
                    try:
                        frame, socket_address = receiver.recvfrom(65535)
                    except BlockingIOError:
                        break
                    observed_ns = time.monotonic_ns()
                    interface = socket_address[0]
                    packet_type = socket_address[2]

                    if interface not in endpoints or packet_type == socket.PACKET_OUTGOING:
                        continue
                    decoded = decode(frame, token)
                    if decoded is None:
                        continue

                    sequence, destination, ttl = decoded
                    if ttl != 63:
                        raise RuntimeError("Probe did not undergo exactly one DUT forwarding hop")

                    # A missing record means this sequence already received a result.
                    record = pending.pop(sequence, None)
                    event_type = "received" if record else "late_or_duplicate"
                    if record and observed_ns - record[1] > packet_timeout * 1e9:
                        on_event(dict(
                            event="not_received",
                            sequence=sequence,
                            destination=destination,
                            sent_ns=record[1],
                            observed_ns=observed_ns,
                        ))
                        event_type = "late"

                    endpoint = endpoints[interface]
                    endpoint_address = endpoint.get("address") or endpoint["address-ipv6"]
                    on_event(dict(
                        event=event_type,
                        sequence=sequence,
                        destination=destination,
                        interface=interface,
                        next_hop=str(ip.ip_interface(endpoint_address).ip),
                        ttl=ttl,
                        sent_ns=record[1] if record else None,
                        observed_ns=observed_ns,
                    ))

            # Socket drops invalidate loss observations and are not DUT packet loss.
            # Linux SOL_PACKET=263 and PACKET_STATISTICS=6 return packets and drops.
            _, dropped_packets = struct.unpack("II", receiver.getsockopt(263, 6, 8))
            if dropped_packets:
                raise RuntimeError(f"Observer socket dropped {dropped_packets} packets")

            # Expire outstanding probes; completed sequences have no pending record.
            now = time.monotonic()
            while deadlines and deadlines[0][0] <= now:
                _, sequence = deadlines.popleft()
                record = pending.pop(sequence, None)
                if record:
                    destination, sent_ns = record
                    on_event(dict(
                        event="not_received",
                        sequence=sequence,
                        destination=destination,
                        sent_ns=sent_ns,
                        observed_ns=time.monotonic_ns(),
                    ))
    finally:
        receiver.close()
        for sender in senders.values():
            sender.close()


# Start packet transport with experiment-owned initial-sample acceptance.
def start_packet_observer(destinations, config_dir, source_interface, pps, packet_timeout,
                          timeout, accept_initial, events, errors):
    stop = threading.Event()
    initialized = threading.Event()
    initial = {}
    pending = set(destinations)
    ready = False

    # Keep readiness and initial outcomes private; publish subsequent results unchanged.
    def receive(event):
        nonlocal ready
        if initialized.is_set():
            events.put(event)
            return
        if event.get("event") == "ready":
            ready = True
        else:
            if not ready:
                raise RuntimeError("Received a packet result before observer readiness")
            accept_initial(event, initial, pending)
        if ready and not pending:
            initialized.set()

    # Run the existing packet reader and report failures to the common error pool.
    def worker():
        try:
            observe(destinations, config_dir=config_dir, source_interface=source_interface,
                    pps=pps, packet_timeout=packet_timeout, on_event=receive, stop_event=stop)
        except BaseException as error:
            errors.put(error)

    thread = threading.Thread(target=worker, name="data-plane-observer")
    thread.start()
    deadline = time.monotonic() + timeout
    try:
        while not initialized.is_set():
            if not errors.empty():
                raise errors.get()
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("Timed out waiting for the packet observer")
            initialized.wait(min(0.1, remaining))
        if not errors.empty():
            raise errors.get()
    except BaseException:
        stop_observer(stop, thread, timeout)
        raise
    return initial, events, errors, stop, thread
