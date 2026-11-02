"""Submit one RAW update file to one established Bngblaster BGP session."""

import ipaddress
import json
from pathlib import Path
import socket
import time

from rov.paths import CONFIG


# Exchange one control command and record when the request has been submitted.
def command(socket_path, name, arguments, timeout):
    request = json.dumps({"command": name, "arguments": arguments}).encode()
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
        connection.settimeout(timeout)
        connection.connect(str(socket_path))
        started = time.monotonic_ns()
        connection.sendall(request)
        submitted = time.monotonic_ns()
        response = bytearray()
        while chunk := connection.recv(65536):
            response.extend(chunk)
    result = json.loads(response)
    if result.get("code") != 200:
        raise RuntimeError(f"Bngblaster command failed: {result}")
    return result, started, submitted


# Build control filters from the transport addresses of one BGP session.
def address_arguments(local_address, peer_address):
    local = ipaddress.ip_address(local_address)
    peer = ipaddress.ip_address(peer_address)
    if local.version != peer.version:
        raise ValueError("BGP session addresses must use the same IP version")

    family = f"ipv{local.version}"
    return {
        f"local-{family}-address": str(local),
        f"peer-{family}-address": str(peer),
    }


# Select exactly one established session using its two transport addresses.
def session(socket_path, local_address, peer_address, timeout):
    arguments = address_arguments(local_address, peer_address)
    result, _, _ = command(socket_path, "bgp-sessions", arguments, timeout)
    sessions = result["bgp-sessions"]
    if len(sessions) != 1:
        raise RuntimeError(f"Expected one BGP session, found {len(sessions)}")
    selected = sessions[0]
    if selected["state"] != "established":
        raise RuntimeError(f"BGP session is not established: {selected['state']}")
    return selected


# Submit one file without waiting for generator completion or DUT convergence.
def send_file(file, local_address, peer_address, socket_path=CONFIG / "bng.sock", timeout=10):
    file = Path(file).resolve(strict=True)
    if not file.is_file() or file.stat().st_size == 0:
        raise ValueError("The RAW input must be a nonempty file")
    selected = session(socket_path, local_address, peer_address, timeout)
    if "raw-update-first-tx-ns" not in selected:
        raise RuntimeError("Bngblaster does not support first UPDATE send timing; rebuild its local binary")
    if selected.get("raw-update-state") in ("wait", "sending"):
        raise RuntimeError("The BGP session still has a pending RAW update")
    arguments = {
        "file": str(file),
        "first-update-timing": True,
        **address_arguments(local_address, peer_address),
    }
    result, started, submitted = command(socket_path, "bgp-raw-update", arguments, timeout)
    status = result["bgp-raw-update"]
    if status["started"] != 1 or status["skipped"] != 0:
        raise RuntimeError(f"RAW update was not started for exactly one session: {status}")
    return {"file": str(file), "write_start_ns": started, "submitted_ns": submitted}
