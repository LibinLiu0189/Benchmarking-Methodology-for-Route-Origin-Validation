"""Inspect native same-connection RTR full responses without owning experiment timers."""

from collections import Counter
from pathlib import Path
import re

from rov.data.vrp import normalize
from rov.measure.bird_connection import BirdConnection


# Identify the established RTR socket owned by this BIRD process, as in exp67.
def bird_rtr_sockets(pid_file):
    pid = int(Path(pid_file).read_text().strip())
    inodes = set()
    for entry in Path(f"/proc/{pid}/fd").iterdir():
        try:
            target = entry.readlink().as_posix()
        except FileNotFoundError:
            continue
        if target.startswith("socket:[") and target.endswith("]"):
            inodes.add(target[8:-1])
    result = []
    for line in Path("/proc/net/tcp").read_text().splitlines()[1:]:
        fields = line.split()
        if (fields[3] == "01" and fields[9] in inodes
                and int(fields[2].split(":")[1], 16) == 3323):
            result.append(dict(local=fields[1], remote=fields[2], inode=fields[9]))
    return sorted(result, key=lambda row: row["remote"])


# Enable packet evidence only after the experiment's ordinary log observer is ready.
def begin_full_sync(trace, socket_path, pid_file, timeout):
    before = bird_rtr_sockets(pid_file)
    if len(before) != 1:
        raise RuntimeError("Full synchronization requires one established BIRD RTR socket")
    connection = BirdConnection(socket_path, timeout)
    try:
        if "Established" not in connection.command("show protocols all rpki_source"):
            raise RuntimeError("RTR baseline is not Established before Cache Reset")
        offset = Path(trace).stat().st_size
        connection.command("debug rpki_source { routes, events, packets }")
    finally:
        connection.close()
    return dict(trace_offset=offset, tcp_before=before)


# Require one ordered control exchange and every full-response identity in native logs.
def verify_reset_exchange(lines, expected):
    controls = []
    prefixes = Counter()
    wanted = ["Received Cache Reset", "Sending Reset Query", "Received Cache Response", "Received End of Data"]
    for line in lines:
        _, separator, message = line.partition(" <TRACE> ")
        if not separator:
            continue
        message = message.rstrip()
        if message.startswith("rpki_source: "):
            text = message.removeprefix("rpki_source: ")
            packet = re.fullmatch(r"(Received|Sending) (.+?) packet(?: .*)?", text)
            if packet:
                kind = f"{packet[1]} {packet[2]}"
                if kind in wanted:
                    if len(controls) >= len(wanted) or kind != wanted[len(controls)]:
                        raise RuntimeError(f"Unexpected full-sync control sequence: {kind}")
                    controls.append(kind)
                elif packet[2] in ("IPv4 Prefix", "IPv6 Prefix") and packet[1] == "Received":
                    if controls != wanted[:3]:
                        raise RuntimeError("Prefix PDU outside the full Cache Response")
                    match = re.search(r"\((\S+/\d+)-(\d+) AS(\d+)\)$", text)
                    if match is None:
                        raise ValueError(f"Unrecognized Prefix PDU trace: {text}")
                    prefixes[normalize(*match.groups())] += 1
                else:
                    raise RuntimeError(f"Unexpected RTR PDU during full synchronization: {text}")
            if (text == "Connected" or "Lost connection" in text or "closed a connection" in text
                    or re.search(r" to (Connecting|Transport-Error|Fatal-Protocol-Error|Down) state$", text)):
                raise RuntimeError(f"RTR connection changed during full synchronization: {text}")
    if controls != wanted:
        raise RuntimeError(f"Incomplete Cache Reset / Reset Query exchange: {controls}")
    if prefixes != Counter(expected):
        raise RuntimeError("Full RTR response identities differ from the expected VRP set")
    return dict(control_sequence=controls, prefix_pdus=sum(prefixes.values()),
                unique_vrps=len(prefixes), complete=True,
                source="Native BIRD packet trace: Reset Query followed by Cache Response, all Prefix PDUs and End of Data")


# Compare all installed authorizations after timed collection, including untouched entries.
def verify_vrp_table(socket_path, timeout, expected):
    actual = set()
    connection = BirdConnection(socket_path, timeout)
    try:
        connection.command("restrict")
        status = connection.command("show protocols all rpki_source")
        for afi in (4, 6):
            for line in connection.command(f"show route table r{afi}").splitlines():
                match = re.match(r"^(\S+/\d+)-(\d+) AS(\d+)\s", line)
                if match:
                    actual.add(normalize(*match.groups()))
    finally:
        connection.close()
    missing, extra = set(expected) - actual, actual - set(expected)
    return dict(correct=not missing and not extra and "Established" in status,
                expected=len(expected), actual=len(actual), missing=len(missing), extra=len(extra),
                missing_examples=sorted(missing)[:10], extra_examples=sorted(extra)[:10],
                scope="Complete installed VRP identity set after timed collection", rtr_status=status)


# Verify protocol, TCP identity and installed state without changing any timing endpoint.
def finish_full_sync(trace, socket_path, pid_file, timeout, before, expected):
    after = bird_rtr_sockets(pid_file)
    if before["tcp_before"] != after:
        raise RuntimeError("Cache Reset did not retain the same BIRD TCP socket")
    with Path(trace).open("rb") as source:
        source.seek(before["trace_offset"])
        exchange = verify_reset_exchange((line.decode() for line in source), expected)
    table = verify_vrp_table(socket_path, timeout, expected)
    if not table["correct"]:
        raise RuntimeError("Complete installed VRP set differs after full synchronization")
    return dict(same_tcp_session=True, tcp_before=before["tcp_before"], tcp_after=after,
                protocol=exchange, vrp_table=table)
