"""Build bounded, labelled ROA queries and count protocol bytes."""

import ipaddress
import re
import select

from .bird_connection import BirdConnection

STATES = {"0": "NotFound", "1": "Valid", "2": "Invalid"}


# Validate literals before embedding them in BIRD expressions.
def expression(target):
    prefix, asn = target
    network = ipaddress.ip_network(prefix)
    if isinstance(asn, bool) or not isinstance(asn, int) or not 0 <= asn <= 4294967295:
        raise ValueError("Origin ASN must be an unsigned 32-bit integer")
    return f"roa_check(r{network.version}, {network}, {asn})"


# Use positional labels so repeated targets remain separate requests.
def labelled(index, target):
    if not 0 <= index <= 0xffffffff:
        raise ValueError("Target index exceeds the label range")
    return f'"q{index:08x}=" ++ format({expression(target)}) ++ ";"'


# Reject missing, reordered, duplicated, truncated, or unexpected label records.
def parse(text, indices):
    records = re.findall(r"q([0-9a-f]{8})=\(enum 34\)([012]);", text)
    rebuilt = "".join(f"q{label}=(enum 34){state};" for label, state in records)
    if rebuilt != text or [int(label, 16) for label, _ in records] != list(indices):
        raise ValueError(f"Incomplete or mismatched labelled ROA reply: {text!r}")
    return [STATES[state] for _, state in records]


class CountedConnection(BirdConnection):
    # Count complete wire lines, including code prefixes and newline terminators.
    def __init__(self, path, timeout):
        self.received_bytes = 0
        super().__init__(path, timeout)

    # Preserve the original stream parser while counting its wire consumption.
    def line(self, deadline):
        text = super().line(deadline)
        self.received_bytes += len(text.encode()) + 1
        return text

    # Reject leftover replies before another labelled request can be sent.
    def assert_idle(self):
        if self.buffer or select.select([self.socket], [], [], 0)[0]:
            self.close()
            raise RuntimeError("Unexpected residual BIRD response; connection closed")


# Split explicit requested groups only when a verified wire bound requires it.
def groups(targets, requested, mode):
    if requested < 1:
        raise ValueError("Group size must be positive")
    result = []
    for start in range(0, len(targets), requested):
        current = []
        for index in range(start, min(start + requested, len(targets))):
            trial = current + [index]
            if mode == "eval":
                command = "eval " + " ++ ".join(labelled(i, targets[i]) for i in trial)
                safe = len(command.encode()) < 1000 and 5 + 21 * len(trial) + 1 <= 511
            else:
                commands = ["eval " + labelled(i, targets[i]) for i in trial]
                safe = all(len(c.encode()) < 1000 for c in commands) and sum(len(c.encode()) + 1 for c in commands) <= 1023
            if not safe:
                if not current:
                    raise ValueError("A single target exceeds the protocol byte limits")
                result.append(current)
                current = [index]
            else:
                current = trial
        if current:
            result.append(current)
    return result
