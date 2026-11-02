"""Read BRIO control and response sends and associate single-batch evidence."""

import ipaddress
import json
from pathlib import Path
import struct


# Decode only RTR IPv4/IPv6 Prefix PDUs, retaining announce/withdraw identity.
def first_vrp(record):
    raw = bytes.fromhex(record["first_pdu_hex"])
    if len(raw) not in (20, 32):
        raise ValueError("Missing or invalid first VRP PDU")
    version, kind, reserved, length = struct.unpack("!BBHI", raw[:8])
    afi = {4: 4, 6: 6}.get(kind)
    if afi is None or length != len(raw) or length != (20 if afi == 4 else 32):
        raise ValueError("First PDU must be an IPv4/IPv6 Prefix PDU")
    flags, prefix_length, maximum, zero = raw[8:12]
    if reserved or zero or flags not in (0, 1):
        raise ValueError("Invalid first VRP PDU flags or reserved bytes")
    address = ipaddress.ip_address(raw[12:-4])
    prefix = str(ipaddress.ip_network(f"{address}/{prefix_length}", strict=True))
    if not prefix_length <= maximum <= address.max_prefixlen:
        raise ValueError("Invalid first VRP maximum length")
    return dict(version=version, afi=afi, action="added" if flags else "removed",
                prefix=prefix, max_length=maximum, asn=struct.unpack("!I", raw[-4:])[0])


# Read the two recorded send-event formats from a complete timing file.
def read_timing_records(path):
    text = Path(path).read_text()
    if text and not text.endswith("\n"):
        raise ValueError("Incomplete BRIO timing record")
    records = [json.loads(line) for line in text.splitlines()]
    for record in records:
        if (record["format"] not in ("brio-first-vrp-v1", "brio-control-send-v1")
                or record["clock"] != "CLOCK_MONOTONIC"):
            raise ValueError("Unsupported BRIO timing format or clock")
    return records


# Read only completed response evidence, excluding independently logged controls.
def read_records(path):
    return [record for record in read_timing_records(path)
            if record["format"] == "brio-first-vrp-v1"]


# Retain every Notify and Cache Reset send for independent source-side analysis.
def read_control_records(path):
    return [record for record in read_timing_records(path)
            if record["format"] == "brio-control-send-v1"]


# Match source controls by FIFO bounds, response identity and notification serial.
def correlate_control_records(path, submission, response=None, pdu_type=0):
    records = read_control_records(path)
    start = submission["write_start_ns"]
    end = response.get("first_vrp_send_ns") if response is not None else None
    identity = ("pid", "client_fd", "peer", "session")
    candidates = [
        record for record in records
        if record["pdu_type"] == pdu_type
        and (pdu_type != 0 or response is None or record["serial"] == response["serial"])
        and record["send_start_ns"] >= start
        and (not end or record["send_start_ns"] <= end)
        and (response is None or all(record[key] == response[key] for key in identity))
    ]
    result = dict(
        brio_control_records=candidates,
        brio_notify_send_ns=None,
        brio_cache_reset_send_ns=None,
        control_timing_status="missing",
    )
    selected = []
    for kind, field in ((0, "brio_notify_send_ns"), (8, "brio_cache_reset_send_ns")):
        matching = [record for record in candidates if record["pdu_type"] == kind]
        if kind == 0 and response is not None:
            matching = [record for record in matching if record["serial"] == response["serial"]]
        if len(matching) == 1 and matching[0]["status"] == "complete":
            result[field] = matching[0]["send_start_ns"]
        if kind == pdu_type:
            selected = matching
    if len(selected) == 1:
        result["control_timing_status"] = selected[0]["status"]
    elif len(selected) > 1:
        result["control_timing_status"] = "uncertain"
        result["control_timing_issue"] = "Multiple control sends match this update"
    else:
        result["control_timing_issue"] = "No matching control PDU send was recorded"
    return result


# Associate a completed response with the preceding baseline and FIFO boundary.
def correlate(before, after, measurement, mode, batch):
    result = dict(batch=batch, mode=mode, clock="CLOCK_MONOTONIC", **measurement,
                  brio_timing_status="missing", brio_first_vrp_send_ns=None,
                  fifo_to_brio_first_vrp_ms=None, first_from_brio_ms=None,
                  last_from_brio_ms=None, full_sync_from_brio_ms=None)
    if not before or len(after) == len(before):
        result["issue"] = "Missing BRIO baseline or update response; no timing fallback"
        return result
    if after[:len(before)] != before or len(after) != len(before) + 1:
        raise ValueError("Expected exactly one new BRIO response for this batch")
    previous, current = before[-1], after[-1]
    for field in ("pid", "client_fd", "peer", "session"):
        if current[field] != previous[field]:
            raise ValueError(f"BRIO response changed {field} during this batch")
    if (previous["status"] not in ("complete", "no_vrp")
            or current["response_id"] <= previous["response_id"]
            or current["query"] != ("reset" if mode == "full" else "serial")
            or (mode == "incremental" and current["query_serial"] != previous["serial"])
            or (mode == "incremental" and current["query_session"] != current["session"])):
        raise ValueError("BRIO query, serial or response identity does not match this update")
    result["brio_response"] = current
    result["baseline_response_id"] = previous["response_id"]
    if current["status"] == "no_vrp":
        if current["vrp_pdus"] != 0 or current["first_vrp_send_ns"] != 0 or current["first_pdu_hex"]:
            raise ValueError("No-VRP response contains a first-PDU timestamp")
        result["brio_timing_status"] = "no_vrp"
        return result
    if current["status"] != "complete":
        result["brio_timing_status"] = current["status"]
        result["issue"] = "BRIO response was not sent completely; no valid start"
        return result
    first = current["first_vrp_send_ns"]
    if current["vrp_pdus"] < 1 or first < measurement["write_start_ns"]:
        raise ValueError("BRIO first VRP precedes this update or has no successful PDU")
    result["first_vrp"] = first_vrp(current)
    result["brio_first_vrp_send_ns"] = first
    result["fifo_to_brio_first_vrp_ms"] = (first - measurement["submitted_ns"]) / 1e6
    for endpoint, field in (("first_observed_ns", "first_from_brio_ms"),
                            ("last_observed_ns", "last_from_brio_ms"),
                            ("full_completed_ns", "full_sync_from_brio_ms")):
        value = measurement[endpoint]
        if value is not None:
            if value < first:
                raise ValueError("BIRD observation precedes the BRIO first VRP send")
            result[field] = (value - first) / 1e6
    result["brio_timing_status"] = "complete"
    return result
