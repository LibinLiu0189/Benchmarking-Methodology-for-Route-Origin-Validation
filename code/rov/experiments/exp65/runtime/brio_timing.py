"""Associate completed BRIO responses conservatively with fixed-cadence batches."""

from rov.experiments.exp61.brio_timing import (
    correlate_control_records, first_vrp, read_control_records, read_records,
)


ENDPOINTS = {
    "first_vrp": {"vrp"}, "last_vrp": {"vrp"}, "roa_query_complete": {"rov"},
    "rib_complete": {"rib"}, "fib_probe_complete": {"fib"},
    "rib_and_sampled_fib": {"rib", "fib"},
}


# Keep missing or incomplete source logs explicit without interrupting the sender.
def snapshot(path):
    try:
        return dict(records=read_records(path), issue=None)
    except (OSError, ValueError, KeyError, TypeError) as error:
        return dict(records=[], issue=f"{type(error).__name__}: {error}")


# Associate only one complete response covering exactly one batch's serial range.
def correlate_batches(before, after, submissions, operations):
    results = [dict(batch=s["batch"], status="missing", issue="Missing BRIO evidence; no fallback",
                    brio_first_vrp_send_ns=None, fifo_write_to_brio_first_vrp_ms=None)
               for s in submissions]
    if not submissions or not before:
        return results
    if after[:len(before)] != before:
        for result in results:
            result.update(status="uncertain", issue="BRIO source log no longer contains the pre-submission baseline")
        return results
    baseline = before[-1]
    identity = ("pid", "client_fd", "peer", "session")
    if (baseline["status"] not in ("complete", "no_vrp")
            or not 0 <= baseline["serial"] <= 0xffffffff
            or baseline["first_vrp_send_ns"] >= submissions[0]["write_start_ns"]
            or any(any(row[k] != baseline[k] for k in identity) for row in before)):
        for result in results:
            result.update(status="uncertain", issue="Unusable baseline or multiple BRIO clients/sessions")
        return results
    if baseline["status"] == "complete":
        first_vrp(baseline)
        if baseline["first_vrp_send_ns"] <= 0 or baseline["vrp_pdus"] < 1:
            raise ValueError("Baseline lacks a valid first Prefix PDU send")
    updates = after[len(before):]
    ids = [row["response_id"] for row in after]
    if (ids != sorted(set(ids))
            or any(any(row[k] != baseline[k] for k in identity) for row in updates)
            or any(row["query"] != "serial" or row["query_session"] != baseline["session"]
                   for row in updates)):
        for result in results:
            result.update(status="uncertain", issue="Reconnect, reset, duplicate identity or unordered responses")
        return results
    serial = baseline["serial"]
    valid_contract = True
    for submission, result in zip(submissions, results, strict=True):
        rows = operations[submission["batch"]]
        start, serial = serial, serial + len(rows)
        result.update(expected_query_serial=start, expected_serial=serial)
        # Each generated add or single-index removal advances BRIO serial once.
        for row in rows:
            words = row["command"].split()
            prefix, maximum, asn = row["vrp"]
            valid_contract &= (
                row["operation"] == "add" and words == ["add", prefix, str(maximum), str(asn)]
                or row["operation"] == "withdraw" and len(words) == 2
                and words[0] == "remove" and words[1].isdigit() and int(words[1]) > 0)
        valid_contract &= len({tuple(row["vrp"]) for row in rows}) == len(rows)
        result["serial_interval_verified"] = valid_contract and serial <= 0xffffffff
        if not valid_contract or serial > 0xffffffff:
            result.update(status="uncertain", issue="Cannot prove one serial increment per operation or serial wrapped")
            continue
        if not rows:
            result.update(status="no_vrp", issue="No batch VRP operations; no Prefix PDU start")
            continue
        overlaps = [row for row in updates if row["query_serial"] < serial and row["serial"] > start]
        if not overlaps:
            continue
        if len(overlaps) != 1 or overlaps[0]["query_serial"] != start or overlaps[0]["serial"] != serial:
            result.update(status="uncertain", issue="Merged, split or repeated response does not identify one batch",
                          response_ids=[row["response_id"] for row in overlaps])
            continue
        response = overlaps[0]
        result["response_id"] = response["response_id"]
        result["brio_response"] = response
        if response["status"] != "complete":
            result.update(status=response["status"], issue="Response has no valid complete Prefix PDU send start")
            continue
        first = response["first_vrp_send_ns"]
        index = response["first_entry_serial"] - start - 1
        if (response["vrp_pdus"] != len(rows) or not 0 <= index < len(rows)
                or first <= 0 or first < submission["write_start_ns"]):
            result.update(status="uncertain", issue="PDU count, entry serial or FIFO boundary does not match")
            continue
        decoded = first_vrp(response)
        operation = rows[index]
        expected = ("added" if operation["operation"] == "add" else "removed", *operation["vrp"])
        actual = (decoded["action"], decoded["prefix"], decoded["max_length"], decoded["asn"])
        if actual != expected:
            result.update(status="uncertain", issue="First Prefix PDU identity disagrees with its operation serial")
            continue
        result.update(status="complete", issue=None, first_vrp=decoded,
                      brio_first_vrp_send_ns=first,
                      fifo_write_to_brio_first_vrp_ms=(first - submission["write_start_ns"]) / 1e6)
    return results


# Add new endpoint intervals while retaining the original FIFO and span metrics.
def attach(churn, operations, path, monitoring_mode):
    baseline = churn.get("brio_timing_baseline", dict(records=[], issue="No pre-submission snapshot"))
    final = snapshot(path)
    if final["issue"] is not None or not final["records"]:
        raise ValueError(final["issue"] or "No BRIO response timing records were saved")
    issue = baseline["issue"] or final["issue"]
    try:
        rows = correlate_batches(baseline["records"], final["records"],
                                 churn.get("submissions", []), operations)
    except (ValueError, KeyError, TypeError) as error:
        rows = []
        issue = f"Invalid BRIO evidence: {type(error).__name__}: {error}"
    lookup = {row["batch"]: row for row in rows}
    controls = read_control_records(path)
    baseline_response = baseline["records"][-1] if baseline["records"] else None
    for submission in churn.get("submissions", []):
        timing = lookup.get(submission["batch"])
        if timing is None:
            continue
        identity = timing.get("brio_response")
        if (identity is None and baseline_response is not None
                and timing.get("serial_interval_verified")):
            identity = dict(baseline_response, serial=timing["expected_serial"])
            identity.pop("first_vrp_send_ns", None)
        if identity is None:
            control = dict(
                brio_control_records=[], brio_notify_send_ns=None,
                brio_cache_reset_send_ns=None, control_timing_status="unattributed",
                control_timing_issue="No verified connection and batch serial interval",
            )
        else:
            control = correlate_control_records(path, submission, response=identity, pdu_type=0)
        timing.update(control)
        submission["brio_notify_send_ns"] = control["brio_notify_send_ns"]
    for batch in churn.get("batch_metrics", []):
        timing = lookup.get(batch["batch"], dict(status="missing", issue=issue or "No BRIO response",
                                                 brio_first_vrp_send_ns=None))
        if issue:
            timing.update(status="missing", issue=issue, brio_first_vrp_send_ns=None)
        batch["brio_timing"] = timing
        timing["endpoint_issues"] = {}
        first = timing["brio_first_vrp_send_ns"]
        for name, required in ENDPOINTS.items():
            if not required.issubset(monitoring_mode):
                continue
            endpoint = batch.get("observed_endpoints_ns", {}).get(name)
            metric = name + "_from_brio_ms"
            batch["metrics"][metric] = None
            if first is None:
                timing["endpoint_issues"][metric] = timing["issue"] or timing["status"]
            elif endpoint is None:
                timing["endpoint_issues"][metric] = "Missing or ambiguous external observation endpoint"
            elif endpoint < first:
                timing["endpoint_issues"][metric] = "External observation precedes first VRP send"
            else:
                batch["metrics"][metric] = (endpoint - first) / 1e6
    churn["brio_timing"] = dict(
        source=str(path), records=final["records"], control_records=controls, batches=rows, issue=issue,
        definition="CLOCK_MONOTONIC immediately before the first successful Prefix PDU send call; no control PDU or FIFO fallback",
        attribution="Exact serial interval, connection/session, PDU count, first PDU identity and FIFO lower bound; merged/split responses unavailable")
