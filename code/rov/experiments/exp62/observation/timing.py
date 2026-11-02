"""Derive experiment 6.2 endpoints from complete prepared net VRP matches."""

from rov.data.vrp import normalize


# Retain independent monotonic endpoints and explain every unavailable interval.
def validation_timing(submission, vrp_events, query_events, monitoring, brio_timing=None):
    required = {normalize(*row["vrp"]): row["action"] for row in monitoring["required_changes"]}
    latest = {}
    for event in vrp_events:
        key = normalize(event["prefix"], event["max_length"], event["asn"])
        if key in required:
            previous = latest.get(key)
            if previous is None or event["observed_ns"] >= previous["observed_ns"]:
                latest[key] = event
    matched = {key: event for key, event in latest.items() if event["action"] == required[key]}
    complete = bool(required) and len(matched) == len(required)
    installed = max(event["observed_ns"] for event in matched.values()) if complete else None
    updated = max((row["observed_ns"] for row in query_events), default=None)
    result = dict(
        clock="time.monotonic_ns",
        fifo_write_start_ns=submission["write_start_ns"],
        fifo_submitted_ns=submission["submitted_ns"],
        vrp_install_complete_ns=installed, query_complete_ns=updated,
        required_net_changes=len(required), matched_net_changes=len(matched),
        fifo_start_to_query_complete_ms=None, post_install_query_ms=None,
        rtr_send_to_query_complete_ms=None,
        unavailable={"rtr_send_to_query_complete_ms": "RTR first-update PDU transmission is not instrumented"},
        installation_definition="Last matching final-action receipt across all prepared net VRP changes",
        send_definition="FIFO command write start; excludes no cache delay and is not RTR PDU transmission",
        completion_definition="Last target ROA query state change; not internal BGP refilter completion",
    )
    if brio_timing is not None:
        result.update(
            clock="CLOCK_MONOTONIC",
            reference="brio_first_prefix_pdu_send",
            brio=brio_timing,
            brio_notify_send_ns=brio_timing.get("brio_notify_send_ns"),
            brio_first_vrp_send_ns=brio_timing.get("brio_first_vrp_send_ns"),
            send_definition="BRIO monotonic timestamp before the first successful Prefix PDU send call",
        )
        result["unavailable"]["rtr_send_to_query_complete_ms"] = "No verified first Prefix PDU send timestamp"
    if not required:
        result["unavailable"]["vrp_install_complete_ns"] = "Not applicable: batch has no net VRP changes"
    elif not complete:
        result["unavailable"]["vrp_install_complete_ns"] = (
            "Incomplete net VRP observations; final-state verification is not an installation timestamp"
        )
    if installed is None:
        result["unavailable"]["post_install_query_ms"] = result["unavailable"]["vrp_install_complete_ns"]
    if updated is None:
        result["unavailable"]["fifo_start_to_query_complete_ms"] = "No target query changes"
        result["unavailable"].setdefault("post_install_query_ms", "No target query changes")
        return result
    result["fifo_start_to_query_complete_ms"] = (updated - submission["write_start_ns"]) / 1e6
    if brio_timing is not None and brio_timing.get("brio_first_vrp_send_ns") is not None:
        first_send = brio_timing["brio_first_vrp_send_ns"]
        if updated >= first_send:
            result["rtr_send_to_query_complete_ms"] = (updated - first_send) / 1e6
            result["unavailable"].pop("rtr_send_to_query_complete_ms", None)
        else:
            result["unavailable"]["rtr_send_to_query_complete_ms"] = (
                "Query observation precedes the first Prefix PDU send"
            )
    if installed is not None:
        if updated < installed:
            result["unavailable"]["post_install_query_ms"] = "Cross-stream observation order produced a negative difference"
        else:
            result["post_install_query_ms"] = (updated - installed) / 1e6
    return result
