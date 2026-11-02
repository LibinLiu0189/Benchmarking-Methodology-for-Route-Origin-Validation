"""Read and normalize VRP snapshots and ordered BRIO text."""

from pathlib import Path
import csv
import ipaddress as ip
import json


# Validate and normalize one prefix, maximum length, and ASN into a VRP tuple.
def normalize(prefix, maximum, asn):
    net = ip.ip_network(prefix, strict=True)
    maximum = int(maximum)
    asn = int(str(asn).removeprefix("AS"))
    if not net.prefixlen <= maximum <= net.max_prefixlen or not 0 <= asn <= 4294967295:
        raise ValueError(f"Invalid VRP: {prefix} {maximum} {asn}")
    return str(net), maximum, asn


# Read the initial VRPs from the converted BRIO text file.
def load_vrp_text(path):
    rows = []
    with Path(path).open(encoding="utf-8-sig") as stream:
        for line in stream:
            if line.strip():
                rows.append(normalize(*line.split()))
    return rows


# Read CSV/JSON snapshots or BRIO text; deduplicate and sort CSV/JSON records
# while preserving BRIO text order and rejecting duplicate text entries.
def load_vrps(path):
    path = Path(path)
    if path.suffix.lower() not in (".csv", ".json"):
        result = load_vrp_text(path)
        if len(result) != len(set(result)):
            raise ValueError(
                "Duplicate VRPs in BRIO text would make withdrawal indices ambiguous"
            )
        return result
    with path.open(encoding="utf-8-sig", newline="") as stream:
        if path.suffix.lower() == ".csv":
            rows = csv.DictReader(stream)
        else:
            data = json.load(stream)
            rows = data["roas"] if isinstance(data, dict) else data
        result = set()
        for row in rows:
            result.add(
                normalize(
                    row.get("prefix", row.get("IP Prefix")),
                    row.get("maxLength", row.get("max_length", row.get("Max Length"))),
                    row.get("asn", row.get("ASN")),
                )
            )
        return sorted(result)
