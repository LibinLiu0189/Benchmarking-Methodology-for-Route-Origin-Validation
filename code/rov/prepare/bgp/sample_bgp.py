"""Sample MRT by AFI and peer after enforcing the local BIRD AS_PATH text limit."""

from pathlib import Path

import argparse
from collections import Counter
import hashlib
import json
import random
import struct
import tempfile

from rov.data.mrt import mrt_blocks
from rov.prepare.bgp.path_limit import MAX_AS_PATH_TEXT_BYTES
from rov.prepare.bgp.path_limit import filter_report
from rov.prepare.bgp.path_limit import path_segments
from rov.prepare.bgp.path_limit import path_text_length


# Read the route origin from four-octet AS_PATH attributes.
def candidate_origin_as(raw):
    origin = 0
    for kind, asns in path_segments(raw):
        origin = asns[-1] if kind in (2, 3) else 0
    return origin


# Allocate an exact sample size across peers using minimum quotas and
# proportional largest-remainder allocation.
def quotas(populations, target, minimum):
    counts = {key: n for key, n in populations.items() if n}
    if target < 0 or minimum < 0:
        raise ValueError("Sample size and minimum quota must not be negative")
    if target > sum(counts.values()):
        raise ValueError("Sample size exceeds the eligible candidate count")
    if not target:
        return dict.fromkeys(counts, 0)
    result = {key: min(minimum, n) for key, n in counts.items()}
    if sum(result.values()) > target:
        raise ValueError(
            "Sample size is too small for --min-per-peer; explicitly set it to 0 if needed"
        )
    remaining = target - sum(result.values())
    capacity = {k: counts[k] - result[k] for k in counts}
    denom = sum(capacity.values())
    if remaining:
        additions = {k: remaining * v // denom for k, v in capacity.items()}
        order = sorted(counts, key=lambda k: (-(remaining * capacity[k] % denom), k))
        for k in order[: remaining - sum(additions.values())]:
            additions[k] += 1
        result = {k: result[k] + additions[k] for k in counts}
    return result


# Count candidates, allocate proportional quotas, and copy selected MRT entries.
def sample(source, output, ipv4, ipv6, seed, minimum=0, exclude_as0=False):
    source = Path(source).resolve()
    output = Path(output).resolve()
    if output.exists():
        raise ValueError(f"Output already exists: {output}; specify a new file")
    if output.suffix != ".mrt":
        raise ValueError("Output must be an uncompressed .mrt file")
    if ipv4 < 0 or ipv6 < 0 or minimum < 0:
        raise ValueError("Sample sizes and minimum quotas must not be negative")

    counts = {4: Counter(), 6: Counter()}
    input_count = 0
    excluded_count = 0
    dropped = Counter()
    affected = {4: set(), 6: set()}
    retained = {4: set(), 6: set()}
    print("Counting candidates in the source MRT snapshot.", flush=True)
    for _, sub, body, entries in mrt_blocks(source):
        if sub == 1:
            continue
        afi = 4 if sub == 2 else 6
        prefix_key = body[4:5 + (body[4] + 7) // 8]
        for index, start, end in entries:
            input_count += 1
            if exclude_as0 and candidate_origin_as(body[start + 8 : end]) == 0:
                excluded_count += 1
                continue
            if path_text_length(body[start + 8:end]) > MAX_AS_PATH_TEXT_BYTES:
                dropped[afi] += 1
                affected[afi].add(prefix_key)
                continue
            retained[afi].add(prefix_key)
            counts[afi][index] += 1

    print("AS_PATH filter: " + json.dumps(filter_report(dropped, affected, retained)),
          flush=True)
    print(
        f"Candidates: input {input_count}, excluded origin AS0 {excluded_count}, "
        f"eligible {input_count - excluded_count - sum(dropped.values())}",
        flush=True,
    )

    allocation = {
        afi: quotas(counts[afi], size, minimum) for afi, size in ((4, ipv4), (6, ipv6))
    }
    selected = {}
    next_selected = {}
    for afi in (4, 6):
        for index, count in counts[afi].items():
            size = allocation[afi][index]
            group_seed = int.from_bytes(
                hashlib.sha256(f"{seed}:{afi}:peer_{index:03}".encode()).digest(), "big"
            )
            positions = (
                range(count)
                if size == count
                else sorted(random.Random(group_seed).sample(range(count), size))
            )
            key = (afi, index)
            selected[key] = iter(positions)
            next_selected[key] = next(selected[key], None)

    visited = Counter()
    written = Counter()
    output.parent.mkdir(parents=True, exist_ok=True)
    print("Copying selected candidates to the sampled MRT file.", flush=True)
    with tempfile.TemporaryDirectory(dir=output.parent, prefix=".sample-") as tmp:
        stage = Path(tmp) / "sample.mrt"
        with stage.open("wb") as stream:
            for timestamp, sub, body, entries in mrt_blocks(source):
                if sub != 1:
                    afi = 4 if sub == 2 else 6
                    kept = []
                    for index, start, end in entries:
                        if exclude_as0 and candidate_origin_as(body[start + 8 : end]) == 0:
                            continue

                        if path_text_length(body[start + 8:end]) > MAX_AS_PATH_TEXT_BYTES:
                            continue

                        key = (afi, index)
                        ordinal = visited[key]
                        visited[key] += 1
                        if ordinal == next_selected[key]:
                            kept.append(body[start:end])
                            written[key] += 1
                            next_selected[key] = next(selected[key], None)
                    if not kept:
                        continue
                    count_offset = 5 + (body[4] + 7) // 8
                    body = (
                        body[:count_offset]
                        + struct.pack("!H", len(kept))
                        + b"".join(kept)
                    )
                stream.write(struct.pack("!IHHI", timestamp, 13, sub, len(body)))
                stream.write(body)
        for afi in (4, 6):
            for index, count in counts[afi].items():
                key = (afi, index)
                if visited[key] != count or written[key] != allocation[afi][index]:
                    raise ValueError(
                        "Source candidate counts changed between sampling passes"
                    )
        stage.rename(output)
    print(f"Sampling complete: {output}; IPv4={ipv4:,}, IPv6={ipv6:,}; seed={seed}")


# Parse sampling arguments and generate the selected BGP dataset.
def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--input", required=True, type=Path)
    p.add_argument("--output", required=True, type=Path)
    p.add_argument("--ipv4", required=True, type=int)
    p.add_argument("--ipv6", required=True, type=int)
    p.add_argument("--seed", required=True, type=int)
    p.add_argument("--min-per-peer", type=int, default=0)
    p.add_argument(
        "--exclude-as0", action="store_true",
        help="Exclude candidates whose origin AS is 0 before sampling",
    )
    a = p.parse_args()
    sample(
        a.input, a.output, a.ipv4, a.ipv6, a.seed,
        a.min_per_peer, a.exclude_as0,
    )


if __name__ == "__main__":
    main()
