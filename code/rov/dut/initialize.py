"""Initialize a BGP baseline using BRIO, BIRD, and Bngblaster."""

import argparse
import fcntl
import ipaddress as ip
import os
from pathlib import Path
from rov.paths import CONFIG, ROOT
import subprocess
import sys
import tempfile

from rov.data.vrp import load_vrp_text

from .output import MANAGED_OUTPUT, supervise
from .start import serve
from .virtualize import describe_network, enter_namespace, require_isolation, setup_network


# Count prepared VRPs by address family without changing the input file.
def count_vrps(path):
    rows = load_vrp_text(path)
    counts = {"4": 0, "6": 0}
    for prefix, maximum, asn in rows:
        if asn == 0:
            raise ValueError("Prepare VRPs with --exclude-as0 before initialization")
        counts[str(ip.ip_network(prefix, strict=True).version)] += 1
    return counts


# Load inputs, prepare the fixed configuration directory, and start initialization.
def initialize(args):
    require_isolation()
    settings = {
        "bin_dir": str(args.bin_dir.resolve()),
        "dut_asn": args.dut_asn,
        "router_id": args.router_id,
        "rtr_port": args.rtr_port,
        "rov": args.rov,
        "roa_trie": args.roa_trie,
        "no_roa_settle": args.no_roa_settle,
        "as_sets": args.as_sets,
        "timeout": args.timeout,
        "measure_full_sync": args.measure_full_sync,
        "bird_trace_log": str(args.bird_trace_log.resolve()) if args.bird_trace_log else None,
        "bird_trace_info": getattr(args, "bird_trace_info", False),
    }
    if not 1 <= args.dut_asn <= 4294967295 or not 1 <= args.rtr_port <= 65535:
        raise ValueError("Invalid DUT ASN or RTR port")
    ip.IPv4Address(args.router_id)
    if args.timeout <= 0:
        raise ValueError("Timeout must be positive")
    for name in ("bird", "birdc", "bngblaster", "brio_rc"):
        binary = args.bin_dir.resolve() / name
        if not binary.is_file() or not os.access(binary, os.X_OK):
            raise ValueError(f"Missing executable: {binary}; run software.build first")
    peers = describe_network(args.bgp, settings)
    if not peers:
        raise ValueError("The BGP input contains no retained peers")
    config_dir = CONFIG
    config_dir.mkdir(parents=True, exist_ok=True)
    vrp_file = args.vrp.resolve()
    vrp_counts = count_vrps(vrp_file)
    endpoint_count = sum(len(peer["endpoints"]) for peer in peers)
    print(
        f"BGP peers: {len(peers)}; receiving endpoints: {endpoint_count}; "
        f"IPv4 VRPs: {vrp_counts['4']}; IPv6 VRPs: {vrp_counts['6']}",
        flush=True,
    )
    # Own the fixed configuration directory before changing the network or files.
    config_fd = os.open(config_dir, os.O_RDONLY | os.O_DIRECTORY)
    try:
        try:
            fcntl.flock(config_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError("Initialization is already using the configuration directory") from None
        occupied = [
            name for name in ("brio.in", "bird.ctl", "bird.pid", "bng.sock")
            if os.path.lexists(config_dir / name)
        ]
        if occupied:
            raise RuntimeError(f"Initialization control files already exist: {', '.join(occupied)}")

        setup_network(peers)
        serve(settings, peers, config_dir, vrp_file, vrp_counts)
    finally:
        os.close(config_fd)


# Parse only baseline input, runtime location, and explicit DUT policy parameters.
def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--bgp", required=True, type=Path,
        help="Converted BGP directory containing bgp.json and RAW files",
    )
    parser.add_argument("--vrp", required=True, type=Path, help="Converted BRIO .vrps file")
    parser.add_argument("--bin-dir", type=Path, default=Path("software/bin"))
    parser.add_argument("--dut-asn", type=int, default=65000)
    parser.add_argument("--router-id", default="192.0.2.1")
    parser.add_argument("--rtr-port", type=int, default=3323)
    parser.add_argument(
        "--rov", choices=("on", "off"), default="on",
        help="Enable or disable import-time ROV; RTR and VRP loading remain enabled",
    )
    parser.add_argument(
        "--roa-trie", action="store_true",
        help="Enable a prefix trie for both BIRD ROA tables (default: disabled)",
    )
    parser.add_argument(
        "--no-roa-settle", action="store_true",
        help="Disable ROA table settle delays before RPKI-triggered route revalidation",
    )
    parser.add_argument("--as-sets", choices=("accept", "reject"), default="reject")
    parser.add_argument(
        "--timeout", type=float, default=600,
        help="Maximum seconds for each initial loading stage",
    )
    parser.add_argument(
        "--measure-full-sync", action="store_true",
        help="Record initial installation endpoints; BRIO_VRP_TIMING_LOG supplies the first Prefix-PDU send origin",
    )
    parser.add_argument(
        "--verbose-shutdown", action="store_true",
        help="Display normal software shutdown output as well as errors",
    )
    parser.add_argument("--bird-trace-log", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--bird-trace-info", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--inside", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if not os.environ.get(MANAGED_OUTPUT):
        log_directory = ROOT / "runtime/logs"
        log_directory.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            mode="wb", prefix="initialize-", suffix=".log",
            dir=log_directory, delete=False,
        ) as log:
            return supervise(
                "rov.dut.initialize", sys.argv[1:], log=log,
                verbose_shutdown=args.verbose_shutdown,
            )
    if not args.inside:
        enter_namespace(sys.argv[1:])
    try:
        initialize(args)
    except KeyboardInterrupt:
        pass
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        print(str(error), file=sys.stderr)
        raise SystemExit(1) from None


if __name__ == "__main__":
    raise SystemExit(main())
