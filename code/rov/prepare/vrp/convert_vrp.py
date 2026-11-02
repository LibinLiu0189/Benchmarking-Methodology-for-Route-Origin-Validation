"""Convert a VRP CSV/JSON snapshot to prefix maxLength ASN text accepted by BRIO append."""

from pathlib import Path

import argparse
import tempfile
import os

from rov.data.bgp import load_bgp
from rov.data.bgp import read_raw
from rov.data.vrp import load_vrps
from rov.predict.preprocess import Prefixes
from .filter import filter_vrps


# Convert a VRP snapshot into BRIO text and publish the file after writing succeeds.
def convert(source, output, exclude_ipv6=False, exclude_as0=False, bgp=None):
    source = Path(source).resolve()
    output = Path(output).resolve()
    if source == output:
        raise ValueError("Output must not overwrite the input snapshot")
    values = filter_vrps(load_vrps(source), exclude_ipv6, exclude_as0)
    if bgp is not None:
        metadata = load_bgp(bgp)
        prefixes = {
            prefix
            for peer in metadata["peers"]
            for prefix, _, _ in read_raw(Path(bgp) / peer["file"])
        }
        index = Prefixes(prefixes)
        values = [row for row in values if index.has_covered(row[0])]

    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", dir=output.parent, delete=False) as stream:
        temp = Path(stream.name)
        try:
            for prefix, maximum, asn in values:
                stream.write(f"{prefix} {maximum} {asn}\n")
            stream.close()
            os.replace(temp, output)
        finally:
            temp.unlink(missing_ok=True)
    print(
        f'VRP conversion complete: {output}; IPv4={sum(":" not in p for p,_,_ in values):,}, IPv6={sum(":" in p for p,_,_ in values):,}'
    )


# Parse input and output paths and invoke the VRP converter.
def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--input", required=True, type=Path)
    p.add_argument("--output", required=True, type=Path)
    p.add_argument(
        "--bgp", type=Path,
        help="Keep VRPs covering a prefix in a converted BGP directory",
    )
    p.add_argument(
        "--exclude-ipv6", action="store_true",
        help="Exclude IPv6 VRPs from the output",
    )
    p.add_argument(
        "--exclude-as0", action="store_true",
        help="Exclude VRPs with ASN 0 from the output",
    )
    a = p.parse_args()
    convert(a.input, a.output, a.exclude_ipv6, a.exclude_as0, a.bgp)


if __name__ == "__main__":
    main()
