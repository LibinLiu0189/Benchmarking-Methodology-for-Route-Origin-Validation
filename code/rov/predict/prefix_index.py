"""Find covering and more-specific prefixes using family-separated indexes."""

from bisect import bisect_left
from bisect import bisect_right
from bisect import insort
import ipaddress as ip


class PrefixIndex:
    # Parse prefixes once and index network addresses by family and length.
    def __init__(self, prefixes):
        self.prefixes = {4: [{} for _ in range(33)], 6: [{} for _ in range(129)]}
        for prefix in prefixes:
            network = ip.ip_network(prefix)
            address = int(network.network_address)
            self.prefixes[network.version][network.prefixlen][address] = str(network)
        self.addresses = {
            family: [sorted(layer) for layer in layers]
            for family, layers in self.prefixes.items()
        }

    # Insert a newly encountered prefix without rebuilding the existing index.
    def add(self, prefix):
        network = ip.ip_network(prefix)
        length = network.prefixlen
        address = int(network.network_address)
        if address not in self.prefixes[network.version][length]:
            self.prefixes[network.version][length][address] = str(network)
            insort(self.addresses[network.version][length], address)

    # Yield existing strict ancestors from the longest prefix to the shortest.
    def covering(self, prefix):
        network = ip.ip_network(prefix)
        address = int(network.network_address)
        for length in range(network.prefixlen - 1, -1, -1):
            host_bits = network.max_prefixlen - length
            parent_address = (address >> host_bits) << host_bits
            parent = self.prefixes[network.version][length].get(parent_address)
            if parent is not None:
                yield parent

    # Yield only existing strict descendants within the target address range.
    def covered(self, prefix):
        network = ip.ip_network(prefix)
        first = int(network.network_address)
        last = int(network.broadcast_address)
        for length in range(network.prefixlen + 1, network.max_prefixlen + 1):
            addresses = self.addresses[network.version][length]
            start = bisect_left(addresses, first)
            end = bisect_right(addresses, last)
            for position in range(start, end):
                address = addresses[position]
                yield self.prefixes[network.version][length][address]
