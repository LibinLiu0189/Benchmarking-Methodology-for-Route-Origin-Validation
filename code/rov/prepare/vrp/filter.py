"""Filter VRP categories for conversion and batch preparation."""


# Filter optional VRP categories without changing the order of retained entries.
def filter_vrps(values, exclude_ipv6=False, exclude_as0=False):
    return [
        (prefix, maximum, asn)
        for prefix, maximum, asn in values
        if not (exclude_ipv6 and ":" in prefix)
        and not (exclude_as0 and asn == 0)
    ]
