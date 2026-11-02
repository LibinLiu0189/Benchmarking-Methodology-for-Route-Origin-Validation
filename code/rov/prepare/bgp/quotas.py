"""Allocate a distinct-prefix sample across IPv4 and IPv6."""

# Allocate the total by distinct baseline prefixes, retaining both address families.
def afi_quotas(count, population):
    if count < 2:
        raise ValueError("At least two prefixes are required to include IPv4 and IPv6")
    if any(population.get(afi, 0) == 0 for afi in (4, 6)):
        raise ValueError("Input BGP data must contain both IPv4 and IPv6 prefixes")
    total = sum(population[afi] for afi in (4, 6))
    if count > total:
        raise ValueError(f"Need {count} distinct prefixes; input contains only {total}")
    quotas = {afi: count * population[afi] // total for afi in (4, 6)}
    order = sorted(quotas, key=lambda afi: (-(count * population[afi] % total), afi))
    for afi in order[:count - sum(quotas.values())]:
        quotas[afi] += 1
    for afi in (4, 6):
        if quotas[afi] == 0:
            quotas[afi] = 1
            other = 6 if afi == 4 else 4
            quotas[other] -= 1
    return quotas
