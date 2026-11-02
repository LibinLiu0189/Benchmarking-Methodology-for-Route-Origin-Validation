"""Pure arithmetic for the offline draft result summary."""


# Calculate a percentile by linear interpolation at position (n - 1) * fraction.
def percentile(values, fraction=0.95):
    values = sorted(values)
    if not values:
        return None
    position = (len(values) - 1) * fraction
    left = int(position)
    right = min(left + 1, len(values) - 1)
    return values[left] + (values[right] - values[left]) * (position - left)
