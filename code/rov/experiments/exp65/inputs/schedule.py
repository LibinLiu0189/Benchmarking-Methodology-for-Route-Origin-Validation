"""Define experiment 65's fixed cadence and minimum operations per batch."""

import math


# Bind batch size and the load window without deriving cadence from pressure.
def workload_schedule(batch_size=100, duration_seconds=300, batch_interval_seconds=1,
                      operations=None):
    if type(batch_size) is not int or not 1 <= batch_size <= 10000:
        raise ValueError("Batch operation target must be an integer from 1 to 10000")
    for label, value in (("Duration", duration_seconds), ("Batch interval", batch_interval_seconds)):
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
            raise ValueError(f"{label} must be finite and positive")
    if batch_interval_seconds < 1e-9:
        raise ValueError("Batch interval must be at least one nanosecond")
    count = duration_seconds / batch_interval_seconds
    if not math.isfinite(count) or count < 1 or not math.isclose(count, round(count), rel_tol=0, abs_tol=1e-9):
        raise ValueError("Duration must be a positive integer multiple of the batch interval")
    batch_count = round(count)
    expected_operations = batch_count * batch_size
    if operations is not None and (type(operations) is not int or operations != expected_operations):
        raise ValueError("Nominal operation target must equal batch_size * duration_seconds / batch_interval_seconds")
    return dict(batch_size=batch_size, operations=expected_operations,
                duration_seconds=duration_seconds, batch_interval_seconds=batch_interval_seconds,
                batch_count=batch_count)


# Reject legacy frequency-driven inputs instead of silently changing their meaning.
def scenario_schedule(scenario):
    required = ("batch_size", "operations", "duration_seconds", "batch_interval_seconds", "batch_count")
    if any(key not in scenario for key in required):
        raise ValueError("Scenario lacks an explicit fixed-cadence schedule; generate a new exp65 input, preserving old data")
    schedule = workload_schedule(scenario["batch_size"], scenario["duration_seconds"],
                                 scenario["batch_interval_seconds"])
    if type(scenario["batch_count"]) is not int or scenario["batch_count"] != schedule["batch_count"]:
        raise ValueError("Scenario batch count disagrees with its load window")
    actual_operations = scenario["operations"]
    if type(actual_operations) is not int or actual_operations < schedule["operations"]:
        raise ValueError("Actual operations must reach the nominal operation target")
    schedule["operations"] = actual_operations
    return schedule
