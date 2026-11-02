"""Summarize only the saved runs reported in Appendix B of the retained draft."""

import argparse
from collections import defaultdict
import csv
import json
from pathlib import Path
from statistics import mean, stdev

from rov.paths import ROOT
from rov.report.statistics import percentile


SCALES = ("50k", "100k", "500k", "1m")
RESULTS = ROOT / "results"
FIELDS = ("experiment", "condition", "metric", "unit", "sample_unit", "n",
          "mean", "sd", "minimum", "maximum", "p95")


# Read an archived successful run without following historical absolute paths.
def read_result(path):
    result = json.loads(path.read_text())
    if result["status"] != "success":
        raise ValueError(f"The retained draft run did not succeed: {path}")
    return result


# Retain unavailable metrics as empty groups instead of converting them to zero.
def record(groups, experiment, condition, metric, value, unit="ms", sample_unit="run"):
    values = groups[(experiment, condition, metric, unit, sample_unit)]
    if value is not None:
        values.append(value)


# Use the BRIO first-Prefix origin already calculated by the 6.1 collector.
def experiment61(groups):
    for scale in SCALES:
        for mode in ("incremental", "full"):
            directory = RESULTS / "exp61" / scale
            if mode == "full":
                directory /= "full"
            for path in sorted(directory.glob("run-*.json")):
                read_result(path)
                reports = []
                with path.with_suffix(".log").open() as stream:
                    for line in stream:
                        line = line.removeprefix(">> ")
                        if line.startswith("BRIO timing: "):
                            reports.append(json.loads(line.removeprefix("BRIO timing: ")))
                if len(reports) != 1:
                    raise ValueError(f"Expected one saved BRIO timing record: {path}")
                record(groups, "6.1", f"{scale}/{mode}", "vrp_installation",
                       reports[0]["last_from_brio_ms"])


# Read the saved post-installation ROV endpoint for each locality and prefix count.
def experiment62(groups):
    for count in (100, 500, 1000):
        for locality in ("localized", "distributed"):
            directory = RESULTS / "exp62" / f"prefixes-{count:06d}" / locality
            for path in sorted(directory.glob("run-*/result.json")):
                result = read_result(path)
                record(groups, "6.2", f"{count}/{locality}", "post_install_rov",
                       result["batch"]["timing"]["post_install_query_ms"])


# Read the two saved joint RIB/FIB timers for the distributed 1,000-prefix input.
def experiment63a(groups):
    for path in sorted((RESULTS / "exp63a").glob("run-*/result.json")):
        timing = read_result(path)["batches"][0]["timing"]
        for metric, key in (("end_to_end_rib_fib", "joint_last_change_from_send_ms"),
                            ("post_install_rib_fib", "joint_last_change_post_install_ms")):
            record(groups, "6.3A", "1000/distributed", metric, timing[key])


# Keep enabled and disabled results separate for each of the six BGP workloads.
def experiment63b(groups):
    kinds = ("mixed", "withdraw_best", "add_valid", "add_invalid",
             "invalid_to_valid", "best_to_invalid")
    for kind in kinds:
        for policy in ("on", "off"):
            directory = RESULTS / "exp63b" / kind / policy
            for path in sorted(directory.glob("run-*/result.json")):
                result = read_result(path)
                record(groups, "6.3B", f"{kind}/{policy}", "rib_fib_last_change",
                       result["batches"][0]["last_change_ms"])


# Use boundary CPU and steady-state RSS with the same units as the scalability table.
def experiment64(groups):
    for scale in SCALES:
        for path in sorted((RESULTS / "exp64" / scale).glob("run-*/result.json")):
            result = read_result(path)
            record(groups, "6.4", scale, "initial_full_sync", result["full_sync_ms"])
            metrics = result["batches"][0]["metrics"]
            record(groups, "6.4", scale, "vrp_installation", metrics["last_vrp_ms"])
            record(groups, "6.4", scale, "post_install_rov", metrics["post_install_query_ms"])
            boundary = result["resources"]["sync_boundary"]
            record(groups, "6.4", scale, "sync_bird_cpu",
                   boundary["bird_cpu_average"] * result["cpu_count"], "%")
            rss = result["resources"]["steady"]["bird_memory_rss"]["average"]
            record(groups, "6.4", scale, "steady_bird_rss", rss / 2**20, "MiB")


# Read the actual target-addition events used to select a complete monitoring round.
def target_additions(directory):
    additions = {}
    with (directory / "monitor-records.jsonl").open() as stream:
        for line in stream:
            if '"event":"batch_targets_added"' in line:
                event = json.loads(line)
                additions[event["batch"]] = event["observed_ns"]
    return additions


# Select the first complete round beginning after each monitored batch adds targets.
def monitoring_rounds(groups, condition, result, directory, component):
    monitoring = result["churn"]["batch_monitoring"]
    rounds = monitoring[component + "_round_timings"]
    count_key = "queried_count" if component == "rov" else "probed_count"
    for added in target_additions(directory).values():
        observed = next((row for row in rounds
                         if row["complete"] and row["started_ns"] >= added), None)
        if observed is not None:
            record(groups, "6.5", condition, component + "_first_round_duration",
                   observed["duration_ms"], sample_unit="selected batch")
            record(groups, "6.5", condition, component + "_first_round_targets",
                   observed[count_key], "targets", "selected batch")


# Keep split ROV and routing measurements distinct and pool only the published VRP cohort.
def experiment65(groups):
    for size in (100, 500, 1000):
        continuous = RESULTS / "exp65/continuous" / f"batch{size}"
        components = (("vrp-rov", (("rov", "roa_query_complete"),)),
                      ("vrp-rib-fib", (("rib", "rib_complete"),
                                       ("fib", "fib_probe_complete"))))
        for mode, metrics in components:
            directory = continuous / mode / "run-01"
            result = read_result(directory / "result.json")
            condition = f"{size}/continuous"
            for batch in result["churn"]["batch_metrics"]:
                if batch["batch"] % 10:
                    continue
                for name, key in metrics:
                    endpoints = batch.get("observed_endpoints_ns", {})
                    installed, completed = endpoints.get("last_vrp"), endpoints.get(key)
                    value = ((completed - installed) / 1e6
                             if installed is not None and completed is not None
                             and completed >= installed else None)
                    record(groups, "6.5", condition, "post_install_" + name,
                           value, sample_unit="selected batch")
            monitoring_rounds(groups, condition, result, directory,
                              "rov" if mode == "vrp-rov" else "fib")

        for mode in ("vrp-installation", "vrp-rib-fib"):
            result = read_result(continuous / mode / "run-01/result.json")
            batches = result["churn"]["batch_metrics"]
            notifications = {}
            for batch in batches:
                end = batch.get("observed_endpoints_ns", {}).get("last_vrp")
                notify = batch.get("brio_timing", {}).get("brio_notify_send_ns")
                notifications[batch["batch"]] = notify
                value = (end - notify) / 1e6 if end is not None and notify is not None else None
                record(groups, "6.5", f"{size}/continuous", "vrp_from_notify", value,
                       sample_unit="batch across two runs")
            for first, last in ((1, 100), (100, 200), (200, 300)):
                for number in range(first + 1, last + 1):
                    before, after = notifications.get(number - 1), notifications.get(number)
                    if before is not None and after is not None:
                        record(groups, "6.5", f"{size}/notify/{first}-{last}", "notify_interval",
                               (after - before) / 1e9, "s", "adjacent interval across two runs")

        for mode, metrics in components:
            directory = RESULTS / "exp65/single-batch" / f"batch{size}" / mode
            for path in sorted(directory.glob("batch-*/run-*/result.json")):
                result = read_result(path)
                batch = result["churn"]["batch_metrics"][0]
                condition = f"{size}/single-batch"
                for name, key in metrics:
                    endpoints = batch.get("observed_endpoints_ns", {})
                    installed, completed = endpoints.get("last_vrp"), endpoints.get(key)
                    value = ((completed - installed) / 1e6
                             if installed is not None and completed is not None
                             and completed >= installed else None)
                    record(groups, "6.5", condition, "post_install_" + name,
                           value, sample_unit="isolated selected batch")
                if mode == "vrp-rib-fib":
                    end = batch.get("observed_endpoints_ns", {}).get("last_vrp")
                    notify = batch.get("brio_timing", {}).get("brio_notify_send_ns")
                    value = (end - notify) / 1e6 if end is not None and notify is not None else None
                    record(groups, "6.5", condition, "vrp_from_notify", value,
                           sample_unit="isolated selected batch")
                monitoring_rounds(groups, condition, result, path.parent,
                                  "rov" if mode == "vrp-rov" else "fib")


# Convert saved CPU capacity shares to one-core percentages and memory bytes to MiB.
def experiment66(groups):
    for path in sorted((RESULTS / "exp66/500k").glob("run-*/result.json")):
        result = read_result(path)
        for phase in ("sync", "steady", "churn_and_rov_convergence"):
            resources = result["resources"][phase]
            for name in ("system_cpu", "bird_cpu"):
                value = (result["resources"]["sync_boundary"][name + "_average"]
                         if phase == "sync" else resources[name]["average"])
                record(groups, "6.6", phase, name, value * result["cpu_count"], "%")
            for name in ("memory_used", "bird_memory_rss"):
                for statistic in ("average", "maximum"):
                    value = resources[name][statistic]
                    record(groups, "6.6", phase, name + "_" + statistic,
                           value / 2**20, "MiB")


# Read the separately defined recovery, Cache Reset and malformed-update endpoints.
def experiment67(groups):
    for path in sorted((RESULTS / "exp67/scenario1/500k").glob("run-*/result.json")):
        recovery = read_result(path)["recovery"]
        for key in ("transport_reconnect_ms", "session_recovery_ms", "vrp_recovery_ms",
                    "validation_recovery_ms", "post_install_rib_sampled_fib_ms"):
            record(groups, "6.7", "scenario1/500k", key, recovery[key])
    for scale in SCALES:
        for path in sorted((RESULTS / "exp67/scenario3" / scale).glob("run-*/result.json")):
            recovery = read_result(path)["recovery"]
            for key in ("full_reload_ms", "rib_sampled_fib_from_send_ms",
                        "post_install_rib_sampled_fib_ms"):
                record(groups, "6.7", f"scenario3/{scale}", key, recovery[key])
    for mode in ("normal", "mixed"):
        for path in sorted((RESULTS / "exp67/scenario4/500k" / mode).glob("run-*/result.json")):
            result = read_result(path)
            condition = f"scenario4/500k/{mode}"
            if mode == "normal":
                for key in ("first_prefix_to_install_ms", "roa_query_complete_ms", "rib_and_sampled_fib_ms"):
                    record(groups, "6.7", condition, key, result["timing"][key])
            else:
                record(groups, "6.7", condition, "error_report_received",
                       int(bool(result["transport"]["error_reports"])), "fraction")
                record(groups, "6.7", condition, "connection_closed",
                       int(result["transport"]["dut_closed"]), "fraction")
                record(groups, "6.7", condition, "authorization_cleanup",
                       int(result["all_baseline_vrps_observed_removed"]), "fraction")
            record(groups, "6.7", condition, "final_vrp_correct",
                   int(result["full_vrp_verification"]["correct"]), "fraction")


# Write one statistics table using run-level or batch-level samples as explicitly labelled.
def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()
    groups = defaultdict(list)
    for collect in (experiment61, experiment62, experiment63a, experiment63b,
                    experiment64, experiment65, experiment66, experiment67):
        collect(groups)
    destination = ROOT / "result.csv"
    with destination.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=FIELDS)
        writer.writeheader()
        for key, values in groups.items():
            row = dict(zip(FIELDS[:5], key))
            row["n"] = len(values)
            if values:
                row.update(mean=mean(values), sd=stdev(values) if len(values) > 1 else None,
                           minimum=min(values), maximum=max(values), p95=percentile(values))
            writer.writerow(row)
    print(f"Draft result statistics: {destination}")


if __name__ == "__main__":
    main()
