"""Report raw system and BIRD process resource samples from Linux procfs."""

import os
from pathlib import Path
import time
import threading

from rov.paths import CONFIG


# Read the aggregate CPU counters and separate idle time from total time.
def read_cpu_counters():
    with Path("/proc/stat").open() as source:
        line = source.readline().split()
    if len(line) < 9 or line[0] != "cpu":
        raise ValueError("Missing aggregate CPU counters in /proc/stat")

    counters = [int(value) for value in line[1:9]]
    return sum(counters), counters[3] + counters[4]


# Read total and available system memory in bytes.
def read_memory():
    values = {}
    with Path("/proc/meminfo").open() as source:
        for line in source:
            name, _, amount = line.partition(":")
            if name in ("MemTotal", "MemAvailable"):
                parts = amount.split()
                if len(parts) != 2 or parts[1] != "kB":
                    raise ValueError(f"Invalid {name} value in /proc/meminfo")
                values[name] = int(parts[0]) * 1024

    if "MemTotal" not in values or "MemAvailable" not in values:
        raise ValueError("Missing total or available memory in /proc/meminfo")
    return values["MemTotal"], values["MemAvailable"]


# Read one BIRD process's CPU ticks, resident pages, and creation time.
def read_bird_process(pid):
    stat = Path(f"/proc/{pid}/stat").read_text()
    name_end = stat.rfind(")")
    if name_end < 0 or stat[stat.find("(") + 1:name_end] != "bird":
        raise RuntimeError(f"PID {pid} is not a BIRD process")
    fields = stat[name_end + 1:].split()
    if len(fields) < 22:
        raise ValueError(f"Incomplete process counters for BIRD PID {pid}")
    cpu_ticks = int(fields[11]) + int(fields[12])
    start_ticks = int(fields[19])
    resident_pages = int(fields[21])
    return cpu_ticks, start_ticks, resident_pages


# Sample CPU with the same aggregate-counter percentage used by Section 6.4.
def observe_cpu(interval, bird_pid, on_sample, stop_event, ready_event):
    previous_total, previous_idle = read_cpu_counters()
    previous_bird_ticks = None
    bird_start_ticks = None
    if bird_pid is not None:
        try:
            previous_bird_ticks, bird_start_ticks, _ = read_bird_process(bird_pid)
        except (OSError, ValueError, RuntimeError):
            bird_pid = None
    ready_event.set()

    while not stop_event.wait(interval):
        total, idle = read_cpu_counters()
        elapsed = total - previous_total
        if elapsed <= 0:
            raise RuntimeError("Aggregate CPU counters did not advance")
        system_cpu = 100 * (elapsed - (idle - previous_idle)) / elapsed
        bird_cpu = None
        if bird_pid is not None:
            try:
                bird_ticks, current_start_ticks, _ = read_bird_process(bird_pid)
                if current_start_ticks != bird_start_ticks:
                    raise RuntimeError(f"BIRD PID {bird_pid} changed during observation")
                bird_cpu = 100 * (bird_ticks - previous_bird_ticks) / elapsed
                previous_bird_ticks = bird_ticks
            except (OSError, ValueError, RuntimeError):
                bird_pid = None

        on_sample(dict(
            observed_ns=time.monotonic_ns(),
            system_cpu=system_cpu,
            bird_cpu=bird_cpu,
        ))
        previous_total, previous_idle = total, idle


# Summarize actual resource samples without treating them as independent runs.
def resource_summary(samples):
    result = {"sample_count": len(samples)}
    for field in ("system_cpu", "bird_cpu", "memory_used", "bird_memory_rss"):
        values = [row[field] for row in samples if row.get(field) is not None]
        ordered = sorted(values)
        position = (len(ordered) - 1) * .95 if ordered else 0
        left = int(position)
        p95 = (ordered[left] + (ordered[min(left + 1, len(ordered) - 1)] - ordered[left]) * (position - left)) if ordered else None
        result[field] = dict(available_samples=len(values), minimum=min(values) if values else None,
                             average=sum(values) / len(values) if values else None,
                             maximum=max(values) if values else None,
                             p95=p95)
    return result


# Collect host and optional BIRD counters without adding events to the quiet clock.
class ResourceRecorder:
    # Keep raw samples, source identity, and sampling failures for the report.
    def __init__(self, interval, pid_path):
        self.interval = interval
        self.pid_path = Path(pid_path)
        self.samples = []
        self.errors = []
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self.observe, name="exp64-resources")

    # Retry an unavailable process and never substitute process CPU for host CPU.
    def observe(self):
        previous_process = None
        try:
            previous_total, previous_idle = read_cpu_counters()
            while not self.stop.wait(self.interval):
                total, idle = read_cpu_counters()
                memory_total, memory_available = read_memory()
                elapsed = total - previous_total
                if elapsed <= 0:
                    continue
                row = dict(observed_ns=time.monotonic_ns(),
                           system_cpu=100 * (elapsed - idle + previous_idle) / elapsed,
                           memory_total=memory_total, memory_available=memory_available,
                           memory_used=memory_total - memory_available,
                           bird_cpu=None, bird_memory_rss=None, bird_unavailable=None)
                try:
                    pid = int(self.pid_path.read_text().strip())
                    ticks, created, pages = read_bird_process(pid)
                    row["bird_memory_rss"] = pages * os.sysconf("SC_PAGE_SIZE")
                    if previous_process and previous_process[:2] == (pid, created):
                        row["bird_cpu"] = 100 * (ticks - previous_process[2]) / elapsed
                    else:
                        row["bird_unavailable"] = "No previous sample for this process identity"
                    previous_process = (pid, created, ticks)
                except (OSError, ValueError, RuntimeError) as error:
                    previous_process = None
                    row["bird_unavailable"] = str(error)
                self.samples.append(row)
                previous_total, previous_idle = total, idle
        except BaseException as error:
            self.errors.append(f"{type(error).__name__}: {error}")

    # Start sampling before initialization so system sync load is observable.
    def start(self):
        self.thread.start()

    # Stop this sampler without touching any DUT service.
    def close(self):
        self.stop.set()
        self.thread.join()


# Capture boundary counters when a synchronization interval is shorter than sampling.
def sync_snapshot():
    total, idle = read_cpu_counters()
    memory_total, available = read_memory()
    row = dict(observed_ns=time.monotonic_ns(), total_ticks=total, idle_ticks=idle,
               memory_used=memory_total-available, bird_cpu_ticks=None, bird_memory_rss=None)
    try:
        pid = int((CONFIG / "bird.pid").read_text().strip())
        ticks, created, pages = read_bird_process(pid)
        row.update(bird_pid=pid, bird_created=created, bird_cpu_ticks=ticks,
                   bird_memory_rss=pages*os.sysconf("SC_PAGE_SIZE"))
    except (OSError, ValueError, RuntimeError) as error:
        row["bird_unavailable"] = str(error)
    return row


# Compute a boundary-average CPU value without inventing sub-interval peaks.
def sync_boundary_summary(result):
    first, last = result.get("sync_start_counters"), result.get("sync_end_counters")
    if first is None or last is None:
        return dict(available=False, reason="Missing synchronization boundary counters")
    elapsed = last["total_ticks"] - first["total_ticks"]
    if elapsed <= 0:
        return dict(available=False, reason="CPU counters did not advance across synchronization")
    summary = dict(available=True, system_cpu_average=100*(elapsed-last["idle_ticks"]+first["idle_ticks"])/elapsed,
                   bird_cpu_average=None, memory_used_end=last["memory_used"],
                   bird_memory_rss_end=last["bird_memory_rss"],
                   observed_interval_ms=(last["observed_ns"]-first["observed_ns"])/1e6,
                   definition="Counter difference across external sync boundaries; not a peak or periodic sample")
    if (first.get("bird_pid"), first.get("bird_created")) == (last.get("bird_pid"), last.get("bird_created")) and first["bird_cpu_ticks"] is not None and last["bird_cpu_ticks"] is not None:
        summary["bird_cpu_average"] = 100*(last["bird_cpu_ticks"]-first["bird_cpu_ticks"])/elapsed
    return summary
