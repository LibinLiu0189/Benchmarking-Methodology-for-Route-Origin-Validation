"""Monitor selected batches' single-transition targets until periodic completion checks."""

import threading
import time

from rov.measure.roa_batch import CountedConnection, groups
from rov.measure.roa_pipeline import query as query_pipeline
from rov.measure.read_log import stop_observer
from rov.measure.probe_transport import ProbeTransport
from rov.experiments.exp65.runtime.completion import CompletionTracker
from rov.experiments.exp65.runtime.target_selection import select_targets


class BatchMonitor:
    # Prepare the retained targets before updates and keep unfinished batches together.
    def __init__(self, state, socket_path, config_dir, pps, poll_interval, timeout,
                 packet_timeout, completion_check_interval, *, query_mode="pipeline",
                 query_group_size=8, monitoring_plan=None):
        self.state = state
        self.socket_path = socket_path
        self.config_dir = config_dir
        self.pps = pps
        self.poll_interval = poll_interval
        self.timeout = min(10, timeout)
        self.packet_timeout = packet_timeout
        self.check_interval_ns = round(completion_check_interval * 1e9)
        self.query_mode = query_mode
        self.query_group_size = query_group_size
        self.components = [name for name in ("rov", "fib") if name in state.monitoring_mode]
        if monitoring_plan is None:
            self.specs, self.selection = select_targets(state)
        else:
            number = monitoring_plan["batch"]
            components = [name for name in ("rov", "rib", "fib") if name in state.monitoring_mode]
            self.specs = {
                batch: {component: {} for component in components}
                for batch in state.vrp.operations
            }
            for component, targets in monitoring_plan["targets"].items():
                if component not in state.monitoring_mode:
                    continue
                self.specs[number][component] = {}
                for target in targets:
                    spec = dict(target)
                    key = tuple(spec["key"]) if component == "rov" else spec["key"]
                    spec["key"] = key
                    if component == "rib":
                        for field in ("before", "after"):
                            if spec[field] is not None:
                                spec[field] = tuple(spec[field])
                    self.specs[number][component][key] = spec
            selection = monitoring_plan["selection"]
            self.selection = dict(
                definition=selection["definition"],
                totals={component: counts for component, counts in selection["totals"].items()
                        if component in state.monitoring_mode},
                by_batch=[dict(
                    batch=batch,
                    **{component: (
                        selection["totals"][component] if batch == number else
                        dict(original=0, retained=0, repeated=0, unavailable=0, unchanged=0)
                    ) for component in components},
                ) for batch in state.vrp.operations],
            )
        self.tracker = CompletionTracker(state, self.specs, packet_timeout)
        self.lock = threading.Lock()
        self.stop = threading.Event()
        self.begun = threading.Event()
        self.completed = threading.Event()
        self.active = {component: {} for component in self.components}
        self.active_lists = {component: () for component in self.components}
        self.submissions = {}
        self.threads = []
        self.checker = None
        self.transport = None
        self.rov_rounds = self.rov_queries = 0
        self.fib_rounds = 0
        self.rov_round_timings = []
        self.fib_round_timings = []
        self.completion_checks = []

    # Queue raw observations without comparing them with expected outcomes.
    def emit(self, event, **fields):
        self.state.events.put(dict(event=event, observed_ns=time.monotonic_ns(), **fields))

    # Create the probe link and independent collection and checkpoint workers.
    def start(self):
        destinations = list(dict.fromkeys(
            key for components in self.specs.values() for key in components.get("fib", {})
        ))
        if destinations:
            self.transport = ProbeTransport(destinations, self.config_dir)
        for component in self.components:
            thread = threading.Thread(
                target=self.worker, args=(component,), name=f"exp65-{component}-collector",
            )
            self.threads.append(thread)
            thread.start()
        self.checker = threading.Thread(target=self.check_worker, name="exp65-completion-check")
        self.checker.start()

    # Start checkpoint cadence at the workload's monotonic origin.
    def begin(self):
        self.begun.set()

    # Add this batch before submission without removing earlier unfinished targets.
    def start_batch(self, number):
        if number not in self.state.monitored_batches:
            return
        with self.lock:
            for component in self.components:
                for key in self.specs[number][component]:
                    self.active[component][(number, key)] = None
                self.active_lists[component] = tuple(self.active[component])
        if self.components:
            self.emit("batch_targets_added", batch=number)

    # Publish the actual FIFO origin without waiting for a sample or checkpoint.
    def submitted(self, number, started_ns):
        with self.lock:
            self.submissions[number] = started_ns

    # Return an immutable target list without copying it for every probe packet.
    def snapshot(self, component):
        with self.lock:
            return self.active_lists[component]

    # Forward worker failures to the experiment owner.
    def worker(self, component):
        try:
            if component == "rov":
                self.run_rov()
            else:
                self.run_fib()
        except BaseException as error:
            self.state.errors.put(error)
            self.emit("collector_error", component=component, error=f"{type(error).__name__}: {error}")

    # Finish each bounded query group before proceeding through the active target set.
    def run_rov(self):
        connection = None
        factory = CountedConnection
        try:
            while not self.stop.is_set():
                targets = self.snapshot("rov")
                if not targets:
                    self.stop.wait(0.02)
                    continue
                round_started_ns = time.monotonic_ns()
                queried_count = 0
                complete = False
                keys = [key for _, key in targets]
                plan = groups(keys, self.query_group_size, "pipeline")
                try:
                    for indices in plan:
                        if self.stop.is_set():
                            break
                        with self.lock:
                            indices = [index for index in indices if targets[index] in self.active["rov"]]
                        if not indices:
                            continue
                        if connection is None:
                            connection = factory(self.socket_path, self.timeout)
                            connection.command("restrict")

                        # Retain ownership and count replies received during this round.
                        def receive(index, value, started_ns, observed_ns):
                            nonlocal queried_count
                            number, key = targets[index]
                            self.emit(
                                "rov_sample", batch=number, key=key, value=value,
                                query_start_ns=started_ns, query_end_ns=observed_ns,
                            )
                            queried_count += 1

                        self.rov_queries += len(indices)
                        query_pipeline(connection, keys, indices, on_response=receive)
                    else:
                        complete = True
                finally:
                    ended_ns = time.monotonic_ns()
                    self.rov_round_timings.append(dict(
                        round=len(self.rov_round_timings) + 1,
                        started_ns=round_started_ns, ended_ns=ended_ns,
                        duration_ms=(ended_ns - round_started_ns) / 1e6,
                        target_count=len(targets), queried_count=queried_count,
                        complete=complete,
                    ))
                with self.lock:
                    self.rov_rounds += 1
                if self.poll_interval:
                    self.stop.wait(self.poll_interval)
        finally:
            if connection is not None:
                connection.close()

    # Share the configured total packet rate across all unfinished FIB targets.
    def run_fib(self):
        if self.transport is None:
            return
        index = 0
        next_send_ns = time.monotonic_ns()
        round_record = None
        pending = set()
        previous_targets = None

        # Save coverage of the starting target set without altering packet selection.
        def finish_round(ended_ns):
            if round_record is not None:
                self.fib_round_timings.append(dict(
                    round=len(self.fib_round_timings) + 1,
                    **round_record, ended_ns=ended_ns,
                    duration_ms=(ended_ns - round_record["started_ns"]) / 1e6,
                    complete=not pending,
                ))
                if not pending:
                    with self.lock:
                        self.fib_rounds += 1

        while not self.stop.is_set():
            targets = self.snapshot("fib")
            now = time.monotonic_ns()
            if round_record is not None and targets is not previous_targets:
                previous_count = len(pending)
                pending.intersection_update(targets)
                round_record["retired_count"] += previous_count - len(pending)
                if not pending:
                    finish_round(now)
                    round_record = None
            previous_targets = targets
            if targets and now >= next_send_ns:
                if round_record is None:
                    pending = set(targets)
                    round_record = dict(
                        started_ns=now, target_count=len(targets),
                        probed_count=0, retired_count=0, packets_sent=0,
                    )
                number, destination = targets[index % len(targets)]
                packet_event = self.transport.send(destination)
                self.emit("probe_packet", batch=number, packet_event=packet_event)
                round_record["packets_sent"] += 1
                if (number, destination) in pending:
                    pending.remove((number, destination))
                    round_record["probed_count"] += 1
                if not pending:
                    finish_round(packet_event["sent_ns"])
                    round_record = None
                index += 1
                next_send_ns = packet_event["sent_ns"] + max(1, round(1e9 / self.pps))
            wait = min(0.02, max(0, (next_send_ns - time.monotonic_ns()) / 1e9)) if targets else 0.02
            for event in self.transport.poll(wait):
                self.emit("probe_packet", packet_event=event)
        finish_round(time.monotonic_ns())

    # Flush only while holding the writer lock, then inspect files without that lock.
    def capture_boundaries(self):
        boundaries = []
        with self.state.capture_lock:
            for name in ("raw_capture", "monitor_capture"):
                stream = getattr(self.state, name, None)
                path = getattr(self.state, name + "_path", None)
                if path is None:
                    continue
                if stream is not None:
                    stream.flush()
                    boundary = stream.tell()
                else:
                    boundary = path.stat().st_size
                boundaries.append((path, boundary))
        return boundaries

    # Periodically inspect saved observations without waiting for a query/probe round.
    def check_worker(self):
        try:
            self.begun.wait()
            if self.stop.is_set():
                return
            next_check_ns = self.state.started_ns + self.check_interval_ns
            while not self.stop.is_set():
                wait = max(0, (next_check_ns - time.monotonic_ns()) / 1e9)
                if self.stop.wait(wait):
                    return
                with self.lock:
                    submissions = dict(self.submissions)
                    checked_round = self.rov_rounds
                    checked_fib_round = self.fib_rounds
                self.tracker.advance(self.capture_boundaries(), submissions, self.state.deadline_ns)
                retired = {component: 0 for component in self.components}
                with self.lock:
                    for component in self.components:
                        for number, key in tuple(self.active[component]):
                            report = self.tracker.reports[(component, number, key)]
                            if report["completed_ns"] is not None:
                                del self.active[component][(number, key)]
                                retired[component] += 1
                        self.active_lists[component] = tuple(self.active[component])
                remaining = self.tracker.remaining()
                checked_ns = time.monotonic_ns()
                self.completion_checks.append(dict(
                    checked_ns=checked_ns, submitted_batches=len(submissions),
                    retired=retired, remaining=remaining, rov_round=checked_round,
                    fib_round=checked_fib_round,
                ))
                print(f"Completion check at ROV/FIB rounds {checked_round}/{checked_fib_round}: remaining {remaining}; retired {retired}", flush=True)
                if len(submissions) == len(self.specs) and not any(remaining.values()):
                    self.completed.set()
                    return
                next_check_ns += self.check_interval_ns
                if next_check_ns <= checked_ns:
                    next_check_ns += ((checked_ns - next_check_ns) // self.check_interval_ns + 1) * self.check_interval_ns
        except BaseException as error:
            self.state.errors.put(error)

    # Stop collection and checking before final interpretation and resource cleanup.
    def close(self):
        self.stop.set()
        self.begun.set()
        failure = None
        for thread in self.threads:
            try:
                stop_observer(self.stop, thread, self.timeout + 1)
            except BaseException as error:
                if failure is None:
                    failure = error
        if self.checker is not None:
            self.checker.join()
        if self.transport is not None:
            try:
                self.transport.close()
            except BaseException as error:
                if failure is None:
                    failure = error
        if failure is not None:
            raise failure

    # Finish the saved observations and expose retained-target metrics for every batch.
    def finish(self, collection_end_ns):
        end_ns = min(collection_end_ns, self.state.deadline_ns)
        self.tracker.advance(self.capture_boundaries(), dict(self.submissions), end_ns, final=True)
        self.state.rov.matched = []
        self.state.rib.matched = []
        self.state.fib.matched = []
        tasks = []
        for number, submitted_ns in self.submissions.items():
            task = dict(batch=number)
            for component, targets in self.specs[number].items():
                reports = []
                for key in targets:
                    report = self.tracker.reports[(component, number, key)]
                    report["submitted_ns"] = submitted_ns
                    reports.append(report)
                    if report["completed_ns"] is not None:
                        matches = getattr(self.state, component).matched
                        count = len(report["peers"]) if component == "rov" else 1
                        matches.extend((number, report["completed_ns"]) for _ in range(count))
                task[component] = dict(targets=reports)
            tasks.append(task)
        result = dict(
            mode="single-transition-continuous-v2",
            selected_batches=[number for number in self.submissions
                              if number in self.state.monitored_batches],
            tasks=tasks,
            completion_check_interval=self.check_interval_ns / 1e9,
            completion_checks=self.completion_checks, collector_errors=self.tracker.collector_errors,
            attribution=(
                "First expected after from a retained single-batch-transition target, "
                "with query/packet start or RIB observation at or after FIFO write start. "
                "Targets persist across batch submissions until a completion checkpoint; "
                "checkpoint times are not latency endpoints."
            ),
        )
        if self.components:
            result["query_records"] = "monitor-records.jsonl"
        if "rov" in self.components:
            result.update(query_mode=self.query_mode, requested_group_size=self.query_group_size,
                          rov_queries_sent=self.rov_queries, rov_rounds=self.rov_rounds,
                          rov_round_timings=self.rov_round_timings,
                          rov_round_definition=(
                              "Active snapshot through query planning and replies; targets retired "
                              "before their group are skipped. Complete means the plan was traversed."
                          ))
        if "fib" in self.components:
            result.update(
                fib_round_timings=self.fib_round_timings,
                fib_round_definition=(
                    "Elapsed time to send at least once to each starting target or retire it. "
                    "New targets retain the existing packet schedule but belong to a later round. "
                    "Probed and retired counts are separate; reception is not awaited."
                ),
                fib_pps_total=self.pps, fib_total_sent=len(self.tracker.packet_starts),
                fib_source_interface=self.transport.source_interface if self.transport else None,
                fib_source_addresses=self.transport.sources if self.transport else {},
                fib_unreceived_packets=self.tracker.unreceived,
                fib_censored_packets=sum(not self.tracker.packet_replies[index] & 2
                                         for index in self.tracker.pending_packets),
                fib_late_receptions=self.tracker.late,
                fib_duplicate_receptions=self.tracker.duplicates,
                packet_timeout_seconds=self.packet_timeout,
                packet_collection_end_ns=end_ns,
                packet_timeout_definition="Per-packet cutoff evaluated from saved records at checkpoints and after collection",
                forwarding_scope="Retained destination forwarding; None means no timely reception",
            )
        return result
