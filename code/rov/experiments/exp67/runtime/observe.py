"""Observe experiment 6.7 sessions separately while reusing native event parsing."""

import threading
import time
from collections import defaultdict

from rov.measure.bird_connection import BirdConnection
from rov.measure.read_log import FileLog, LogParser
from rov.measure.roa_batch import groups
from rov.experiments.exp62.observation.query_observer import query_group


# Preserve actual grouped query replies without calculating online diagnostics.
def start_reset_rov_observer(targets, socket_path, poll_interval, timeout, events, errors):
    stop = threading.Event()
    candidates = defaultdict(list)
    for target in targets.values():
        candidates[(target['prefix'], target['origin_as'])].append(target['peer'])
    queries = list(candidates)
    plan = groups(queries, 8, "pipeline")
    records = []

    # Keep initial readiness and changed-state events identical to the existing monitoring flow.
    def worker():
        previous = {}
        initial = defaultdict(dict)
        connection = None

        # Attribute each prefix/origin query to every candidate sharing that identity.
        def receive_sample(index, value, started_ns, completed_ns):
            key = queries[index]
            known = key in previous
            before = previous.get(key)
            previous[key] = value
            if not known:
                for peer in candidates[key]:
                    initial[key[0]][peer] = dict(
                        rov=value, origin_as=key[1], observed_ns=completed_ns)
                if len(previous) == len(queries):
                    for prefix, states in initial.items():
                        events.put(dict(event='initial', prefix=prefix, candidates=states))
                    events.put(dict(event='ready', component='cli', prefixes=len(initial)))
            elif before != value:
                events.put(dict(event='rov', prefix=key[0], origin_as=key[1],
                                before=before, after=value,
                                query_start_ns=started_ns,
                                query_end_ns=completed_ns, observed_ns=completed_ns))

        try:
            connection = BirdConnection(socket_path, timeout)
            connection.command("restrict")
            while not stop.is_set():
                for group, indices in enumerate(plan):
                    if stop.is_set():
                        break
                    try:
                        values, record = query_group(connection, queries, indices)
                    except BaseException as error:
                        records.append(dict(group=group, end_ns=time.monotonic_ns(), error=repr(error)))
                        raise
                    records.append(dict(group=group, end_ns=record["end_ns"]))
                    for index, value, completed_ns in zip(indices, values, record["response_end_ns"], strict=True):
                        receive_sample(index, value, record["start_ns"], completed_ns)
                if poll_interval > 0 and stop.wait(poll_interval):
                    break
        except BaseException as error:
            errors.put(error)
        finally:
            if connection is not None:
                connection.close()

    thread = threading.Thread(target=worker, name='exp67-reset-rov-observer')
    thread.start()
    return stop, thread, records


# Derive query diagnostics from completed group records after collection has stopped.
def query_observations(targets, records, cutoff_ns=None):
    queries = list(dict.fromkeys((row["prefix"], row["origin_as"]) for row in targets.values()))
    plan = groups(queries, 8, "pipeline")
    rows = [row for row in records if cutoff_ns is None or row["end_ns"] <= cutoff_ns]
    completed = [row for row in rows if "error" not in row]
    full_rounds = sum(row["group"] == len(plan) - 1 for row in completed)
    started_rounds = sum(row["group"] == 0 for row in rows)
    failures = [row["error"] for row in rows if "error" in row]
    result = dict(
        mode="pipeline", requested_group_size=8, target_count=len(queries),
        actual_group_sizes=[len(indices) for indices in plan], groups_per_round=len(plan),
        byte_limit_splits=len(plan) - (len(queries) + 7) // 8,
        complete_rounds=full_rounds, partial_rounds=started_rounds - full_rounds,
        groups_sent=len(completed), command_errors=len(failures),
    )
    if failures:
        result["failure"] = failures[0]
    return result


# Retain the direct session log and report EOF without verifying expected identities online.
def start_observer(socket_path, timeout, events, errors, log_path):
    stop = threading.Event()

    # Follow the dedicated file and disable only debugging enabled by this observer.
    def worker():
        connection = BirdConnection(socket_path, timeout)
        source = None
        enabled = []
        parser = LogParser()
        file_drained = False

        # Invalidate an earlier EOF as soon as the reader finds another record.
        def publish_drained(drained):
            nonlocal file_drained
            if drained or file_drained:
                events.put(dict(event="drained", drained=drained, observed_ns=time.monotonic_ns()))
            file_drained = drained

        # Decode observed records while retaining their actual source protocol.
        def publish(line):
            publish_drained(False)
            text = line.removeprefix('+')
            protocol = 'rpki_source'
            if text == 'rpki_source: All ROAs expired':
                events.put(dict(event='rtr', protocol='rpki_source', before='Transport-Error',
                                after='Expired', observed_ns=time.monotonic_ns()))
                return
            event = parser.parse(text, time.monotonic_ns())
            if event is not None:
                if event['event'] in ('vrp', 'rtr'):
                    event['protocol'] = protocol
                events.put(event)
        try:
            source = FileLog(log_path)
            protocols = [line.split()[0] for line in connection.command('show protocols').splitlines() if line.split()]
            for name in ('rpki_source',):
                connection.command(f'debug {name} {{ routes, events }}')
                enabled.append(name)
            for name in ('fib4', 'fib6'):
                if name in protocols:
                    connection.command(f'debug {name} {{ routes }}')
                    enabled.append(name)
            events.put(dict(event='ready', component='read_log', planes=['vrp', 'rib', 'rtr']))
            while not stop.is_set():
                try:
                    line = source.line(time.monotonic() + .2)
                except TimeoutError:
                    publish_drained(not source.buffer and not parser.pending_rib)
                    continue
                publish(line)
            for name in enabled:
                connection.command(f'debug {name} off')
            enabled.clear()
            while True:
                try:
                    publish(source.line(time.monotonic()))
                except TimeoutError:
                    break
            source.check_complete()
            if parser.pending_rib:
                raise RuntimeError('Unpaired selected-path records at end of session observation')
        except BaseException as error:
            errors.put(error)
        finally:
            if source is not None:
                source.close()
            connection.close()
            cleanup = BirdConnection(socket_path, timeout)
            try:
                for name in enabled:
                    cleanup.command(f'debug {name} off')
            except BaseException as error:
                errors.put(error)
            finally:
                cleanup.close()

    thread = threading.Thread(target=worker, name='exp67-session-log')
    thread.start()
    return stop, thread
