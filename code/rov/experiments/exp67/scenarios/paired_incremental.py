"""Run one normal or mixed update group for Section 6.7 scenario 4."""

import argparse
from collections import Counter
import fcntl
import gc
import ipaddress as ip
import json
import os
from pathlib import Path
import queue
import re
import signal
import struct
import subprocess
import sys
import threading
import time

from rov.data.predictions import load_rov_targets
from rov.data.sequence import load_sequence
from rov.data.vrp import load_vrp_text
from rov.dut.managed import (capture_environment, check_initialization,
    forward_prompt_sync_output, stop_initialization, stop_requested, wait_for_initialization)
from rov.dut.output import MANAGED_OUTPUT, supervise
from rov.dut.virtualize import enter_namespace
from rov.experiments.exp67.runtime.measure import run as run_incremental
from rov.experiments.exp61.brio_timing import (
    read_records, correlate, correlate_control_records,
)
from rov.experiments.exp67.scenarios.paired_transport import PairedRelay
from rov.experiments.exp67.inputs.scenario import load_scenario
from rov.experiments.exp67.faults.transport import bird_rtr_sockets, fault_pdu
from rov.measure.bird_connection import BirdConnection
from rov.measure.cli import query_roa
from rov.measure.read_log import start_log_observer, stop_observer
from rov.measure.vrp_state import verify_final
from rov.paths import CONFIG
from rov.send.send_vrp import submit_with_backpressure


# Refuse user-owned resources before reserving an output or starting an initializer.
def check_unoccupied():
    occupied = [name for name in ('bird.pid', 'bird.ctl', 'brio.in', 'bng.sock')
                if os.path.lexists(CONFIG / name)]
    for path in Path('/proc').iterdir():
        if not path.name.isdigit():
            continue
        try:
            name = (path / 'comm').read_text().strip()
        except (OSError, PermissionError):
            continue
        if name in ('bird', 'brio_rc', 'bngblaster'):
            occupied.append(f'{name} pid={path.name}')
    CONFIG.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(CONFIG, os.O_RDONLY | os.O_DIRECTORY)
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            occupied.append('DUT configuration directory lock')
    finally:
        os.close(descriptor)
    if occupied:
        raise RuntimeError('DUT occupied; no process was stopped: ' + ', '.join(occupied))


# Read complete VRP identities without assuming an RTR session remains Established.
def read_table(timeout):
    connection = BirdConnection(CONFIG / 'bird.ctl', timeout)
    actual = set()
    try:
        connection.command('restrict')
        status = connection.command('show protocols all rpki_source')
        for afi in (4, 6):
            for line in connection.command(f'show route table r{afi}').splitlines():
                match = re.match(r'^(\S+/\d+)-(\d+) AS(\d+)\s', line)
                if match:
                    prefix, maximum, asn = match.groups()
                    actual.add((str(ip.ip_network(prefix)), int(maximum), int(asn)))
    finally:
        connection.close()
    return actual, status


# Compare a captured VRP table after timed observation has stopped.
def table_verification(expected, actual, status):
    return dict(correct=actual == expected, expected=len(expected), actual=len(actual),
                missing=len(expected - actual), extra=len(actual - expected),
                missing_examples=sorted(expected - actual)[:10],
                extra_examples=sorted(actual - expected)[:10], rtr_status=status,
                scope='All installed IPv4 and IPv6 VRP identities')


# Check a live final table only after the collection boundary.
def verify_table(expected, timeout):
    actual, status = read_table(timeout)
    return table_verification(expected, actual, status)


# Capture actual baseline query values without comparing prepared states online.
def read_queries(targets, timeout):
    queries = dict.fromkeys((target['prefix'], target['origin_as']) for target in targets.values())
    connection = BirdConnection(CONFIG / 'bird.ctl', timeout)
    try:
        for prefix, origin in queries:
            value, _, _ = query_roa(connection, prefix, origin)
            queries[prefix, origin] = value
    finally:
        connection.close()
    return queries


# Compare the saved initial query samples after timed collection has stopped.
def query_verification(targets, observed):
    expected = {}
    for target in targets.values():
        key = target['prefix'], target['origin_as']
        if key in expected and expected[key] != target['initial']:
            raise ValueError('Conflicting prepared initial query states')
        expected[key] = target['initial']
    mismatches = [
        dict(prefix=prefix, origin_as=origin, expected=wanted, actual=observed.get((prefix, origin)))
        for (prefix, origin), wanted in expected.items()
        if observed.get((prefix, origin)) != wanted
    ]
    return dict(correct=not mismatches, distinct_queries=len(expected),
                candidate_count=len(targets), mismatches=mismatches)


# Move only the owned BIRD to the relay using the existing scenario timer settings.
def prepare_relay(args, relay, check_alive):
    path = CONFIG / 'bird.conf'
    text = path.read_text()
    original = '  remote 127.0.0.1 port 3324;'
    if text.count(original) != 1:
        raise ValueError('Expected the owned primary RTR configuration on port 3324')
    text = text.replace(original, '  remote 127.0.0.1 port 3323;\n  refresh keep 60;\n  retry keep 1;\n  expire keep 600;')
    path.write_text(text)
    subprocess.run([str(args.bin_dir / 'bird'), '-p', '-c', str(path)], check=True, timeout=10)
    connection = BirdConnection(CONFIG / 'bird.ctl', args.timeout)
    try:
        connection.command('configure')
        deadline = time.monotonic() + args.timeout
        while ('Established' not in connection.command('show protocols all rpki_source') or
               not any(row.get('pdu_type') == 7 for row in relay.snapshot())):
            check_alive()
            if time.monotonic() >= deadline:
                raise TimeoutError('Relay baseline did not establish')
            time.sleep(.1)
    finally:
        connection.close()


# Convert existing normal metrics to the first successful Prefix-PDU send-call origin.
def normal_timing(churn, relay, events_path):
    report = relay.report()
    expected_updates = report['planned_total']
    if len(report['sent_updates']) != expected_updates or report['send_failure']:
        raise RuntimeError(f'Normal group lacks all {expected_updates} successful send calls')
    start = report['sent_updates'][0]['send_start_ns']
    rows = [json.loads(line) for line in events_path.read_text().splitlines()]
    vrps = [row['observed_ns'] for row in rows if row['event'] == 'vrp' and row['observed_ns'] >= start]
    established = [row['observed_ns'] for row in churn['rtr_events']
                   if row['after'] == 'Established' and row['observed_ns'] >= start]
    if len(vrps) != expected_updates or not established:
        raise RuntimeError(
            f'Normal group lacks {expected_updates} VRP events or transaction Established evidence'
        )
    installed = max(max(vrps), max(established))
    fifo = churn['submissions'][0]['write_start_ns']
    metrics = churn['batch_metrics'][0]['metrics']
    converted = {}
    for name in ('roa_query_complete_ms', 'rib_complete_ms', 'fib_probe_complete_ms', 'rib_and_sampled_fib_ms'):
        if name in metrics:
            value = metrics[name] - (start - fifo) / 1e6
            converted[name] = value if value >= 0 else None
    submission = churn['submissions'][0]
    controls = [row for row in report['evidence'] if row['event'] == 'control_sent'
                and row['send_start_ns'] >= submission['write_start_ns']]
    notify = [row['send_start_ns'] for row in controls if row['pdu_type'] == 0]
    return dict(fifo_write_start_ns=submission['write_start_ns'],
                fifo_submitted_ns=submission['submitted_ns'],
                relay_notify_send_ns=notify[0] if len(notify) == 1 else None,
                first_prefix_send_start_ns=start, last_vrp_observed_ns=max(vrps),
                established_observed_ns=max(established), vrp_install_complete_ns=installed,
                first_prefix_to_last_vrp_ms=(max(vrps) - start) / 1e6,
                first_prefix_to_install_ms=(installed - start) / 1e6, **converted,
                definition='First successful relay Prefix-PDU sendall invocation to external file/query/probe observations; install endpoint is max(last VRP, Established); not internal execution or packet capture time',
                quiet_included=False)


# Observe the first fatal handling, then stop after actual VRP changes become quiet.
def run_mixed(scenario, operations, targets, args, trace, relay, result, raw, check_alive):
    initial = set(load_vrp_text(scenario['vrp']))
    events, errors, stop, thread = start_log_observer(
        CONFIG / 'bird.ctl', ('vrp', 'rtr'), args.timeout, log_path=trace)
    deadline = time.monotonic() + args.timeout
    last_change_ns = time.monotonic_ns()
    fatal = None
    drained_ns = 0
    cutoff_ns = None
    vrp_events = []
    failure = None
    gc_was_enabled = None
    result.update(processing_latency_ms=None, latency_unavailable='Fatal error terminates this batch; no recovery stimulus or recovery timing',
                  rtr_events=[], collection_scope='All VRP identity changes and RTR transitions; final affected ROA queries')

    # Retain actual events and reader progress without checking expected identities.
    def collect(event):
        nonlocal fatal, last_change_ns, drained_ns
        if cutoff_ns is not None and event['observed_ns'] > cutoff_ns:
            return
        raw.write(json.dumps(event, separators=(',', ':')) + '\n')
        kind = event['event']
        if kind == 'drained':
            drained_ns = event['observed_ns'] if event['drained'] else 0
        elif kind == 'rtr':
            drained_ns = 0
            result['rtr_events'].append(event)
            if event['after'] == 'Fatal-Protocol-Error' and fatal is None:
                fatal = event
        elif kind == 'vrp':
            drained_ns = 0
            vrp_events.append(event)
            last_change_ns = max(last_change_ns, event['observed_ns'])
        else:
            raise RuntimeError(f'Unexpected mixed-group observation: {kind}')

    try:
        directory, batches, _ = load_sequence(scenario['sequence'])
        gc_was_enabled = gc.isenabled()
        gc.disable()
        result['submission'] = submit_with_backpressure(directory / batches[0]['script'], CONFIG / 'brio.in',
                                                       args.timeout, time.sleep, check_alive)
        while True:
            check_alive()
            if not errors.empty():
                raise errors.get()
            for _ in range(events.qsize()):
                collect(events.get_nowait())
            transport = relay.snapshot()
            start = result['submission']['write_start_ns']
            error_ns = next((row['observed_ns'] for row in transport
                             if row['observed_ns'] >= start and row.get('pdu_type') == 10
                             and row.get('direction') == 'dut_to_cache'), None)
            closed_ns = next((row['observed_ns'] for row in transport
                              if row['observed_ns'] >= start and row['event'] in ('peer_eof', 'peer_reset')
                              and row.get('direction') == 'dut_to_cache'), None)
            if fatal is not None and error_ns is not None and closed_ns is not None:
                quiet_start_ns = max(last_change_ns, fatal['observed_ns'], error_ns, closed_ns)
                if (drained_ns - quiet_start_ns >= args.quiet_seconds * 1e9
                        and events.empty()):
                    cutoff_ns = time.monotonic_ns()
                    result['quiet_confirmed_ns'] = cutoff_ns
                    result['termination'] = 'fatal_batch_terminated_and_quiet'
                    break
            if time.monotonic() >= deadline:
                raise TimeoutError('Missing fatal error, peer close, or drained quiet evidence')
            time.sleep(.01)

        # Stop RTR retry logging and drain its final records after the timed cutoff.
        control = BirdConnection(CONFIG / 'bird.ctl', args.timeout)
        try:
            control.command('debug rpki_source off')
        finally:
            control.close()
        debug_off_ns = time.monotonic_ns()
        trace_drained = False
        while not trace_drained or not events.empty():
            check_alive()
            if not errors.empty():
                raise errors.get()
            try:
                event = events.get(timeout=.1)
            except queue.Empty:
                pass
            else:
                if event['observed_ns'] >= debug_off_ns:
                    trace_drained = event['drained'] if event['event'] == 'drained' else False
                collect(event)
            if time.monotonic() >= deadline:
                raise TimeoutError('BIRD trace did not drain after disabling RPKI debug')
    except BaseException as error:
        failure = error
        result['termination'] = 'timeout' if isinstance(error, TimeoutError) else 'error'
    finally:
        try:
            if cutoff_ns is None:
                cutoff_ns = time.monotonic_ns()
            result['collection_end_ns'] = cutoff_ns
            stop.set()
            try:
                stop_observer(stop, thread, args.timeout)
            except BaseException as error:
                if failure is None:
                    failure = error
            while not events.empty():
                collect(events.get_nowait())
            if not errors.empty():
                if failure is None:
                    failure = errors.get()

        finally:
            if gc_was_enabled:
                gc.enable()

    # Replay recorded identity changes only after the producer has stopped.
    current = set(initial)
    counts = Counter()
    installed_keys, removed_keys = set(), set()
    legal_updates = []
    observation_errors = []
    intended = {(row['operation'], *row['vrp']) for row in operations}
    for event in vrp_events:
        key = event['prefix'], event['max_length'], event['asn']
        operation = 'add' if event['action'] == 'added' else 'withdraw'
        if (fatal is None or event['observed_ns'] < fatal['observed_ns']) and (operation, *key) in intended:
            legal_updates.append(dict(operation=operation, vrp=list(key)))
        if event['action'] == 'added':
            if key in current:
                observation_errors.append(f'Duplicate observed VRP addition: {key}')
            current.add(key)
            installed_keys.add(key)
        else:
            if key not in current:
                observation_errors.append(f'Unknown observed VRP removal: {key}')
            current.discard(key)
            removed_keys.add(key)
        counts[event['action']] += 1
    result.update(vrp_event_counts=dict(counts), tracked_remaining_vrps=len(current),
                  all_baseline_vrps_observed_removed=initial <= removed_keys,
                  legal_additions_observed=sorted(installed_keys),
                  legal_updates_observed_before_fatal=legal_updates, fatal_event=fatal)
    result['errors'].extend(observation_errors)
    if failure is not None:
        raise failure
    quiet_start_ns = max(last_change_ns, fatal['observed_ns'], error_ns, closed_ns)
    if (cutoff_ns - quiet_start_ns < args.quiet_seconds * 1e9
            or drained_ns - quiet_start_ns < args.quiet_seconds * 1e9):
        raise RuntimeError('An in-flight observation invalidated the mixed-group quiet interval')

    result['full_vrp_verification'] = verify_table(set(), args.timeout)
    expectation = dict(entries={tuple(row['vrp']): dict(final=False) for row in operations})
    expected_queries = {(row['prefix'], row['origin_as']): 'NotFound' for row in targets.values()}
    result['final_verification'] = verify_final(CONFIG / 'bird.ctl', args.timeout, expectation, expected_queries)
    protocol = relay.report()['error_reports']
    expected_pdu = fault_pdu('unexpected')[0]
    confirmed = []
    for row in protocol:
        pdu = bytes.fromhex(row['pdu_hex'])
        size = struct.unpack_from('!I', pdu, 8)[0]
        if row['error_code'] == 5 and pdu[12:12 + size] == expected_pdu:
            confirmed.append(row)
    result['protocol_correct'] = bool(confirmed) and relay.report()['dut_closed'] and fatal is not None
    result['confirmed_error_pdu_receipts'] = len(confirmed)
    first_error_position = relay.report()['first_error_position']
    result['receipt_scope'] = (
        f'ErrorReport encapsulation confirms the first type-255 PDU at position '
        f'{first_error_position}; later identical errors are not individually attributable'
    )
    sent_by_identity = {tuple(row['vrp']): row['position'] for row in relay.original_batch}
    result['legal_positions_observed_installed'] = sorted(sent_by_identity[key] for key in installed_keys if key in sent_by_identity)
    result['legal_positions_observed_before_fatal'] = sorted(sent_by_identity[tuple(row['vrp'])] for row in legal_updates)
    result['correct'] = (result['protocol_correct'] and result['full_vrp_verification']['correct']
                         and result['final_verification']['correct'] and not current
                         and initial <= removed_keys and not observation_errors)
    result['verdict_scope'] = (
        'Observed fatal-error path and complete resulting VRP table; '
        'not proof that all planned errors reached the DUT'
    )


# Own one fresh baseline and always retain a result and clean up only owned children.
def run_managed(args):
    _, scenario = load_scenario(args.scenario)
    directory, batches, operation_groups = load_sequence(scenario['sequence'])
    operations = operation_groups[batches[0]['number']]
    targets, _ = load_rov_targets(directory, batches, Path(scenario['predictions']))
    initial = set(load_vrp_text(scenario['vrp']))
    if len(initial) != scenario['size'] or len(batches) != 1 or len(operations) < 10:
        raise ValueError('Paired scenario 4 requires the declared initial VRP count and one batch of at least 10 operations')
    normal = None
    if args.group == 'mixed':
        normal = json.loads(args.normal_result.read_text())
        if normal['status'] != 'success' or Path(normal['scenario']).resolve() != args.scenario.resolve():
            raise ValueError('Mixed group requires the successful matching normal result')
    result = dict(format='exp67-paired-incremental-v1', group=args.group, scenario=str(args.scenario.resolve()),
                  input=scenario, repetitions=1, status='failed', errors=[], baseline_ready=False,
                  timeout=args.timeout, quiet_seconds=args.quiet_seconds, steady_seconds=args.steady_seconds,
                  poll_interval=args.poll_interval, pps=args.pps, packet_timeout=args.packet_timeout,
                  cleanup_complete=False, initialization_timing='Not measured; file-only initialization',
                  group_initialization='Fresh independent owned DUT using the shared unchanged baseline')
    trace = args.output / 'bird-trace.log'
    trace.touch(exist_ok=False)
    brio_trace = args.output / 'brio-timing.jsonl'
    brio_trace.touch(exist_ok=False)
    result['brio_timing_log'] = str(brio_trace)
    relay = PairedRelay(operations, args.group == 'mixed')
    process = thread = None
    messages = queue.Queue()
    initial_table = initial_status = initial_queries = None

    # Check the owned initializer and relay without inspecting or stopping other jobs.
    def check_alive():
        check_initialization(process, messages)
        if not relay.thread.is_alive() or relay.errors:
            raise RuntimeError('Owned scenario-4 relay failed')
        if any(row['event'] == 'transport_error' for row in relay.snapshot()):
            raise RuntimeError('Unexpected relay transport or workload validation error')

    try:
        brio = args.bin_dir / 'brio_rc'
        if (not brio.is_file() or any(marker not in brio.read_bytes() for marker in
                (b'brio-first-vrp-v1', b'brio-control-send-v1'))):
            raise RuntimeError('The selected BRIO lacks Prefix-PDU and control-PDU timing support')
        command = [sys.executable, '-u', '-m', 'rov.dut.initialize', '--inside',
                   '--bgp', scenario['bgp'], '--vrp', scenario['vrp'], '--rov', 'on',
                   '--dut-asn', '65000', '--as-sets', 'reject', '--rtr-port', '3324',
                   '--bird-trace-log', str(trace), '--bird-trace-info', '--timeout', str(args.timeout),
                   '--bin-dir', str(args.bin_dir)]
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                   env=dict(os.environ, **{MANAGED_OUTPUT: '1',
                                       'BRIO_VRP_TIMING_LOG': str(brio_trace)}))
        thread = threading.Thread(target=forward_prompt_sync_output, args=(process, messages, result))
        thread.start()
        wait_for_initialization(process, messages)
        prepare_relay(args, relay, check_alive)
        until = time.monotonic() + args.steady_seconds
        while time.monotonic() < until:
            check_alive()
            time.sleep(.1)
        initial_table, initial_status = read_table(args.timeout)
        initial_queries = read_queries(targets, args.timeout)
        result['baseline_ready'] = True
        capture_environment(args.output, args.timeout)
        result['tcp_before'] = bird_rtr_sockets(CONFIG / 'bird.pid')
        before_brio = read_records(brio_trace)
        relay.arm([row['original_pdu_hex'] for row in normal['transport']['original_batch']] if normal else None)
        with (args.output / 'events.jsonl').open('x') as raw:
            if args.group == 'normal':
                result['churn'] = {}
                run_incremental(directory, Path(scenario['predictions']), Path(scenario['path_predictions']),
                    CONFIG / 'bird.ctl', CONFIG / 'brio.in', 1.0, args.poll_interval, args.timeout,
                    config_dir=CONFIG, pps=args.pps, packet_timeout=args.packet_timeout,
                    quiet_seconds=args.quiet_seconds,
                    log_path=trace, result=result['churn'], raw_output=raw, check_alive=check_alive,
                    suspend_gc=True)
                expected = set(initial)
                for row in operations:
                    (expected.add if row['operation'] == 'add' else expected.remove)(tuple(row['vrp']))
                result['full_vrp_verification'] = verify_table(expected, args.timeout)
                result['timing'] = normal_timing(result['churn'], relay, args.output / 'events.jsonl')
                result['correct'] = result['churn']['status'] == 'success' and result['full_vrp_verification']['correct']
            else:
                run_mixed(scenario, operations, targets, args, trace, relay, result, raw, check_alive)
        submission = (result['churn']['submissions'][0]
                      if args.group == 'normal' else result['submission'])
        timing_submission = {key: value for key, value in submission.items() if key != 'batch'}
        after_brio = read_records(brio_trace)
        update_responses = [row for row in after_brio[len(before_brio):]
                            if row.get('status') != 'no_vrp']
        if after_brio[:len(before_brio)] == before_brio and len(update_responses) == 1:
            source = correlate(before_brio, before_brio + update_responses,
                               dict(**timing_submission, first_observed_ns=None,
                                    last_observed_ns=None, full_completed_ns=None),
                               'incremental', batches[0]['number'])
            source.update(correlate_control_records(brio_trace, submission, source.get('brio_response')))
            result['source_timing'] = source
        if args.group == 'mixed':
            report = relay.report()
            sent = report['sent_updates']
            controls = [row for row in report['evidence'] if row['event'] == 'control_sent'
                        and row['send_start_ns'] >= submission['write_start_ns']]
            notify = [row['send_start_ns'] for row in controls if row['pdu_type'] == 0]
            result['timing'] = dict(
                fifo_write_start_ns=submission['write_start_ns'],
                fifo_submitted_ns=submission['submitted_ns'],
                relay_notify_send_ns=notify[0] if len(notify) == 1 else None,
                first_prefix_send_start_ns=sent[0]['send_start_ns'] if sent else None,
                definition='First successful relay Prefix-PDU sendall invocation; fatal handling ends the batch before full processing',
            )
        result['tcp_after'] = bird_rtr_sockets(CONFIG / 'bird.pid')
        if args.group == 'normal':
            result['same_tcp_session'] = len(result['tcp_before']) == 1 and result['tcp_before'] == result['tcp_after']
            result['correct'] = result['correct'] and result['same_tcp_session']
        if not result['correct']:
            raise RuntimeError('Scenario-4 correctness evidence is incomplete or mismatched')
        check_alive()
        result['status'] = 'success'
    except BaseException as error:
        result['errors'].append(f'{type(error).__name__}: {error}')
        result['status'] = 'interrupted' if isinstance(error, KeyboardInterrupt) else 'failed'
    finally:
        if initial_table is not None and initial_queries is not None:
            try:
                result['initial_vrp_verification'] = table_verification(initial, initial_table, initial_status)
                result['initial_query_verification'] = query_verification(targets, initial_queries)
                if not result['initial_vrp_verification']['correct'] or not result['initial_query_verification']['correct']:
                    result['errors'].append('Shared baseline VRP or affected query verification failed')
                    result['correct'] = False
            except BaseException as error:
                result['errors'].append(f'Initial state verification: {error}')
                result['correct'] = False
        try:
            relay.close()
            result['transport'] = relay.report()
        except BaseException as error:
            result['errors'].append(f'Relay cleanup: {error}')
        if process is not None:
            try:
                result['cleanup_complete'] = stop_initialization(process, thread) == 0
            except BaseException as error:
                result['errors'].append(f'Initializer cleanup: {error}')
        else:
            result['cleanup_complete'] = True
        result['remaining_control_files'] = [name for name in ('bird.pid', 'bird.ctl', 'brio.in', 'bng.sock')
                                             if os.path.lexists(CONFIG / name)]
        if result['errors'] or not result['cleanup_complete'] or result['remaining_control_files']:
            result['status'] = 'failed'
        (args.output / 'result.json').write_text(json.dumps(result, indent=2) + '\n')
        print(json.dumps(dict(group=args.group, status=result['status'], correct=result.get('correct'),
                              timing=result.get('timing'), errors=result['errors'], output=str(args.output))), flush=True)
    return 0 if result['status'] == 'success' else 1


# Keep old scenario entry points unchanged and reserve one new output per requested group.
def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('scenario', type=Path)
    parser.add_argument('--group', choices=('normal', 'mixed'), required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--normal-result', type=Path)
    parser.add_argument('--bin-dir', type=Path, default=Path('software/bin-brio-first-vrp'))
    parser.add_argument('--timeout', type=float, default=600)
    parser.add_argument('--quiet-seconds', type=float, default=10,
                        help='Seconds without new state-change events before quiet completion (default: 10)')
    parser.add_argument('--steady-seconds', type=float, default=30)
    parser.add_argument('--poll-interval', type=float, default=0)
    parser.add_argument('--pps', type=float, default=10000,
                        help='Aggregate FIB probe rate in packets per second (default: 10000)')
    parser.add_argument('--packet-timeout', type=float, default=1)
    parser.add_argument('--inside', action='store_true', help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.group == 'mixed' and args.normal_result is None:
        parser.error('Mixed group requires --normal-result')
    args.output = args.output.resolve()
    args.bin_dir = args.bin_dir.resolve()
    if not os.environ.get(MANAGED_OUTPUT):
        check_unoccupied()
        args.output.mkdir(parents=True, exist_ok=False)
        with (args.output / 'run.log').open('xb') as log:
            return supervise('rov.experiments.exp67.scenarios.paired_incremental', sys.argv[1:], log=log)
    if not args.inside:
        enter_namespace(sys.argv[1:], module='rov.experiments.exp67.scenarios.paired_incremental')
    return run_managed(args)


if __name__ == '__main__':
    signal.signal(signal.SIGTERM, stop_requested)
    signal.signal(signal.SIGINT, stop_requested)
    raise SystemExit(main())
