"""Measure 6.7 full reload with 6.1 protocol evidence and 6.3A collection semantics."""

import gc
import json
from pathlib import Path
import queue
import time

from rov.paths import CONFIG
from rov.data.sequence import load_sequence
from rov.data.vrp import load_vrp_text
from rov.data.predictions import load_rov_targets
from rov.experiments.exp61.__main__ import batch_for_mode, full_batch_commands
from rov.experiments.exp61.brio_timing import (
    read_records, correlate, correlate_control_records,
)
from rov.experiments.exp63a.__main__ import (
    check_forwarding, check_log_boundary,
    check_rib_event, check_rib_result, rib_path_fields, load_vrp_expectations,
)
from rov.experiments.exp67.runtime.observe import start_reset_rov_observer, query_observations
from rov.experiments.exp67.runtime.measure import check_initial_rov
from rov.measure.cli import read_primary_rib
from rov.measure.forwarding import ForwardingObserver, forwarding_state
from rov.measure.read_log import start_log_observer, stop_observer
from rov.measure.full_sync import begin_full_sync, finish_full_sync
from rov.measure.vrp_state import accept_vrp_event, pending_vrp_changes, verify_final
from rov.measure.vrp_streams import RovState
from rov.send.send_vrp import send_batch


# Wait for the same grouped candidate-query baseline used by the existing 6.7 observer.
def query_baseline(events, errors, timeout):
    initial = {}
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not errors.empty():
            raise errors.get()
        try:
            event = events.get(timeout=0.1)
        except queue.Empty:
            continue
        if event['event'] == 'initial':
            initial[event['prefix']] = event['candidates']
        elif event['event'] == 'ready':
            return initial
        else:
            raise RuntimeError('ROA query changed before baseline readiness')
    raise TimeoutError('ROA query baseline timed out')


# Collect every selected-path log and target probe continuously, then match predictions offline.
def run(scenario, args, trace, brio_trace, result, raw_output, check_alive):
    directory, batches, operations = load_sequence(scenario['sequence'])
    if len(batches) != 1:
        raise ValueError('Full reset requires exactly one batch')
    info = batches[0]
    number = info['number']
    expectation = load_vrp_expectations(directory, batches, operations)[number]
    targets, transitions = load_rov_targets(directory, batches, Path(scenario['predictions']))
    rov = RovState(targets, transitions)
    prediction = next(json.loads(line) for line in Path(scenario['path_predictions']).read_text().splitlines()
                      if json.loads(line)['batch'] == number)
    expected_forwarding = {row['probe']['destination']: row['probe']['expected_next_hop']
                           for row in prediction['changes'] if row['probe']['status'] == 'ready'}
    expected_vrps = set(load_vrp_text(scenario['vrp']))
    for row in operations[number]:
        if row['operation'] == 'add':
            expected_vrps.add(tuple(row['vrp']))
        else:
            expected_vrps.remove(tuple(row['vrp']))
    result.update(status='failed', method='cache-reset-send-v2',
                  termination='not_submitted', submissions=[], rtr_events=[], vrp_events=[],
                  rib_events=[], forwarding_changes=[], rov_events=[],
                  query_scope=dict(candidates=len(targets),
                                   distinct_prefix_origin_queries=len({(r['prefix'], r['origin_as']) for r in targets.values()}),
                                   definition='Affected prepared candidates only; not global ROV rebuild'),
                  fib_scope=dict(destinations=len(expected_forwarding),
                                 unavailable=[dict(prefix=r['prefix'], **r['probe']) for r in prediction['changes']
                                              if r['probe']['status'] != 'ready']),
                  trigger_commands=full_batch_commands(directory, info),
                  completion_rule='Resetting then Sync-Running to Established, drained file, empty queues, and quiet interval without VRP/RIB/ROA-query/forwarding state changes; predictions checked afterwards')
    events, errors, log_stop, log_thread = start_log_observer(
        CONFIG / 'bird.ctl', ('vrp', 'rib', 'rtr'), args.timeout, log_path=trace)
    query_events = queue.Queue()
    query_observer = packet_observer = None
    failure = None
    submission = None
    cutoff_ns = None
    latest_ns = None
    quiet_ns = round(args.quiet_seconds * 1e9)
    drained_ns = 0
    reset_ns = completed_ns = None
    batch_before = {}
    vrp_actions = {}
    current_forwarding = {}
    final_samples = {}
    initial_rov = {}
    gc_was_enabled = None

    # Retain actual changes without comparing them against prepared expectations.
    def collect(event):
        nonlocal drained_ns, reset_ns, completed_ns, latest_ns
        if cutoff_ns is not None and event['observed_ns'] > cutoff_ns:
            return
        raw_output.write(json.dumps(event, separators=(',', ':')) + '\n')
        kind = event['event']
        if kind == 'drained':
            drained_ns = event['observed_ns'] if event['drained'] else 0
            return

        if kind in ('received', 'not_received'):
            destination = event['destination']
            state = forwarding_state(event)
            current_forwarding[destination] = state
            final_samples[destination] = {
                key: value for key, value in event.items() if key != 'before'
            }
            if event['observed_ns'] < submission['write_start_ns']:
                return
            result['forwarding_changes'].append(dict(
                destination=destination, before=event['before'], after=state,
                observed_ns=event['observed_ns'], sent_ns=event['sent_ns'],
            ))
            latest_ns = max(latest_ns, event['observed_ns'])
            return

        if event['observed_ns'] < submission['write_start_ns']:
            raise RuntimeError('Business event predates full-reset submission')
        changed = True
        if kind in ('vrp', 'rib', 'rtr'):
            drained_ns = 0
        if kind == 'rtr':
            result['rtr_events'].append(event)
            if event['after'] == 'Resetting':
                reset_ns = event['observed_ns']
                completed_ns = None
            if (reset_ns is not None and event['before'] == 'Sync-Running'
                    and event['after'] == 'Established'):
                completed_ns = event['observed_ns']
        elif kind == 'vrp':
            result['vrp_events'].append(event)
            key = event['prefix'], event['max_length'], event['asn']
            changed = vrp_actions.get(key) != event['action']
            vrp_actions[key] = event['action']
        elif kind == 'rib':
            prefix = event['prefix']
            previous = current_rib.get(prefix)
            batch_before.setdefault(prefix, previous)
            event['before'] = previous
            changed = rib_path_fields(previous) != rib_path_fields(event['after'])
            if event['after'] is None:
                current_rib.pop(prefix, None)
            else:
                current_rib[prefix] = event['after']
            result['rib_events'].append(event)
        elif kind == 'rov':
            result['rov_events'].append(event)
            rov.observed[(event['prefix'], event['origin_as'])].append(event)
        else:
            raise RuntimeError(f'Unexpected full-reset observation: {kind}')
        if changed:
            latest_ns = max(latest_ns, event['observed_ns'])

    try:
        current_rib = read_primary_rib(CONFIG / 'bird.ctl', args.timeout, include_attributes=True)
        result['initial_rib_prefixes'] = len(current_rib)
        check_log_boundary(events, errors, 'BIRD changed while reading the initial RIB')
        query_observer = start_reset_rov_observer(targets, CONFIG / 'bird.ctl', args.poll_interval,
                                                  args.timeout, query_events, errors)
        initial_rov = query_baseline(query_events, errors, args.timeout)
        initial_forwarding = {}
        if expected_forwarding:
            packet_observer = ForwardingObserver(
                list(expected_forwarding), CONFIG, args.pps, args.packet_timeout, events, errors,
            )
            initial_forwarding = packet_observer.start(args.timeout)
        current_forwarding = {key: forwarding_state(row)
                              for key, row in initial_forwarding.items()}
        final_samples = {key: dict(row) for key, row in initial_forwarding.items()}
        result['initial_forwarding'] = initial_forwarding
        result['final_forwarding_samples'] = final_samples
        pending_packets = check_log_boundary(events, errors, 'BIRD changed before full-reset submission')
        evidence = begin_full_sync(trace, CONFIG / 'bird.ctl', CONFIG / 'bird.pid', args.timeout)
        before_brio = read_records(brio_trace)
        print('Full-reset observers ready: complete RIB logs, affected ROA queries, target UDP probes.', flush=True)
        with batch_for_mode(directory, info, 'full') as staged:
            gc_was_enabled = gc.isenabled()
            gc.disable()
            submission = send_batch(directory / staged['script'], CONFIG / 'brio.in', atomic=True)
            result['submissions'].append(dict(batch=number, operations=len(operations[number]), **submission))
            latest_ns = submission['submitted_ns']
            deadline = time.monotonic() + args.timeout
            for event in pending_packets:
                collect(event)
            while True:
                check_alive()
                if not errors.empty():
                    raise errors.get()
                for source in (events, query_events):
                    for _ in range(source.qsize()):
                        collect(source.get_nowait())
                now = time.monotonic_ns()
                if (completed_ns is not None and drained_ns >= completed_ns
                        and drained_ns - latest_ns >= quiet_ns
                        and events.empty() and query_events.empty()):
                    cutoff_ns = now
                    result.update(termination='quiet', quiet_confirmed_ns=cutoff_ns)
                    break
                if time.monotonic() >= deadline:
                    raise TimeoutError('Full reset did not complete with drained logs and quiet state')
                time.sleep(0.001)
    except BaseException as error:
        failure = error
        result['termination'] = 'timeout' if isinstance(error, TimeoutError) else 'error'
    finally:
        try:
            if cutoff_ns is None:
                cutoff_ns = time.monotonic_ns()
            result['collection_end_ns'] = cutoff_ns
            observers = [(log_stop, log_thread)]
            if query_observer is not None:
                observers.append(query_observer[:2])
            if packet_observer is not None:
                observers.append((packet_observer.stop, packet_observer.thread))
            for stop, _ in observers:
                stop.set()
            for stop, thread in observers:
                try:
                    stop_observer(stop, thread, args.timeout)
                except BaseException as error:
                    if failure is None:
                        failure = error
            if submission is not None:
                try:
                    for source in (events, query_events):
                        while not source.empty():
                            collect(source.get_nowait())
                    if packet_observer is not None:
                        for destination, event in packet_observer.snapshot().items():
                            if event['observed_ns'] <= cutoff_ns:
                                final_samples[destination] = event
                    if result['termination'] == 'quiet':
                        if completed_ns is None:
                            raise RuntimeError('An in-flight session event invalidated full-reset completion')
                        if cutoff_ns - latest_ns < quiet_ns:
                            raise RuntimeError('An in-flight state change invalidated the quiet interval')
                        if drained_ns - latest_ns < quiet_ns:
                            raise RuntimeError('An in-flight log notification invalidated the drained quiet interval')
                except BaseException as error:
                    if failure is None:
                        failure = error
            if not errors.empty() and failure is None:
                failure = errors.get()
            if query_observer is not None:
                result['query_observations'] = query_observations(targets, query_observer[2], cutoff_ns)
            raw_output.flush()
        finally:
            if gc_was_enabled:
                gc.enable()

    if failure is not None:
        if result['termination'] == 'quiet':
            result['termination'] = 'error'
        result['error'] = f'{type(failure).__name__}: {failure}'
        raise failure

    # Verify initial and collected states only after every producer has stopped.
    check_initial_rov(initial_rov, targets)
    for event in result['rib_events']:
        check_rib_event(event, event['before'])
    if any(event['after'] in ('Transport-Error', 'Fatal-Protocol-Error', 'Down')
           for event in result['rtr_events']):
        raise RuntimeError('RTR failed during full resynchronization')

    observed_vrps = {}
    for event in result['vrp_events']:
        accept_vrp_event(event, expectation, observed_vrps, number)
    if pending_vrp_changes(expectation, observed_vrps):
        raise RuntimeError('Missing net VRP changes after quiet collection')
    result['full_sync_verification'] = finish_full_sync(trace, CONFIG / 'bird.ctl', CONFIG / 'bird.pid',
                                                       args.timeout, evidence, expected_vrps)
    result['final_rib_changes'] = [dict(prefix=p, before=before, after=current_rib.get(p))
                                  for p, before in batch_before.items()
                                  if rib_path_fields(before) != rib_path_fields(current_rib.get(p))]
    check_rib_result(prediction, result['final_rib_changes'], number)
    check_forwarding(expected_forwarding, current_forwarding, number)
    final_rib = read_primary_rib(CONFIG / 'bird.ctl', args.timeout, include_attributes=True)
    rib_mismatches = sum(rib_path_fields(current_rib.get(p)) != rib_path_fields(final_rib.get(p))
                         for p in current_rib.keys() | final_rib.keys())
    result['complete_rib_verification'] = dict(checked_prefixes=len(current_rib.keys() | final_rib.keys()),
                                              mismatches=rib_mismatches, correct=rib_mismatches == 0)
    if rib_mismatches:
        raise RuntimeError('Complete final RIB differs from continuous observation')
    rov.match_offline([(number, submission['write_start_ns'])])
    expected_queries = {(prefix, origin): after for _, prefix, _, origin, _, after in transitions}
    result['final_verification'] = verify_final(CONFIG / 'bird.ctl', args.timeout, expectation, expected_queries)
    result.update(transition_observation_complete=len(rov.matched) == len(transitions),
                  unobserved_vrp_events=0, unexpected_vrp_events=0,
                  query_transitions_expected=len(transitions), query_transitions_observed=len(rov.matched))
    if not result['final_verification']['correct'] or not result['transition_observation_complete']:
        raise RuntimeError('Affected ROA-query final state or transition mismatch')
    vrp_first = min(row['observed_ns'] for row in result['vrp_events'])
    vrp_last = max(row['observed_ns'] for row in result['vrp_events'])
    timing = correlate(before_brio, read_records(brio_trace), dict(**submission,
                       first_observed_ns=vrp_first, last_observed_ns=vrp_last,
                       full_completed_ns=completed_ns), 'full', number)
    timing.update(correlate_control_records(
        brio_trace, submission, timing.get('brio_response'), pdu_type=8,
    ))
    if timing['brio_timing_status'] != 'complete':
        raise RuntimeError('Missing verified first-VRP send timestamp')
    sent_ns = timing['brio_cache_reset_send_ns']
    if timing['control_timing_status'] != 'complete' or sent_ns is None:
        raise RuntimeError('Missing verified Cache Reset send timestamp')
    installed_ns = max(completed_ns, vrp_last)
    rib_end = max((row['observed_ns'] for row in result['rib_events']
                   if rib_path_fields(row['before']) != rib_path_fields(row['after'])), default=None)
    fib_end = max((row['observed_ns'] for row in result['forwarding_changes']), default=None)
    query_end = max((ns for _, ns in rov.matched), default=None)
    joint_end = max(rib_end, fib_end) if rib_end is not None and fib_end is not None else None
    result['timing'] = dict(brio=timing, reference='brio_cache_reset_send',
                          cache_reset_send_ns=sent_ns,
                          first_vrp_send_ns=timing['brio_first_vrp_send_ns'], reset_observed_ns=reset_ns,
                          full_sync_observed_ns=completed_ns, install_complete_observed_ns=installed_ns,
                          rib_last_change_ns=rib_end, sampled_fib_last_change_ns=fib_end,
                          affected_query_complete_ns=query_end, joint_last_change_ns=joint_end,
                          quiet_confirmed_ns=result['quiet_confirmed_ns'],
                          full_reload_ms=(installed_ns-sent_ns)/1e6,
                          reset_to_established_ms=(completed_ns-reset_ns)/1e6,
                          rib_from_send_ms=None if rib_end is None else (rib_end-sent_ns)/1e6,
                          sampled_fib_from_send_ms=None if fib_end is None else (fib_end-sent_ns)/1e6,
                          affected_query_from_send_ms=None if query_end is None else (query_end-sent_ns)/1e6,
                          rib_sampled_fib_from_send_ms=None if joint_end is None else (joint_end-sent_ns)/1e6,
                          post_install_rib_sampled_fib_ms=None if joint_end is None or joint_end < installed_ns else (joint_end-installed_ns)/1e6,
                          quiet_confirmed_from_send_ms=(result['quiet_confirmed_ns']-sent_ns)/1e6,
                          post_install_quiet_confirmed_ms=(result['quiet_confirmed_ns']-installed_ns)/1e6,
                          definition='Successful BRIO Cache Reset send start to external file/query/probe observations, including the Reset Query exchange; full reload requires Established and all changed VRP events; quiet confirmation retained separately; negative cross-stream intervals unavailable')
    result['status'] = 'success'
    print('Full-reset verified: ' + json.dumps({k:v for k,v in result['timing'].items() if k.endswith('_ms')}), flush=True)
    return result
