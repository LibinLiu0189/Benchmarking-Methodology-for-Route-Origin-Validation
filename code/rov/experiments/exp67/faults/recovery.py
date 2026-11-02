"""Calculate fault recovery stages without equating TCP, VRP, ROV, RIB and forwarding."""


# Use external evidence for each distinct stage and keep inverted intervals unavailable.
def fault_recovery(churn, lab):
    fault = lab.result.get('fault')
    result = dict(transport_reconnect_ms=None, session_recovery_ms=None,
                  vrp_recovery_ms=None, validation_recovery_ms=None,
                  post_install_query_ms=None, post_install_rib_sampled_fib_ms=None,
                  unavailable={}, evidence_complete=False)
    if fault is None:
        result['unavailable']['all'] = 'Fault was not triggered'
        return result
    start = fault['observed_ns']
    events = [row for row in churn.get('rtr_events', []) if row['observed_ns'] >= start]
    protocol = 'rpki_source'
    established = next((row['observed_ns'] for row in events
                        if row.get('protocol', 'rpki_source') == protocol and row.get('after') == 'Established'), None)
    connected = lab.reconnected_ns
    result['command_write_start_ns'] = fault['command_write_start_ns']
    result['server_shutdown_ns'] = fault['shutdown_ns']
    result['reconnect_accepted_ns'] = connected
    result['transport_reconnect_ms'] = (connected - start) / 1e6 if connected is not None else None
    result['session_recovery_ms'] = (established - start) / 1e6 if established is not None else None
    result['evidence_complete'] = connected is not None and established is not None
    batch = churn.get('batch_metrics', [{}])[0]
    metrics = batch.get('metrics', {})
    endpoints = batch.get('observed_endpoints_ns', {})
    last_vrp = endpoints.get('last_vrp')
    installed = last_vrp if last_vrp is not None else (
        start + round(metrics['last_vrp_ms'] * 1e6) if metrics.get('last_vrp_ms') is not None else None)
    if installed is not None and established is not None:
        installed = max(installed, established)
    else:
        installed = None
    result['vrp_install_complete_ns'] = installed
    result['vrp_recovery_ms'] = (installed - start) / 1e6 if installed is not None else None
    result['validation_recovery_ms'] = metrics.get('roa_query_complete_ms')
    for destination, source in (('post_install_query_ms', 'roa_query_complete_ms'),
                                ('post_install_rib_sampled_fib_ms', 'rib_and_sampled_fib_ms')):
        endpoint = metrics.get(source)
        if endpoint is None or installed is None:
            result['unavailable'][destination] = 'Required completion observation unavailable or no predicted transition'
        else:
            delta = endpoint - (installed - start) / 1e6
            if delta >= 0:
                result[destination] = delta
            else:
                result['unavailable'][destination] = 'Cross-stream observation order produced a negative difference'
    result['session_established_observed_ns'] = established
    result['rov_query_complete_observed_ns'] = endpoints.get('roa_query')
    result['rov_quiet_observed_ns'] = (churn.get('collection_end_ns')
                                       if churn.get('status') == 'success' and churn.get('termination') == 'quiet'
                                       else None)
    result['evidence_complete'] = (result['evidence_complete'] and installed is not None
                                   and result['rov_query_complete_observed_ns'] is not None
                                   and result['rov_quiet_observed_ns'] is not None)
    result['definition'] = ('BRIO monotonic timestamp after successful shutdown() to BRIO registration of the new BIRD TCP client, '
                            'RTR Established, changed VRP and ROV observations, and the external quiet boundary; '
                            'socket closure and reconnection are not packet-capture instants')
    return result


# Use the last restored target state after a fault, including legitimate intermediate changes.
def match_recovery_endpoints(state):
    number, start = state.submitted[0]
    state.rov.matched = []
    for batch, prefix, peer, origin, before, after in state.rov.transitions:
        rows = [row for row in state.rov.observed[(prefix, origin)] if row['observed_ns'] >= start]
        if rows and rows[-1]['after'] == after:
            state.rov.matched.append((batch, rows[-1]['query_end_ns']))
    state.rib.matched = []
    for prefix, before, after in state.rib.changes[number]:
        rows = [row for row in state.rib.observed[prefix] if row[2] >= start]
        if rows and rows[-1][1] == after:
            state.rib.matched.append((number, rows[-1][2]))
    state.fib.matched = []
    for predictions in state.fib.changes.values():
        for batch, destination, before, after in predictions:
            wanted = ('not_received', None) if after is None else ('received', after)
            rows = [row for row in state.fib.observed[destination] if row[2] >= start]
            if not rows or rows[-1][0] != wanted:
                continue
            position = len(rows) - 1
            while position > 0 and rows[position - 1][0] == wanted:
                position -= 1
            state.fib.matched.append((batch, rows[position][2]))


