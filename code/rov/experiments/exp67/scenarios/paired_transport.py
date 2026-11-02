"""Forward one shared update batch, replacing selected PDUs for scenario 4."""

from collections import Counter
import ipaddress as ip
import select
import struct
import time

from rov.experiments.exp67.faults.transport import RtrRelay, fault_pdu, frames


# Decode a legal Prefix PDU into the existing operation identity.
def prefix_operation(pdu):
    if pdu[1] not in (4, 6) or len(pdu) != (20 if pdu[1] == 4 else 32):
        raise ValueError('Expected a legal IPv4 or IPv6 Prefix PDU')
    prefix = str(ip.ip_network((ip.ip_address(pdu[12:-4]), pdu[9])))
    return dict(operation='add' if pdu[8] & 1 else 'withdraw',
                vrp=[prefix, pdu[10], struct.unpack('!I', pdu[-4:])[0]])


class PairedRelay(RtrRelay):
    # Keep the existing owned relay lifecycle and isolate new behavior to scenario 4.
    def __init__(self, operations, mixed):
        self.operations = operations
        self.mixed = mixed
        self.armed = False
        self.batch_done = False
        self.buffered_updates = []
        self.original_batch = []
        self.sent_updates = []
        self.send_failure = None
        self.expected_wire = None
        super().__init__(3323, 3324)

    # Record the common prepared workload boundary before the existing FIFO sender runs.
    def arm(self, expected_wire=None):
        with self.lock:
            self.expected_wire = expected_wire
            self.armed = True
            self.record('batch_armed', planned_updates=len(self.operations),
                        planned_errors=len(range(10, len(self.operations) + 1, 10)) if self.mixed else 0)

    # Require exactly the shared legal batch before attempting any mutated send.
    def send_updates(self, client):
        packets = self.buffered_updates
        wanted = Counter((row['operation'], *row['vrp']) for row in self.operations)
        actual = Counter((row['operation'], *row['vrp']) for row in map(prefix_operation, packets))
        if len(packets) != len(self.operations) or actual != wanted:
            raise ValueError('BRIO incremental batch differs from the shared legal operations')
        originals = [pdu.hex() for pdu in packets]
        if self.expected_wire is not None and originals != self.expected_wire:
            raise ValueError('Mixed group original wire order differs from the normal group')
        self.original_batch = [dict(position=i, original_pdu_hex=pdu.hex(), **prefix_operation(pdu))
                               for i, pdu in enumerate(packets, 1)]
        self.record('batch_verified', updates=len(packets), common_wire_order=self.expected_wire is not None)
        self.batch_done = True
        for position, original in enumerate(packets, 1):
            changed = self.mixed and position % 10 == 0
            pdu = fault_pdu('unexpected')[0] if changed else original
            if changed:
                # Refuse a subsequent baseline reload; never interrupt this active connection.
                self.paused.set()
            started = time.monotonic_ns()
            try:
                client.sendall(pdu)
            except OSError as error:
                self.send_failure = self.record('update_send_failed', position=position,
                                                send_start_ns=started, detail=str(error),
                                                delivery='Partial or zero bytes possible; not counted as sent')
                break
            row = self.record('update_sent', position=position, malformed=changed,
                              send_start_ns=started, send_end_ns=time.monotonic_ns(), pdu_hex=pdu.hex())
            self.sent_updates.append(row)
        self.buffered_updates = []

    # Preserve stream framing and return explicit DUT ErrorReport and EOF evidence.
    def _relay(self, client, backend):
        buffers = {client: bytearray(), backend: bytearray()}
        while not self.stop.is_set():
            ready, _, _ = select.select([client, backend], [], [], .1)
            for source in ready:
                try:
                    data = source.recv(65536)
                except ConnectionResetError as error:
                    self.record('peer_reset', direction='dut_to_cache' if source is client else 'cache_to_dut',
                                detail=str(error))
                    return
                if not data:
                    self.record('peer_eof', direction='dut_to_cache' if source is client else 'cache_to_dut')
                    return
                buffers[source].extend(data)
                packets = frames(buffers[source])
                direction = 'dut_to_cache' if source is client else 'cache_to_dut'
                for pdu in packets:
                    if pdu[1] in (0, 1, 2, 3, 7, 8, 10):
                        values = dict(direction=direction, pdu_type=pdu[1], length=len(pdu))
                        if pdu[1] == 10:
                            values.update(error_code=struct.unpack_from('!H', pdu, 2)[0], pdu_hex=pdu.hex())
                        self.record('pdu', **values)
                if not self.armed:
                    (backend if source is client else client).sendall(data)
                    continue
                for pdu in packets:
                    if source is client:
                        if pdu[1] != 10:
                            backend.sendall(pdu)
                        continue
                    if pdu[1] in (4, 6):
                        if self.batch_done:
                            raise ValueError('Unexpected additional Prefix PDU after the one shared batch')
                        self.buffered_updates.append(pdu)
                        continue
                    if pdu[1] == 7 and self.buffered_updates:
                        if len(self.buffered_updates) > len(self.operations):
                            raise ValueError('BRIO sent more Prefix PDUs than the shared legal operations')
                        if len(self.buffered_updates) == len(self.operations):
                            self.send_updates(client)
                    if self.send_failure is None:
                        try:
                            started = time.monotonic_ns()
                            client.sendall(pdu)
                            if pdu[1] in (0, 8):
                                self.record('control_sent', pdu_type=pdu[1],
                                            send_start_ns=started,
                                            send_end_ns=time.monotonic_ns())
                            if pdu[1] == 7 and self.batch_done:
                                self.record('batch_eod_sent')
                        except OSError as error:
                            self.send_failure = self.record('control_send_failed', pdu_type=pdu[1], detail=str(error))

    # Separate sender acceptance from peer evidence and planned mutations.
    def report(self):
        evidence = self.snapshot()
        errors = [row for row in evidence if row.get('pdu_type') == 10 and row.get('direction') == 'dut_to_cache']
        closed = any(row['event'] in ('peer_eof', 'peer_reset') and row.get('direction') == 'dut_to_cache'
                     for row in evidence)
        total = len(self.operations)
        mutation_positions = list(range(10, total + 1, 10)) if self.mixed else []
        return dict(planned_total=total, planned_legal=total - len(mutation_positions),
                    planned_errors=len(mutation_positions),
                    mutation_positions=mutation_positions,
                    error_type='Unsupported PDU Type 255' if self.mixed else None,
                    expected_error_code=5 if self.mixed else None,
                    first_error_position=mutation_positions[0] if mutation_positions else None,
                    original_batch=self.original_batch, sent_updates=self.sent_updates,
                    successful_send_calls=len(self.sent_updates),
                    sent_error_pdus=sum(row['malformed'] for row in self.sent_updates),
                    send_failure=self.send_failure, error_reports=errors, dut_closed=closed,
                    evidence=evidence,
                    delivery_definition='Successful sendall proves sender socket acceptance only, not DUT receipt or processing')
