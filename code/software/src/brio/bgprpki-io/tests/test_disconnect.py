"""Exercise BRIO's selected-client disconnect using isolated loopback sockets."""

import os
from pathlib import Path
import re
import select
import socket
import struct
import subprocess
import sys
import tempfile
import time
import unittest


BRIO_BINARY = None


class DisconnectTest(unittest.TestCase):
    # Start only the supplied test binary on an unused loopback port.
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.sockets = []
        self.output = bytearray()
        with socket.socket() as reservation:
            reservation.bind(('127.0.0.1', 0))
            port = reservation.getsockname()[1]
        self.port = port
        self.process = subprocess.Popen(
            [str(BRIO_BINARY), str(port)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            cwd=self.directory.name,
        )

    # Stop only the process and sockets created by this test.
    def tearDown(self):
        for connection in self.sockets:
            connection.close()
        if self.process.poll() is None:
            self.process.stdin.write(b'quit\n')
            self.process.stdin.flush()
            try:
                self.process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self.process.terminate()
                self.process.wait(timeout=3)
        self.process.stdout.close()
        self.process.stdin.close()
        self.directory.cleanup()

    # Connect a client once BRIO begins listening.
    def connect(self):
        deadline = time.monotonic() + 5
        while True:
            try:
                connection = socket.create_connection(('127.0.0.1', self.port), timeout=1)
                connection.settimeout(2)
                self.sockets.append(connection)
                return connection
            except OSError:
                if self.process.poll() is not None or time.monotonic() >= deadline:
                    self.fail('BRIO did not start listening')
                time.sleep(.05)

    # Read one command response up to a unique echo marker.
    def command(self, value):
        marker = f'DISCONNECT_TEST_{time.monotonic_ns()}'.encode()
        terminator = b'\n' + marker + b'\n'
        self.process.stdin.write(value.encode() + b'\necho ' + marker + b'\n')
        self.process.stdin.flush()
        deadline = time.monotonic() + 5
        while terminator not in self.output:
            remaining = deadline - time.monotonic()
            if remaining <= 0 or self.process.poll() is not None:
                self.fail(f'No response to {value!r}: {self.output[-500:]!r}')
            ready, _, _ = select.select([self.process.stdout], [], [], remaining)
            if ready:
                self.output.extend(os.read(self.process.stdout.fileno(), 4096))
        response, _, remaining = self.output.partition(terminator)
        self.output = bytearray(remaining)
        return response.decode(errors='replace')

    # Match a stable BRIO identifier to this socket's ephemeral source port.
    def client_id(self, connection, listing):
        source_port = connection.getsockname()[1]
        match = re.search(rf'(\d+): fd=\d+ [^\r\n]*:{source_port}(?:\r|\n)', listing)
        self.assertIsNotNone(match, listing)
        return int(match.group(1))

    # Wait for the accept thread to publish every newly opened client.
    def listing_for(self, *connections):
        deadline = time.monotonic() + 3
        while True:
            listing = self.command('clients')
            if all(re.search(rf'fd=\d+ [^\r\n]*:{connection.getsockname()[1]}(?:\r|\n)', listing)
                   for connection in connections):
                return listing
            if time.monotonic() >= deadline:
                self.fail(f'Connected client missing from BRIO list: {listing}')
            time.sleep(.01)

    # Prove a remaining client can still obtain a normal RTR response.
    def assert_query_works(self, connection):
        connection.sendall(struct.pack('!BBHI', 1, 2, 0, 8))
        types = []
        while not types or types[-1] != 7:
            header = self.recv_exact(connection, 8)
            length = struct.unpack_from('!I', header, 4)[0]
            self.assertGreaterEqual(length, 8)
            self.recv_exact(connection, length - 8)
            types.append(header[1])
        self.assertEqual(types[0], 3)

    # Consume the precise byte count requested by an RTR frame header.
    def recv_exact(self, connection, length):
        data = bytearray()
        while len(data) < length:
            chunk = connection.recv(length - len(data))
            self.assertTrue(chunk, 'RTR query closed the client connection')
            data.extend(chunk)
        return data

    # Drain any queued PDUs and require the selected TCP connection to close.
    def assert_closed(self, connection):
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            try:
                if connection.recv(4096) == b'':
                    return
            except socket.timeout:
                continue
        self.fail('Selected client TCP connection stayed open')

    # Cover invalid targets, repeat calls, client isolation and reconnect.
    def test_selected_disconnect(self):
        first = self.connect()
        second = self.connect()
        listing = self.listing_for(first, second)
        first_id = self.client_id(first, listing)
        second_id = self.client_id(second, listing)
        self.assertNotEqual(first_id, second_id)
        self.command('add 192.0.2.0/24 24 64496')
        initial_cache = self.command('cache')
        initial_entries = re.findall(r'\[ROA\]:[^\r\n]*Serial=\d+[^\r\n]*', initial_cache)
        self.assertEqual(len(initial_entries), 1, initial_cache)
        initial_session = self.command('sessionID')

        for invalid in ('0', 'abc', '9999999999'):
            self.assertIn('Error:', self.command(f'disconnect {invalid}'))
        self.assert_query_works(first)
        self.assert_query_works(second)

        reply = self.command(f'disconnect {first_id}')
        match = re.search(rf'BRIO_DISCONNECT client={first_id} shutdown_ns=(\d+)', reply)
        self.assertIsNotNone(match, reply)
        self.assertGreater(int(match.group(1)), 0)
        self.assert_closed(first)
        self.assertIn('Error:', self.command(f'disconnect {first_id}'))
        self.assert_query_works(second)

        third = self.connect()
        third_id = self.client_id(third, self.listing_for(third))
        self.assertGreater(third_id, max(first_id, second_id))
        self.assert_query_works(third)
        self.assertEqual(
            initial_entries,
            re.findall(r'\[ROA\]:[^\r\n]*Serial=\d+[^\r\n]*', self.command('cache')),
        )
        self.assertEqual(
            re.search(r'Current SESSION ID: \d+', initial_session).group(),
            re.search(r'Current SESSION ID: \d+', self.command('sessionID')).group(),
        )

    # Exercise the existing invalid-session path that calls dropSession.
    def test_mismatched_session_drops_only_its_client(self):
        first = self.connect()
        second = self.connect()
        self.listing_for(first, second)

        first.sendall(struct.pack('!BBHII', 1, 1, 1, 12, 0))
        self.assert_closed(first)
        self.assert_query_works(second)

        third = self.connect()
        self.listing_for(third)
        self.assert_query_works(third)


if __name__ == '__main__':
    if len(sys.argv) != 2:
        raise SystemExit('Usage: python3 test_disconnect.py <isolated-brio-binary>')
    BRIO_BINARY = Path(sys.argv.pop()).resolve()
    unittest.main()
