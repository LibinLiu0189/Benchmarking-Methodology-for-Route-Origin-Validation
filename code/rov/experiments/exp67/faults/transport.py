"""Own a loopback RTR relay for controlled transport faults and PDU injection."""

import select
import socket
import struct
import threading
import time


# Frame TCP streams without assuming one receive call is one RTR PDU.
def frames(buffer):
    output = []
    while len(buffer) >= 8:
        length = struct.unpack_from('!I', buffer, 4)[0]
        if not 8 <= length <= 1048576:
            raise ValueError(f'Invalid RTR frame length: {length}')
        if len(buffer) < length:
            break
        output.append(bytes(buffer[:length]))
        del buffer[:length]
    return output


# Use deterministic invalid inputs with distinct RFC 8210 error expectations.
def fault_pdu(mode):
    if mode == 'unexpected':
        return struct.pack('!BBHI', 1, 255, 0, 8), 5  # Unsupported PDU Type.
    raise ValueError(f'No injected PDU for mode {mode}')


class RtrRelay:
    # Bind only loopback in the experiment namespace and retain control-PDU evidence.
    def __init__(self, listen_port, backend_port):
        self.backend_port = backend_port
        self.listener = socket.socket()
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind(('127.0.0.1', listen_port))
        self.listener.listen()
        self.listener.settimeout(.1)
        self.stop = threading.Event()
        self.paused = threading.Event()
        self.lock = threading.RLock()
        self.events = []
        self.errors = []
        self.client = self.backend = None
        self.connection = 0
        self.thread = threading.Thread(target=self._serve, name='exp67-rtr-relay')
        self.thread.start()

    # Store monotonic control evidence without logging every VRP payload.
    def record(self, kind, **values):
        with self.lock:
            row = dict(event=kind, observed_ns=time.monotonic_ns(), connection=self.connection, **values)
            self.events.append(row)
            return row

    # Accept new sessions while preserving every distinct connection identity.
    def _serve(self):
        while not self.stop.is_set():
            try:
                client, address = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:
                if not self.stop.is_set():
                    self.errors.append('RTR relay listener failed')
                break
            if self.paused.is_set():
                client.close()
                continue
            backend = None
            try:
                backend = socket.create_connection(('127.0.0.1', self.backend_port), timeout=2)
                client.settimeout(2)
                with self.lock:
                    self.client, self.backend = client, backend
                    self.connection += 1
                    self.record('connected', peer=list(address))
                self._relay(client, backend)
            except (OSError, ValueError) as error:
                if not self.stop.is_set() and not self.paused.is_set():
                    self.record('transport_error', detail=str(error))
            finally:
                with self.lock:
                    self.record('disconnected')
                    self.client = self.backend = None
                client.close()
                if backend is not None:
                    backend.close()


    # Return a stable copy while the relay thread may still be collecting.
    def snapshot(self):
        with self.lock:
            return list(self.events)

    # Stop and join the owned relay and release all sockets on every exit path.
    def close(self):
        self.stop.set()
        self.listener.close()
        with self.lock:
            for endpoint in (self.client, self.backend):
                if endpoint is not None:
                    try:
                        endpoint.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass
        self.thread.join(5)
        if self.thread.is_alive():
            raise RuntimeError('RTR relay did not stop')


# Identify established RTR sockets actually owned by BIRD for same-TCP reset evidence.
def bird_rtr_sockets(pid_file):
    from pathlib import Path
    pid = int(Path(pid_file).read_text().strip())
    inodes = set()
    for entry in Path(f'/proc/{pid}/fd').iterdir():
        try:
            target = entry.readlink().as_posix()
        except FileNotFoundError:
            continue
        if target.startswith('socket:[') and target.endswith(']'):
            inodes.add(target[8:-1])
    result = []
    for line in Path('/proc/net/tcp').read_text().splitlines()[1:]:
        fields = line.split()
        if fields[3] == '01' and fields[9] in inodes and int(fields[2].split(':')[1], 16) in (3323, 3324, 3325):
            result.append(dict(local=fields[1], remote=fields[2], inode=fields[9]))
    return sorted(result, key=lambda row: row['remote'])
