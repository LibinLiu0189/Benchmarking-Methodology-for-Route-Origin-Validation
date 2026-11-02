"""Keep complete child output while making normal service shutdown concise."""

import os
from pathlib import Path
import re
import selectors
import signal
import subprocess
import sys


MANAGED_OUTPUT = "ROV_OUTPUT_MANAGED"
SHUTDOWN_START = b"Stopping owned services."
SHUTDOWN_END = b"Owned services stopped; namespace resources are released on exit."
ERROR_TEXT = re.compile(rb"error|fail|invalid|warn|exception|traceback|panic|fatal|assert", re.I)


# Record both streams before filtering only normal shutdown output for display.
def copy_output(process, log=None, verbose_shutdown=False):
    shutdown = False
    display_failed = False
    tail = bytearray()
    buffers = {}

    # Preserve errors on stdout and all stderr except the known normal stop banner.
    def display(line, is_stderr):
        nonlocal shutdown, display_failed
        boundary = SHUTDOWN_START in line or SHUTDOWN_END in line
        if SHUTDOWN_START in line:
            shutdown = True
        normal_banner = is_stderr and line.rstrip().endswith((
            b"<INFO> Shutting down", b"<FATAL> Shutdown completed",
        ))
        zero_counter = re.fullmatch(rb"\s*RX Protocol Error:\s+0 packets\s*", line) is not None
        diagnostic = (is_stderr and not normal_banner) or (
            ERROR_TEXT.search(line) is not None and not zero_counter and not normal_banner
        )
        show = not shutdown or verbose_shutdown or boundary or diagnostic
        if shutdown and not show:
            tail.extend(line)
            del tail[:-8192]
        if show and not display_failed:
            try:
                position = 0
                while position < len(line):
                    position += os.write(2 if is_stderr else 1, line[position:])
            except OSError:
                display_failed = True
        if SHUTDOWN_END in line:
            shutdown = False

    with selectors.DefaultSelector() as selector:
        for stream, is_stderr in ((process.stdout, False), (process.stderr, True)):
            if stream is not None:
                descriptor = stream.fileno()
                selector.register(descriptor, selectors.EVENT_READ, is_stderr)
                buffers[descriptor] = bytearray()
        while selector.get_map():
            for key, _ in selector.select():
                chunk = os.read(key.fd, 65536)
                pending = buffers[key.fd]
                if not chunk:
                    if pending:
                        display(bytes(pending), key.data)
                    selector.unregister(key.fd)
                    continue
                if log is not None:
                    log.write(chunk)
                    log.flush()
                pending.extend(chunk)
                while b"\n" in pending:
                    line, _, rest = pending.partition(b"\n")
                    pending[:] = rest
                    display(bytes(line) + b"\n", key.data)
                if len(pending) > 65536:
                    display(bytes(pending), key.data)
                    pending.clear()
    # Unknown diagnostics on stdout remain available even if their wording is unusual.
    if process.wait() != 0 and tail and not verbose_shutdown:
        display(b"Shutdown output tail (child exited unsuccessfully):\n", True)
        display(bytes(tail), True)
    return display_failed


# Stop only this supervisor's child group and preserve its cleanup output.
def stop_child(process):
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass


# Reuse the same capture path for direct initialization and direct experiment runs.
def supervise(module, arguments, *, log=None, verbose_shutdown=False):
    environment = dict(os.environ, **{MANAGED_OUTPUT: "1"})
    process = None
    interrupted = False
    path = Path(log.name).resolve() if log is not None else None
    if path is not None:
        print(f"Complete run log: {path}", flush=True)
    try:
        process = subprocess.Popen(
            [sys.executable, "-u", "-m", module, *arguments],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            env=environment, start_new_session=True,
        )
        try:
            display_failed = copy_output(process, log, verbose_shutdown)
        except KeyboardInterrupt:
            interrupted = True
            signal.signal(signal.SIGINT, signal.SIG_IGN)
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
            stop_child(process)
            display_failed = copy_output(process, log, verbose_shutdown)
        code = process.wait()
        if interrupted:
            return 130
        if display_failed:
            return 1
        return code if code >= 0 else 128 - code
    except OSError as error:
        message = f"Output capture failed: {error}\n"
        if log is not None:
            log.write(message.encode())
            log.flush()
        print(message, end="", file=sys.stderr, flush=True)
        return 1
    finally:
        if process is not None:
            if process.poll() is None:
                stop_child(process)
                process.stdout.close()
                process.stderr.close()
                process.wait()
            else:
                process.stdout.close()
                process.stderr.close()
        if path is not None:
            print(f"Complete run log: {path}", flush=True)
