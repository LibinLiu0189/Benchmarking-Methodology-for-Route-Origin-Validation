"""Submit a prepared BRIO batch script to the running cache."""

import errno
import os
from pathlib import Path
import stat
import time

from rov.paths import CONFIG


# Submit one script without claiming cache completion or an RTR wire timestamp.
def send_batch(script, fifo=CONFIG / "brio.in", *, atomic=False):
    script = Path(script).resolve(strict=True)
    if not script.is_file():
        raise ValueError("The batch script must be a file")
    operation = "runBatch" if atomic else "run"
    command = f"{operation} {script}\n".encode()
    if any(c.isspace() for c in str(script)) or "%" in str(script) or len(command) >= 254:
        raise ValueError("BRIO script path contains unsupported characters or is too long")
    descriptor = os.open(fifo, os.O_WRONLY | os.O_NONBLOCK)
    try:
        if not stat.S_ISFIFO(os.fstat(descriptor).st_mode):
            raise ValueError("The BRIO input must be a FIFO")
        started = time.monotonic_ns()
        if os.write(descriptor, command) != len(command):
            raise RuntimeError("Incomplete BRIO command submission")
        submitted = time.monotonic_ns()
    finally:
        os.close(descriptor)
    return {"script": str(script), "write_start_ns": started, "submitted_ns": submitted}


# Retry only an atomic FIFO write that accepted no bytes, retaining sender backpressure.
def submit_with_backpressure(script, fifo, timeout, wait, check_alive=None):
    attempted_ns = time.monotonic_ns()
    deadline = time.monotonic() + timeout
    retries = 0
    while True:
        if check_alive is not None:
            check_alive()
        try:
            submission = send_batch(script, fifo)
            submission.update(first_attempt_ns=attempted_ns, fifo_backpressure_retries=retries,
                              fifo_backpressure_ms=(submission["write_start_ns"] - attempted_ns) / 1e6)
            return submission
        except BlockingIOError as error:
            # send_batch validates a <254-byte command; Linux PIPE_BUF is >=4096.
            # EAGAIN therefore accepted no bytes and retry cannot duplicate a command.
            if error.errno not in (errno.EAGAIN, errno.EWOULDBLOCK):
                raise
            retries += 1
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f"FIFO backpressure did not clear after {retries} retries") from error
            wait(min(0.005, remaining))
