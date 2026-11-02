"""Send bounded newline-separated eval commands before reading ordered replies."""

import time
from .roa_batch import labelled, parse


# Bind every complete response to its unique position and abandon errors atomically.
def query(connection, targets, indices, *, on_response=None):
    commands = ["eval " + labelled(i, targets[i]) for i in indices]
    payload = ("\n".join(commands) + "\n").encode()
    if any(len(c.encode()) >= 1000 for c in commands) or len(payload) > 1023:
        raise ValueError("Pipeline group exceeds a verified input byte limit")
    connection.assert_idle()
    received = connection.received_bytes
    started = time.monotonic_ns()
    deadline = time.monotonic() + connection.timeout
    states, completed = [], []
    try:
        connection.socket.settimeout(connection.timeout)
        connection.socket.sendall(payload)
        sent = time.monotonic_ns()
        for index in indices:
            text = connection.reply(deadline)
            completed.append(time.monotonic_ns())
            state = parse(text, [index])[0]
            states.append(state)
            if on_response is not None:
                on_response(index, state, started, completed[-1])
        connection.assert_idle()
    except BaseException:
        # Never reuse a stream containing outstanding replies after any failure.
        connection.close()
        raise
    return states, dict(
        start_ns=started, send_end_ns=sent, end_ns=completed[-1],
        response_end_ns=completed,
        queue_wait_bounds_ns=[[0, end - started] for end in completed],
        sent_bytes=len(payload),
        received_bytes=connection.received_bytes - received,
    )
