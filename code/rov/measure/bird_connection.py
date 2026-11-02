"""Communicate with BIRD through its control socket."""

import re
import socket
import time


class BirdError(RuntimeError):
    # Keep the server error code so callers can distinguish a missing route.
    def __init__(self, code, text):
        super().__init__(f"BIRD {code}: {text}")
        self.code = code


class BirdConnection:
    # Open one persistent Unix socket and consume its welcome message.
    def __init__(self, path, timeout=10):
        self.socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.buffer = bytearray()
        self.timeout = timeout
        self.on_log = None
        try:
            self.socket.settimeout(timeout)
            self.socket.connect(str(path))
            self.reply(time.monotonic() + timeout)
        except BaseException:
            self.close()
            raise

    # Read complete lines while retaining partial lines across idle timeouts.
    def line(self, deadline):
        while b"\n" not in self.buffer:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("BIRD reply timed out")
            self.socket.settimeout(remaining)
            block = self.socket.recv(65536)
            if not block:
                raise ConnectionError("BIRD closed the control connection")
            self.buffer.extend(block)
        line, _, rest = self.buffer.partition(b"\n")
        self.buffer = bytearray(rest)
        text = line.decode("utf-8", errors="replace")
        if "messages lost>" in text:
            raise RuntimeError(f"BIRD log overflow: {text}")
        return text

    # Consume a complete command reply, dispatching interleaved log messages.
    def reply(self, deadline):
        lines = []
        error = None
        while True:
            text = self.line(deadline)
            if text.startswith("+"):
                if self.on_log is not None:
                    self.on_log(text[1:], time.monotonic_ns())
                continue
            if text.startswith(" "):
                lines.append(text[1:])
                continue
            match = re.fullmatch(r"(\d{4})([ -])(.*)", text)
            if not match:
                raise RuntimeError(f"Malformed BIRD response: {text}")
            code, separator, content = match.groups()
            code = int(code)
            if code:
                lines.append(content)
            if code >= 8000:
                error = (code, content)
            if separator == " ":
                if error:
                    raise BirdError(*error)
                return "\n".join(lines)

    # Send one command and drain its reply before sending another command.
    def command(self, command):
        if "\n" in command or "\r" in command or len(command.encode()) >= 1000:
            raise ValueError("Expected one short BIRD command")
        try:
            deadline = time.monotonic() + self.timeout
            self.socket.settimeout(self.timeout)
            self.socket.sendall((command + "\n").encode())
            return self.reply(deadline)
        except BirdError:
            raise
        except BaseException:
            self.close()
            raise

    # Release the control connection without stopping BIRD.
    def close(self):
        self.socket.close()
