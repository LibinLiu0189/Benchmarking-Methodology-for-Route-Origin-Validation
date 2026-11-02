"""Read persistent official birdc log echo without changing BIRD's log buffer."""

from collections import deque
import os
import re
import selectors
import subprocess
import time


class BirdcLog:
    # Keep stdin open and use an evaluated marker to acknowledge the subscription.
    def __init__(self, binary, socket_path, timeout, classes):
        self.selector = selectors.DefaultSelector()
        self.partial = b""
        self.lines = deque()
        self.logs = deque()
        self.closed = False
        self.process = None
        self.subscription = f"echo {{ {classes} }} 65536"
        try:
            self.process = subprocess.Popen(
                ["stdbuf", "-oL", "-eL", str(binary), "-v", "-s", str(socket_path)],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                start_new_session=True,
            )
            self.selector.register(self.process.stdout, selectors.EVENT_READ, "stdout")
            self.selector.register(self.process.stderr, selectors.EVENT_READ, "stderr")
            self.process.stdin.write((self.subscription + "\neval 620019\n").encode())
            self.process.stdin.flush()
            deadline = time.monotonic() + timeout
            while True:
                text = self._read_line(deadline)
                if text == "0023 620019":
                    if self.process.poll() is not None:
                        raise RuntimeError(f"birdc exited during subscription ({self.process.returncode})")
                    break
                log = self._translate(text)
                if log is not None:
                    self.logs.append(log)
        except BaseException:
            self.close()
            raise

    # Expose unfinished bytes for the existing drained-stream check.
    @property
    def buffer(self):
        return self.partial

    # Drain ready pipes in chunks; stderr, EOF, and unexpected exits are failures.
    def _read_line(self, deadline):
        while not self.lines:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("Timed out reading persistent birdc output")
            ready = self.selector.select(remaining)
            if not ready:
                raise TimeoutError("Timed out reading persistent birdc output")
            errors = []
            stdout_closed = False
            for key, _ in ready:
                block = os.read(key.fd, 65536)
                if not block:
                    self.selector.unregister(key.fileobj)
                    stdout_closed |= key.data == "stdout"
                    continue
                if key.data == "stderr":
                    errors.append(block.decode("utf-8", errors="replace").strip())
                    continue
                parts = (self.partial + block).split(b"\n")
                self.partial = parts.pop()
                self.lines.extend(parts)
            if errors:
                raise RuntimeError("birdc stderr: " + "\n".join(errors))
            if stdout_closed:
                raise ConnectionError(
                    f"birdc stdout closed unexpectedly (exit={self.process.poll()})"
                )
        text = self.lines.popleft().decode("utf-8", errors="replace")
        if "messages lost>" in text:
            raise RuntimeError(f"BIRD log overflow: {text}")
        # Readline may redraw a prompt with carriage returns even on a pipe.
        text = text.rsplit("\r", 1)[-1].strip()
        while text.startswith("bird>"):
            text = text[5:].lstrip()
        return text

    # Remove only known client framing and retain the shared parser's plus prefix.
    def _translate(self, text):
        if not text or text in (self.subscription, "eval 620019"):
            return None
        if text.startswith("0001 BIRD "):
            return None
        if text.startswith(">>> "):
            return "+" + text[4:]
        match = re.match(r"(\d{4})[ -](.*)", text)
        if match and int(match[1]) >= 8000:
            raise RuntimeError(f"birdc command failed: {text}")
        raise RuntimeError(f"Unexpected birdc output: {text}")

    # Return one native log line, leaving timestamp assignment in the shared reader.
    def line(self, deadline):
        if self.logs:
            return self.logs.popleft()
        while True:
            text = self._read_line(deadline)
            log = self._translate(text)
            if log is not None:
                return log

    # Stop only this owned client and reap it even after an output or protocol error.
    def close(self):
        if self.closed:
            return
        self.closed = True
        try:
            if self.process is not None:
                if self.process.poll() is None:
                    self.process.terminate()
                try:
                    self.process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                    self.process.wait()
        finally:
            self.selector.close()
            if self.process is not None:
                for stream in (self.process.stdin, self.process.stdout, self.process.stderr):
                    if stream is not None:
                        stream.close()
