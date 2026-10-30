"""Thread-safe in-memory key-value storage and FIFO task queue."""

import threading
import time
from collections import deque


class Storage:
    def __init__(self, max_queue_size=10000):
        self._values = {}
        self._values_lock = threading.Lock()

        self._tasks = deque()
        self._max_queue_size = max_queue_size
        self._queue_condition = threading.Condition(threading.Lock())

    def put(self, key, value):
        with self._values_lock:
            self._values[key] = value

    def get(self, key):
        # Return a separate found flag because JSON null is a valid value.
        with self._values_lock:
            if key not in self._values:
                return False, None
            return True, self._values[key]

    def enqueue(self, value):
        with self._queue_condition:
            if len(self._tasks) >= self._max_queue_size:
                return False

            self._tasks.append(value)
            self._queue_condition.notify()
            return True

    def dequeue(self, timeout=0):
        deadline = time.monotonic() + timeout

        with self._queue_condition:
            # A loop is required because waking does not guarantee
            # an item is still available.
            while not self._tasks:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False, None

                # wait() releases the lock while sleeping and
                # reacquires it before returning.
                self._queue_condition.wait(remaining)

            return True, self._tasks.popleft()
