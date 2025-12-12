import threading
from typing import Callable


class RepeatedScheduler:
    def __init__(self, interval_seconds: int, func: Callable[[], None]) -> None:
        self.interval = interval_seconds
        self.func = func
        self._timer: threading.Timer | None = None
        self._running = False

    def _run(self) -> None:
        if not self._running:
            return
        self.func()
        self._timer = threading.Timer(self.interval, self._run)
        self._timer.daemon = True
        self._timer.start()

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._timer = threading.Timer(0, self._run)
        self._timer.daemon = True
        self._timer.start()

    def stop(self) -> None:
        self._running = False
        if self._timer:
            self._timer.cancel()
            self._timer = None

    def running(self) -> bool:
        return self._running
