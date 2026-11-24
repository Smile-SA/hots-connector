from __future__ import annotations

import time


class TickScheduler:
    def __init__(self, interval_seconds: int) -> None:
        self.interval_seconds = max(1, int(interval_seconds))

    def sleep_until_next_tick(self, started_at: float) -> None:
        # monotonic: wall-clock time.time() can jump (NTP sync, DST, manual
        # clock changes), which could turn `elapsed` negative or huge and
        # produce a wrong sleep duration for a long-running loop.
        elapsed = time.monotonic() - started_at
        time.sleep(max(0.0, self.interval_seconds - elapsed))
