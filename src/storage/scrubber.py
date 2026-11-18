from __future__ import annotations

import threading
import time
from typing import Optional

from . import jsonlog
from .metrics import Metrics
from .store import BlockStore, RateLimiter, VerifyResult


class Scrubber(threading.Thread):
    def __init__(self, store: BlockStore, interval: float, max_bytes_per_sec: int, metrics: Optional[Metrics] = None):
        super().__init__(name="storage-scrubber", daemon=True)
        self.store = store
        self.interval = interval
        self.metrics = metrics
        self._halt = threading.Event()
        self._wake = threading.Event()
        self._limiter = RateLimiter(max_bytes_per_sec, self._halt)
        self._pass_lock = threading.Lock()
        self.passes_completed = 0

    def trigger(self) -> None:
        self._wake.set()

    def stop(self) -> None:
        self._halt.set()
        self._wake.set()

    def run(self) -> None:
        while not self._halt.is_set():
            timeout = self.interval if self.interval > 0 else None
            self._wake.wait(timeout=timeout)
            if self._halt.is_set():
                break
            self._wake.clear()
            self.run_pass()

    def run_pass(self) -> dict:
        with self._pass_lock:
            ids = self.store.block_ids()
            started = time.monotonic()
            jsonlog.emit("storage.scrub_started", blocks=len(ids))
            counts = {r: 0 for r in VerifyResult}
            self._limiter = RateLimiter(self._limiter.max_bps, self._halt)
            for block_id in ids:
                if self._halt.is_set():
                    break
                counts[self.store.verify(block_id, "scrub", self._limiter)] += 1
            duration = time.monotonic() - started
            verified = counts[VerifyResult.OK] + counts[VerifyResult.CORRUPT] + counts[VerifyResult.MISSING]
            self.passes_completed += 1
            if self.metrics:
                self.metrics.scrub_done(verified, duration)
            summary = {"blocks": len(ids), "verified": verified, "corrupt": counts[VerifyResult.CORRUPT],
                       "missing": counts[VerifyResult.MISSING], "duration_ms": int(duration * 1000)}
            jsonlog.emit("storage.scrub_completed", **summary)
            return summary
