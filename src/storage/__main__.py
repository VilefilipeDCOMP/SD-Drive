from __future__ import annotations

import signal
import sys

from . import jsonlog
from .config import from_env
from .node import StorageNode


def main(argv=None) -> int:
    cfg = from_env(argv)
    node = StorageNode(cfg)
    try:
        node.start()
    except Exception as exc:
        jsonlog.emit("storage.start_failed", level="critical", error=str(exc))
        return 1

    def shutdown(signum, _frame):
        jsonlog.emit("storage.signal", signal=signal.Signals(signum).name)
        node.stop()

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    if hasattr(signal, "SIGUSR1"):
        signal.signal(signal.SIGUSR1, lambda *_: node.scrubber.trigger())
    node.wait()
    return 0


if __name__ == "__main__":
    sys.exit(main())
