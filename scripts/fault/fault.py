"""atalho de linha de comando para injetar e reverter falhas no cluster docker.

exemplos:
    python scripts/fault/fault.py kill storage-3
    python scripts/fault/fault.py start storage-3
    python scripts/fault/fault.py pause storage-2
    python scripts/fault/fault.py unpause storage-2
    python scripts/fault/fault.py disconnect storage-4
    python scripts/fault/fault.py reconnect storage-4
    python scripts/fault/fault.py netem storage-1 --delay-ms 200 --loss-percent 5
    python scripts/fault/fault.py netem-clear storage-1
    python scripts/fault/fault.py corrupt storage-2 <block_id>
    python scripts/fault/fault.py restart central
    python scripts/fault/fault.py list
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from observability.faults import main

if __name__ == "__main__":
    sys.exit(main())

