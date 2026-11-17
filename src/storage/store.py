from __future__ import annotations

import errno
import hashlib
import json
import os
import re
import shutil
import threading
import time
import uuid
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Callable, Optional

from . import jsonlog

READ_CHUNK = 1024 * 1024
_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$")

class StoreError(Exception):
    pass


class InvalidBlockId(StoreError):
    pass


class BlockNotFound(StoreError):
    pass


class BlockCorrupt(StoreError):
    pass


class BlockConflict(StoreError):
    pass

class InsufficientSpace(StoreError):
    def __init__(self, needed: int, available: int):
        super().__init__(f"espaço insuficiente: necessário {needed} B, disponível {available} B")
        self.needed = needed
        self.available = available


class SizeMismatch(StoreError):
    pass


class HashMismatch(StoreError):
    def __init__(self, expected: bytes, observed: bytes):
        super().__init__(f"SHA-256 divergente: esperado {expected.hex()}, observado {observed.hex()}")
        self.expected = expected
        self.observed = observed


class VerifyResult(Enum):
    OK = "ok"
    CORRUPT = "corrupt"
    MISSING = "missing"
    SKIPPED = "skipped"


class Existing(Enum):
    ABSENT = "absent"
    SAME = "same"
    STALE = "stale"


def validate_block_id(block_id: str) -> str:
    if not isinstance(block_id, str) or not _ID_RE.match(block_id):
        raise InvalidBlockId(f"block_id inválido: {block_id!r}")
    return block_id

@dataclass
class BlockRecord:
    block_id: str
    size: int
    sha256: bytes
    version: int
    upload_id: str
    stored_at_ms: int
    corrupt: bool = False
    last_verified: float = 0.0


@dataclass(frozen=True)
class Usage:
    total_bytes: int
    used_bytes: int
    available_bytes: int
    blocks: int


class RateLimiter:

    def __init__(self, max_bps: int, halt: Optional[threading.Event] = None):
        self.max_bps = max_bps
        self._halt = halt or threading.Event()
        self._start = time.monotonic()
        self._sent = 0

    def __call__(self, nbytes: int) -> None:
        if self.max_bps <= 0:
            return
        self._sent += nbytes
        expected = self._sent / self.max_bps
        elapsed = time.monotonic() - self._start
        if expected > elapsed:
            self._halt.wait(expected - elapsed)


def fsync_dir(path: Path) -> None:
    try:
        fd = os.open(str(path), os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def hash_file(path: Path, limiter: Optional[Callable[[int], None]] = None) -> tuple[bytes, int]:
    h = hashlib.sha256()
    total = 0
    with open(path, "rb") as f:
        while True:
            chunk = f.read(READ_CHUNK)
            if not chunk:
                break
            h.update(chunk)
            total += len(chunk)
            if limiter:
                limiter(len(chunk))
    return h.digest(), total


class PendingWrite:

    def __init__(self, store: "BlockStore", block_id: str, expected_size: int, expected_sha: bytes,
                 version: int, upload_id: str):
        self.store = store
        self.block_id = block_id
        self.expected_size = expected_size
        self.expected_sha = expected_sha
        self.version = version
        self.upload_id = upload_id
        self.path = store.tmp_dir / f"{block_id}.{uuid.uuid4().hex}.part"
        self.written = 0
        self.reserved = 0
        self._h = hashlib.sha256()
        self._f = None
        self._done = False

    def _open(self) -> None:
        self._f = open(self.path, "wb")

    def write(self, data: bytes) -> None:
        if self.written + len(data) > self.expected_size:
            raise SizeMismatch(
                f"bloco excede o tamanho declarado ({self.expected_size} B) ao receber {self.written + len(data)} B")
        self._f.write(data)
        self._h.update(data)
        self.written += len(data)

    def commit(self) -> BlockRecord:
        try:
            if self.written != self.expected_size:
                raise SizeMismatch(f"tamanho recebido {self.written} B difere do declarado {self.expected_size} B")
            digest = self._h.digest()
            if digest != self.expected_sha:
                raise HashMismatch(self.expected_sha, digest)
            self._f.flush()
            if self.store.fsync:
                os.fsync(self._f.fileno())
            self._f.close()
            self._f = None
            record = self.store._publish(self)
            self._done = True
            return record
        except BaseException:
            self.abort()
            raise

    def abort(self) -> None:
        if self._done:
            return
        self._done = True
        if self._f is not None:
            try:
                self._f.close()
            except OSError:
                pass
            self._f = None
        try:
            self.path.unlink()
        except FileNotFoundError:
            pass
        except OSError:
            pass
        self.store._release(self)


class BlockStore:
    def __init__(self, block_dir: Path, capacity_bytes: int, node_id: str = "", fsync: bool = True):
        self.block_dir = Path(block_dir)
        self.tmp_dir = self.block_dir / ".tmp"
        self.capacity = int(capacity_bytes)
        self.node_id = node_id
        self.fsync = fsync
        self.corruption_listener: Optional[Callable[[str, bytes, str], None]] = None
        self._lock = threading.RLock()
        self._stripes = [threading.Lock() for _ in range(64)]
        self._index: dict[str, BlockRecord] = {}
        self._used = 0
        self._reserved = 0
        self._load()

    def _dir(self, block_id: str) -> Path:
        return self.block_dir / block_id[:2]

    def path_for(self, block_id: str) -> Path:
        return self._dir(block_id) / f"{block_id}.blk"

    def _meta_path(self, block_id: str) -> Path:
        return self._dir(block_id) / f"{block_id}.meta"

    def _marker_path(self, block_id: str) -> Path:
        return self._dir(block_id) / f"{block_id}.corrupt"

    def _block_lock(self, block_id: str) -> threading.Lock:
        return self._stripes[hash(block_id) % len(self._stripes)]

    def _load(self) -> None:
        self.block_dir.mkdir(parents=True, exist_ok=True)
        self.tmp_dir.mkdir(parents=True, exist_ok=True)
        for leftover in self.tmp_dir.iterdir():
            try:
                leftover.unlink()
                jsonlog.emit("storage.partial_write_removed", level="warning", file=leftover.name)
            except OSError:
                pass
        for sub in sorted(self.block_dir.iterdir()):
            if not sub.is_dir() or sub.name.startswith("."):
                continue
            seen_meta: set[str] = set()
            for meta_path in sorted(sub.glob("*.meta")):
                block_id = meta_path.stem
                blk = sub / f"{block_id}.blk"
                try:
                    meta = json.loads(meta_path.read_text(encoding="utf-8"))
                    record = BlockRecord(
                        block_id=block_id, size=int(meta["size"]), sha256=bytes.fromhex(meta["sha256"]),
                        version=int(meta.get("version", 0)), upload_id=str(meta.get("upload_id", "")),
                        stored_at_ms=int(meta.get("stored_at_ms", 0)))
                except (OSError, ValueError, KeyError):
                    jsonlog.emit("storage.orphan_removed", level="warning", block_id=block_id, reason="meta_invalid")
                    self._unlink_all(block_id)
                    continue
                if not blk.exists():
                    jsonlog.emit("storage.orphan_removed", level="warning", block_id=block_id, reason="blk_missing")
                    self._unlink_all(block_id)
                    continue
                seen_meta.add(block_id)
                if blk.stat().st_size != record.size or self._marker_path(block_id).exists():
                    record.corrupt = True
                    try:
                        self._marker_path(block_id).touch()
                    except OSError:
                        pass
                self._index[block_id] = record
                self._used += record.size
            for blk in sub.glob("*.blk"):
                if blk.stem not in seen_meta:
                    jsonlog.emit("storage.orphan_removed", level="warning", block_id=blk.stem, reason="meta_missing")
                    self._unlink_all(blk.stem)
            for marker in sub.glob("*.corrupt"):
                if marker.stem not in self._index:
                    try:
                        marker.unlink()
                    except OSError:
                        pass

    def _unlink_all(self, block_id: str) -> None:
        for p in (self.path_for(block_id), self._meta_path(block_id), self._marker_path(block_id)):
            try:
                p.unlink()
            except FileNotFoundError:
                pass
            except OSError:
                pass

    def _available_locked(self) -> int:
        logical = max(0, self.capacity - self._used - self._reserved)
        try:
            free = shutil.disk_usage(self.block_dir).free
        except OSError:
            return logical
        physical = max(0, free - self._reserved)
        return min(logical, physical)

    def usage(self) -> Usage:
        with self._lock:
            return Usage(total_bytes=self.capacity, used_bytes=self._used + self._reserved,
                         available_bytes=self._available_locked(), blocks=len(self._index))

    def get_record(self, block_id: str) -> Optional[BlockRecord]:
        with self._lock:
            return self._index.get(block_id)

    def block_ids(self) -> list[str]:
        with self._lock:
            return list(self._index.keys())

    def corrupt_block_ids(self) -> list[str]:
        with self._lock:
            return [b for b, r in self._index.items() if r.corrupt]

    def inventory(self) -> list[BlockRecord]:
        with self._lock:
            return [r for r in self._index.values() if not r.corrupt]

    def classify_existing(self, block_id: str, size: int, sha256: bytes) -> Existing:
        rec = self.get_record(block_id)
        if rec is None:
            return Existing.ABSENT
        if rec.corrupt:
            return Existing.STALE
        if rec.sha256 != sha256 or rec.size != size:
            raise BlockConflict(
                f"block_id {block_id} já existe com conteúdo diferente (blocos são imutáveis)")
        result = self.verify(block_id, "store")
        return Existing.SAME if result == VerifyResult.OK else Existing.STALE

    def begin_write(self, block_id: str, expected_size: int, expected_sha: bytes, version: int,
                    upload_id: str) -> PendingWrite:
        pw = PendingWrite(self, block_id, expected_size, expected_sha, version, upload_id)
        with self._lock:
            available = self._available_locked()
            if expected_size > available:
                raise InsufficientSpace(expected_size, available)
            self._reserved += expected_size
            pw.reserved = expected_size
        try:
            pw._open()
        except BaseException:
            pw.abort()
            raise
        return pw

    def _release(self, pw: PendingWrite) -> None:
        with self._lock:
            self._reserved -= pw.reserved
            pw.reserved = 0

    def _publish(self, pw: PendingWrite) -> BlockRecord:
        bid = pw.block_id
        with self._block_lock(bid):
            with self._lock:
                old = self._index.get(bid)
                if old is not None and not old.corrupt and (
                        old.sha256 != pw.expected_sha or old.size != pw.expected_size):
                    raise BlockConflict(f"block_id {bid} já existe com conteúdo diferente (blocos são imutáveis)")
            d = self._dir(bid)
            d.mkdir(parents=True, exist_ok=True)
            now_ms = int(time.time() * 1000)
            meta = {"block_id": bid, "size": pw.expected_size, "sha256": pw.expected_sha.hex(),
                    "version": pw.version, "upload_id": pw.upload_id, "stored_at_ms": now_ms}
            meta_tmp = self.tmp_dir / f"{bid}.{uuid.uuid4().hex}.meta"
            with open(meta_tmp, "w", encoding="utf-8") as f:
                json.dump(meta, f)
                f.flush()
                if self.fsync:
                    os.fsync(f.fileno())
            os.replace(meta_tmp, self._meta_path(bid))
            os.replace(pw.path, self.path_for(bid))
            try:
                self._marker_path(bid).unlink()
            except FileNotFoundError:
                pass
            if self.fsync:
                fsync_dir(d)
            record = BlockRecord(block_id=bid, size=pw.expected_size, sha256=pw.expected_sha, version=pw.version,
                                 upload_id=pw.upload_id, stored_at_ms=now_ms, last_verified=time.time())
            with self._lock:
                if old is not None:
                    self._used -= old.size
                self._index[bid] = record
                self._used += record.size
                self._reserved -= pw.reserved
                pw.reserved = 0
            return record

    def verify(self, block_id: str, source: str,
               limiter: Optional[Callable[[int], None]] = None) -> VerifyResult:
        rec = self.get_record(block_id)
        if rec is None or rec.corrupt:
            return VerifyResult.SKIPPED
        try:
            digest, size = hash_file(self.path_for(block_id), limiter)
        except FileNotFoundError:
            if self.get_record(block_id) is not rec:
                return VerifyResult.SKIPPED
            self._handle_missing(rec, source)
            return VerifyResult.MISSING
        except OSError as exc:
            if exc.errno == errno.EIO:
                self.mark_corrupt(rec, b"", source)
                return VerifyResult.CORRUPT
            jsonlog.emit("storage.verify_error", level="error", block_id=block_id, error=str(exc))
            return VerifyResult.SKIPPED
        if digest == rec.sha256 and size == rec.size:
            rec.last_verified = time.time()
            return VerifyResult.OK
        if self.get_record(block_id) is not rec:
            return VerifyResult.SKIPPED
        self.mark_corrupt(rec, digest, source, observed_size=size)
        return VerifyResult.CORRUPT

    def mark_corrupt(self, rec: BlockRecord, observed: bytes, source: str,
                     observed_size: Optional[int] = None) -> bool:
        with self._lock:
            if rec.corrupt or self._index.get(rec.block_id) is not rec:
                return False
            rec.corrupt = True
        try:
            self._marker_path(rec.block_id).touch()
        except OSError:
            pass
        jsonlog.emit("storage.corruption_detected", level="error", block_id=rec.block_id, source=source,
                     expected_sha256=rec.sha256, observed_sha256=observed or None, expected_size=rec.size,
                     observed_size=observed_size)
        if self.corruption_listener:
            self.corruption_listener(rec.block_id, observed, source)
        return True

    def _handle_missing(self, rec: BlockRecord, source: str) -> None:
        with self._block_lock(rec.block_id):
            with self._lock:
                if self._index.get(rec.block_id) is not rec:
                    return
                del self._index[rec.block_id]
                self._used -= rec.size
            self._unlink_all(rec.block_id)
        jsonlog.emit("storage.block_missing", level="error", block_id=rec.block_id, source=source)
        if self.corruption_listener:
            self.corruption_listener(rec.block_id, b"", "missing")

    def delete(self, block_id: str) -> bool:
        with self._block_lock(block_id):
            with self._lock:
                rec = self._index.pop(block_id, None)
                if rec is not None:
                    self._used -= rec.size
            self._unlink_all(block_id)
        return rec is not None

    def discard_if_corrupt(self, block_id: str) -> bool:
        with self._block_lock(block_id):
            with self._lock:
                rec = self._index.get(block_id)
                if rec is None or not rec.corrupt:
                    return False
                del self._index[block_id]
                self._used -= rec.size
            self._unlink_all(block_id)
        return True