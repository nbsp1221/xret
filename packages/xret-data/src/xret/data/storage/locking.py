"""Portable, stdlib-only inter-process file locking.

V1 serializes a dataset's sync with `dataset_lock`.  A distinct short
`catalog_gate` coordinates only catalog bootstrap, publication metadata, and
maintenance boundaries; provider/network work, scans, and file preparation
must remain outside it.

Lock-timeout errors are mapped by lock kind: dataset-scoped timeouts raise
`SyncError`, while catalog-gate timeouts raise `CatalogError`.
"""

from __future__ import annotations

import errno
import sys
import time
from pathlib import Path
from typing import IO, TYPE_CHECKING, Final

from xret.data.errors import CatalogError, SyncError, XretDataError
from xret.data.storage import paths

if TYPE_CHECKING:
    from xret.data.models import DatasetKey

__all__ = [
    "DEFAULT_LOCK_TIMEOUT",
    "DEFAULT_POLL_INTERVAL",
    "FileLock",
    "dataset_lock",
]

#: Default time budget for acquiring a lock before raising the mapped
#: timeout error.
DEFAULT_LOCK_TIMEOUT: Final[float] = 30.0

#: Poll interval between non-blocking acquisition attempts.
DEFAULT_POLL_INTERVAL: Final[float] = 0.05

_IS_WINDOWS: Final[bool] = sys.platform == "win32"

if _IS_WINDOWS:  # pragma: no cover - exercised only on Windows
    import msvcrt

    def _try_lock(handle: IO[bytes]) -> bool:
        try:
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError as exc:
            if exc.errno in {errno.EACCES, errno.EAGAIN}:
                return False
            raise
        return True

    def _unlock(handle: IO[bytes]) -> None:
        handle.seek(0)
        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)

else:
    import fcntl

    def _try_lock(handle: IO[bytes]) -> bool:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            if exc.errno in {errno.EACCES, errno.EAGAIN}:
                return False
            raise
        return True

    def _unlock(handle: IO[bytes]) -> None:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


class FileLock:
    """An advisory, exclusive, inter-process lock backed by one file.

    Non-reentrant: acquiring twice from the same process without an
    intervening `release` raises `timeout_error` once the timeout
    elapses (POSIX `flock` is per-open-file-description, not per-process,
    so a naive re-acquire would otherwise deadlock silently).

    `timeout_error` is the error class raised on contention timeout or
    same-instance re-acquisition. Dataset locks map to `SyncError`; the
    catalog gate maps to `CatalogError`.
    """

    def __init__(
        self,
        path: Path,
        *,
        timeout: float = DEFAULT_LOCK_TIMEOUT,
        poll_interval: float = DEFAULT_POLL_INTERVAL,
        timeout_error: type[XretDataError] = SyncError,
    ) -> None:
        self.path = path
        self.timeout = timeout
        self.poll_interval = poll_interval
        self._timeout_error = timeout_error
        self._handle: IO[bytes] | None = None

    def acquire(self) -> None:
        if self._handle is not None:
            raise self._timeout_error(f"lock already held by this instance: {self.path}")

        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            handle = self.path.open("a+b")
        except OSError as exc:
            raise self._timeout_error(f"failed to set up lock: {self.path}") from exc

        deadline = time.monotonic() + self.timeout
        while True:
            try:
                acquired = _try_lock(handle)
            except OSError as exc:
                handle.close()
                raise self._timeout_error(f"failed to acquire lock: {self.path}") from exc
            if acquired:
                self._handle = handle
                return
            if time.monotonic() >= deadline:
                handle.close()
                raise self._timeout_error(f"timed out acquiring lock: {self.path}")
            time.sleep(self.poll_interval)

    def release(self) -> None:
        handle, self._handle = self._handle, None
        if handle is not None:
            try:
                _unlock(handle)
            except OSError as exc:
                raise self._timeout_error(f"failed to release lock: {self.path}") from exc
            finally:
                handle.close()

    def __enter__(self) -> FileLock:
        self.acquire()
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.release()


def dataset_lock(
    state_dir: Path,
    key: DatasetKey,
    *,
    timeout: float = DEFAULT_LOCK_TIMEOUT,
    poll_interval: float = DEFAULT_POLL_INTERVAL,
) -> FileLock:
    """The per-dataset inter-process commit lock for `key`.

    Different datasets use different lock files and may be held
    concurrently by different processes; the same dataset serializes.
    Timeout/re-acquire failures raise `SyncError` (P-1): dataset locks
    only ever guard a `sync` commit sequence.
    """
    return FileLock(
        paths.lock_file_path(state_dir, key),
        timeout=timeout,
        poll_interval=poll_interval,
        timeout_error=SyncError,
    )


def catalog_gate(
    state_dir: Path,
    *,
    timeout: float = DEFAULT_LOCK_TIMEOUT,
    poll_interval: float = DEFAULT_POLL_INTERVAL,
) -> FileLock:
    """Return the short catalog coordination gate.

    This is deliberately distinct from dataset locks and is never a
    provider/network or scan lock.  The existing catalog-wide lock path is
    retained as its private backing file so maintenance and publication
    boundaries coordinate on one primitive.
    """
    return FileLock(
        paths.maintenance_lock_file_path(state_dir),
        timeout=timeout,
        poll_interval=poll_interval,
        timeout_error=CatalogError,
    )
