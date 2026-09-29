"""Platform-correct process-liveness probe shared by the offline tests.

``os.kill(pid, 0)`` is not a liveness test on Windows: it returns ``None`` for
an exited process (so a wait loop never observes the exit) and raises
``OSError`` with ``WinError 87`` — not ``ProcessLookupError`` — for a bogus
pid. On Windows the exit code is read through ``OpenProcess`` instead. On POSIX
``os.kill(pid, 0)`` is retained, and ``PermissionError`` still means the process
exists.

Standard-library only, and intentionally not a ``test_*`` module: unittest
discovery ignores it.
"""

from __future__ import annotations

import ctypes
import os

if os.name == "nt":  # pragma: no cover - Windows-only binding
    _PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
    _STILL_ACTIVE = 259
    _kernel32 = ctypes.windll.kernel32


def pid_alive(pid: int) -> bool:
    """Return ``True`` while *pid* names a live process on this platform."""

    if os.name == "nt":  # pragma: no cover - exercised on Windows only
        handle = _kernel32.OpenProcess(
            _PROCESS_QUERY_LIMITED_INFORMATION, False, int(pid)
        )
        if not handle:
            # Gone, or not queryable under the limited right: not observably alive.
            return False
        try:
            code = ctypes.c_ulong()
            if not _kernel32.GetExitCodeProcess(handle, ctypes.byref(code)):
                return False
            return code.value == _STILL_ACTIVE
        finally:
            _kernel32.CloseHandle(handle)
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except OSError:
        # e.g. EPERM: the process exists but belongs to another user.
        return True
    return True
