"""Small OS adapter for pausing a conformance worker process tree."""
from __future__ import annotations

import os
import signal


def _windows_process_tree(root_pid: int) -> list[int]:
    """Return root and descendants using a Toolhelp process snapshot."""
    import ctypes
    from ctypes import wintypes

    class ProcessEntry(ctypes.Structure):
        _fields_ = [
            ("dwSize", wintypes.DWORD), ("cntUsage", wintypes.DWORD),
            ("th32ProcessID", wintypes.DWORD), ("th32DefaultHeapID", ctypes.c_size_t),
            ("th32ModuleID", wintypes.DWORD), ("cntThreads", wintypes.DWORD),
            ("th32ParentProcessID", wintypes.DWORD), ("pcPriClassBase", wintypes.LONG),
            ("dwFlags", wintypes.DWORD), ("szExeFile", wintypes.WCHAR * 260),
        ]

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    snapshot = kernel32.CreateToolhelp32Snapshot
    snapshot.argtypes = [wintypes.DWORD, wintypes.DWORD]
    snapshot.restype = wintypes.HANDLE
    first = kernel32.Process32FirstW
    first.argtypes = [wintypes.HANDLE, ctypes.POINTER(ProcessEntry)]
    first.restype = wintypes.BOOL
    following = kernel32.Process32NextW
    following.argtypes = [wintypes.HANDLE, ctypes.POINTER(ProcessEntry)]
    following.restype = wintypes.BOOL
    close = kernel32.CloseHandle
    close.argtypes = [wintypes.HANDLE]
    close.restype = wintypes.BOOL

    handle = snapshot(0x00000002, 0)  # TH32CS_SNAPPROCESS
    if handle == wintypes.HANDLE(-1).value:
        raise OSError(ctypes.get_last_error(), "CreateToolhelp32Snapshot(processes) failed")
    try:
        parents: dict[int, int] = {}
        entry = ProcessEntry()
        entry.dwSize = ctypes.sizeof(entry)
        ok = first(handle, ctypes.byref(entry))
        while ok:
            parents[int(entry.th32ProcessID)] = int(entry.th32ParentProcessID)
            ok = following(handle, ctypes.byref(entry))
        descendants = {root_pid}
        changed = True
        while changed:
            changed = False
            for pid, parent in parents.items():
                if parent in descendants and pid not in descendants:
                    descendants.add(pid)
                    changed = True
        if root_pid not in parents:
            raise ProcessLookupError(f"worker process {root_pid} is not present in the snapshot")
        return sorted(descendants)
    finally:
        close(handle)


def _windows_threads(process_ids: list[int]) -> list[tuple[int, int]]:
    """Return thread IDs owned by the listed processes."""
    import ctypes
    from ctypes import wintypes

    class ThreadEntry(ctypes.Structure):
        _fields_ = [
            ("dwSize", wintypes.DWORD), ("cntUsage", wintypes.DWORD),
            ("th32ThreadID", wintypes.DWORD), ("th32OwnerProcessID", wintypes.DWORD),
            ("tpBasePri", wintypes.LONG), ("tpDeltaPri", wintypes.LONG),
            ("dwFlags", wintypes.DWORD),
        ]

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    snapshot = kernel32.CreateToolhelp32Snapshot
    snapshot.argtypes = [wintypes.DWORD, wintypes.DWORD]
    snapshot.restype = wintypes.HANDLE
    first = kernel32.Thread32First
    first.argtypes = [wintypes.HANDLE, ctypes.POINTER(ThreadEntry)]
    first.restype = wintypes.BOOL
    following = kernel32.Thread32Next
    following.argtypes = [wintypes.HANDLE, ctypes.POINTER(ThreadEntry)]
    following.restype = wintypes.BOOL
    close = kernel32.CloseHandle
    close.argtypes = [wintypes.HANDLE]
    close.restype = wintypes.BOOL

    handle = snapshot(0x00000004, 0)  # TH32CS_SNAPTHREAD
    if handle == wintypes.HANDLE(-1).value:
        raise OSError(ctypes.get_last_error(), "CreateToolhelp32Snapshot(threads) failed")
    try:
        selected = set(process_ids)
        threads: list[tuple[int, int]] = []
        entry = ThreadEntry()
        entry.dwSize = ctypes.sizeof(entry)
        ok = first(handle, ctypes.byref(entry))
        while ok:
            if int(entry.th32OwnerProcessID) in selected:
                threads.append((int(entry.th32OwnerProcessID), int(entry.th32ThreadID)))
            ok = following(handle, ctypes.byref(entry))
        return threads
    finally:
        close(handle)


def _windows_change_threads(root_pid: int, *, suspend: bool) -> int:
    import ctypes
    from ctypes import wintypes

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    open_thread = kernel32.OpenThread
    open_thread.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    open_thread.restype = wintypes.HANDLE
    suspend_thread = kernel32.SuspendThread
    suspend_thread.argtypes = [wintypes.HANDLE]
    suspend_thread.restype = wintypes.DWORD
    resume_thread = kernel32.ResumeThread
    resume_thread.argtypes = [wintypes.HANDLE]
    resume_thread.restype = wintypes.DWORD
    close = kernel32.CloseHandle
    close.argtypes = [wintypes.HANDLE]
    close.restype = wintypes.BOOL

    process_ids = _windows_process_tree(root_pid)
    threads = _windows_threads(process_ids)
    changed = 0
    changed_threads: list[int] = []
    access = 0x0002  # THREAD_SUSPEND_RESUME
    # Suspend the launcher first so it cannot start new children. Resume it
    # last, after all children are ready to run again.
    if suspend:
        threads.sort(key=lambda item: (item[0] != root_pid, item[1]))
    else:
        threads.sort(key=lambda item: (item[0] == root_pid, item[1]))
    try:
        for _owner_pid, thread_id in threads:
            handle = open_thread(access, False, thread_id)
            if not handle:
                error = ctypes.get_last_error()
                # A thread can exit between snapshot and OpenThread. Any other
                # failure means the process tree was not safely controlled.
                if error == 87:  # ERROR_INVALID_PARAMETER: thread no longer exists
                    continue
                raise OSError(error, f"OpenThread({thread_id}) failed")
            try:
                result = suspend_thread(handle) if suspend else resume_thread(handle)
                if result == 0xFFFFFFFF:
                    error = ctypes.get_last_error()
                    if error == 87:
                        continue
                    raise OSError(error, f"{'SuspendThread' if suspend else 'ResumeThread'}({thread_id}) failed")
                changed += 1
                if suspend:
                    changed_threads.append(thread_id)
            finally:
                close(handle)
    except Exception:
        # Never leave a half-suspended owner tree when a Windows thread exits
        # or access is denied partway through the snapshot.
        if suspend:
            for thread_id in reversed(changed_threads):
                handle = open_thread(access, False, thread_id)
                if handle:
                    try:
                        resume_thread(handle)
                    finally:
                        close(handle)
        raise
    if changed == 0:
        raise ProcessLookupError(f"no live threads found for worker process {root_pid}")
    return changed


def process_scope(root_pid: int) -> dict:
    """Describe and validate the exact OS-owned scope this harness controls."""
    if os.name == "nt":
        process_ids = _windows_process_tree(root_pid)
        return {"kind": "windows_descendants_of_worker", "process_ids": process_ids}
    group_id = os.getpgid(root_pid)
    if group_id != root_pid:
        raise RuntimeError(
            f"worker {root_pid} is not an isolated process-group leader (group {group_id})"
        )
    return {"kind": "isolated_process_group", "process_group_id": group_id}


def suspend_process_tree(root_pid: int) -> int:
    """Suspend a process group/tree and return the number of controlled units."""
    if os.name == "nt":
        return _windows_change_threads(root_pid, suspend=True)
    if os.getpgid(root_pid) != root_pid:
        raise RuntimeError(f"refusing to suspend non-isolated process group for worker {root_pid}")
    os.killpg(root_pid, signal.SIGSTOP)
    return 1


def resume_process_tree(root_pid: int) -> int:
    """Resume a process group/tree previously suspended by this harness."""
    if os.name == "nt":
        return _windows_change_threads(root_pid, suspend=False)
    if os.getpgid(root_pid) != root_pid:
        raise RuntimeError(f"refusing to resume non-isolated process group for worker {root_pid}")
    os.killpg(root_pid, signal.SIGCONT)
    return 1
