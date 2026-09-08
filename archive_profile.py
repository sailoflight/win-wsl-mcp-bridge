#!/usr/bin/env python3
"""Deterministic, safely extractable directory-result archive profile.

The Bridge's negotiated ``artifacts/1`` workspace push transfers single regular
files.  A business MCP that produces a multi-file/directory result therefore
publishes ONE archive file created by this module; consumers expand it with
:func:`extract_archive`.  The profile is deliberately strict and deterministic:

* ZIP container, every member ``ZIP_STORED`` with a fixed timestamp
  (1980-01-01 00:00:00), ``create_system=3`` (Unix) and mode ``0o100644`` so the
  same source bytes produce byte-identical archives on Windows and POSIX and
  across runs (member order is sorted by UTF-8 name; no data descriptors).
* Members are regular files only, named with normalized relative POSIX paths.
  Absolute paths, drive/UNC prefixes, traversal (``..``), backslashes, NUL,
  empty/``.`` components, Windows reserved device names and trailing dot/space
  components are rejected both when building and when extracting.
* Exact duplicate names, case-colliding names (``A.txt`` vs ``a.txt``) and
  file/directory prefix conflicts (``a`` vs ``a/b``) are rejected.
* Symlinks, devices, FIFOs and sockets are never stored and never extracted.
* Bounded entry count and bounded expanded size are enforced while building
  and again (from central-directory headers, before any destination byte is
  written) while extracting, which is the decompression-bomb guard.
* The archive embeds a ``manifest.sha256.json`` member listing every file
  member with its size and SHA-256; extraction verifies the manifest against
  the written bytes.
* Extraction is atomic and never overwrites: everything is written and fsynced
  under a unique sibling ``*.partial-*`` directory which is then renamed onto
  the destination only if the destination does not exist.  A failure removes
  the partial tree and leaves the workspace untouched.

This module is standard-library only and contains no Bridge runtime imports so
it can be used independently by business MCPs and by the receiving side.
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import re
import shutil
import stat
import tempfile
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

ARCHIVE_PROFILE = "win-wsl-mcp-bridge/archive-v1"
MANIFEST_MEMBER = "manifest.sha256.json"
#: Fixed member timestamp (ZIP stores local time without a zone; the profile
#: defines it as 1980-01-01 00:00:00 for every member).
ARCHIVE_FIXED_TIME = (1980, 1, 1, 0, 0, 0)
_READ_CHUNK = 65536

# Defaults chosen to mirror the runtime's single-file artifact bounds.
DEFAULT_MAX_ENTRIES = 10_000
DEFAULT_MAX_EXPANDED_BYTES = 1024 * 1024 * 1024
DEFAULT_MAX_MEMBER_BYTES = 512 * 1024 * 1024
MAX_NAME_BYTES = 1024
MAX_COMPONENT_BYTES = 255

#: Windows reserved device names (any case, with or without an extension).
_DEVICE_NAME = re.compile(r"^(con|prn|aux|nul|com[1-9]|lpt[1-9])(\..*)?$", re.IGNORECASE)
_WINDOWS_DRIVE = re.compile(r"^[A-Za-z]:", re.IGNORECASE)


class ArchiveProfileError(Exception):
    """A deterministic-archive profile violation.

    ``kind`` is a stable machine-readable category:

    ``name`` invalid or unsafe member name; ``duplicate`` exact duplicate
    member name; ``case`` case-colliding member names; ``conflict`` member
    collides with another member's ancestor directory; ``type`` non-regular
    member (symlink/device/FIFO/socket/directory/encrypted); ``count`` entry
    count bound; ``size`` expanded-size bound; ``member-size`` single-member
    size bound; ``manifest`` manifest missing/inconsistent; ``crc`` stored
    bytes failed CRC/size verification; ``destination`` destination exists or
    parent missing; ``io`` filesystem error; ``profile`` other profile error.
    """

    def __init__(self, kind: str, message: str):
        super().__init__(message)
        self.kind = kind


@dataclass(frozen=True)
class Limits:
    max_entries: int = DEFAULT_MAX_ENTRIES
    max_expanded_bytes: int = DEFAULT_MAX_EXPANDED_BYTES
    max_member_bytes: int = DEFAULT_MAX_MEMBER_BYTES

    def __post_init__(self) -> None:
        if self.max_entries < 1:
            raise ValueError("max_entries must be >= 1")
        if self.max_expanded_bytes < 1 or self.max_member_bytes < 1:
            raise ValueError("size bounds must be >= 1")


DEFAULT_LIMITS = Limits()


# --------------------------------------------------------------------------
# Name and member classification rules (shared by build and extract)
# --------------------------------------------------------------------------

def validate_member_name(name: str) -> str:
    """Validate one archive member name (already normalized at build time;
    foreign archives are re-checked with the same rules).

    Returns the validated name.  Raises :class:`ArchiveProfileError` with
    ``kind='name'`` on any unsafe name.
    """
    if not isinstance(name, str) or not name:
        raise ArchiveProfileError("name", "empty member name")
    if "\x00" in name:
        raise ArchiveProfileError("name", f"member name contains NUL: {name!r}")
    if name.startswith("/"):
        raise ArchiveProfileError("name", f"absolute member name: {name!r}")
    if "\\" in name:
        raise ArchiveProfileError("name", f"member name uses backslash separators: {name!r}")
    if _WINDOWS_DRIVE.match(name):
        raise ArchiveProfileError("name", f"member name has a Windows drive prefix: {name!r}")
    if name.startswith("//"):
        raise ArchiveProfileError("name", f"member name has a UNC prefix: {name!r}")
    if name.endswith("/"):
        raise ArchiveProfileError("name", f"explicit directory members are not part of the profile: {name!r}")
    for component in name.split("/"):
        if not component or component == ".":
            raise ArchiveProfileError("name", f"member name has an empty or '.' component: {name!r}")
        if component == "..":
            raise ArchiveProfileError("name", f"member name traverses upward: {name!r}")
        if component.endswith(".") or component.endswith(" "):
            raise ArchiveProfileError("name", f"member component ends in '.' or space: {name!r}")
        if _DEVICE_NAME.match(component):
            raise ArchiveProfileError("name", f"member component is a reserved device name: {name!r}")
        if len(component.encode("utf-8")) > MAX_COMPONENT_BYTES:
            raise ArchiveProfileError("name", f"member component too long: {component!r}")
    if len(name.encode("utf-8")) > MAX_NAME_BYTES:
        raise ArchiveProfileError("name", f"member name too long: {name[:64]!r}...")
    return name


def member_kind(info: zipfile.ZipInfo) -> str:
    """Return ``'file'`` or raise for a non-regular member.

    Classification uses the Unix mode stored in ``external_attr`` when present
    and the name's trailing slash.  Encrypted members, symlinks, devices,
    FIFOs, sockets and explicit directory members are rejected by the profile.
    """
    if info.flag_bits & 0x1:
        raise ArchiveProfileError("type", f"encrypted member: {info.filename!r}")
    if info.is_dir():
        raise ArchiveProfileError("type", f"directory member not in profile: {info.filename!r}")
    mode = (info.external_attr >> 16) & 0xFFFF
    file_type = stat.S_IFMT(mode)
    if file_type:
        if file_type != stat.S_IFREG:
            label = {
                stat.S_IFLNK: "symlink",
                stat.S_IFCHR: "character device",
                stat.S_IFBLK: "block device",
                stat.S_IFIFO: "FIFO",
                stat.S_IFSOCK: "socket",
            }.get(file_type, f"special file (mode {mode:o})")
            raise ArchiveProfileError("type", f"{label} member rejected: {info.filename!r}")
    return "file"


def _folded(name: str) -> str:
    return name.lower()


def check_name_set(names: list[str]) -> None:
    """Enforce uniqueness, case collisions and file/ancestor conflicts.

    ``names`` must already be individually validated.  Diagnostics are
    deterministic (names are examined in sorted order).
    """
    folded: dict[str, str] = {}
    for name in sorted(names, key=lambda n: n.encode("utf-8")):
        key = _folded(name)
        previous = folded.get(key)
        if previous is not None:
            if previous == name:
                raise ArchiveProfileError("duplicate", f"duplicate member name: {name!r}")
            raise ArchiveProfileError(
                "case", f"case-colliding member names: {previous!r} and {name!r}"
            )
        folded[key] = name
    # A member may not nest under another member used as a file, in either the
    # exact or the case-folded spelling (matters on case-insensitive disks).
    file_folded = {_folded(n): n for n in names}
    for name in sorted(names, key=lambda n: n.encode("utf-8")):
        prefix = ""
        for component in name.split("/")[:-1]:
            prefix = f"{prefix}/{component}" if prefix else component
            folded_prefix = file_folded.get(_folded(prefix))
            if folded_prefix is not None:
                raise ArchiveProfileError(
                    "conflict",
                    f"member {name!r} nests under file member {folded_prefix!r}",
                )


# --------------------------------------------------------------------------
# Deterministic profile builder
# --------------------------------------------------------------------------

def _collect_source_files(source_dir: Path) -> list[tuple[Path, str]]:
    """Return ``(absolute path, normalized relative name)`` for every regular
    file under *source_dir*, sorted by normalized name, never following
    symlinks.  Any non-regular object or unreadable directory fails the build
    explicitly instead of being silently dropped.
    """
    collected: list[tuple[Path, str]] = []
    pending = [source_dir]
    while pending:
        current = pending.pop()
        try:
            with os.scandir(current) as scanner:
                entries = sorted(scanner, key=lambda e: e.name.encode("utf-8"))
        except OSError as exc:
            raise ArchiveProfileError("io", f"cannot read directory {current}: {exc}") from exc
        subdirs: list[Path] = []
        for entry in entries:
            try:
                if entry.is_dir(follow_symlinks=False):
                    subdirs.append(Path(entry.path))
                    continue
                if entry.is_symlink():
                    raise ArchiveProfileError(
                        "type", f"symlink in source rejected: {Path(entry.path).relative_to(source_dir).as_posix()!r}"
                    )
                kind_mode = entry.stat(follow_symlinks=False).st_mode
                if not stat.S_ISREG(kind_mode):
                    raise ArchiveProfileError(
                        "type",
                        f"non-regular object in source rejected: "
                        f"{Path(entry.path).relative_to(source_dir).as_posix()!r}",
                    )
                rel = Path(entry.path).relative_to(source_dir).as_posix()
                validate_member_name(rel)
                collected.append((Path(entry.path), rel))
            except OSError as exc:
                raise ArchiveProfileError(
                    "io", f"cannot stat source member {entry.name!r}: {exc}"
                ) from exc
        # Deterministic depth-first order; final list is re-sorted by name.
        pending.extend(reversed(subdirs))
    collected.sort(key=lambda item: item[1].encode("utf-8"))
    return collected


def _file_sha256(path: Path) -> tuple[int, str]:
    digest = hashlib.sha256()
    size = 0
    with open(path, "rb") as handle:
        while True:
            chunk = handle.read(_READ_CHUNK)
            if not chunk:
                break
            size += len(chunk)
            digest.update(chunk)
    return size, digest.hexdigest()


def _member_info(name: str) -> zipfile.ZipInfo:
    info = zipfile.ZipInfo(filename=name, date_time=ARCHIVE_FIXED_TIME)
    info.compress_type = zipfile.ZIP_STORED
    info.create_system = 3  # Unix, regardless of the building OS
    info.external_attr = (0o100644 & 0xFFFF) << 16
    info.flag_bits |= 0x800  # UTF-8 filename
    return info


def build_archive(source_dir: Path | str, *, limits: Limits = DEFAULT_LIMITS) -> bytes:
    """Deterministically archive one local directory tree.

    *source_dir* must be an existing directory owned by the caller (this is the
    business-MCP side: never pass a peer-supplied path).  Returns the complete
    archive bytes: sorted, ZIP_STORED, fixed timestamp, with a
    ``manifest.sha256.json`` member.  Profile v1 is files-only: empty
    directories are not representable and are intentionally not part of the
    package (document this in the consumer contract).
    """
    source = Path(source_dir)
    if not source.is_dir():
        raise ArchiveProfileError("io", f"source is not a directory: {source}")
    members = _collect_source_files(source)
    if len(members) > limits.max_entries:
        raise ArchiveProfileError(
            "count",
            f"source has {len(members)} files exceeding max_entries={limits.max_entries}",
        )
    manifest_files: list[dict[str, Any]] = []
    total_bytes = 0
    for _path, name in members:
        size, digest = _file_sha256(_path)
        if size > limits.max_member_bytes:
            raise ArchiveProfileError(
                "member-size",
                f"member {name!r} is {size} bytes exceeding max_member_bytes={limits.max_member_bytes}",
            )
        total_bytes += size
        if total_bytes > limits.max_expanded_bytes:
            raise ArchiveProfileError(
                "size",
                f"expanded size {total_bytes} bytes exceeds max_expanded_bytes={limits.max_expanded_bytes}",
            )
        manifest_files.append({"name": name, "size": size, "sha256": digest})
    manifest = {
        "profile": ARCHIVE_PROFILE,
        "memberCount": len(manifest_files),
        "totalBytes": total_bytes,
        "files": manifest_files,
    }
    manifest_bytes = json.dumps(
        manifest, ensure_ascii=False, separators=(",", ":"), sort_keys=False
    ).encode("utf-8")

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, mode="w", compression=zipfile.ZIP_STORED) as archive:
        # Every member (including the manifest) is written in one sorted order
        # so the archive name sequence is itself deterministic and sorted.
        items: list[tuple[str, tuple[str, Path | bytes]]] = [
            (name, ("path", path)) for path, name in members
        ]
        items.append((MANIFEST_MEMBER, ("bytes", manifest_bytes)))
        items.sort(key=lambda item: item[0].encode("utf-8"))
        for name, (kind, payload) in items:
            if kind == "path":
                _write_member_streaming(archive, name, payload)  # type: ignore[arg-type]
            else:
                archive.writestr(_member_info(name), payload)  # type: ignore[arg-type]
    return buffer.getvalue()


def _write_member_streaming(archive: zipfile.ZipFile, name: str, path: Path) -> None:
    with open(path, "rb") as source_handle, archive.open(_member_info(name), "w") as out_handle:
        shutil.copyfileobj(source_handle, out_handle, _READ_CHUNK)


def _load_zip(source: Path | str | bytes) -> zipfile.ZipFile:
    if isinstance(source, (bytes, bytearray, memoryview)):
        return zipfile.ZipFile(io.BytesIO(bytes(source)), mode="r")
    return zipfile.ZipFile(os.fspath(source), mode="r")


# --------------------------------------------------------------------------
# Strict, atomic extractor
# --------------------------------------------------------------------------

def inspect_archive(source: Path | str | bytes) -> dict[str, Any]:
    """Read-only structural inspection for tests and diagnostics.

    Returns member names, per-member header sizes and the presence of the
    manifest member.  Never writes to disk.
    """
    with _load_zip(source) as archive:
        members = [
            {
                "name": info.filename,
                "size": info.file_size,
                "compress_type": info.compress_type,
                "is_dir": info.is_dir(),
            }
            for info in archive.infolist()
        ]
        return {
            "memberCount": len(members),
            "hasManifest": any(m["name"] == MANIFEST_MEMBER for m in members),
            "members": members,
        }


def _validate_entries(source: Path | str | bytes, limits: Limits) -> zipfile.ZipFile:
    """Full read-only pre-pass: bounds, names, kinds, collisions and sizes.

    Runs before any destination byte is written.  Returns the opened archive
    (the caller must close it) after all structural gates pass.
    """
    archive = _load_zip(source)
    try:
        infolist = archive.infolist()
        if len(infolist) > limits.max_entries:
            raise ArchiveProfileError(
                "count",
                f"archive has {len(infolist)} members exceeding max_entries={limits.max_entries}",
            )
        names: list[str] = []
        total_declared = 0
        for info in infolist:
            name = validate_member_name(info.filename)
            member_kind(info)
            names.append(name)
            size = info.file_size
            if size > limits.max_member_bytes:
                raise ArchiveProfileError(
                    "member-size",
                    f"member {name!r} declares {size} bytes exceeding "
                    f"max_member_bytes={limits.max_member_bytes}",
                )
            total_declared += size
            if total_declared > limits.max_expanded_bytes:
                raise ArchiveProfileError(
                    "size",
                    f"archive declares {total_declared} expanded bytes exceeding "
                    f"max_expanded_bytes={limits.max_expanded_bytes}",
                )
        if MANIFEST_MEMBER not in names:
            raise ArchiveProfileError(
                "manifest", f"profile archive lacks {MANIFEST_MEMBER!r} member"
            )
        check_name_set([n for n in names if n != MANIFEST_MEMBER])
        return archive
    except ArchiveProfileError:
        archive.close()
        raise
    except (zipfile.BadZipFile, OSError, ValueError) as exc:
        archive.close()
        raise ArchiveProfileError("profile", f"invalid ZIP container: {exc}") from exc


def extract_archive(
    source: Path | str | bytes,
    destination: Path | str,
    *,
    limits: Limits = DEFAULT_LIMITS,
) -> dict[str, Any]:
    """Atomically extract a profile archive beneath *destination*.

    *destination* must not already exist (no-overwrite guarantee) and its
    parent must exist.  All members are pre-validated, written and fsynced
    under a unique sibling ``.partial-*`` directory, verified against the
    embedded manifest, and only then renamed onto *destination*.  On any
    failure the partial tree is removed and the workspace is left unchanged.

    Returns ``{'destination', 'memberCount', 'totalBytes', 'sha256s'}`` where
    ``sha256s`` maps every file member to its verified digest.
    """
    destination_path = Path(destination)
    parent = destination_path.parent
    if destination_path.exists():
        raise ArchiveProfileError(
            "destination", f"destination already exists: {destination_path}"
        )
    if not parent.is_dir():
        raise ArchiveProfileError("destination", f"destination parent missing: {parent}")

    archive = _validate_entries(source, limits)
    temporary: Path | None = None
    try:
        temporary = Path(
            tempfile.mkdtemp(
                prefix=f".{destination_path.name}.partial-", dir=os.fspath(parent)
            )
        )
        temporary_real = os.path.realpath(temporary)

        manifest_raw: bytes | None = None
        sha256s: dict[str, str] = {}
        for info in archive.infolist():
            if info.filename == MANIFEST_MEMBER:
                with archive.open(info, "r") as handle:
                    manifest_raw = handle.read()
                continue
            name = info.filename
            target = os.path.join(os.fspath(temporary), name)
            target_real = os.path.realpath(target)
            if target_real != temporary_real and not target_real.startswith(
                temporary_real + os.sep
            ):
                raise ArchiveProfileError("name", f"member escapes extraction root: {name!r}")
            os.makedirs(os.path.dirname(target), exist_ok=True)
            digest = hashlib.sha256()
            size = 0
            with archive.open(info, "r") as source_handle, open(
                target, "wb"
            ) as out_handle:
                while True:
                    chunk = source_handle.read(_READ_CHUNK)
                    if not chunk:
                        break
                    size += len(chunk)
                    if size > info.file_size:
                        raise ArchiveProfileError(
                            "crc", f"member {name!r} expanded beyond its declared size"
                        )
                    digest.update(chunk)
                    out_handle.write(chunk)
                out_handle.flush()
                os.fsync(out_handle.fileno())
            sha256s[name] = digest.hexdigest()

        if manifest_raw is None:
            raise ArchiveProfileError(
                "manifest", f"profile archive lacks {MANIFEST_MEMBER!r} member"
            )
        try:
            manifest = json.loads(manifest_raw.decode("utf-8"))
        except (ValueError, UnicodeDecodeError) as exc:
            raise ArchiveProfileError("manifest", f"unreadable manifest: {exc}") from exc
        if not isinstance(manifest, dict) or manifest.get("profile") != ARCHIVE_PROFILE:
            raise ArchiveProfileError("manifest", "manifest profile mismatch")
        files = manifest.get("files")
        if not isinstance(files, list):
            raise ArchiveProfileError("manifest", "manifest files entry missing")
        member_names = list(sha256s)
        file_members = [
            info for info in archive.infolist() if info.filename != MANIFEST_MEMBER
        ]
        declared_total = sum(info.file_size for info in file_members)
        if manifest.get("memberCount") != len(member_names):
            raise ArchiveProfileError(
                "manifest",
                f"manifest memberCount {manifest.get('memberCount')!r} != actual {len(member_names)}",
            )
        if manifest.get("totalBytes") != declared_total:
            raise ArchiveProfileError(
                "manifest",
                f"manifest totalBytes {manifest.get('totalBytes')!r} != declared {declared_total}",
            )
        expected: dict[str, tuple[int, str]] = {}
        for item in files:
            if not isinstance(item, dict) or not isinstance(item.get("name"), str):
                raise ArchiveProfileError("manifest", "manifest file entry malformed")
            expected[item["name"]] = (int(item["size"]), str(item["sha256"]))
        if set(expected) != set(member_names):
            missing = sorted(set(expected) - set(member_names))
            extra = sorted(set(member_names) - set(expected))
            raise ArchiveProfileError(
                "manifest",
                f"manifest does not match archive members (missing={missing}, extra={extra})",
            )
        for name, (declared_size, declared_sha256) in expected.items():
            actual_sha = sha256s[name]
            actual_size = os.path.getsize(os.path.join(os.fspath(temporary), name))
            if declared_size != actual_size or declared_sha256 != actual_sha:
                raise ArchiveProfileError(
                    "manifest", f"manifest hash/size mismatch for {name!r}"
                )

        # fsync the directory so the rename is durable after a crash.
        try:
            dir_fd = os.open(temporary, os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except OSError:
            pass  # Directory fsync is not available on every platform.
        try:
            os.rename(temporary, destination_path)
        except OSError as exc:
            if destination_path.exists():
                raise ArchiveProfileError(
                    "destination",
                    f"destination appeared during extraction: {destination_path}",
                ) from exc
            raise ArchiveProfileError("io", f"cannot commit extraction: {exc}") from exc
        temporary = None
        return {
            "destination": str(destination_path),
            "memberCount": len(member_names),
            "totalBytes": declared_total,
            "sha256s": sha256s,
        }
    except ArchiveProfileError:
        raise
    except (zipfile.BadZipFile, OSError, ValueError) as exc:
        raise ArchiveProfileError("profile", f"invalid ZIP container: {exc}") from exc
    finally:
        archive.close()
        if temporary is not None:
            shutil.rmtree(temporary, ignore_errors=True)
