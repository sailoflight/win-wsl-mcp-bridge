#!/usr/bin/env python3
"""Focused tests for the deterministic safe archive profile (P3).

Covers: deterministic byte-identical builds, strict member-name gates
(zip-slip, absolute, drive/UNC, backslashes, NUL semantics, device names,
trailing dot/space), duplicate/case-collision/prefix-conflict rejection,
symlink/device/FIFO/directory/encrypted member rejection, entry-count and
expanded-size bounds (decompression bomb), manifest integrity, atomic
no-overwrite extraction and partial-tree cleanup.

Runs offline with the standard library only.
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import stat
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from archive_profile import (
    ARCHIVE_FIXED_TIME,
    ARCHIVE_PROFILE,
    MANIFEST_MEMBER,
    ArchiveProfileError,
    Limits,
    build_archive,
    check_name_set,
    extract_archive,
    inspect_archive,
    member_kind,
    validate_member_name,
)


def make_tree(root: Path, files: dict[str, bytes]) -> None:
    for rel, content in files.items():
        target = root / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)


def sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def craft_zip(members: list[tuple[str, bytes]], *, mode: int | None = None,
              manifest: dict | None = None, compress: bool = False,
              omit_manifest: bool = False) -> bytes:
    """Build an arbitrary (possibly malicious) zip for extractor tests.

    When *manifest* is None and the archive is not meant to be malformed, a
    correct manifest is derived from *members* so that name/type/count gates
    are what the test exercises.
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, mode="w",
                         compression=zipfile.ZIP_DEFLATED if compress else zipfile.ZIP_STORED) as zf:
        for name, content in members:
            info = zipfile.ZipInfo(filename=name, date_time=ARCHIVE_FIXED_TIME)
            info.compress_type = zipfile.ZIP_DEFLATED if compress else zipfile.ZIP_STORED
            info.create_system = 3
            info.external_attr = (0o100644 & 0xFFFF) << 16 if mode is None else mode
            info.flag_bits |= 0x800
            zf.writestr(info, content)
        if not omit_manifest and not any(n == MANIFEST_MEMBER for n, _ in members):
            files = [
                {"name": n, "size": len(c), "sha256": sha256_bytes(c)}
                for n, c in members
            ]
            entry = manifest if manifest is not None else {
                "profile": ARCHIVE_PROFILE,
                "memberCount": len(members),
                "totalBytes": sum(len(c) for _, c in members),
                "files": files,
            }
            info = zipfile.ZipInfo(filename=MANIFEST_MEMBER, date_time=ARCHIVE_FIXED_TIME)
            info.compress_type = zipfile.ZIP_STORED
            info.create_system = 3
            info.external_attr = (0o100644 & 0xFFFF) << 16
            info.flag_bits |= 0x800
            zf.writestr(info, json.dumps(entry, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
    return buffer.getvalue()


class DeterminismTest(unittest.TestCase):
    def test_round_trip_preserves_tree_and_hashes(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            source = root / "src"
            source.mkdir()
            payload = {
                "readme.txt": b"hello deterministic archive\n",
                "sub/dir/nested data.bin": bytes(range(256)) * 40,
                "unicode-\u4e2d\u6587/\u6587\u4ef6.txt": "内容".encode("utf-8"),
                "empty.txt": b"",
            }
            make_tree(source, payload)
            blob = build_archive(source)
            self.assertIsInstance(blob, bytes)
            # Inspection
            info = inspect_archive(blob)
            self.assertTrue(info["hasManifest"])
            names = {m["name"] for m in info["members"]}
            self.assertEqual(names, set(payload) | {MANIFEST_MEMBER})

            dest = root / "out"
            result = extract_archive(blob, dest)
            self.assertEqual(result["memberCount"], len(payload))
            for rel, content in payload.items():
                self.assertEqual((dest / rel).read_bytes(), content)
                self.assertEqual(
                    result["sha256s"][rel.replace(os.sep, "/")], sha256_bytes(content)
                )

    def test_build_is_byte_deterministic(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            source = root / "src"
            source.mkdir()
            make_tree(source, {"a.txt": b"a", "b/x.bin": os.urandom(2048), "c/d/e": b"deep"})
            first = build_archive(source)
            second = build_archive(source)
            self.assertEqual(first, second)
            # Filesystem mtime/metadata changes must not affect the bytes.
            for path in source.rglob("*"):
                if path.is_file():
                    os.utime(path, (946684800, 946684800))
            third = build_archive(source)
            self.assertEqual(first, third)
            # Member timestamps are the fixed profile timestamp.
            with zipfile.ZipFile(io.BytesIO(first)) as zf:
                for info in zf.infolist():
                    self.assertEqual(info.date_time, ARCHIVE_FIXED_TIME)
                    self.assertEqual(info.create_system, 3)
                    self.assertEqual(info.compress_type, zipfile.ZIP_STORED)

    def test_member_order_is_sorted(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            source = Path(td) / "src"
            source.mkdir()
            make_tree(source, {"zeta": b"1", "alpha/deep": b"2", "beta": b"3"})
            blob = build_archive(source)
            with zipfile.ZipFile(io.BytesIO(blob)) as zf:
                names = zf.namelist()
            self.assertEqual(names, sorted(names))


class NameGateTest(unittest.TestCase):
    def test_rejects_unsafe_names_individually(self) -> None:
        unsafe = [
            "../escape.txt", "a/../../b", "/etc/passwd", "//server/share",
            "C:/windows/evil.txt", "c:\\evil", "a\\..\\b", "a/./b", "./x",
            "a//b", "x/", "dir/../file", "CON", "con.txt", "NUL", "com1.log",
            "aux.any", "lpt9", "trailing.", "trailing ", "a\x00b",
            "a/" + "x" * 300,
            "/".join(["y" * 250] * 4) + "/" + "y" * 50,
        ]
        for name in unsafe:
            with self.assertRaises(ArchiveProfileError, msg=name) as ctx:
                validate_member_name(name)
            self.assertEqual(ctx.exception.kind, "name", name)
        valid = ["a.txt", "sub/dir/file", "under_score-1.x", "a b/c (d).dat"]
        for name in valid:
            self.assertEqual(validate_member_name(name), name)

    def test_rejects_zip_slip_absolute_and_drive_names_through_extract(self) -> None:
        cases = ["../evil.txt", "/etc/passwd", "a/../../b", "C:/evil.txt", "..\\evil", "c:\\tmp\\x"]
        with tempfile.TemporaryDirectory() as td:
            for name in cases:
                blob = craft_zip([(name, b"x")])
                with self.assertRaises(ArchiveProfileError, msg=name) as ctx:
                    extract_archive(blob, Path(td) / "dest")
                self.assertEqual(ctx.exception.kind, "name", name)
                self.assertFalse((Path(td) / "dest").exists())

    def test_rejects_duplicate_case_collision_and_prefix_conflict(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            duplicate = craft_zip([("a.txt", b"1"), ("a.txt", b"2")])
            with self.assertRaises(ArchiveProfileError) as ctx:
                extract_archive(duplicate, Path(td) / "d1")
            self.assertEqual(ctx.exception.kind, "duplicate")

            case_collision = craft_zip([("A.txt", b"1"), ("a.txt", b"2")])
            with self.assertRaises(ArchiveProfileError) as ctx:
                extract_archive(case_collision, Path(td) / "d2")
            self.assertEqual(ctx.exception.kind, "case")

            conflict = craft_zip([("data", b"file"), ("data/inner.txt", b"nested")])
            with self.assertRaises(ArchiveProfileError) as ctx:
                extract_archive(conflict, Path(td) / "d3")
            self.assertEqual(ctx.exception.kind, "conflict")

            folded_conflict = craft_zip([("Dir", b"file"), ("dir/x", b"nested")])
            with self.assertRaises(ArchiveProfileError) as ctx:
                extract_archive(folded_conflict, Path(td) / "d4")
            self.assertEqual(ctx.exception.kind, "conflict")

    def test_check_name_set_unit(self) -> None:
        with self.assertRaises(ArchiveProfileError) as ctx:
            check_name_set(["x/y", "X/Y"])
        self.assertEqual(ctx.exception.kind, "case")
        with self.assertRaises(ArchiveProfileError) as ctx:
            check_name_set(["f", "f/g"])
        self.assertEqual(ctx.exception.kind, "conflict")


class MemberKindGateTest(unittest.TestCase):
    def _zipinfo(self, name: str, mode: int, encrypted: bool = False) -> zipfile.ZipInfo:
        info = zipfile.ZipInfo(filename=name, date_time=ARCHIVE_FIXED_TIME)
        info.external_attr = (mode & 0xFFFF) << 16
        if encrypted:
            info.flag_bits |= 0x1
        return info

    def test_member_kind_rejects_specials(self) -> None:
        for mode, label in [
            (stat.S_IFLNK | 0o777, "symlink"),
            (stat.S_IFCHR | 0o600, "device"),
            (stat.S_IFBLK | 0o600, "device"),
            (stat.S_IFIFO | 0o644, "fifo"),
            (stat.S_IFSOCK | 0o600, "socket"),
        ]:
            with self.assertRaises(ArchiveProfileError) as ctx:
                member_kind(self._zipinfo("target", mode))
            self.assertEqual(ctx.exception.kind, "type", label)
        encrypted = self._zipinfo("secret", 0o100644, encrypted=True)
        with self.assertRaises(ArchiveProfileError) as ctx:
            member_kind(encrypted)
        self.assertEqual(ctx.exception.kind, "type")
        self.assertEqual(member_kind(self._zipinfo("plain", 0o100644)), "file")
        directory_info = zipfile.ZipInfo(filename="dir/", date_time=ARCHIVE_FIXED_TIME)
        with self.assertRaises(ArchiveProfileError) as ctx:
            member_kind(directory_info)
        self.assertEqual(ctx.exception.kind, "type")

    def test_symlink_and_directory_members_rejected_through_extract(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            symlink_blob = craft_zip(
                [("link", b"/etc/passwd")], mode=(stat.S_IFLNK | 0o777) << 16
            )
            with self.assertRaises(ArchiveProfileError) as ctx:
                extract_archive(symlink_blob, Path(td) / "d1")
            self.assertEqual(ctx.exception.kind, "type")

            # A trailing-slash member is rejected by the name gate first.
            directory_blob = craft_zip(
                [("folder/", b"")], omit_manifest=True
            )
            with self.assertRaises(ArchiveProfileError) as ctx:
                extract_archive(directory_blob, Path(td) / "d2")
            self.assertEqual(ctx.exception.kind, "name")


class BoundGateTest(unittest.TestCase):
    def test_entry_count_overflow_rejected_at_build_and_extract(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            source = root / "src"
            source.mkdir()
            make_tree(source, {f"f{i}.txt": b"x" for i in range(6)})
            small = Limits(max_entries=5, max_expanded_bytes=1024 * 1024, max_member_bytes=1024)
            with self.assertRaises(ArchiveProfileError) as ctx:
                build_archive(source, limits=small)
            self.assertEqual(ctx.exception.kind, "count")

            blob = craft_zip([(f"m{i}.txt", b"x") for i in range(6)], omit_manifest=True)
            with self.assertRaises(ArchiveProfileError) as ctx:
                extract_archive(blob, root / "out", limits=small)
            self.assertEqual(ctx.exception.kind, "count")

    def test_decompression_bomb_rejected_from_headers(self) -> None:
        # ~24 MiB of zeros stored as a ~24 KiB DEFLATED member: the archive is
        # small but declares a large expansion.
        with tempfile.TemporaryDirectory() as td:
            bomb = craft_zip(
                [
                    ("big-a.dat", b"\x00" * (5 * 1024 * 1024)),
                    ("big-b.dat", b"\x00" * (5 * 1024 * 1024)),
                ],
                compress=True,
                omit_manifest=True,
            )
            self.assertLess(len(bomb), 2 * 1024 * 1024)
            tight = Limits(max_entries=10, max_expanded_bytes=8 * 1024 * 1024, max_member_bytes=6 * 1024 * 1024)
            with self.assertRaises(ArchiveProfileError) as ctx:
                extract_archive(bomb, Path(td) / "out", limits=tight)
            self.assertEqual(ctx.exception.kind, "size")
            self.assertFalse((Path(td) / "out").exists())

            member_tight = Limits(max_entries=10, max_expanded_bytes=64 * 1024 * 1024, max_member_bytes=1024)
            with self.assertRaises(ArchiveProfileError) as ctx:
                extract_archive(bomb, Path(td) / "out2", limits=member_tight)
            self.assertEqual(ctx.exception.kind, "member-size")


class ManifestIntegrityTest(unittest.TestCase):
    def test_missing_manifest_rejected(self) -> None:
        blob = craft_zip([("a.txt", b"data")], omit_manifest=True)
        with tempfile.TemporaryDirectory() as td:
            with self.assertRaises(ArchiveProfileError) as ctx:
                extract_archive(blob, Path(td) / "out")
            self.assertEqual(ctx.exception.kind, "manifest")

    def test_wrong_manifest_fields_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            bad_count = craft_zip(
                [("a.txt", b"data")],
                manifest={"profile": ARCHIVE_PROFILE, "memberCount": 99, "totalBytes": 4, "files": [{"name": "a.txt", "size": 4, "sha256": sha256_bytes(b"data")}]},
            )
            with self.assertRaises(ArchiveProfileError) as ctx:
                extract_archive(bad_count, Path(td) / "d1")
            self.assertEqual(ctx.exception.kind, "manifest")

            bad_profile = craft_zip(
                [("a.txt", b"data")],
                manifest={"profile": "someone-else/archive-v1", "memberCount": 1, "totalBytes": 4, "files": [{"name": "a.txt", "size": 4, "sha256": sha256_bytes(b"data")}]},
            )
            with self.assertRaises(ArchiveProfileError) as ctx:
                extract_archive(bad_profile, Path(td) / "d2")
            self.assertEqual(ctx.exception.kind, "manifest")

            bad_hash = craft_zip(
                [("a.txt", b"data")],
                manifest={"profile": ARCHIVE_PROFILE, "memberCount": 1, "totalBytes": 4, "files": [{"name": "a.txt", "size": 4, "sha256": "0" * 64}]},
            )
            with self.assertRaises(ArchiveProfileError) as ctx:
                extract_archive(bad_hash, Path(td) / "d3")
            self.assertEqual(ctx.exception.kind, "manifest")


class AtomicExtractionTest(unittest.TestCase):
    def test_no_overwrite_destination(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            blob = build_archive(root)  # empty tree -> manifest-only archive
            existing = root / "occupied"
            existing.mkdir()
            (existing / "keep.txt").write_bytes(b"keep me")
            with self.assertRaises(ArchiveProfileError) as ctx:
                extract_archive(blob, existing)
            self.assertEqual(ctx.exception.kind, "destination")
            self.assertEqual((existing / "keep.txt").read_bytes(), b"keep me")
            # No partial directories left behind.
            leftovers = [p for p in root.iterdir() if ".partial-" in p.name]
            self.assertEqual(leftovers, [])

    def test_failure_leaves_no_partial_and_no_destination(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            source = root / "src"
            source.mkdir()
            make_tree(source, {"ok.txt": b"fine"})
            blob = build_archive(source)
            # Corrupt one stored byte (length-preserving) so CRC verification
            # fails mid-extract.
            idx = blob.find(b"fine")
            self.assertGreaterEqual(idx, 0)
            tampered = blob[:idx] + b"XXXX" + blob[idx + 4 :]
            self.assertEqual(len(tampered), len(blob))
            with self.assertRaises(ArchiveProfileError) as ctx:
                extract_archive(tampered, root / "out")
            self.assertIn(ctx.exception.kind, {"profile", "crc", "manifest"})
            self.assertFalse((root / "out").exists())
            self.assertEqual(
                [p.name for p in root.iterdir() if ".partial-" in p.name], []
            )


if __name__ == "__main__":
    unittest.main(verbosity=2)
