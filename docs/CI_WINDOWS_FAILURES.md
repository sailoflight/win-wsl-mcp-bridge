# Windows CI failure diagnosis

Status record, not current capability documentation. It is the evidence base for
the remaining Windows work: read it before changing a Windows-sensitive test or
re-enabling the Windows CI leg as a gate.

Provenance: produced by a read-only diagnosis run on 2026-09-29 against commit
`a76b228`, with a local Windows reproduction at `C:\MCP\ci-repro`. The
repository was not modified by that run.

Update since: root cause RC1 (a sqlite handle that is never closed) is fixed for
the production runtime and the projection package in commit `7d366d5`, via
`bridge_runtime.connect_sqlite` and `_ClosingConnection`. RC2 through RC9 are
worked through separately, as is the test-side half of RC1.

One item this report left open is now resolved. Its closing section could not
determine why `test_windows_mcp_pushes_artifact_into_wsl_workspace` fails on an
artifact SHA-256 mismatch rather than a path, and suspected a real Windows
transfer defect. It is not one: `tests/fixtures/fixture_mcp.py` staged the
artifact with `Path.write_text(text, encoding="utf-8")`, which translates `\n`
to `os.linesep`, so on Windows the published bytes were CRLF while the caller
compared against the LF digest. The delivered file still *read back* as the
original text — universal newlines hide the difference — which is exactly why
the mismatch showed up on the digest and not on the content. The write now
passes `newline="\n"`, so the published digest covers the bytes the caller
asked for.

A sweep for the same defect class elsewhere found no second instance: every
production write whose bytes are hashed or transferred is opened binary
(`"wb"`, `os.fdopen(..., "wb")`, or an explicit `O_BINARY` flag), including the
artifact spool, the journal, and the projection's `_atomic_replace_bytes`. The
class existed only in the artifact fixture.

The related locale-decoding class was swept too. A call that passes `text=True`
(or `universal_newlines=True`) without `encoding=` decodes the child's output
with the platform locale codec — the ANSI code page on Windows — so a non-ASCII
payload raises `UnicodeDecodeError` or silently arrives as mojibake. Sixty-three
such calls exist across the repository, but only those that carry non-ASCII
payloads can fail, so two were fixed rather than all of them:
`installer/projection.py::_run_tool` (it parses `claude`/`codex` CLI output as
JSON; those CLIs are Node programs that emit UTF-8) and the facade harness in
`tests/test_compatibility_resilience.py`, which is the residual
`UnicodeDecodeError: 'gbk' codec` bucket from §6. The remaining calls are latent,
not benign: each one is a latent locale bug if its child ever emits non-ASCII.

Two of the report's platform claims were also re-checked first-hand on the real
Windows interpreter, rather than taken on trust:

- RC2 — with only `HOME` set, `pathlib.Path.home()` still returned the real
  profile (`C:\Users\<user>`); it honoured the value only once `USERPROFILE` was
  set too. The harness's "hermetic home" was therefore inert on Windows, and
  setting `USERPROFILE`/`HOMEDRIVE`/`HOMEPATH` is both necessary and sufficient.
- RC4 — `os.kill(exited_pid, 0)` returned `None` **without raising**, so a wait
  loop never observes the exit, and `os.kill(bogus_pid, 0)` raised
  `OSError [WinError 87]` rather than `ProcessLookupError`. Both failure modes
  the report names are real.

---

# Windows CI failure diagnosis — `.github/workflows/ci.yml` (`verify`)

Analysis commit: `a76b228c2b84b5254a47a5a67116541b0a90242b` (the `headSha` of failed run `36534646035`).
All line numbers below are for that commit. Nothing in the repository was modified.

---

## 1. Headline

Every one of the 4 matrix legs runs the same **632 tests**. The Windows legs fail **179 distinct test ids**
(union of the two Windows legs; 171 on 3.11 + 178 on 3.13) and they fall into **8 root causes**, but they are
overwhelmingly concentrated:

| # | Root cause | distinct tests | share |
|---|---|---:|---:|
| RC1 | SQLite connection handle never closed → temp-dir cleanup `WinError 32` | **111** | 62 % |
| RC2 | Hermetic test harness sets `HOME` but not `USERPROFILE` → tests read the real `C:\Users\<user>\.claude.json` | **40** | 22 % |
| RC3 | `select.select()` on a subprocess *pipe* fd | **8** | 4 % |
| RC4 | `os.kill(pid, 0)` PID-liveness check is wrong on Windows | **12** | 7 % |
| RC5 | Test helper writes text files with CRLF but hashes LF → engine digest mismatch | **5** | 3 % |
| RC6 | `Path.read_text()` without encoding → locale codec (`charmap`/`gbk`) | 1 | 0.6 % |
| RC7 | `_windows_path()` fallback renders backslashes instead of POSIX | 1 | 0.6 % |
| RC8 | 8.3 short path (`RUNNER~1`) compared against long path (`runneradmin`) | 1 | 0.6 % |

**RC1+RC2+RC3+RC4+RC5 = 176 / 179 = 98 %.** This is not ~180 independent problems: five small,
central fixes turn the Windows legs essentially green. RC1 alone is a single systemic
resource-lifecycle defect. *(Counts are disjoint: each test is attributed to its highest-priority cause.)*

Confirmed end-to-end locally: running the **whole 632-test suite** on Windows with the central fixes
emulated in-process takes the result from **179 broken results → 30** (173 → 30 distinct failing tests:
**148 cleared**, no source edits; §6). Of the residual 30, 9 are RC3/RC7 which were deliberately not
emulated, 6 are POSIX fake-CLI shims, 4 are handle leaks held by *spawned* bridge subprocesses (which
only a production fix can reach), and 3 are a latent subprocess-text-encoding bug that CI's cp1252
happens to mask.

The `ubuntu-latest` legs are green except **one timing flake per leg** (see §5). They should stay blocking.

---

## 2. Method / evidence

* Log: `gh run view 36534646035 --log-failed` → kept at `/tmp/ci-failed.log` (4.4 MB, 24 826 lines).
* Extraction: per-leg FAIL/ERROR blocks parsed with timestamps/ANSI stripped; every failing test
  attributed to a normalized exception signature; see `/tmp/classification.txt`, `/tmp/rc_map.json`.
* Local reproduction: see §6 — **yes**, the Windows suite reproduces on this machine, with the
  baseline aggregate matching CI almost exactly (632 tests, 179 broken).

WinError census in that log (this is the run analysed; note `WinError 10022` from the brief does **not**
appear in this log — the parameter error here is `WinError 87`):

```
434 × WinError 32      (217 error blocks: file still open at temp cleanup)
 24 × WinError 10038   (16 blocks: select() on a pipe fd)
  6 × WinError 87      (os.kill(pid,0) on a non-existent pid)
  1 × WinError 10053   (connection aborted during a deliberate shutdown race)
  2 × WinError 2       (expected payload inside test_process_start_failure_is_redacted_from_peer, not a failure)
```

---

## 3. Root causes, code locations, minimal fixes

### RC1 — SQLite connections are never closed (111 tests) — **the one big one**

**Exact text**

```
PermissionError: [WinError 32] The process cannot access the file because it is being used by
another process: 'C:\Users\RUNNER~1\AppData\Local\Temp\tmpXXXX\registry.sqlite3'
```
Raised from `tempfile.TemporaryDirectory.cleanup()` → `shutil.rmtree`. Affected file names seen:
`registry.sqlite3`, `events.sqlite3`, `win.events.sqlite3`, `win.sqlite3`, `journal.sqlite3`,
`projection.sqlite3`, `fresh.sqlite3`, `registry-noopt.sqlite3`.

**Representative test id**

`tests.test_bridge.ArtifactV2DeliveryTest.test_v2_foreign_tokenless_begin_never_reveals_state`
(other big clusters: `RegistryTest` 24, `EventJournalTraceRecordTest` 10,
`ArtifactV2DeliveryTest` 7, `HttpControlContractExecutionTest` 6,
`HttpManagedSupervisionExecutionTest` 6, `JournalMaintenanceTest` 5, `ProjectionDatabasePreviewTest` 5,
`EvidenceObserverNodeTest` 5, `SharedBackendBaselineUnitTest` 4 …).

**Mechanism (measured, not guessed)**

`with sqlite3.connect(...) as connection:` is a **transaction** context manager, not a closing one.
On CPython the `sqlite3.Connection` participates in a reference cycle, so the handle is only released
when the cyclic GC runs. On Linux the subsequent `unlink()` succeeds anyway, which is exactly why the
ubuntu legs are green. On Windows the file stays locked and `TemporaryDirectory.cleanup()` raises.

Minimal probe on the Windows host (Python 3.14.6 / SQLite 3.50.4):

```
with sqlite3.connect(...) as c         -> LOCKED
with closing(connect()) as c           -> unlink OK
explicit c.close()                     -> unlink OK
with closing(connect()) as c, c (idiom)-> unlink OK
after gc.collect()                     -> unlink OK
```

Instrumented failing test: at cleanup time 2 live `sqlite3.Connection` objects still referenced the
fixture DB; `gc.collect()` then made `cleanup()` succeed → the tests themselves pass, only teardown fails.
With a process-wide "close on `__exit__`" emulation the same 81-test subset went **37 broken → 7 broken**.

**Responsible code (all `with sqlite3.connect(...) as connection:` = leak; fix = close deterministically)**

* `bridge_runtime.py` — `EventJournal`: **1455, 1502, 1515, 1529, 1625, 1715, 1774, 1843**.
* `bridge_runtime.py` — `Registry`: **1888, 2409, 2419** (`with self._connect() as connection:`)
  plus the factory `Registry._connect()` at **1897**, and bare
  `connection = sqlite3.connect(...)` at **1897, 1925**.
* `installer/projection.py` — `ProjectionDatabase._connect()` factory at **334–341** and its
  **17 call sites** at 2671, 2886, 2976, 2997, 3100, 3226, 3237, 3254, 3278, 3985, 4002, 4117, 4186
  (`with database._connect(...) as connection:`), plus bare connects at 263, 2593.
* Tests (same pattern, also leaks): **56** `with sqlite3.connect(...)` sites and 16 `with X._connect(...)`
  sites — `tests/test_bridge.py` (49 + 2), `tests/test_projection_preview.py` (5 + 3),
  `tests/test_legacy_profiles.py` (1), `tests/test_client_enrollment_verification.py` (1 + 11).
  One already-correct example to copy: `tests/test_journal_maintenance.py` (uses `closing(...)`).

**Minimal fix (prose)**

1. Add one tiny shared helper next to the SQLite users, e.g. in `bridge_runtime.py`:

   ```python
   class _ClosingConnection(sqlite3.Connection):
       """Context-manager exit also releases the OS handle (Windows temp cleanup)."""
       def __exit__(self, *exc):
           try:
               return super().__exit__(*exc)   # keep commit/rollback semantics
           finally:
               self.close()

   def connect_sqlite(*args, **kwargs):
       kwargs.setdefault("factory", _ClosingConnection)
       return sqlite3.connect(*args, **kwargs)
   ```

   Passing `factory=_ClosingConnection` is what makes a *one-line* change at the factories
   (`Registry._connect()`, `ProjectionDatabase._connect()`) fix all 20 of their call sites at once,
   and preserves commit-on-success because `__exit__` still runs the base implementation first.
2. Replace `sqlite3.connect(` with `connect_sqlite(` at the 10 inline sites in `bridge_runtime.py`
   (8 in `EventJournal` + the 2 inside `Registry._connect()`/`initialize_database`) and the 4 in
   `installer/projection.py`; the 3 `with self._connect()` and 17 `with database._connect(...)` call
   sites then need no edit at all, because the factories already return a self-closing connection.
   (Equivalent alternative for the inline sites: `with closing(sqlite3.connect(...)) as c, c:`.)
3. Same substitution at the 56 test-side `with sqlite3.connect(...)` sites; the pure-test sites can
   alternatively use `contextlib.closing`. No test assertion changes.

**Effort** ~1.5–2 h, mechanical. **Risk** low; the only semantic care is keeping the commit
(`__exit__` of the base class) and not closing a connection a caller still uses after the `with` block.

---

### RC2 — Tests depend on the developer's real home directory (40 tests)

**Exact text**

```
bridge_runtime.BridgeError: candidate cand-b16df78e83e92d49f205 refers to a missing configuration
that is not a Bridge-created owned document: C:\Users\runneradmin\.claude.json
```

Other faces of the same bug:

```
AssertionError: 'C:\\Users\\runneradmin\\.claude.json' !=
                 'C:\\...\\tmp\\p0b-test-XXXX\\home\\.claude.json'      # enroll_revalidates_candidate…
AssertionError: 'cand-937dfcf45b0f91e15494' == 'cand-937dfcf45b0f91e15494'  # stale-id detection sees no change
AssertionError: 'user-server' not found in {'user-codex'}
AssertionError: False is not true   # local_path.is_relative_to(self.wsl_workspace)
```

**Representative test id**

`tests.test_bridge.ProjectionReconcileTest.test_file_mode_adds_preserves_and_removes_claude_entries`
(biggest clusters: `ProjectionReconcileTest` 15, `ProjectionNativeHttpTest` 10,
`ProjectionStdioToHttpTest` 6, `ProjectionScannerOutboxEnrollTest` 4+1,
`ProjectionFinalCoverageTest` 2, `BidirectionalIntegrationTest` 2).

**Responsible code**

`tests/test_bridge.py`, class `ProjectionHarness` (line **4945**):

* **4948–4951** — `_env_snapshot` records only `HOME, CODEX_HOME, DSH_HOME, PATH, FAKE_STATE, FAKE_KIND`.
* **4963** — `os.environ["HOME"] = str(self.home)`.
* **4976–4982** — `_restore_environment()` restores exactly that key list.

On Windows `pathlib.Path.home()` / `os.path.expanduser("~")` read **`USERPROFILE`** (then
`HOMEDRIVE`+`HOMEPATH`) and ignore `HOME`. Measured on the Windows host:

```
HOME-only  Path.home() -> C:\Users\075526249        (the real profile)
USERPROFILE set -> C:\MCP\ci-repro\tmp\fakehome     (honoured)
```

So the "hermetic per-test environment" is not hermetic on Windows, and
`installer/projection.py:1535` (`locations.append((CLIENT_KIND_CLAUDE, "user", Path.home() / ".claude.json", "scan"))`)
scans the CI runner's real profile. This is a genuine **test-harness bug**, not a product bug.

**Minimal fix (prose)**

In `ProjectionHarness`, add `USERPROFILE`, `HOMEDRIVE`, `HOMEPATH` to the `_env_snapshot` key tuple
(4949–4951) and set them in `setUp` next to `os.environ["HOME"]` (4963):

```python
os.environ["USERPROFILE"] = str(self.home)   # Windows: what Path.home() actually reads
os.environ["HOMEDRIVE"], os.environ["HOMEPATH"] = str(self.home.drive), str(self.home)[2:]
```

`_restore_environment()` then restores them unchanged. Optionally also make `set_path()` (4984) keep the
real Windows `SystemRoot`/`System32` ahead of the POSIX `/usr/bin:/bin` placeholders.
Verified locally: with `Path.home()` honouring the fake home, `ProjectionScannerOutboxEnrollTest`
goes 3 failures → 0, and `ProjectionReconcileTest` goes 11 failures + 2 errors → 1 + 1.

**Effort** 15 min. **Risk** very low (test-only).

---

### RC3 — `select.select()` on a subprocess pipe (8 tests)

**Exact text**

```
OSError: [WinError 10038] An operation was attempted on something that is not a socket
```

**Representative test id**

`tests.test_bridge.PersistentConnectorStdioTest.test_handshake_carries_connector_version_and_core_version`
(7 × `PersistentConnectorStdioTest` + 1 × `BidirectionalIntegrationTest.test_connector_stays_alive_and_reconnects_after_remote_close`).

**Responsible code**

`tests/test_bridge.py`, class `FdLineReader` (≈ **403–440**), specifically
`select.select([self.fd], [], [], 0.2)` at **412** and `select.select([self.fd], [], [], 0.1)` at **431**.
`self.fd` is a subprocess pipe fd; Winsock `select()` accepts sockets only.
(The production `select.select` at `stdio_http_facade.py:1557` is on a real socket and is fine, as is
`tests/test_modern_http_facade.py:467`.)

**Minimal fix (prose)**

Give `FdLineReader` a Windows path: start one daemon thread per reader that does blocking
`os.read(self.fd, 65536)` and pushes chunks to a `queue.Queue`; `read_line(timeout)` then waits on the
queue with the same deadline and keeps the existing byte-buffer/`\n` splitting. `select.select` is
retained for POSIX. Roughly 30–40 lines in one helper class, no test-logic changes.
A cheaper fallback is `@unittest.skipIf(os.name == "nt", "select() on pipe fds is POSIX-only")` on
`PersistentConnectorStdioTest`, at the cost of losing the Windows coverage.

**Effort** 30–45 min. **Risk** low (test helper), medium only if the thread must be joined to avoid
leaking readers between tests.

---

### RC4 — `os.kill(pid, 0)` PID liveness check (12 tests)

**Exact text**

```
AssertionError: Timed out waiting for bounded fixture cleanup
AssertionError: owned child process 5076 did not exit
OSError: [WinError 87] The parameter is incorrect
StopIteration      (waiting for a "backend-exit:<pid>:" row that never appears)
```

**Representative test id**

`tests.test_modern_http_facade.ModernFacadeTests.test_timeout_and_crash_have_uncertainty_no_replay_and_proxy_cleanup`
(10 × `ModernFacadeTests`, 2 × `tests.test_bridge.SharedBackendAcceptanceTest`).

**Mechanism (measured)**

On Windows `os.kill(pid, 0)` does **not** raise for an exited process — it returns `None`:

```
process status after exit: 0
os.kill(pid, 0) returned None   -> pid_gone() says "still alive" forever -> wait_until times out
os.kill(999999, 0) -> OSError [WinError 87] (not ProcessLookupError)
```

It does *not* kill a live process (verified), so this is a correctness bug in the check, not a hazard.

**Responsible code**

* `tests/test_modern_http_facade.py:215–221` `pid_gone()` (`os.kill(pid, 0)` at **217**, caught as
  `ProcessLookupError` only → WinError 87 escapes `tearDown`), and `wait_until(...)` /
  `self.fail("Timed out waiting for bounded fixture cleanup")` at **236–240**.
* `tests/test_bridge.py:2262–2270` `_wait_pid_exit` (**2266** `os.kill(pid, 0)`).
* `tests/test_bridge.py:3608–3615` `_wait_pid_exit` (**3611**).
* `tests/test_bridge.py:3264` and **3284** — `with self.assertRaises(OSError): os.kill(child_pid, 0)`
  (on Windows this raises nothing, so `assertRaises` fails).

**Minimal fix (prose)**

Add one shared test helper, e.g. `tests/pid_liveness.py` / a function in `tests/test_bridge.py`:

```python
def pid_alive(pid: int) -> bool:
    if os.name == "nt":
        PROCESS_QUERY_LIMITED_INFORMATION, STILL_ACTIVE = 0x1000, 259
        handle = ctypes.windll.kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
        if not handle:
            return False                      # gone (or not queryable)
        try:
            code = ctypes.c_ulong()
            if not ctypes.windll.kernel32.GetExitCodeProcess(handle, ctypes.byref(code)):
                return False
            return code.value == STILL_ACTIVE
        finally:
            ctypes.windll.kernel32.CloseHandle(handle)
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
```

then replace the 5 call sites. `pid_gone()` becomes `not pid_alive(pid)` and must also catch `OSError`
so WinError 87 can never escape `tearDown`. Note `test_04` additionally needs the exit *row* in the
event log, which depends on the same stop path — re-check it after the fix (locally it is still failing
in the combined run; see §7).

**Effort** 30 min. **Risk** low (test-only).

---

### RC5 — test helper writes CRLF, hashes LF (5 tests)

**Exact text**

```
AssertionError: 'connector-engine' != 'engine-b'      (and engine-env / engine-x / engine-old / 'engine')
```
`connector-engine` is the built-in fallback engine, i.e. every scanned bundle failed verification.

**Representative test id** `tests.test_connector_core.EngineScanTest.test_scan_picks_newest_verified_engine`

**Responsible code** `tests/test_connector_core.py`, `_write_bundle()`:
digest computed over LF text at **70** (`hashlib.sha256(pristine.encode("utf-8"))`) but the file written in
text mode at **71** (`module.write_text(pristine, encoding="utf-8")` → `\n` becomes `\r\n` on Windows).
`connector_core.py:235–245` then compares the manifest sha256 against the on-disk bytes and rejects.
Measured: expected `be493561…` vs on-disk `2c2c898e…`, raw bytes `b'ENGINE_NAME = "engine-b"\r\n'`.

**Minimal fix (prose)** line **71**:
`module.write_text(pristine, encoding="utf-8", newline="\n")`.
**Effort** 1 min. **Risk** none.

---

### RC6 — `read_text()` without an explicit encoding (1 test)

**Exact text** `UnicodeDecodeError: 'charmap' codec can't decode byte 0x9d in position 545`
(gbk locally, cp1252 on the runner).

**Representative test id**
`tests.test_repository_layout.RepositoryLayoutTest.test_canonical_document_index_links_exist`

**Responsible code** `tests/test_repository_layout.py:40` — `index.read_text()` on `docs/INDEX.md`.
**Minimal fix** `index.read_text(encoding="utf-8")` (line 40). **Effort** 1 min. **Risk** none.

---

### RC7 — `_windows_path()` fallback renders a Windows path (1 test)

**Exact text** `AssertionError: '\\opt\\bridge\\registry.sqlite3' != '/opt/bridge/registry.sqlite3'`

**Representative test id**
`tests.test_dsh_registry_supervisor.SupervisorPathTest.test_windows_path_falls_back_to_a_sibling_for_non_interop_paths`

**Responsible code** `installer/dsh_node_registry_entry.py:184`
(`return str(node.parent.parent.parent.joinpath(*suffix))`) — `Path.joinpath` uses the host separator,
so on Windows the documented "plain POSIX path" comes back with backslashes. This is production code.

**Minimal fix** render POSIX explicitly, e.g.
`return posixpath.join(str(node.parent.parent.parent).replace(os.sep, "/"), *suffix)`.
On POSIX hosts this is byte-identical to today's behaviour. **Effort** 5 min. **Risk** low.

---

### RC8 — 8.3 short path vs long path (1 test)

**Exact text**

```
AssertionError: 'C:\Users\RUNNER~1\AppData\Local\Temp\tmp_h8htb5f\registry.sqlite3' not found in
'initialized registry: C:\Users\runneradmin\AppData\Local\Temp\tmp_h8htb5f\registry.sqlite3\n'
```

**Representative test id** `tests.test_bridge.RegistryTest.test_registry_init_cli_creates_versioned_sqlite_database`

**Responsible code** `tests/test_bridge.py:566` — `self.assertIn(str(database), process.stdout)`.
`tempfile` hands back the 8.3 form while the child process prints the long form.

**Minimal fix** compare resolved/canonical paths, e.g. assert on
`Path(process.stdout.split("initialized registry: ", 1)[1].strip())` equals `database` via
`os.path.samefile`, or normalise both with `os.path.realpath`. **Effort** 15 min. **Risk** low.
(Does not reproduce on this machine because its temp root has no 8.3 alias — see §7.)

---

### RC9 (folded into RC4) — UTF-8 written, locale decoded, in the facade fixture

One `ModernFacadeTests` test carries a second, independent cause:

**Exact text**

```
AssertionError: {'arguments': {… 'text': ' padded 世界 '}} != {'arguments': {… 'text': ' padded 涓栫晫 '}}
```
(the CI log shows the same asymmetry; the ANSI-code-page rendering is `Ã¤Â¸Â...`.)

**Representative test id**
`tests.test_modern_http_facade.ModernFacadeTests.test_annotated_headers_decode_and_validate_without_changing_body`

**Responsible code** `tests/test_modern_http_facade.py:71` — the fixture's read loop
`for line in sys.stdin:` uses text mode with the platform default codec, while
`stdio_http_facade.py:1183–1184` (`_modern_encode`) writes **UTF-8** JSON to the child.
Measured locally: expected `' padded \u4e16\u754c '`, echoed `' padded \u6d93\u682b\u666b '`
(UTF-8 bytes decoded as GBK).

**Minimal fix** at the top of the `BACKEND` fixture source (and the sibling fixtures that use
`sys.stdin`/`print`): `sys.stdin.reconfigure(encoding="utf-8")` and
`sys.stdout.reconfigure(encoding="utf-8")` (or spawn with `PYTHONIOENCODING=utf-8`).
**Effort** 10 min. **Risk** low.
*Product note:* the same mismatch is a real interoperability risk for any Windows-hosted Python stdio
business MCP — the bridge assumes UTF-8 on both ends, and a Windows Python child defaults to the ANSI
code page. Worth an explicit `PYTHONIOENCODING=utf-8`/`-X utf8` in the spawn path, or a documented
requirement, independently of CI.

---

## 4. What this means for "2–3 central fixes"

Yes. RC1 is one systemic defect; RC2+RC4+RC5+RC6 are four tiny test/production edits; RC3 is one helper
rewrite. Fixing RC1+RC2+RC3+RC4+RC5 covers **98 %** of the 179 Windows failures. No Windows failure
requires a per-test investigation.

---

## 5. The ubuntu legs (1 flake each) — keep blocking

| leg | test | text |
|---|---|---|
| ubuntu 3.11 | `tests.test_streamable_http_stdio.ConcurrentSessionLossTest.test_concurrent_404_recovery_is_exactly_once_and_never_deadlocks` | `AssertionError: {3: 'raised HttpTransportError', 1:'ok', 0:'ok', 2:'ok'} != {0:'ok',1:'ok',2:'ok',3:'ok'}` |
| ubuntu 3.13 | `tests.test_modern_http_facade.ModernFacadeTests.test_bounded_request_response_and_http_worker_lifecycle` | `AssertionError: 504 != 200` (line 430) |

Both are genuine timing-sensitive concurrency/capacity assertions (`initialize_delay_s = 0.6` + 4 threads
for the first; a `max_inflight` backpressure race for the second) and the same test passes on the sibling
leg in the same run. **There is no obvious deterministic fix** that does not weaken a deliberately
strict concurrency contract (the 404 test is an explicit regression guard for a deadlock and says so in
its docstring — do not mark it `expectedFailure`). Recommendation: keep the ubuntu leg blocking and add a
**bounded, once-only rerun of the failed tests** at workflow level (e.g. a second `python -m unittest`
invocation over the failed ids, or a two-attempt step), plus an issue reference so the flake is tracked.

---

## 6. Local Windows reproduction — worked

* Repo copied out of the WSL tree to a project-specific Windows directory:
  **`C:\MCP\ci-repro\win-wsl-mcp-bridge`** (from `git archive a76b228`; `C:\MCP\ci-repro\win-wsl-mcp-bridge-fixed`
  holds the patched variant). **Nothing was created in or deleted from the Windows Temp directory**:
  the default `TemporaryDirectory` location was redirected in-process to
  **`C:\MCP\ci-repro\tmp`** (`tempfile.tempdir`) by the out-of-repo runners
  `C:\MCP\ci-repro\{win_runner,gc_runner,fix_runner,combo_runner,diag_runner}.py`.
  Note: two early exploratory invocations, before that redirection was in place, let the suite's own
  `TemporaryDirectory` create and fail to remove a handful of `tmp*` directories under
  `C:\Users\075526~1\AppData\Local\Temp`; those were **not** deleted (per the hard rule).
* Interpreter: `C:\MCP\CadQ\.venv\Scripts\python.exe` → **Python 3.14.6** (the only Windows Python on this
  host; `%LOCALAPPDATA%\WinWslMcpBridge\runtime\Scripts\python.exe` is also 3.14.6). CI uses 3.11/3.13,
  so version-specific differences are possible, but every failure class reproduced.
* Method: `python.exe ..\win_runner.py ALL` from the repo copy with
  `PYTHONDONTWRITEBYTECODE=1`; `unittest.TestLoader().discover("tests", top_level_dir=".")` to match CI.
* Result: **632 tests, 35 failures + 144 errors = 179 broken in 290 s** vs CI's 171/178 distinct on the
  two Windows legs. Reproduction is faithful in aggregate.
* **Full-suite controlled experiment.** The same suite was then re-run with the central fixes emulated
  in-process (no source edits): close-on-`with`-exit for every `sqlite3.connect` (RC1), `Path.home()`
  honouring `HOME` (RC2), POSIX-like PID liveness (RC4), UTF-8/LF defaults for `Path` text helpers
  (RC5/RC6) and UTF-8 stdio/`PYTHONUTF8` for spawned children (RC9):
  `C:\MCP\ci-repro\combo_runner.py`.

  | run | tests | failures | errors | broken |
  |---|---:|---:|---:|---:|
  | baseline (`win_runner.py ALL`) | 632 | 35 | 144 | **179** |
  | RC1+RC2+RC4+RC5+RC6+RC9 emulated (`combo_runner.py ALL`) | 632 | 10 | 20 | **30** |

  **148 of the 173 locally-failing tests are cleared by those fixes alone (83 %); 179 → 30.**
  (RC3 and RC7 were deliberately *not* emulated, so their 9 tests are inside the residual 30.)
* Measured controls on identical 81-test subsets (`test_journal_maintenance`,
  `test_projection_preview`, `test_stream_evidence`, `test_journal_evidence`, `test_legacy_profiles`,
  `test_http_protocol_registration`, `test_connector_core`):

  | variant | broken |
  |---|---:|
  | baseline | 37 |
  | `gc.collect()` before every temp cleanup | 11 |
  | every `sqlite3.connect` closes on `with` exit (RC1 only) | 7 |

  The `gc` column proves the RC1 mechanism; the closing column shows RC1 is a bounded fix, and the
  residual 7 are projection-preview sites and the RC5 `EngineScanTest` failures.
* **Residual 30, classified** (from `/tmp/win-combo-full.txt`):

  | residual bucket | count | note |
  |---|---:|---|
  | `WinError 10038` — RC3, not emulated | 8 | `PersistentConnectorStdioTest` ×7 + `BidirectionalIntegrationTest` ×1 |
  | `official_cli_*` fake-CLI tests | 6 | POSIX shell-script `claude`/`codex` shims; demote/skip on Windows |
  | `WinError 32` in *class-level* teardown (`win.events.sqlite3`, `win.sqlite3`) | 4 | `SharedBackendAcceptanceTest`, `LegacySharedAcceptanceTest`, `ModernFacadeTests`, `EvidencePairCorrelationTest` |
  | RC7 `_windows_path`, not emulated | 1 | |
  | `unicodedata`/`UnicodeDecodeError: 'gbk' codec` in `CompatibilityFacadeResilienceTest` | 3 | parent decodes a child's UTF-8 output with the locale codec; latent, masked by CI's cp1252 |
  | remaining `ModernFacadeTests` Windows socket/timing semantics | 4 | `10053`, `'application/json' != 'text/event-stream'`, `504 not in (200, 502)`, sigterm-CLI |
  | other single cases | 4 | `test_04` (`StopIteration`), `test_windows_mcp_pushes_artifact_into_wsl_workspace` (now a SHA mismatch), the 404-recovery flake, and `test_project_has_only_two_component_directories`† |

  † `RegistryTest.test_project_has_only_two_component_directories` failed only because my exploratory
  runs left a `__pycache__` directory in the copy — a local artifact, not a CI failure. Excluding it the
  residual is 29.

  Two findings worth acting on:
  1. **The class-level `WinError 32` residual proves the fix must be in production code.** Those 4 tests
     fail in `tearDownClass` while the *spawned* bridge node processes still hold `win.events.sqlite3`.
     Emulating `closing` inside the test process cannot help there — the leaking handles belong to
     real `bridge_runtime.EventJournal`/`Registry` instances in subprocesses.
  2. One test-side bare connect remains (`writer = sqlite3.connect(path)`,
     `tests/test_projection_preview.py:137`); the real patch must cover bare connects as well as `with`
     statements.
* Per-cause spot checks: `ProjectionScannerOutboxEnrollTest` 3 → 0 with the RC2 emulation;
  `ProjectionReconcileTest` 13 → 2 with RC1+RC2+RC4+RC5+RC9 emulated.

---

## 7. Recommendation table

| root cause | verdict | effort | risk | patch (prose) |
|---|---|---|---|---|
| **RC1** sqlite handle never closed (111) | **fix-now** | 1.5–2 h | low | `_ClosingConnection(sqlite3.Connection)` whose `__exit__` closes after `super().__exit__`; pass as `factory=` (one line each) in `Registry._connect()` and `ProjectionDatabase._connect()`, use a `connect_sqlite()` helper for the 14 inline sites, and switch the 56 test sites (or `closing(...)` there). **The production half is mandatory**: the residual class-level `WinError 32` comes from spawned bridge subprocesses. |
| **RC1b** bare connects (`writer = sqlite3.connect(...)`) | **fix-now** | 10 min | low | `tests/test_projection_preview.py:137` — wrap in `closing(...)`/close explicitly. |
| **RC2** harness sets `HOME`, not `USERPROFILE` (40) | **fix-now** | 15 min | very low | add `USERPROFILE`/`HOMEDRIVE`/`HOMEPATH` to `ProjectionHarness`'s snapshot and `setUp` (`tests/test_bridge.py:4949`, `:4963`). |
| **RC3** `select()` on a pipe (8) | **fix-now** | 30–45 min | low | give `FdLineReader` (`tests/test_bridge.py:~403`) a Windows path: daemon thread + `queue.Queue` over `os.read`, keep `select` on POSIX. |
| **RC4** `os.kill(pid,0)` liveness (12) | **fix-now** | 30 min | low | one shared `pid_alive()` (ctypes `OpenProcess`+`GetExitCodeProcess` on Windows) used by the 5 call sites; `pid_gone()` must also catch `OSError`. |
| **RC5** CRLF vs hashed LF (5) | **fix-now** | 1 min | none | `tests/test_connector_core.py:71` → `newline="\n"`. |
| **RC6** locale file read (1) | **fix-now** | 1 min | none | `tests/test_repository_layout.py:40` → `encoding="utf-8"`. |
| **RC7** `_windows_path` separators (1) | **fix-now** | 5 min | low | `installer/dsh_node_registry_entry.py:184` → `posixpath.join(str(...).replace(os.sep, "/"), *suffix)`. |
| **RC8** 8.3 vs long temp path (1) | **fix-now** | 15 min | low | `tests/test_bridge.py:566` compare resolved paths (`os.path.realpath`/`samefile`) instead of raw strings. |
| **RC9** UTF-8 vs locale stdin in facade fixture (1) | **fix-now** | 10 min | low | `sys.stdin/stdout.reconfigure(encoding="utf-8")` in the `BACKEND` fixture (`tests/test_modern_http_facade.py:71`); consider the same for real Windows stdio backends in production. |
| **RC9b** `subprocess` text mode without `encoding=` (`tests/test_compatibility_resilience.py`, 3 locally) | **fix-now** | 15 min | low | pass `encoding="utf-8"` (or decode bytes explicitly) wherever a spawned child's output is read as text; currently decoded with the locale codec. |
| **RC10** residual: `SharedBackendAcceptanceTest.test_04_bridge_restart_has_no_generation_overlap` | **investigate, then fix-now** | ≤1 h | low–medium | expected an `backend-exit:<pid>:` journal row; only after RC4 is fixed can the real cause be judged (it still failed under the combined emulation). If it turns out to be Windows process-group/exit-code semantics, treat as demote. |
| **RC10b** residual: 4 `ModernFacadeTests` Windows socket/timing cases (`10053`, `application/json != text/event-stream`, `504 not in (200, 502)`, sigterm-CLI) | **investigate, then decide** | ~1–2 h | medium | small in count but they touch real Windows socket-shutdown/deadline semantics; re-measure in CI after RC1/RC4 land before spending time. |
| **RC11** residual: 6 `official_cli_*` fake-CLI tests (`'bridge-file' != 'official-cli'`, `FileNotFoundError … claude-state.json`) | **demote (scoped)** | — | — | the fixtures exec POSIX-style fake `claude`/`codex` binaries; running them on Windows is a real project. Skip them on `os.name == "nt"` with an explicit reason until a Windows CLI shim exists. |
| **ubuntu 404-recovery flake** | **quarantine-by-retry** | 15 min | low | keep blocking; add a bounded single rerun of failed tests in the workflow. No deterministic fix without weakening the assert. |
| **ubuntu 504 flake** | **quarantine-by-retry** | (same step) | low | same retry; if it flakes again, widen the fixture's timing window, do not delete the assert. |

Suggested order: RC5/RC6 (minutes) → RC2 → RC1 → RC4 → RC9 → RC3 → RC7 → RC8 → RC10. Expected result:
Windows legs at ~0 failures with a handful of explicitly-skipped/fake-CLI cases; ubuntu legs steady with
a documented retry.

---

## 8. What I could not determine / open items

1. **RC10** (`SharedBackendAcceptanceTest.test_04_bridge_restart_has_no_generation_overlap`) — the
   `StopIteration` on the missing `backend-exit:<pid>:` row is still failing under the combined
   emulation; whether it is only RC4 or also a Windows process-exit/`start_new_session` difference is
   unresolved. It needs one focused debug session after RC4 lands.
2. **Residual RC1 sites** — note the two distinct leftovers:
   * `writer = sqlite3.connect(path)` (`tests/test_projection_preview.py:137`) — a bare connect, never
     closed; the real patch must cover bare connects, not only `with` statements.
   * Class-level teardown `WinError 32` on `win.events.sqlite3` / `win.sqlite3`
     (`SharedBackendAcceptanceTest`, `LegacySharedAcceptanceTest`, `ModernFacadeTests`,
     `EvidencePairCorrelationTest`) — the handles are held by **spawned bridge subprocesses** running
     unpatched `bridge_runtime.py`. This is positive evidence that the production-side fix
     (`Registry._connect()` / `EventJournal`) is required; a test-only workaround will not clear them.
3. **Latent encoding bug outside CI's reach** — `tests/test_compatibility_resilience.py` decodes a
   spawned child's UTF-8 output with the locale codec (`UnicodeDecodeError: 'gbk' codec` locally). CI's
   cp1252 decodes those bytes without raising, so it is masked there and may be silently corrupting
   comparisons. Worth fixing with the RC9 family (`subprocess.run(..., encoding="utf-8")`).
4. **`test_windows_mcp_pushes_artifact_into_wsl_workspace`** — after the home/path assertions are fixed
   this one fails locally on an artifact **SHA-256 mismatch**, not on the path. Needs a focused look;
   it may be a genuinely Windows-specific transfer/encoding defect rather than a test problem.
5. **`WinError 10022`** from the brief does **not** occur in run `36534646035`; the parameter error here
   is `WinError 87` (from `os.kill`). If 10022 was observed in an older run it may be a third signature
   worth grepping for separately.
6. **Python versions** — reproduction used CPython 3.14.6 on Windows; CI uses 3.11/3.13. All failure
   classes reproduced, but the exact residual counts after fixing can only be confirmed on 3.11/3.13.
7. **8.3 short paths (RC8)** did not reproduce locally (this machine's temp root has no 8.3 alias), so
   that fix is derived from the CI log alone.
8. **Local-vs-CI environment difference** — this host's ANSI code page is GBK; GitHub's Windows runners
   use cp1252. Bugs whose *symptom* is a decode error appear locally but are silent on CI (item 3), and
   the reverse can also happen; treat the exact residual list as approximate and re-measure in CI after
   the fixes land.
