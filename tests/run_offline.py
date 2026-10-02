"""Run the offline suite, optionally without its load-sensitive tests.

A handful of tests assert wall-clock and interleaving behaviour (a bounded
handler deadline, a concurrent 404 recovery, a spawn/initialize count under
load, a fixture cleanup deadline). They are meaningful on an idle host and
intermittent on a saturated one — a shared CI runner, or a workstation running
several Agents — which is what made the whole `verify` check untrustworthy.

`--deterministic-only` runs every test except the named set below. The default
runs the whole suite. The names are explicit rather than pattern-based so that
adding one is a deliberate act, and each entry names the pressure it is
sensitive to.
"""

from __future__ import annotations

import argparse
import sys
import unittest

#: Tests whose verdict depends on how much else is running. Evidence for each
#: entry is a recorded intermittent failure, not a guess. Two
#: `ModernFacadeTests` members left this list once their deadline stopped
#: doubling as a cold-spawn budget (see the constants in
#: tests/test_modern_http_facade.py): the recorded failures were about spawn
#: latency, never about the protocol behaviour those members assert.
LOAD_SENSITIVE: frozenset[str] = frozenset(
    {
        # Concurrent recovery: a saturated runner loses more sessions before
        # recovery settles, so "exactly once" is measured against a moving set.
        "tests.test_streamable_http_stdio.ConcurrentSessionLossTest"
        ".test_concurrent_404_recovery_is_exactly_once_and_never_deadlocks",
        # Spawn / initialize counting and connector liveness under thread load.
        "tests.test_bridge.SharedBackendAcceptanceTest"
        ".test_01_concurrent_clients_share_spawn_and_route_ids",
        "tests.test_bridge.BidirectionalIntegrationTest"
        ".test_connector_stays_alive_and_reconnects_after_remote_close",
        "tests.test_legacy_profiles.LegacySharedAcceptanceTest"
        ".test_03_crash_recovery_renegotiates_the_same_actual_without_replay",
    }
)

#: Whole classes whose every member shares one load-sensitive mechanism, so
#: listing members one by one would only invite the unlisted ones to flake.
LOAD_SENSITIVE_CLASSES: frozenset[str] = frozenset(
    {
        # Every member collects connector output through FdLineReader.read_line
        # with a 0.3 s poll; on a saturated host a message that arrives late is
        # indistinguishable from one that never arrives.
        "tests.test_bridge.PersistentConnectorStdioTest",
    }
)


def _is_load_sensitive(test_id: str) -> bool:
    if test_id in LOAD_SENSITIVE:
        return True
    owner, _, _ = test_id.rpartition(".")
    return owner in LOAD_SENSITIVE_CLASSES


def _flatten(suite: unittest.TestSuite):
    for item in suite:
        if isinstance(item, unittest.TestSuite):
            yield from _flatten(item)
        else:
            yield item


def build_suite(deterministic_only: bool) -> unittest.TestSuite:
    discovered = unittest.TestLoader().discover(
        start_dir="tests", top_level_dir="."
    )
    selected = unittest.TestSuite()
    for test in _flatten(discovered):
        if deterministic_only and _is_load_sensitive(test.id()):
            continue
        selected.addTest(test)
    return selected


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--deterministic-only",
        action="store_true",
        help="skip the named load-sensitive tests",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument(
        "--list-load-sensitive",
        action="store_true",
        help="print the load-sensitive test ids and exit",
    )
    arguments = parser.parse_args(argv)
    if arguments.list_load_sensitive:
        for test_id in sorted(LOAD_SENSITIVE):
            print(test_id)
        for class_id in sorted(LOAD_SENSITIVE_CLASSES):
            print(f"{class_id}.*")
        return 0
    suite = build_suite(arguments.deterministic_only)
    result = unittest.TextTestRunner(verbosity=2 if arguments.verbose else 1).run(
        suite
    )
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    sys.exit(main())
