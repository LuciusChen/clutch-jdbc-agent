# 023 — Optional JDBC Savepoint Release

## Background

Oracle JDBC reports savepoint support and successfully creates and rolls back
to savepoints, but `Connection.releaseSavepoint` raises
`SQLFeatureNotSupportedException`. The agent originally coupled explicit
release to both successful batch completion and rollback-to-savepoint recovery.
Clutch therefore reported an uncertain transaction even after the meaningful
transaction operation had succeeded.

## Decision

`ConnectionManager`, which owns JDBC savepoint objects and their opaque
handles, normalizes only `SQLFeatureNotSupportedException` from explicit
savepoint release. It removes the local handle and lets the enclosing commit or
rollback discard the database-side savepoint. A rollback-to-savepoint failure
still propagates, as does every other `SQLException` raised while releasing.

After a successful rollback, the manager also removes every later savepoint
handle in that session before attempting optional release. JDBC savepoint ids
are allocated monotonically by the agent, so a greater local id in the same
session denotes a savepoint created after the rollback target. Keeping one of
those handles would expose database state that no longer exists.

The same ordering rule applies when explicit release returns normally, whether
the driver performed it or the narrowly normalized unsupported-method path
established the logical release. JDBC defines release as removing the target
and all subsequent savepoints, so the manager discards all corresponding local
handles. A broader release failure still preserves them because its outcome is
not known.

## Rationale

Skipping savepoints entirely would make staged manual-commit submissions
non-atomic. Detecting Oracle in Emacs would move a JDBC capability rule into the
wrong process and would not cover another driver with the same optional-method
gap. Treating every release exception as harmless would hide genuine session or
transaction failures. The exact JDBC exception type preserves the atomicity
contract while keeping unknown outcomes visible.

## Verification

The fail-first `ConnectionManagerTest` reproduces unsupported release after
both a successful batch boundary and a rollback-to-savepoint boundary. It also
asserts that each opaque handle is consumed. Its nested case creates an outer
and inner savepoint, rolls back the outer one, and requires the later inner
handle to be rejected locally without another driver release call. Existing
tests continue to require ordinary release failures to propagate and failed
rollback handles to become unusable.

A separate supported-driver fail-first test releases an outer savepoint and
requires its later inner handle to be rejected without a second driver call.
This pins JDBC's target-and-subsequent release contract independently of
Oracle's unsupported explicit-release behavior.

An Oracle Podman reproduction confirmed the driver boundary: rolling back the
outer savepoint made a later rollback fail with `ORA-01086`, while releasing
that same stale handle appeared to succeed only because Oracle's unsupported
explicit release was normalized. The consuming Clutch suite retains an Oracle
container regression that requires the agent to answer `Unknown savepoint id`
for the later handle instead.

The consuming Clutch repository ran its complete Podman matrix with the locally
built candidate copied into its isolated runtime. The source and runtime jar
SHA-256 values matched. All 17 Oracle backend tests passed, including the
nested-savepoint regression. A reconstructed pre-fix candidate made the SQL
Server nested-release regression fail because a later stale handle incorrectly
returned success; the fixed candidate passed all five SQL Server backend tests.
The complete matrix passed 122 tests with 39 expected capability skips and no
unexpected result, then removed every started container.

This fix is prepared as version 0.2.20. The Clutch pin must remain unchanged
until the exact release artifact is published and its checksum is verified.
