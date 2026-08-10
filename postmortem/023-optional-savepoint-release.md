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
asserts that each opaque handle is consumed. Existing tests continue to require
ordinary release failures to propagate and failed rollback handles to become
unusable.

The consuming Clutch repository must also run its Oracle container-backed UI
and JDBC backend live suites with the patched agent before updating its pinned
agent version and checksum.

This fix is prepared as version 0.2.20. The Clutch pin must remain unchanged
until the exact release artifact is published and its checksum is verified.
