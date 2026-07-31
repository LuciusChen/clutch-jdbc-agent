# 021 — Savepoint Primitives for Atomic Manual Submissions

## Background

Clutch stages several result-grid mutations locally and submits them as one
confirmed batch. In JDBC manual-commit mode, executing those statements as
independent RPCs let an early statement remain in the user's transaction when
a later statement failed. Clutch retained the complete local batch, so retrying
could execute that early statement twice.

## Decision

Expose three direct JDBC primitives:

- `create-savepoint`, returning an opaque agent-local `savepoint-id`
- `rollback-savepoint`, which rolls back to and releases that savepoint
- `release-savepoint`, which releases it after successful work

`ConnectionManager` owns the `Savepoint` objects because they are session
resources that cannot be reconstructed from SQL names. Creation fails before
any staged DML when the primary connection is in auto-commit mode or
`DatabaseMetaData.supportsSavepoints()` is false. `ConnectionManager` owns the
transaction calls as well as the handles: a successful commit, rollback, or
auto-commit mode change invalidates every handle under the same session lock.
Dispatcher only routes those operations and cannot clear lifecycle state
independently. A failed outer boundary does not proactively clear the handles
while the session remains live; disconnect or fatal invalidation removes them
with the session. A `rollback-savepoint` attempt consumes that specific handle
even if rollback or release fails, because the agent cannot safely reuse a
partially completed recovery.

The Emacs client still owns transaction workflow policy and statement order.
The agent neither accepts a batch of SQL nor chooses when to create a
savepoint.

## Rationale

Emitting `SAVEPOINT` SQL in Emacs would duplicate database dialect behavior and
would not work uniformly across JDBC drivers. Recording per-statement
submission state in the UI would leak partial database execution into the
staged-edit model and make retry semantics substantially larger. Standard JDBC
savepoints preserve the existing all-or-nothing batch model while leaving
earlier user work in the outer transaction untouched.

Opaque ids keep JDBC objects inside their lifecycle owner. A client cannot
forge a JDBC `Savepoint`, and stale ids fail explicitly instead of referring to
another transaction.

## Failure Semantics

If rollback-to-and-release recovery reports a failure, the client cannot know
which JDBC step completed and the outer transaction's contents are uncertain.
The agent surfaces that JDBC error normally; it does not commit, silently roll
back the whole user transaction, or replay SQL. The client must require an
explicit outer rollback or reconnect before further work.

A commit error is also not evidence that the commit failed before reaching the
server. The client must treat that outcome as unknown and must not use a later
rollback response as proof that the transaction did not commit.

## Release Note

This is a protocol addition for 0.2.18. The published jar and Clutch's version
and checksum pin must be updated together when the release asset is published.
