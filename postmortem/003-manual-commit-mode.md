# 003 — Manual commit mode enters the JDBC agent protocol

## Background

The original JDBC agent deliberately stopped at auto-commit execution:
open a connection, execute SQL, stream rows, and leave transaction policy to
the JDBC default (`autoCommit=true`).

That was acceptable for early bring-up, but it diverged from real Oracle query
console expectations.  Oracle DBAs typically expect ad-hoc DML to remain
pending until an explicit `COMMIT` or `ROLLBACK`.

## Decision

Extend the JSON protocol in three small ways:

- `connect` accepts a boolean `auto-commit` parameter, defaulting to `true`
- new `commit` op commits a live connection by `conn-id`
- new `rollback` op rolls back a live connection by `conn-id`

The agent still does not implement transaction orchestration.  It only exposes
the minimal JDBC primitives needed by the Emacs UI.

## Why Here

Manual-commit mode cannot be emulated safely in Emacs alone.  Once JDBC has
already auto-committed a statement, the UI cannot reconstruct the prior
transaction boundary.

The correct control point is connection creation:

- `Connection#setAutoCommit(false)` when requested
- explicit `Connection#commit()` / `Connection#rollback()` on demand

## Scope Control

This does **not** add:

- savepoints
- transaction nesting
- isolation-level controls
- long-lived transaction state tracking inside the agent

The agent still owns only JDBC lifecycle and direct protocol translation.
Dirty-state UI and disconnect warnings remain in `clutch.el`.

## Release Note

This is a protocol change.  The published jar version and the consuming
`clutch-jdbc-agent-version` / checksum in `clutch` must be updated together
when the new release artifact is actually published.
