# 012 - Recover metadata without replacing the primary session

Extended by [015](015-oracle-bad-packet-recovery.md) for Oracle bad-packet classification and fail-closed schema restoration.

## Context

Each logical Clutch JDBC connection owns a primary session and an isolated
metadata session. Servers can close the metadata session after an idle period
while foreground SQL on the primary session still succeeds. Treating that as a
lost logical connection leaves schema refresh stuck and needlessly discards
transaction state.

## Decision

`ConnectionManager` retains the connection configuration and remembered schema
for each logical session. When a metadata operation fails with a recoverable or
SQLState `08` connection error, or the metadata connection fails JDBC liveness
checks, the agent replaces only that connection. `Dispatcher` restores the
schema and retries the metadata operation once.

Ordinary query errors do not trigger recovery, and foreground `execute` never
uses this path. A failed retry surfaces normally through the existing diagnostic
boundary.

## Consequences

Metadata idle timeouts no longer invalidate a healthy primary session or its
transaction. Recovery adds no pool and no silent loop: one failed metadata
request can cause at most one replacement and one retry.
