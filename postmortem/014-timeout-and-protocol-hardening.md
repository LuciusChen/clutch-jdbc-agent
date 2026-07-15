# 014 — Timeout and protocol hardening for 0.2.9

## Context

The bounded row count introduced in 0.2.8 did not prove that a timed-out JDBC
worker had actually stopped. Some drivers ignore thread interruption and
`Statement.cancel()`. Releasing the foreground lock in that state allowed the
next request to use the same non-thread-safe `Connection` concurrently.

Metadata used a separate JDBC connection, but it had no separate lock. Schema
changes, metadata recovery, metadata cursors, and disconnect could therefore
race on that session. Diagnostics also understood only query parameters and
SQL Server properties, leaving DB2 and legacy Oracle credential URLs exposed.

## Decision

- Execute and fetch workers count down a completion latch in `finally`. After a
  timeout, the dispatcher interrupts and cancels the statement, then waits 250
  ms for actual worker completion. If the latch is still open, it removes the
  logical connection and abandons its cursor registrations before returning;
  JDBC resource close then runs on the existing daemon executor so a broken
  driver cannot delay invalidation.
- Foreground and metadata work use independent locks per logical connection.
  Operations touching both sessions acquire foreground first, then metadata.
  Oracle metadata cursors retain their metadata-session identity so later
  fetch and close operations use the correct lock.
- Primary `execute` no longer performs an unconditional `Connection.isValid`
  round trip. `SQLRecoverableException` and SQLState class `08` failures close
  the logical connection instead.
- Protocol integers must be exact signed 32-bit values. JDBC URL diagnostics
  redact secret assignments after `?`, `&`, `;`, or DB2-style `:`, plus legacy
  Oracle `user/password@...` credentials.
- Binary values above 64 KiB remain placeholders without text decoding, and a
  textual response cell above 1,048,576 characters fails closed before JSON
  serialization.
- Oracle table discovery closes an already-registered metadata cursor when its
  initial fetch fails.

## Scope

This adds no scheduler, connection pool, retry loop, or response-streaming
protocol. The two lock lanes mirror the two existing JDBC sessions. Total
response-byte budgeting remains separate from the per-cell safety bound.
