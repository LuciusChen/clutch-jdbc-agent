# 017 — Validate idle primary sessions before user SQL

## Context

A database, proxy, firewall, or suspended workstation can leave a logical JDBC connection pointing at a dead primary session. Discovering that only while creating or executing the next statement makes the outcome less clear: depending on the driver and failure point, the server may already have received user SQL.

## Decision

The `connect` operation accepts `validate-after-idle-seconds`. Omission or zero disables the policy. Each logical session records primary foreground activity with a wall clock so workstation sleep counts as idle; metadata-only activity does not refresh that timestamp. Once the configured interval is reached, `execute` and `execute-params` call the primary connection's standard `isValid(3)` before obtaining or creating any statement. Unsupported validation capability is skipped without a vendor-specific fallback.

If validation returns false or fails, the agent removes that logical connection before responding. The structured diagnostics include both `connection-invalidated=true` and `execution-not-started=true`. The latter is emitted only while the dispatcher is in the narrowly scoped preflight phase and the authoritative connection map confirms removal. That phase ends before `createStatement` or `prepareStatement`, so a later failure cannot inherit the safety fact through thread reuse.

## Rationale

The policy avoids a validation round trip on the normal hot path while giving the client an authoritative distinction when an idle connection is known dead before user SQL begins. JDBC `Connection.isValid` provides the database-neutral contract. The agent does not execute database-specific validation SQL or call `setAutoCommit`, `commit`, or `rollback` during preflight; the driver's internal implementation of `isValid` remains driver-defined. Session-owned time state disappears with the logical connection and cannot leak across ids.

## Consequences

The current SQL request can be treated differently from a failure after statement creation, but no prior transaction state is implied to be recoverable. Other logical connections in the shared agent are unaffected. A supported validation call adds one bounded driver round trip only after the configured idle interval.

## Deliberate limit

There remains an unavoidable race between successful validation and later statement work. Any failure after preflight is handled by the existing conservative invalidation rules and never carries `execution-not-started`; the agent does not reconnect or replay SQL.
