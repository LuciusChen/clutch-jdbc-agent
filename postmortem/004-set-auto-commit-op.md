# 004 — `set-auto-commit` Op (v0.1.7)

## Background

After adding manual-commit mode as the default for Oracle connections (v0.1.6, see
postmortem 003), users had no way to switch between manual-commit and auto-commit
after a connection was established. DataGrip offers this as a live session toggle.
The missing capability was notable: a user who connects to Oracle and decides they
want auto-commit for a one-off script had to disconnect and reconnect with different
params.

## Decision

Add a `set-auto-commit` op to the agent that calls `Connection.setAutoCommit(boolean)`.
The op returns the new auto-commit state in the response so the caller can confirm
the transition without an extra round-trip.

Agent version bumped from 0.1.6 → 0.1.7.

## Rationale

`Connection.setAutoCommit` is standard JDBC (java.sql.Connection since JDK 1.1).
Its semantics are well-defined: switching from manual→auto commits any pending
transaction; switching from auto→manual starts a new transaction on the next DML.
The op is therefore simple, safe, and valuable.

## The Boolean Cast Bug

The first implementation used `(boolean) req.params.getOrDefault("auto-commit", true)`.
This triggers an unboxing cast from `Object` to `boolean`, which fails with
`ClassCastException` when the JSON `false` value arrives as a String rather than a
`Boolean`.

### Root cause

The Elisp side sends `auto-commit=false` using `clutch-jdbc--json-false`, a sentinel
symbol created with `make-symbol`. When `json-encode` serializes this symbol, it
produces a JSON string (`"clutch-jdbc-json-false"`), not the JSON literal `false`.

The existing `connect` handler already handled this correctly using:

```java
Object autoCommitValue = req.params.get("auto-commit");
boolean autoCommit = autoCommitValue == null || Boolean.TRUE.equals(autoCommitValue);
```

This pattern treats any non-`Boolean.TRUE` value (including strings) as `false`,
which is the correct interpretation given the Elisp encoding convention.

The `setAutoCommit` handler was fixed to use the same pattern.

### Why the connect tests didn't catch this earlier

The `DispatcherTest.java` unit tests pass real `Boolean.FALSE` via `req.params.put`,
bypassing the JSON serialization path entirely. The bug only manifests in the live
path where Elisp → JSON → Java string conversion occurs.

**Lesson:** When a handler handles a boolean parameter, always use
`Boolean.TRUE.equals(value)` rather than a direct cast.

## Alternatives Considered

**Fix the Elisp encoder** to emit real JSON `false` for `clutch-jdbc--json-false`.
Rejected: the sentinel approach is an intentional design choice shared across all
boolean parameters (connecting, etc.). Changing it would require auditing every
boolean parameter site.

**Add a dedicated `setAutoCommitFalse` op** to avoid the boolean parameter entirely.
Rejected: adds protocol surface area for no benefit.

## Known Limitations

- Switching manual→auto commits the pending transaction silently at the JDBC level.
  The Elisp layer prompts for confirmation and clears the dirty-tx flag, but the
  agent itself has no visibility into whether a transaction was implicitly committed.
  This is acceptable: the JDBC spec guarantees the commit happens, and the Emacs UI
  reflects it.
