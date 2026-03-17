# 005 — Thread execute op to prevent deadlock on JDBC lock-wait

## Background

When an Oracle session held a row-level lock (e.g. from a previous clutch
session killed uncleanly), executing an UPDATE in clutch hung indefinitely
and eventually surfaced:

```
if: clutch-jdbc-agent: timeout waiting for response to request N
```

Two independent sub-problems caused this:

**Sub-problem 1 (timeout race):** `clutch-query-timeout-seconds` and
`clutch-jdbc-rpc-timeout-seconds` were both 30s. Emacs's RPC timer races
Oracle's `setQueryTimeout`. When Emacs won, the "timeout" error was raised
but the agent was still blocked inside JDBC; the connection was left in a
partially broken state.

**Sub-problem 2 (single-threaded deadlock):** The agent's main loop was
synchronous. When `stmt.execute(sql)` blocked on a lock-wait, the entire
process was stuck. It could not read any new stdin requests — not even a
`disconnect`. The only recovery was killing the agent process.

## Decision

**Phase 1 (clutch):** Clamp `query-timeout-seconds` sent to the agent to
`min(query-timeout, max(1, rpc-timeout - 5))`. With defaults (both 30s) this
yields 25s for Oracle, 5s before Emacs's RPC timer fires.

**Phase 2 (agent):** Run `stmt.execute(sql)` in a daemon thread from a cached
thread pool (`ExecutorService executePool`). The main loop stays in
`future.get(executeTimeout + 1, ...)`. On `TimeoutException`, cancel the
future (interrupts the thread), call `stmt.cancel()` (Oracle-side), close the
statement, and return `Response.error` — main loop is never blocked again.

A `DEFAULT_EXECUTE_TIMEOUT` of 29s acts as a safety net when the client sends
no `query-timeout-seconds` (e.g. non-Oracle databases or older clutch clients).

## Rationale

The thread approach was chosen over an async/reactive pattern because:
- The agent is a bridge, not a server; one extra thread per in-flight query
  is the minimal change that unblocks the main loop.
- `ExecutorService` with daemon threads has no external dependencies and is
  trivially testable (mock statement sleeps, verify cancel/close called).
- The CLAUDE.md for this repo explicitly defers async/reactive execution as
  out-of-scope for v1; this change stays within that bound.

`stmt.setQueryTimeout` is retained as a belt-and-suspenders mechanism: Oracle
should cancel the server-side cursor first (t+25s), then the thread timeout
fires (t+26s). The 1-second gap prevents a tight race between the two.

## Alternatives considered

- **Cancel via `future.cancel(true)` only (no `stmt.cancel()`):**: Oracle JDBC
  does not reliably respond to thread interruption alone — `stmt.cancel()` is
  the supported API to abort a running query from another thread.
- **Increase only the rpc-timeout:** This would delay the Emacs-side error but
  still leave the agent stuck if Oracle never cancels (network partition, driver
  bug).
- **Kill-and-restart the agent process:** Too disruptive; the user loses all
  open cursors and connection state.

## Known limitations

- `stmt.cancel()` may itself block briefly on some drivers (Oracle thin driver
  sends a cancel RPC to the server). In pathological cases the agent thread
  stays busy longer than `executeTimeout + 1` seconds; the main loop is still
  unblocked because the future is already cancelled.
- Multiple concurrent `execute` ops on the same connection will race on the
  shared JDBC `Connection` object (Oracle connections are not thread-safe).
  This is acceptable because clutch sends at most one execute per connection at
  a time (enforced by the `busy` flag on the Emacs side).
