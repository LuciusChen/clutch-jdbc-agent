# 007 — JDBC cancel op and request-level concurrency

## Background

Postmortem 005 already changed the agent so one stuck `execute` no longer
wedges the whole process. That solved the deadlock/timeout problem, but it did
not yet define a recoverable user interrupt path.

`clutch` still treated raw `quit` during JDBC execution as an unrecoverable
path and disconnected the session. That was safe for protocol cleanup, but it
was the wrong UX for query-console work: interrupting one slow statement should
not force the user to throw away the whole connection and reconnect before the
next SQL.

By this point the agent could accept work off the stdin loop, but there was
still no formal control-plane operation saying "cancel the currently running
statement on this connection". An Elisp-only workaround would have remained a
timing heuristic rather than a protocol guarantee.

This repo's `CLAUDE.md` previously deferred cancel/interrupt support for v1.
That deferral stopped being tenable once query-console interruption became a
real user-facing correctness issue rather than a hypothetical feature request.

## Relationship to earlier records

- **005** explains why `execute` had to leave the old synchronous main loop.
  This record builds on that foundation and defines the next step: once the
  agent can accept a second request, make that request an explicit `cancel`
  instead of continuing to model interrupt as disconnect.
- **006** is orthogonal. It covers Oracle object-discovery filtering, not query
  execution or control-plane behavior.

## Decision

Add a formal `cancel` RPC and make the agent able to accept it while another
request is already running.

Concretely:

- the stdin loop now hands decoded requests to a request pool instead of
  synchronously running each request inline
- the dispatcher serializes most work per `conn-id` with a per-connection lock
- `cancel` bypasses that lock, finds the currently running `Statement` for the
  target connection, and calls `Statement.cancel()`
- running statements are tracked by `conn-id` so both `execute` and `fetch`
  can be cancelled
- `cancel` returns explicit protocol state (`cancelled: true/false`, plus
  `request-id` when a statement was actually found)

This keeps the semantic model simple: one foreground JDBC operation at a time
per connection, but a control-plane cancel can still cut in.

## Rationale

This approach is the smallest change that solves the real failure mode.

- It preserves the v1 design constraint that one JDBC connection should not
  execute multiple foreground statements concurrently.
- It avoids a larger async/reactive redesign. The agent is still a local bridge,
  not a general-purpose database server.
- It gives the Elisp side an explicit protocol guarantee instead of a timing
  heuristic. `quit` can now map to "send cancel" rather than "hope the running
  request unwinds before we fall back to disconnect".
- It keeps the concurrency model narrower than "fully parallel agent". Request
  decoding can proceed concurrently, but normal work is still serialized per
  connection and only the cancel path bypasses that lock.
- Tracking the running `Statement` by `conn-id` is enough for clutch's model,
  because clutch already treats a connection as having at most one in-flight
  foreground request.

## Alternatives considered

- **Keep the old disconnect-on-quit behavior**: operationally safe, but it
  preserves the bad UX this change exists to fix.
- **Handle cancel only in Elisp without a protocol op**: unreliable. The client
  cannot force cancellation if the agent cannot accept a second request while
  one request is stuck inside JDBC.
- **Allow full concurrent execution on one JDBC connection**: not safe. JDBC
  connections and statements are not generally thread-safe in that way, and
  clutch does not need that capability.
- **Kill and restart the whole agent on interrupt**: too destructive. It drops
  all JDBC connections and open cursors, not just the one running statement.

## Known limitations

- `Statement.cancel()` is still driver-dependent. Some drivers may report
  cancellation via vendor-specific messages such as `ORA-01013`.
- `cancel` is best-effort for the currently running statement only. It does not
  provide arbitrary out-of-band transaction management.
- Most control ops still respect the per-connection lock. If a driver wedges so
  badly that cancel cannot complete, later work on that same connection still
  has to fail or be abandoned by the client.
