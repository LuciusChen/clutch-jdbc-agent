# 007 — JDBC Cancel Needs a First-Class Protocol Op

## Context

The original agent serialized every request through one dispatcher thread.
That was simple, but it made one user-visible path fundamentally wrong:

- Emacs sends `C-g` while a JDBC query is still running
- Elisp can notice `quit`
- the JVM side cannot receive a second RPC until the first one finishes

That meant "interrupt query" could only degrade into:

- wait for the driver to finish on its own, or
- tear the whole connection down from the client side

The latter kept surfacing as `quit -> disconnect`, even when the database and
driver could have cancelled the current `Statement` cleanly.

## Decision

Add a first-class `cancel` RPC and keep the agent's main loop responsive while
SQL is running.

Implementation shape:

- stdin parsing stays single-threaded and ordered
- request bodies execute on a worker pool
- foreground SQL tracks the currently running `Statement` by `conn-id`
- `cancel` looks up that running statement and calls `Statement.cancel()`
- the cancel response reports whether a live statement was actually cancelled

This keeps protocol ownership where it belongs: Emacs asks for cancellation,
the JVM/JDBC layer performs real JDBC cancellation.

## Why this boundary

Only the agent has the object that matters here: the live JDBC `Statement`.

Trying to solve this purely in Elisp was structurally unstable:

- raw `quit` still happens outside carefully wrapped call sites
- different drivers react differently to thread interruption alone
- client-side disconnect is a much stronger action than query cancel

If the desired behavior is "stop this SQL but keep the session", the protocol
must have an op that says exactly that.

## Consequences

- `execute` and `fetch` can now be interrupted without blocking the entire
  agent's control plane
- a cancelled request still settles its own response later, so the client must
  ignore that late response if it has already moved on
- `disconnect` still cancels any running statement before closing the JDBC
  connection, because a wedged statement should not outlive connection teardown

## Alternatives considered

- **Keep one-thread dispatch and rely on Elisp-side disconnect**
  Rejected because it preserves the broken UX: `C-g` drops the whole session.

- **Use thread interruption only**
  Rejected because many JDBC drivers do not reliably translate thread interrupt
  into server-side SQL cancellation.

- **Track running work globally rather than by `conn-id`**
  Rejected because the user-facing operation is scoped to one clutch
  connection, not "whatever statement happened to start last".

## Known limits

- `cancel` is best-effort and depends on driver/database support for
  `Statement.cancel()`.
- If the agent is already gone, the socket is broken, or the JDBC session is
  corrupt, the client must still fall back to disconnect/reconnect.
