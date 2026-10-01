# 030 — Let a client ask for no statement timeout

## Evidence

Clutch now runs interactive statements without blocking Emacs, where `C-g` cancels them, and wants them to run until they finish. The agent could not express that. An omitted or non-positive `query-timeout-seconds` fell back to the 29-second `DEFAULT_EXECUTE_TIMEOUT`, which set `Statement.setQueryTimeout`, bounded the wait for the statement and for its first batch, and then cancelled it. The primary session also carried `network-timeout-seconds`, so a statement that kept its socket silent past that bound failed, usually taking the connection with it.

## Decision

- An explicit `query-timeout-seconds` of 0 means no limit for `execute`, `execute-params` and `fetch`: `setQueryTimeout(0)`, and the agent waits for the statement and its batches until they end. Cancel and force-disconnect end such a wait early. Omission keeps 29 seconds, so older clients keep their safety net, and a negative value is rejected before JDBC work or cursor advancement instead of silently becoming that default.
- `network-timeout-seconds` applies to the metadata and bulk sessions only. The primary session runs user statements, whose limit is the statement timeout their request asks for.

No deadline, executor or scheduling rule was added; the waits that keep a positive timeout are unchanged.

## Limits

- Clutch keeps sending a positive timeout for requests it waits on, within its own RPC timeout; only statements it runs without blocking ask for none.
- A statement without a limit holds its request thread, one of the 16 execution slots and any database locks until it ends. Postmortem 028's open debt now has no deadline behind it: a driver that ignores both cancel and close keeps those threads until the agent restarts. Force-disconnect still removes the logical connection at once.
- Without a network timeout, a primary session whose peer vanished mid-statement waits for the statement's own timeout, a cancel or a force-disconnect. Idle validation before new SQL still catches a connection dropped while idle.

## Verification

Dispatcher tests cover the explicit 0 reaching `setQueryTimeout`, negative values on `execute`, `execute-params` and `fetch`, and cancellation of a running execute and fetch with and without a limit; a ConnectionManager test checks that only the metadata session gets the network timeout. The release notes list the live workflows run against the published jar.
