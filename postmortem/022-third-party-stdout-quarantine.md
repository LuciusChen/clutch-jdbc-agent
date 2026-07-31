# 022 — Quarantine Third-Party Stdout From the JSON Protocol

## Context

The agent reserves stdout for one JSON response per line, but that rule originally governed only agent-owned code. Snowflake's external-browser authenticator prints a login-status message through `System.out`, so Clutch attempted to parse human text as JSON and killed the agent before browser authentication could finish. Any JDBC driver with similar console output could corrupt framing.

Agent 0.2.17 made protocol writes faster and more reliable by writing directly to `FileDescriptor.out`. That did not isolate the channel: Java's global `System.out` still referenced the same operating-system file descriptor, so protocol bytes and driver text continued to share one pipe.

## Decision

`Agent.main` opens its dedicated buffered protocol stream from `FileDescriptor.out` before any third-party code loads. It then redirects Java's global `System.out` to a UTF-8 `PrintStream` over `System.err` before resolving or loading driver jars.

The ordering is part of the boundary. Redirecting only before `connect` would miss driver static initializers and `ServiceLoader` constructors; redirecting after the ready message would already allow non-JSON startup output. The process owns the global stream for its lifetime, so it does not restore the original `System.out`.

Clutch already captures agent stderr separately. Driver prompts and status text remain available in `*clutch-jdbc-agent-stderr*` without entering the JSON parser.

## Verification

A real subprocess test packages a service-loaded test driver whose static initializer writes to global stdout. It launches `Agent.main`, sends a ping, and requires stdout to contain exactly the parseable ready and ping JSON lines while the driver's marker appears only on stderr. A `serve`-level test would not prove the global stream setup or its ordering relative to driver loading.

## Rejected Alternatives

- Filtering non-JSON stdout lines in Clutch would hide genuine protocol corruption and could discard driver text that happens to resemble JSON.
- Special-casing Snowflake would leave every other noisy driver exposed.
- Passing a logging callback to drivers is not a general JDBC capability and cannot control unconditional `System.out` calls.

Native code that writes directly to file descriptor 1, or a launcher that merges stderr back into stdout, remains outside this Java-level quarantine. Clutch starts the agent with separate stdout and stderr pipes, and the reported JDBC path uses Java `System.out`.

## Release Note

This changes the published jar without changing request or response fields, so the fix is prepared for agent 0.2.19. After that asset is published, Clutch must update its pinned agent version and checksum from the downloaded release jar.
