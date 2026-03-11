# clutch-jdbc-agent Development Guide

Java best practices for a thin JDBC ↔ JSON bridge. The goal is a minimal,
debuggable sidecar — not a general-purpose database framework.

## First Principles

- **Question every abstraction**: Before adding a class, interface, or layer, ask
  "is this solving a real problem right now?" If the answer is hypothetical, don't add it.
- **Simplify relentlessly**: Three similar methods are better than a premature
  abstraction. A single readable class is better than a hierarchy of one.
- **This is a bridge, not a backend**: All business logic (pagination UI, schema
  caching, relation graphs, query history) lives in Emacs. The agent only converts
  JDBC calls to JSON and back. If a feature belongs in Emacs, put it in Emacs.
- **Delete, don't deprecate**: If something is unused, remove it entirely.
  No backward-compatibility shims, no `@Deprecated` stubs left in place.
- **Prefer boring code**: A straightforward `if/else` chain is easier to debug
  over a network than a clever polymorphic dispatch hierarchy.

## Architecture Boundaries

The agent is responsible for exactly:
- JVM startup and driver loading
- JDBC connection lifecycle (open / close)
- SQL execution and result streaming via cursors
- `DatabaseMetaData` queries for schema introspection
- Type conversion: JDBC → JSON-safe Java types

The agent must NOT contain:
- Pagination logic (page numbers, offsets — that is Emacs's job)
- SQL rewriting or analysis
- Schema caching (Emacs caches; agent always queries `DatabaseMetaData` fresh)
- Connection profiles or credential storage
- UI concepts of any kind

## Package Structure

```
clutch.jdbc
  Agent.java             ← main(), process loop, driver loading
  ConnectionManager.java ← connId → Connection map
  CursorManager.java     ← cursorId → (Statement, ResultSet), fetch pagination
  DriverLoader.java      ← scan drivers/, URLClassLoader + ServiceLoader
  DriverShim.java        ← wrap external Driver for DriverManager acceptance
  TypeConverter.java     ← JDBC column value → JSON-safe object

clutch.jdbc.handler
  Dispatcher.java        ← route op strings to handler methods

clutch.jdbc.model
  Request.java           ← {"id", "op", "params"}
  Response.java          ← {"id", "ok", "result" / "error"}
```

Only split a class when it has a genuinely distinct responsibility. Do not create
`XxxService`, `XxxFactory`, or `XxxHelper` wrappers for the sake of pattern-following.

## Naming

- **Classes**: `PascalCase`. Name them after what they *are*, not what they *do*
  (`CursorManager`, not `CursorManagementService`).
- **Methods**: `camelCase`. Name them after what they *return or produce*
  (`getSchemas`, `fetch`, `connect`), not implementation details.
- **Private fields**: `camelCase`, no Hungarian prefix.
- **Constants**: `UPPER_SNAKE_CASE`.
- **Package-private**: prefer over `public` when a class is not part of the
  external API. There is no external API — everything is internal.

## Protocol Rules

- **stdout strictly for protocol**: never `System.out.println` a log message.
  All logging goes to `System.err` (or `System.Logger`).
- **One JSON object per line**: no pretty-printing, no multi-line messages.
- **Every request gets exactly one response**: no partial responses, no streaming
  mid-message. The cursor model handles large results via `fetch`.
- **Error responses are not exceptions**: return `Response.error(id, msg)` for
  expected failure modes (bad SQL, unknown conn-id, etc.). Let uncaught exceptions
  bubble to the Agent-level catch block which also returns an error response.
- **id=-1 for unparseable requests**: if JSON parsing fails, respond with id=-1
  so Emacs can distinguish protocol errors from request errors.

## Control Flow

- Prefer flat, linear control flow. Avoid deep nesting — extract a helper method
  rather than adding another indent level.
- Use `switch` expressions (Java 21) for op dispatch and type branching.
  Pattern matching in `switch` is clear and exhaustive.
- Use `record` for simple data carriers (`FetchResult`). Do not add behavior to
  records beyond accessor methods.
- Avoid checked exceptions leaking across layer boundaries. Handler methods declare
  `throws Exception`; the Dispatcher catches and converts to `Response.error`.

## Error Handling

- **Protocol errors** (bad JSON, unknown op, missing param): return `Response.error`,
  never crash the process.
- **JDBC errors** (`SQLException`): return `Response.error` with
  `e.getMessage()`. Do not expose stack traces to Emacs.
- **Resource cleanup**: always use try-with-resources for `ResultSet`, `Statement`,
  `Connection` when the scope is local. For long-lived resources (cursor lifecycle),
  ensure `close()` is called in `finally` or on shutdown.
- **Shutdown**: on stdin EOF, call `connMgr.disconnectAll()` before exiting.
  Do not leave JDBC connections open.
- Error messages should state what is wrong: `"Unknown connection id: 5"`,
  not `"Connection operation failed"`.

## State Management

There are exactly two stateful components:

- `ConnectionManager`: `ConcurrentHashMap<Integer, Connection>`. Each `connect`
  call gets an auto-incremented integer id. No pooling. One JDBC `Connection`
  per clutch connection.
- `CursorManager`: `ConcurrentHashMap<Integer, Cursor>`. Each `execute` that
  returns a `ResultSet` gets a cursor id. The `ResultSet` stays open until
  `fetch` returns `done=true` or `close-cursor` is called explicitly.

No other global state. No singletons beyond these two managers.

## Driver Loading

- Drivers live in `drivers/` next to the jar — never embedded in the fat jar.
- Use `URLClassLoader` + `ServiceLoader<java.sql.Driver>` to discover drivers.
- Always wrap loaded drivers in `DriverShim` before calling
  `DriverManager.registerDriver()`. Without the shim, `DriverManager` rejects
  drivers whose classloader is not an ancestor of the system classloader.
- Log loaded driver class names to stderr for debuggability.
- Do not fail hard if `drivers/` is empty — many users only need one database.

## Type Conversion

Rules for `TypeConverter.convert()`:

- `null` / `wasNull()` → JSON `null`
- `Boolean` → JSON boolean
- `Integer`, `Long`, `Short`, `Byte` → JSON number
- `Double`, `Float` → JSON number, **but** NaN and Infinity → JSON string
  (NaN/Inf are not valid JSON)
- `BigDecimal` → **String** (use `toPlainString()`). Preserves precision;
  avoids JavaScript float rounding on the Emacs side.
- `Timestamp`, `Date`, `Time` → ISO-8601 String via `toInstant()` /
  `toLocalDate()` / `toLocalTime()`. Oracle `DATE` has a time component —
  always use `getTimestamp()`, never `getDate()`, for Oracle columns.
- `Clob` → `{"__type":"clob","length":N,"preview":"..."}` (first 256 chars)
- `Blob`, `byte[]` → `{"__type":"blob","length":N}`
- Anything else → `rs.getString(col)` fallback

Stability over perfection. The Emacs side (`clutch-db-format-temporal`) handles
ISO-8601 strings natively.

## Method Design

- Keep methods under ~30 lines. Extract a private helper when a method exceeds this.
- Name helpers after what they compute, not where they're called from.
- Handler methods in `Dispatcher` follow a consistent pattern:
  1. Extract params (call `getInt` / `getString` helpers — throw on missing)
  2. Delegate to manager(s)
  3. Build and return `Response.ok(...)`
- Pure computation (type conversion, metadata parsing) must be separate from
  I/O (reading `ResultSet`, writing response).

## Pre-Submit Review

Before committing significant changes, review the whole diff:

- **No heuristic shortcuts**: if a fix feels "good enough for now", document why
  it is deferred. Don't leave silent partial implementations.
- **No redundancy**: remove duplicated logic or dead code introduced by the change.
- **Protocol stability**: any change to request/response field names or semantics
  is a breaking change for the Emacs side. Coordinate with `clutch-db-jdbc.el`.
- **No stdout pollution**: `grep 'System.out' src/` must return zero results
  outside `Agent.java`'s protocol writes.
- **Compile clean**: `mvn package` must produce zero warnings.

## Quality Checks

Before releasing:
- `mvn package` produces no warnings.
- All `public` methods and classes have Javadoc.
- `System.out` is used **only** in `Agent.java` for protocol output.
- All `System.err` / logger calls use structured messages (no string concatenation
  in hot paths).
- Smoke test: `echo '{"id":1,"op":"ping","params":{}}' | java -jar target/clutch-jdbc-agent-*.jar`
  must print `{"id":1,"ok":true,"result":{"pong":true}}`.

## Postmortems

The `postmortem/` directory contains design decision records. **Read them before
making significant changes.**

Each file is named `NNN-topic.md` and records: background, decision, rationale,
alternatives considered, and known limitations.

**Write a postmortem when:**
- Changing the protocol (field names, new ops, error semantics)
- Choosing between non-obvious implementation approaches
- Adding a new driver-loading strategy or classloader trick
- Reverting or abandoning an approach — especially document *why* it was wrong
- Discovering a limitation that is deliberately deferred (e.g. CLOB streaming)

**What to write:** focus on *why*, not *what*. The code already shows what was
done. A record that only restates the code adds no value.

## What NOT to Build (v1)

Explicitly deferred — do not add these without a postmortem justifying the need:

- Connection pooling (HikariCP, c3p0, etc.)
- Async/reactive execution (CompletableFuture, Project Reactor)
- Full JSON-RPC framing (jsonrpc id types, batch requests, notifications)
- SQL parsing or query analysis
- Schema caching inside the agent
- Cancel/interrupt support for running queries
- CLOB/BLOB full content streaming (placeholders are sufficient for v1)
- Transaction management beyond autocommit
- Multiple result sets from a single execute (stored procedures)
- A separate configuration file for the agent
