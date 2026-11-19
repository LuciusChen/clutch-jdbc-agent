# clutch-jdbc-agent

A minimal JVM sidecar process that bridges [clutch](https://github.com/LuciusChen/clutch)
(an Emacs database client) to JDBC-compatible databases.

## Requirements

- Java 17+
- Maven (to build from source)

## Build

```
mvn package
```

Produces `target/clutch-jdbc-agent-<version>.jar` with Jackson bundled. See [Driver Setup](#driver-setup) for external JDBC jars.

## Usage

The agent is not meant to be invoked manually.
`clutch-db-jdbc.el` manages the process lifecycle automatically:

```elisp
(require 'clutch-db-jdbc)
(clutch-jdbc-ensure-agent)          ; downloads the jar from GitHub Releases
(clutch-jdbc-install-driver 'oracle) ; downloads ojdbc8.jar + orai18n.jar from Maven Central
```

The [Clutch JDBC backend guide](https://github.com/LuciusChen/clutch/blob/main/docs/jdbc-backend.org) lists supported drivers and connection options.

## Protocol

One JSON object per line on stdin/stdout. The agent reserves the operating-system
stdout handle for protocol output, then redirects Java's global `System.out` to
UTF-8 stderr before loading third-party drivers. Agent logs and driver console
messages therefore remain visible on stderr without corrupting protocol framing.

### Request format

```json
{"id": 1, "op": "execute", "params": {"conn-id": 0, "sql": "SELECT 1"}}
```

Timeout-related params are explicit:

- `connect-timeout-seconds`: JDBC login/connect timeout for `connect`
- `network-timeout-seconds`: socket/network timeout for the metadata and bulk sessions; the primary session, which runs user statements, has none
- `query-timeout-seconds`: statement timeout for `execute`, `execute-params` and `fetch`. A positive value limits the statement, and the agent waits one second longer before cancelling it itself. `0` sets no limit: the statement runs until it ends, is cancelled, or its connection is force-disconnected. Omitting it keeps a 29-second limit for older clients. Negative values are rejected before JDBC work or cursor advancement.
- `validate-after-idle-seconds`: validate the primary connection before new SQL, and the metadata or bulk session before a metadata request, after this many idle seconds; omitted or zero disables the check

`execute`, `execute-params`, and `fetch` accept an optional integer
`fetch-size` from 1 through 10,000. The default is 500. Values outside that
range are rejected before JDBC work or cursor advancement.

Every protocol integer must be represented as an exact signed 32-bit integer;
fractional and overflowing JSON numbers are rejected instead of truncated.
After `Statement.cancel()` returns, execute/fetch timeout recovery waits for the
driver worker to stop within a short grace period before reusing the session.
Otherwise it retires the logical connection, or only the metadata or bulk session
for a fetch on one of those. Cancellation itself is driver-dependent and may block.

Boolean params are JSON booleans, not strings or numeric sentinels. Staged DML
uses `execute-params` with a positional JSON `values` array; the agent binds each
entry through `PreparedStatement` instead of rendering it into SQL text.
Ordinary scalar, array, and object values retain that behavior. A binary value
may instead use the reserved typed envelope:

```json
{"__clutch_jdbc_param":"binary","jdbc-type":"BLOB","base64":"eyJzdGF0dXMiOiJyZWFkeSJ9"}
```

The marker must be exactly `binary`. `jdbc-type` is the declared JDBC type from
column metadata, and `base64` carries the exact bytes. A JSON `null` `base64`
means a typed SQL `NULL`, while `""` means a non-null zero-byte value. BLOB
values with at least one byte use the standard
`PreparedStatement.setBinaryStream` data interface with an exact length; this
lets drivers direct-bind small values instead of forcing a temporary-LOB round
trip. A zero-byte BLOB uses an empty `Connection.createBlob` value because
Oracle otherwise stores an empty binary stream as SQL `NULL`. The agent frees
that `Blob` after execution, or after a timed-out worker actually exits.
RAW/BINARY-family values use `setBytes`. Typed nulls use the corresponding JDBC
null type. Malformed or unsupported envelopes fail before JDBC execution. This
is a narrow binary binding contract, not a general JDBC type-tagging or
BLOB-streaming API.
When `auto-commit=false` is requested, connection creation fails unless the
driver accepts `setAutoCommit(false)`; the agent never silently continues in
auto-commit mode.

### Response format

```json
{"id": 1, "ok": true, "result": {"type": "query", "columns": ["1"], "rows": [[1]], "done": true}}
```

An `execute` or `execute-params` result also carries `messages` when the driver attached warnings to the statement while it ran: the text of its `SQLWarning` chain, oldest first, such as SQL Server `PRINT` output or PostgreSQL notices. Warnings attached later, to a following result or while fetching, are not read. At most 100 are sent, followed by one message counting the rest. A failure to read the chain becomes one message, since the statement itself succeeded.

Error response:
```json
{"id": 1, "ok": false, "error": "ORA-01435: user does not exist", "diag": {"category": "metadata", "op": "set-current-schema", "request-id": 1, "context": {"schema": "APP", "generated-sql": "ALTER SESSION SET CURRENT_SCHEMA = \"APP\""}}}
```

`diag` is optional and is intended for troubleshooting, not for the default
user-facing error string.  It may include request metadata such as category,
connection id, exception class, SQLState, vendor code, cause chain, and
redacted request context.  When a hidden/internal query path fails,
`diag.context.generated-sql` carries the actual SQL text that the agent ran.

Malformed SQL parameters are rejected before JDBC access with a protocol diagnostic. Non-string SQL is omitted from diagnostic context rather than converted to text; valid SQL contributes its length, not its contents. The same rule applies to the opt-in debug context.

`diag.connection-invalidated=true` is also a stable lifecycle signal. It is present when an error response references a local logical connection that the agent no longer owns, including a follow-up request after the original failure response was ignored. The agent derives this from its connection map, not exception text or client-side JDBC error rules. Metadata-session failure alone does not set the marker. Foreground SQL is never replayed automatically because its transaction outcome may be unknown.

When idle preflight conclusively rejects a primary connection before the agent creates a `Statement` or `PreparedStatement`, the same response also includes `diag.execution-not-started=true`. This is evidence only about the current SQL request; it does not say that an earlier manual transaction can be recovered, and the agent still performs no automatic reconnect or SQL replay. The preflight uses JDBC `Connection.isValid`; the agent does not execute database-specific validation SQL or call `setAutoCommit`, `commit`, or `rollback` as part of that check.

When the client sends `params.debug=true`, failures may also include an
additional optional `debug` payload.  That payload is still redacted and is
meant for opt-in troubleshooting only; today it carries the redacted request
context and a redacted Java stack trace.

### Operations

See the canonical [operation inventory and session contract](https://github.com/LuciusChen/clutch/blob/main/docs/jdbc-agent-protocol.md#core-operations).
This includes lock-bypassing `force-disconnect` for retiring an unresponsive
logical connection without blocking on driver cleanup.

## Type Conversion

JDBC column values are converted to a stable set of JSON-safe types:

| JDBC type                   | JSON output                                   |
|-----------------------------|-----------------------------------------------|
| `null` / `wasNull()`        | `null`                                        |
| `Boolean`                   | `true` / `false`                              |
| `Integer`, `Long`, `Short`, `Byte` | JSON number                          |
| `Double`, `Float`           | JSON number (NaN/Infinity → string)           |
| `BigDecimal`                | String (preserves precision)                  |
| `Timestamp`                 | Local wall-clock string (`2024-01-15 13:45:30`) |
| `Date`                      | Date string (`2024-01-15`)                    |
| `Time`                      | Time string (`13:45:30`)                      |
| `Clob`                      | `{"__type":"clob","length":N,"preview":"..."}`; at most 256 UTF-16 units, without splitting surrogate pairs; original length retained |
| `Blob` / `byte[]` / `ByteBuffer` | `{"__type":"blob","length":N,"preview-base64":"..."}` with at most 256 leading bytes, plus complete `text` and `encoding` for UTF-8/GB18030 JSON/XML up to 64 KiB. Larger JDBC `Blob` locators carry only the length to avoid extra reads. `ByteBuffer` is read from its current position without moving it. |
| String / textual fallback   | String, limited to 1,048,576 characters per cell |
| Anything else               | `rs.getString(col)` fallback                  |

## Architecture

```
clutch-db-jdbc.el (Emacs)
        │  stdin/stdout (JSON, one object per line)
        ▼
clutch-jdbc-agent (JVM process)
  ├── Agent.java          — main(), stdin loop, driver loading
  ├── ConnectionManager   — connId → primary + metadata JDBC sessions, and a bulk session on Oracle
  ├── CursorManager       — cursorId → (Statement, ResultSet)
  ├── TypeConverter       — JDBC value → JSON-safe Java object
  └── handler/
      ├── Dispatcher      — request routing and execution lifecycle
      ├── MetadataOps     — metadata and dialect introspection
      └── DispatcherDiagnostics — error diagnostics and redaction
        │
        ▼
  drivers/*.jar           — JDBC drivers (loaded at runtime via URLClassLoader)
```

The dispatcher uses separate locks for foreground SQL and metadata. Oracle also gets a session for schema-wide listings. See the [runtime model](https://github.com/LuciusChen/clutch/blob/main/docs/jdbc-agent-protocol.md#runtime-model) for lock ordering, session recovery and cancellation.

Savepoint operations are direct translations of the standard JDBC
`Connection` API. `create-savepoint` rejects auto-commit sessions and checks
`DatabaseMetaData.supportsSavepoints()` before creating any savepoint.
Savepoint ids are opaque agent-local handles. `ConnectionManager` owns both
the JDBC objects and their lifecycle: successful commit, rollback, or
autocommit transition invalidates them under the same session lock, while a
failed outer boundary does not proactively clear them while the session remains
live. A rollback-to-savepoint recovery consumes its handle even on failure
because its partial outcome is unknown. Once rollback to a savepoint succeeds,
every handle created after it is invalid too; the database has discarded those
later savepoints even when explicit release is unsupported. A successful or
logically normalized explicit release likewise invalidates its target and every
later handle, matching the JDBC contract. Disconnect or fatal session
invalidation removes every remaining handle. The agent does not interpret SQL
or decide when a client workflow should create a savepoint.

Some drivers, including Oracle JDBC, support savepoints but not the optional
`Connection.releaseSavepoint` operation. After a successful rollback-to-savepoint
or successful batch body, that specific unsupported cleanup is treated as a
logical release: the agent forgets its opaque handle and the database discards
the physical savepoint at the enclosing commit or rollback. Other release
errors still propagate because their transaction outcome is not known.

## Driver Setup

Drivers are loaded from `drivers/` next to `clutch-jdbc-agent.jar` at startup.
Each jar is scanned via `ServiceLoader<java.sql.Driver>`.

## License

GPL-3.0-or-later. See [LICENSE](LICENSE).
