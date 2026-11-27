# clutch-jdbc-agent

A small JVM sidecar that bridges [clutch](https://github.com/LuciusChen/clutch) (an Emacs database client) to JDBC databases.

## Requirements

- Java 17+
- Maven (to build from source)

## Build

```sh
mvn package
```

Produces `target/clutch-jdbc-agent-<version>.jar` with Jackson bundled. See [Driver Setup](#driver-setup) for external JDBC jars.

## Usage

`clutch-db-jdbc.el` starts and manages the agent automatically; you do not need to run it by hand:

```elisp
(require 'clutch-db-jdbc)
(clutch-jdbc-ensure-agent)          ; downloads the jar from GitHub Releases
(clutch-jdbc-install-driver 'oracle) ; downloads ojdbc8.jar + orai18n.jar from Maven Central
```

See the [Clutch JDBC backend guide](https://github.com/LuciusChen/clutch/blob/main/docs/jdbc-backend.md) for supported drivers and connection options.

## Protocol

One JSON object per line over stdin/stdout. Before loading drivers, the agent saves the raw stdout handle for JSON replies and redirects Java's `System.out` to UTF-8 stderr so driver console output stays visible in diagnostics without breaking framing.

### Request format

```json
{"id": 1, "op": "execute", "params": {"conn-id": 0, "sql": "SELECT 1"}}
```

Timeout parameters:

- `connect-timeout-seconds`: JDBC login timeout on `connect`.
- `network-timeout-seconds`: socket timeout for `metadata` and `bulk` sessions (`primary` has no socket timeout).
- `query-timeout-seconds`: statement timeout for `execute`, `execute-params`, and `fetch`. Positive values limit the statement with +1s grace before agent-side cancellation; `0` means no limit; omitting keeps a 29s default for older clients; negative values are rejected.
- `validate-after-idle-seconds`: runs `Connection.isValid` before new SQL on `primary` or metadata requests on `metadata`/`bulk` after this many idle seconds (`0` or omitted disables).

Parameter and execution rules:

- `fetch-size` (optional on `execute`, `execute-params`, `fetch`): integer 1–10,000, default 500. All protocol integers must fit in a signed 32-bit integer (floats and overflows are rejected). Booleans must be JSON booleans.
- Oracle metadata sessions enable `useFetchSizeWithLongColumn` by default so column defaults fetch in batches (an explicit driver property overrides it).
- When `auto-commit=false` is requested, `connect` fails unless the driver accepts `setAutoCommit(false)`.
- After `Statement.cancel()`, timeout recovery waits briefly for the driver worker to stop before reusing the session; otherwise it retires the logical connection (or only the `metadata`/`bulk` session for a metadata fetch).
- `execute-params` binds a positional JSON `values` array via `PreparedStatement`. Binary values use the typed envelope `{"__clutch_jdbc_param":"binary","jdbc-type":"BLOB","base64":"..."}`: `base64: null` binds typed SQL `NULL`, `""` binds a zero-byte `Connection.createBlob` (so Oracle does not turn an empty stream into `NULL`, freed after execution), non-empty BLOBs bind via `setBinaryStream` with exact length, and RAW/BINARY types bind via `setBytes`. Malformed envelopes fail before execution.

### Response format

```json
{"id": 1, "ok": true, "result": {"type": "query", "columns": ["1"], "rows": [[1]], "done": true}}
```

When a driver attaches `SQLWarning` entries to an `execute` or `execute-params` statement (such as SQL Server `PRINT` or PostgreSQL notices), the result includes `messages` (oldest first, capped at 100 plus an omitted count).

Error response:

```json
{"id": 1, "ok": false, "error": "ORA-01435: user does not exist", "diag": {"category": "metadata", "op": "set-current-schema", "request-id": 1, "context": {"schema": "APP", "generated-sql": "ALTER SESSION SET CURRENT_SCHEMA = \"APP\""}}}
```

- `diag` (optional) carries troubleshooting metadata (`category`, `conn-id`, exception class, SQLState, vendor code, cause chain, redacted context, and `generated-sql` when internal SQL fails). Valid SQL contributes only its length; non-string SQL is rejected before JDBC access.
- `diag.connection-invalidated=true` (derived from the agent's connection map, never exception text) signals that the agent no longer owns that logical connection. If a foreground statement succeeds before the primary connection closes during reply finalization, top-level `connection-invalidated=true` lets the client keep the result while retiring its handle.
- `diag.execution-not-started=true` accompanies `diag.connection-invalidated=true` only when idle `Connection.isValid` preflight rejects a primary connection before `createStatement`/`prepareStatement`. The agent never reconnects or replays SQL on its own.
- When the client sets `params.debug=true`, failures may also include a redacted `debug` object (request context and Java stack trace).

### Operations

SQL Server connections advertise `describe-source-columns` to look up a projection's source columns on the metadata session without executing, and report the driver's current database on connect and foreground replies.

See the full [operation inventory and session contract](https://github.com/LuciusChen/clutch/blob/main/docs/jdbc-agent-protocol.md#core-operations) in the Clutch repository (including lock-bypassing `cancel` and `force-disconnect`).

## Type Conversion

| JDBC type | JSON output |
|---|---|
| `null` / `wasNull()` | `null` |
| `Boolean` | `true` / `false` |
| `Integer`, `Long`, `Short`, `Byte` | JSON number |
| `Double`, `Float` | JSON number (`NaN`/`Infinity` → string) |
| `BigDecimal` | String (preserves precision) |
| `Timestamp` | Local wall-clock string (`2024-01-15 13:45:30`) |
| `Date` | Date string (`2024-01-15`) |
| `Time` | Time string (`13:45:30`) |
| `Clob` | `{"__type":"clob","length":N,"preview":"..."}` (up to 256 UTF-16 units without splitting surrogate pairs; original length retained) |
| `Blob` / `byte[]` / `ByteBuffer` | `{"__type":"blob","length":N,"preview-base64":"..."}` (up to 256 leading bytes, plus `text` and `encoding` for UTF-8/GB18030 JSON/XML up to 64 KiB; >64 KiB JDBC `Blob` locators omit bytes; `ByteBuffer` reads from its current position without advancing it) |
| String / text | String (capped at 1,048,576 characters per cell) |
| Anything else | `rs.getString(col)` fallback |

## Architecture

```text
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

Foreground SQL, metadata, and Oracle bulk listings use separate locks; see the [runtime model](https://github.com/LuciusChen/clutch/blob/main/docs/jdbc-agent-protocol.md#runtime-model) for lock order, recovery, and cancellation.

Savepoint RPCs wrap standard JDBC `Connection` savepoints behind opaque agent-local IDs owned by `ConnectionManager`: `create-savepoint` rejects auto-commit sessions and checks `supportsSavepoints()`, rollback or release invalidates the target handle and every handle created after it, and commit/rollback/autocommit-change/disconnect clear all handles. For drivers such as Oracle that lack `Connection.releaseSavepoint`, an unsupported-release error after a successful batch or rollback is treated as a logical release (other release errors still propagate).

## Driver Setup

Place driver jars in `drivers/` next to `clutch-jdbc-agent.jar`; each jar is loaded at startup via `ServiceLoader<java.sql.Driver>`.

## License

GPL-3.0-or-later. See [LICENSE](LICENSE).
