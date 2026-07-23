# clutch-jdbc-agent

A minimal JVM sidecar process that bridges [clutch](https://github.com/LuciusChen/clutch)
(an Emacs database client) to JDBC-compatible databases.

clutch has pure Elisp backends for MySQL, PostgreSQL, and SQLite.
For everything else — Oracle, SQL Server, DB2, Snowflake, Amazon Redshift, and
any other database with a JDBC driver — `clutch-db-jdbc.el` delegates to this
agent over a simple JSON protocol on stdin/stdout.

## Supported Databases

| Database             | Driver                          | Auto-install via Maven |
|----------------------|---------------------------------|------------------------|
| Oracle Database      | `ojdbc8` (default) / `ojdbc11`  | Yes                    |
| Microsoft SQL Server | `mssql-jdbc` (Microsoft)        | Yes                    |
| Amazon Redshift      | `redshift-jdbc` (AWS)           | Yes                    |
| Snowflake            | `snowflake-jdbc` (Snowflake)    | Yes                    |
| IBM DB2              | `db2jcc4` (IBM)                 | No — manual download   |
| Any JDBC database    | Place jar in `drivers/`         | —                      |

## Requirements

- Java 17+
- Maven (to build from source)

## Build

```
mvn package
```

Produces `target/clutch-jdbc-agent-<version>.jar` (fat jar with Jackson bundled).
JDBC driver jars are **not** bundled — they are loaded at runtime from a
`drivers/` directory next to the jar.

## Usage

The agent is not meant to be invoked manually.
`clutch-db-jdbc.el` manages the process lifecycle automatically:

```elisp
(require 'clutch-db-jdbc)
(clutch-jdbc-ensure-agent)          ; downloads the jar from GitHub Releases
(clutch-jdbc-install-driver 'oracle) ; downloads ojdbc8.jar + orai18n.jar from Maven Central
```

Then connect as usual:

```elisp
(setq clutch-connection-alist
      '(("prod-oracle" . (:backend oracle
                           :host "db.corp.com" :port 1521
                           :database "ORCL"
                           :user "scott" :pass-entry "prod-oracle"))))
```

See the [clutch README](https://github.com/LuciusChen/clutch#jdbc-backend-clutch-db-jdbcel)
for the full setup guide.

## Protocol

One JSON object per line on stdin/stdout. stdout is exclusively protocol output;
all logging goes to stderr.

### Request format

```json
{"id": 1, "op": "execute", "params": {"conn-id": 0, "sql": "SELECT 1"}}
```

Timeout-related params are explicit:

- `connect-timeout-seconds`: JDBC login/connect timeout for `connect`
- `network-timeout-seconds`: socket/network timeout applied to the JDBC `Connection`
- `query-timeout-seconds`: statement timeout applied before `execute`
- `validate-after-idle-seconds`: validate the primary connection before new SQL after this many idle seconds; omitted or zero disables the check

`execute`, `execute-params`, and `fetch` accept an optional integer
`fetch-size` from 1 through 10,000. The default is 500. Values outside that
range are rejected before JDBC work or cursor advancement.

Every protocol integer must be represented as an exact signed 32-bit integer;
fractional and overflowing JSON numbers are rejected instead of truncated.
If an execute/fetch timeout cannot stop the driver worker within a short grace
period, the logical connection is closed and cannot be reused concurrently.

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
values are bound with `PreparedStatement.setBlob`; RAW/BINARY-family values are
bound with `setBytes`. Typed nulls use the corresponding JDBC null type.
Malformed or unsupported envelopes fail before JDBC execution. This is a narrow
binary binding contract, not a general JDBC type-tagging or BLOB-streaming API.
When `auto-commit=false` is requested, connection creation fails unless the
driver accepts `setAutoCommit(false)`; the agent never silently continues in
auto-commit mode.

### Response format

```json
{"id": 1, "ok": true, "result": {"type": "query", "columns": ["1"], "rows": [[1]], "done": true}}
```

Error response:
```json
{"id": 1, "ok": false, "error": "ORA-01435: user does not exist", "diag": {"category": "metadata", "op": "set-current-schema", "request-id": 1, "context": {"schema": "APP", "generated-sql": "ALTER SESSION SET CURRENT_SCHEMA = \"APP\""}}}
```

`diag` is optional and is intended for troubleshooting, not for the default
user-facing error string.  It may include request metadata such as category,
connection id, exception class, SQLState, vendor code, cause chain, and
redacted request context.  When a hidden/internal query path fails,
`diag.context.generated-sql` carries the actual SQL text that the agent ran.

`diag.connection-invalidated=true` is also a stable lifecycle signal. It is present when an error response references a local logical connection that the agent no longer owns, including a follow-up request after the original failure response was ignored. The agent derives this from its connection map, not exception text or client-side JDBC error rules. Metadata-session failure alone does not set the marker. Foreground SQL is never replayed automatically because its transaction outcome may be unknown.

When idle preflight conclusively rejects a primary connection before the agent creates a `Statement` or `PreparedStatement`, the same response also includes `diag.execution-not-started=true`. This is evidence only about the current SQL request; it does not say that an earlier manual transaction can be recovered, and the agent still performs no automatic reconnect or SQL replay. The preflight uses JDBC `Connection.isValid`; the agent does not execute database-specific validation SQL or call `setAutoCommit`, `commit`, or `rollback` as part of that check.

When the client sends `params.debug=true`, failures may also include an
additional optional `debug` payload.  That payload is still redacted and is
meant for opt-in troubleshooting only; today it carries the redacted request
context and a redacted Java stack trace.

### Operations

| Op                | Description                                      |
|-------------------|--------------------------------------------------|
| `ping`            | Health check                                     |
| `connect`         | Open a JDBC connection, returns `conn-id`        |
| `disconnect`      | Close a connection and its open cursors          |
| `commit`          | Commit the current transaction                   |
| `rollback`        | Roll back the current transaction                |
| `set-auto-commit` | Toggle JDBC autocommit on the primary session    |
| `set-current-schema` | Update current schema on primary + metadata sessions |
| `cancel`          | Cancel the currently running statement for a connection |
| `execute`         | Execute SQL; returns first batch + `cursor-id`   |
| `execute-params`  | Execute SQL with positional prepared values      |
| `fetch`           | Fetch next batch from an open cursor             |
| `close-cursor`    | Close a cursor explicitly                        |
| `get-schemas`     | List schemas via `DatabaseMetaData`              |
| `get-tables`      | List schema/browser tables; generic JDBC includes `REMARKS` as optional `comment`; Oracle uses direct SQL over `user_*`, `user_synonyms`, and accessible `all_*` views |
| `search-tables`   | Prefix-search tables/views for completion; generic JDBC includes optional `comment`; Oracle also includes low-privilege synonym / accessible-owner paths, with system owners filtered in SQL except for `PUBLIC SYNONYM` |
| `get-columns`     | List columns for a table, including optional `default` expressions |
| `search-columns`  | Prefix-search columns for completion, including optional `default` expressions |
| `get-primary-keys`| List primary key columns                         |
| `get-foreign-keys`| List imported foreign keys                       |
| `get-indexes` / `get-index-columns` | Index metadata                  |
| `get-sequences`   | Sequence discovery                               |
| `get-procedures` / `get-functions` | Routine discovery                |
| `get-procedure-params` / `get-function-params` | Routine parameter metadata |
| `get-triggers`    | Trigger discovery                                |
| `get-object-source` / `get-object-ddl` | Source / DDL fetch             |
| `get-referencing-objects` | Referencing-object discovery              |

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
| `Clob`                      | `{"__type":"clob","length":N,"preview":"..."}` |
| `Blob` / `byte[]` (≤64 KB)  | `{"__type":"blob","length":N}` or `{"__type":"blob","length":N,"text":"..."}` if valid UTF-8 JSON |
| String / textual fallback   | String, limited to 1,048,576 characters per cell |
| Anything else               | `rs.getString(col)` fallback                  |

## Architecture

```
clutch-db-jdbc.el (Emacs)
        │  stdin/stdout (JSON, one object per line)
        ▼
clutch-jdbc-agent (JVM process)
  ├── Agent.java          — main(), stdin loop, driver loading
  ├── ConnectionManager   — connId → primary + metadata JDBC session
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

The agent no longer runs requests on one global synchronous lane. The stdin loop parses requests and hands them to a small request pool. The dispatcher uses independent per-connection foreground and metadata locks, so ordinary queries and metadata may run in parallel without allowing two operations to race on either JDBC session. Schema changes and disconnect acquire both locks in foreground-then-metadata order. If an idle timeout or Oracle `ORA-12592` makes the metadata session unsafe, the agent replaces only that session, restores the remembered schema, and retries the metadata request once; a failed retry or schema restore invalidates that replacement for the next request instead of reusing uncertain state. The primary transaction is untouched. `cancel` is the deliberate exception to serialization: it can arrive while `execute` or `fetch` is running, locate the live `Statement`, and call `Statement.cancel()` without tearing down the whole session.

This is still intentionally much simpler than a fully async server: no
connection pooling, no SQL rewriting, and no multi-statement scheduling inside
one JDBC connection.

For metadata recovery, a driver that does not implement `Connection.isValid`
is treated as lacking that optional probe, not as having a dead session. A
non-null, open metadata connection remains usable unless the triggering failure
is itself classified as a connection failure.

## Driver Setup

Drivers are loaded from `drivers/` next to `clutch-jdbc-agent.jar` at startup.
Each jar is scanned via `ServiceLoader<java.sql.Driver>`.

From Emacs:
```elisp
(clutch-jdbc-install-driver 'oracle)     ; → drivers/ojdbc8.jar + drivers/orai18n.jar
(clutch-jdbc-install-driver 'oracle-11)  ; → drivers/ojdbc11.jar + drivers/orai18n.jar
(clutch-jdbc-install-driver 'oracle-i18n); → drivers/orai18n.jar  (installed automatically for oracle)
(clutch-jdbc-install-driver 'sqlserver)  ; → drivers/mssql-jdbc.jar
(clutch-jdbc-install-driver 'snowflake)  ; → drivers/snowflake-jdbc.jar
(clutch-jdbc-install-driver 'redshift)   ; → drivers/redshift-jdbc.jar
```

For DB2, download `db2jcc4.jar` from IBM manually and place it in the `drivers/`
directory (`~/.emacs.d/clutch-jdbc/drivers/` by default).

For Oracle, `clutch-jdbc-install-driver 'oracle` now chooses `ojdbc8` by
default because it is the safest line across Oracle 11g/12c/19c. Installing
`oracle` automatically removes a conflicting `ojdbc11.jar`; installing
`oracle-11` does the inverse.

## License

GPL-3.0-or-later
