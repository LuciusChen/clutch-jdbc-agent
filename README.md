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

Produces `target/clutch-jdbc-agent-0.1.7.jar` (fat jar with Jackson bundled).
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

### Response format

```json
{"id": 1, "ok": true, "result": {"type": "query", "columns": ["1"], "rows": [[1]], "done": true}}
```

Error response:
```json
{"id": 1, "ok": false, "error": "Database error: ORA-00942: table or view does not exist"}
```

### Operations

| Op                | Description                                      |
|-------------------|--------------------------------------------------|
| `ping`            | Health check                                     |
| `connect`         | Open a JDBC connection, returns `conn-id`        |
| `disconnect`      | Close a connection and its open cursors          |
| `execute`         | Execute SQL; returns first batch + `cursor-id`   |
| `fetch`           | Fetch next batch from an open cursor             |
| `close-cursor`    | Close a cursor explicitly                        |
| `get-schemas`     | List schemas via `DatabaseMetaData`              |
| `get-tables`      | List tables/views in a schema                    |
| `search-tables`   | Prefix-search tables/views for completion        |
| `get-columns`     | List columns for a table                         |
| `search-columns`  | Prefix-search columns for completion             |
| `get-primary-keys`| List primary key columns                         |
| `get-foreign-keys`| List imported foreign keys                       |

## Type Conversion

JDBC column values are converted to a stable set of JSON-safe types:

| JDBC type                   | JSON output                                   |
|-----------------------------|-----------------------------------------------|
| `null` / `wasNull()`        | `null`                                        |
| `Boolean`                   | `true` / `false`                              |
| `Integer`, `Long`, `Short`, `Byte` | JSON number                          |
| `Double`, `Float`           | JSON number (NaN/Infinity → string)           |
| `BigDecimal`                | String (preserves precision)                  |
| `Timestamp`                 | ISO-8601 string (`2024-01-15T13:45:30Z`)      |
| `Date`                      | ISO-8601 date string (`2024-01-15`)           |
| `Time`                      | ISO-8601 time string (`13:45:30`)             |
| `Clob`                      | `{"__type":"clob","length":N,"preview":"..."}` |
| `Blob` / `byte[]` (≤64 KB)  | `{"__type":"blob","length":N}` or `{"__type":"blob","length":N,"text":"..."}` if valid UTF-8 JSON |
| Anything else               | `rs.getString(col)` fallback                  |

## Architecture

```
clutch-db-jdbc.el (Emacs)
        │  stdin/stdout (JSON, one object per line)
        ▼
clutch-jdbc-agent (JVM process)
  ├── Agent.java          — main(), stdin loop, driver loading
  ├── ConnectionManager   — connId → JDBC Connection
  ├── CursorManager       — cursorId → (Statement, ResultSet)
  ├── TypeConverter       — JDBC value → JSON-safe Java object
  └── handler/Dispatcher  — routes op strings to handler methods
        │
        ▼
  drivers/*.jar           — JDBC drivers (loaded at runtime via URLClassLoader)
```

The agent is a single-threaded process; all requests are handled sequentially.
No connection pooling, no async execution, no SQL rewriting — by design.

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
