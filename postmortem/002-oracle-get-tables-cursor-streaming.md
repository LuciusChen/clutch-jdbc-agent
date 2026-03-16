# 002 — Oracle get-tables: cursor streaming replaces single-batch collection

## Background

An Oracle schema with 23,559 tables caused `get-tables` to always fail.
The handler collected all rows into a `List` before returning, which required
Oracle JDBC (default fetch size = 10) to make ~2,356 sequential round-trips
before the agent could send any response.  The 15 s `setQueryTimeout` expired
before all rows arrived.

## Decision

Split `getTables()` into two private methods:

**`oracleTablesCursor()`** — opens a `PreparedStatement` without
try-with-resources (the cursor owns the lifecycle), calls `rs.setFetchSize(1000)`
after `executeQuery()` (consistent with the `execute` handler's comment about
Oracle 11g parse-error behaviour), registers the `PreparedStatement` + ResultSet
with `CursorManager`, fetches the first batch of 1,000 rows, and returns a
cursor-format response identical to `execute`:

```json
{"cursor-id": 5, "columns": ["name","type","schema"], "rows": [...], "done": false}
```

If the PS or cursor setup throws, the PS is closed in the catch block before
rethrowing — the cursor manager never sees an invalid entry.

**`jdbcTablesOneBatch()`** — non-Oracle JDBC path; materializes via
`DatabaseMetaData.getTables()` as before, but wraps the result in the same
cursor-format with `cursor-id: null` and `done: true` so the Emacs side needs
no branching.

SQL column aliases are normalized to `name`/`type`/`schema` in both paths.

## Why Not Multithreading?

Considered and rejected.  The bottleneck is JDBC row streaming over one network
connection — parallel threads would share the same bandwidth.  The project
design (single-threaded bridge, no async/reactive) also explicitly defers this.

## setQueryTimeout

Reverted to 15 s.  The Oracle JDBC `setQueryTimeout` covers server-side
execution time for `user_tables ∪ user_views`, which is fast (<1 s).  Row
streaming latency is now absorbed by the cursor model rather than a single
inflated timeout.
