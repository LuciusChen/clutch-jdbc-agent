# 018 — Column defaults are optional JDBC metadata

## Background

Clutch needs to offer `SET column = DEFAULT` only when the selected column has a database default.  The JDBC agent's `get-columns` response previously kept only name, type, nullability, and ordinal position, even though standard JDBC metadata exposes `COLUMN_DEF`.  Oracle's direct metadata queries also omitted `DATA_DEFAULT`, so the Emacs side could not distinguish a column with a default from one without one.

## Decision

Expose an optional `default` field on column metadata:

- generic JDBC metadata reads `COLUMN_DEF` from `DatabaseMetaData.getColumns()`;
- Oracle's existing `USER_TAB_COLUMNS` / `ALL_TAB_COLUMNS` fast paths select `DATA_DEFAULT` and return it under the same field name;
- a SQL `NULL` metadata value omits `default` from the response.

The agent returns the driver or database expression unchanged.  It does not parse, evaluate, trim, or rewrite default expressions.

## Rationale

This keeps the agent a thin metadata bridge while giving Clutch the column capability it needs.  The optional field is protocol-compatible with clients that ignore unknown fields, and omission accurately represents metadata that the driver cannot provide.

Oracle remains on direct SQL instead of moving back to `DatabaseMetaData.getColumns()`.  The dedicated path exists to keep metadata lookup predictable on large and low-privilege schemas; adding one selected column preserves that design.

## Alternatives considered

- **Infer a default from nullability or type.** Rejected because an implicit value is not the same as a declared column default.
- **Add a separate default-lookup operation.** Rejected because the value is already part of column metadata and a second request would add protocol and latency without new information.
- **Normalize expressions in the agent.** Rejected because default syntax is dialect-specific and belongs to the database, not the JDBC bridge.

## Known limitations

- JDBC drivers differ in how completely they populate `COLUMN_DEF`.
- A driver may report SQL `NULL` for both no default and an explicit `DEFAULT NULL`; the agent cannot recover a distinction the metadata source does not provide.
