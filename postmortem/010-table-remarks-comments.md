# 010 — Table remarks stay on the generic JDBC metadata path

## Background

Clutch's Eldoc can show table comments when the backend exposes them.  Native
MySQL and PostgreSQL have direct comment metadata, but the JDBC agent previously
discarded `DatabaseMetaData.getTables()` remarks and returned no table comment
field to Emacs.

Oracle also has table comments, but Clutch's Oracle discovery path deliberately
does not use `DatabaseMetaData.getTables()` for normal table listing.  Earlier
Oracle work moved table discovery to direct SQL because the metadata path can be
slow or unreliable on large/low-privilege schemas.

## Decision

Expose table comments only on the generic JDBC metadata path:

- `jdbcTablesOneBatch()` appends a `comment` column populated from `REMARKS`.
- `searchJdbcMetadataTables()` includes optional `comment` in each entry.
- Blank remarks are normalized to `null`.

The Oracle direct SQL path keeps its existing row shape and does not join
`USER_TAB_COMMENTS` / `ALL_TAB_COMMENTS`.  Emacs treats `comment` as optional;
missing means unknown or unsupported.

## Rationale

This gives SQL Server, DB2, Snowflake, Redshift, generic JDBC URLs, MongoDB SQL
Interface, and other metadata-backed drivers the comment data their driver
already provides without adding backend-specific SQL.

Oracle is intentionally different.  Joining comment views would make table
discovery heavier and risks reintroducing the same class of metadata latency
that led to the Oracle direct-SQL path.  The agent should remain a thin bridge,
so it should not grow a per-vendor comment subsystem just to fill one optional
field.

## Alternatives considered

- **Join Oracle comment views in `get-tables` / `search-tables`**
  Rejected because it adds cost to hot metadata paths and works against the
  previous Oracle reliability decisions.

- **Add a separate `get-table-comment` operation**
  Rejected for now.  It would add protocol surface area and per-driver SQL while
  Emacs can already use comments opportunistically from table discovery.

## Known limitations

- Oracle table comments remain unavailable through JDBC until there is a
  measured, low-risk design for fetching one table's comment without affecting
  table discovery.
- Drivers differ in whether they populate `REMARKS`; a missing `comment` field
  or `null` value is not treated as an error.
