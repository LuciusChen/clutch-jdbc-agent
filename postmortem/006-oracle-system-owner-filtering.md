# 006 — Oracle System-Owner Filtering Belongs in SQL

## Background

Oracle low-privilege object discovery now combines:

- `user_tables` / `user_views`
- `user_synonyms`
- accessible `all_tables` / `all_views`
- public synonyms for completion

The first implementation left two inconsistencies:

1. `get-tables` filtered Oracle system owners in SQL, but `search-tables`
   fetched them and discarded them later in Java.
2. The system-owner list was duplicated in two forms:
   a SQL string literal and a Java `Set`.

Both worked, but neither was robust.

## Decision

Use a single canonical owner list and derive the SQL literal from it.

Then apply system-owner filtering at the SQL level in both Oracle table paths:

- `get-tables`
- `search-tables`

`PUBLIC SYNONYM` remains intentionally exempt from that filtering so dictionary
objects such as `USER_TABLES` stay available to completion.

## Rationale

Filtering in SQL is the correct boundary here:

- it avoids pulling rows we already know we will discard
- it keeps `get-tables` and `search-tables` behavior aligned
- it removes a class of drift where one path silently diverges from the other

The single-source owner list solves the maintenance problem directly.  If the
owner set changes, both the Java lookup and the SQL literal change together.

## Alternatives considered

- **Keep Java-side filtering in `search-tables`**
  Rejected because it wastes work and leaves path-specific behavior harder to
  reason about.

- **Keep the duplicated SQL string and `Set`**
  Rejected because it creates an unnecessary consistency hazard with no upside.

- **Parameterize the `NOT IN` list**
  Rejected because Oracle needs a fixed number of placeholders here and the set
  is static.  Generating the literal from the canonical list is simpler.

## Known limitations

- The Oracle system-owner list is still curated manually.  If Oracle adds a new
  internal owner that should be hidden, the list must be updated explicitly.
- This filtering policy is tuned for query-console relevance, not for a full
  DBA object browser.  A future admin-focused explorer may need a broader view.
