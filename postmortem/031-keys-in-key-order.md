# Foreign keys in key order, in both directions

Clutch groups the columns of a foreign key into one relation for its ER view and for following a key from a record, so it needs to know which columns form one key and in what order. `get-foreign-keys` reported one entry per column with no key identity, and in the order `DatabaseMetaData.getImportedKeys` defines: by referenced table, then position. Two keys over two columns that reference the same table therefore came interleaved, `x1, x2, y1, y2`, and no client could regroup them.

Each entry now carries `fk-name`, the driver's `FK_NAME`, when the driver reports one, and the agent sorts the columns by referenced table, key name and `KEY_SEQ`, so the columns of one key come together in key order. A driver that reports no `FK_NAME` keeps JDBC's order and gives no `fk-name`; the client then cannot tell keys apart, and Clutch treats each column as a key of its own rather than guessing. The field is an addition: a client that ignores it sees the same entries as before.

`get-referencing-keys` is new and reports the other direction, from `getExportedKeys`, as `fk-table`, `fk-schema`, `fk-column`, `pk-column` and `fk-name`, in the same order. `get-referencing-objects` was kept: it answers a different question, which tables depend on a table, one entry per table.

Oracle runs its own catalog queries. Its foreign-key query joined the referenced constraint by name alone, so a same-named constraint of another owner could supply the referenced columns; it now also matches `r_owner`, and orders by constraint name and position. Its referencing-keys query follows the same joins. The Oracle queries are covered by recorded-SQL unit tests only; H2 and a PostgreSQL driver ran the JDBC path end to end.
