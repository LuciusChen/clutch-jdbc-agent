# 020 - BLOB Data Interface Binding

## Context

Typed binary parameters initially bound non-null BLOB values with
`PreparedStatement.setBlob(InputStream, length)`. That fixed Oracle's
`ORA-01465` conversion failure, but it selected Oracle's LOB-locator binding
path.

With the deployed Oracle JDBC 19.21 driver, `setBlob` creates a temporary BLOB,
writes and flushes the complete value, and binds its locator before
`PreparedStatement.execute` starts. Closing the statement then frees the
temporary BLOB. Those operations can add multiple database round trips to every
BLOB parameter. They also happen before the agent installs its execution
timeout and running-statement cancellation state.

The same driver rejects a non-null empty stream passed to `setBlob(stream, 0)`.
A length-bearing `setBinaryStream` avoids that exception, but a real Oracle
round trip showed that it stores zero bytes as SQL `NULL`. Neither stream setter
therefore preserves the protocol distinction between typed NULL and a zero-byte
BLOB.

## Decision

Bind non-empty BLOB bytes with the standard JDBC data interface:

```java
stmt.setBinaryStream(index, stream, byteLength);
```

For a non-null zero-byte BLOB, create an empty value with
`Connection.createBlob()` and bind it through `setBlob(index, blob)`. Free every
created Blob after execution. If a timed-out JDBC worker ignores cancellation,
defer `free()` until that worker actually exits rather than freeing a locator
that may still be in use.

Continue to use `setNull(index, Types.BLOB)` for a typed NULL. RAW and other
binary-family values continue to use `setBytes`.

The typed envelope and its byte-oriented semantics do not change. In
particular, an empty base64 string remains a non-null zero-byte value.

## Why the Data Interface

Oracle documents `setBytes` and length-bearing `setBinaryStream` as its data
interface for BLOB input. For SQL statements, small values can use direct
binding in the statement's network operation, while larger values can use
stream binding. In contrast, LOB binding creates and fills a temporary LOB and
therefore requires additional round trips.

`setBinaryStream` is preferable here because it is standard JDBC, preserves the
existing stream-shaped boundary, and lets the driver select direct or streamed
transfer from the exact byte length already known by the agent.

The empty value is deliberately exceptional. `Connection.createBlob` is also
standard JDBC, represents a non-null empty LOB without payload transfer, and
gives the agent an explicit resource to free.

Reference:
<https://docs.oracle.com/en/database/oracle/oracle-database/19/jjdbc/LOBs-and-BFiles.html>

## Alternatives Considered

- `setBlob(InputStream, length)` is type-explicit but forces the temporary-LOB
  path that caused the regression for every non-empty value and fails for an
  empty stream in Oracle JDBC 19.21.
- `setBytes` also participates in Oracle's data interface, but
  `setBinaryStream` gives the driver a stream and exact length without requiring
  a vendor-specific size policy in the agent.
- Oracle's proprietary `setBytesForBlob` was rejected because it explicitly
  forces LOB binding, is not portable, and treats an empty byte array as NULL in
  the inspected driver.
- Using `Connection.createBlob` for every value would retain the locator-style
  lifecycle and its extra work. It is reserved for the zero-byte case where a
  locator is necessary to distinguish an empty BLOB from SQL NULL.

## Verification and Limits

The dispatcher test exercises the actual `execute-params` route and verifies
the selected JDBC methods, exact UTF-8 and arbitrary bytes, typed NULL, non-null
empty values, cleanup after success and failure, and deferred cleanup for an
uncooperative timed-out worker. The H2 protocol integration test verifies
complete JSON decoding, binding, execution, and retrieval while distinguishing
populated, empty, and NULL binary values.

The real Oracle 19.21 rollback matrix verified exact non-null BLOB contents at
1, 1,389, 1,400, 1,500, 1,600, 1,800, 1,998, 1,999, 2,000, 2,001, 3,999,
4,000, 4,001, 32,765, 32,766, 32,767, 32,768, and 65,536 bytes. A separate
zero-byte probe verified that the empty-Blob branch remains non-null with
length zero. On the same connection, a small non-empty update fell from roughly
66–76 ms with locator binding to 27–29 ms with the data interface. H2 cannot
establish Oracle network-round-trip counts, so future driver upgrades should
retain the real rollback matrix.
