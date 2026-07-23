# 019 - Typed Binary Prepared Parameters

## Context

`execute-params` originally carried only positional JSON values and bound every value with `PreparedStatement.setObject`. That kept the protocol portable for ordinary strings, numbers, booleans, structured JSON text, and SQL NULL, but it erased the one distinction Oracle needs for binary storage. When Clutch edited textual content stored in a `BLOB`, the driver received a Java string through `setObject`, attempted a database-specific hexadecimal conversion, and failed with `ORA-01465: invalid hex number`.

Clutch already receives the declared JDBC column type from result and schema metadata. The missing boundary was not more type inference in the agent; it was a way for the client to preserve that known binary type and transport exact bytes.

## Decision

Keep ordinary positional values unchanged and add one reserved binary parameter envelope:

```json
{"__clutch_jdbc_param":"binary","jdbc-type":"BLOB","base64":"eyJzdGF0dXMiOiJyZWFkeSJ9"}
```

The marker must be exactly `binary`. `jdbc-type` contains the declared JDBC type name, and `base64` contains the exact bytes. The agent recognizes BLOB types separately from RAW/BINARY-family types:

- BLOB values use `PreparedStatement.setBlob` with a byte stream and exact length.
- RAW/BINARY-family values use `PreparedStatement.setBytes`.
- A JSON `null` `base64` uses `setNull` with the corresponding JDBC binary type.
- An empty string `base64` decodes to a non-null zero-byte value.

Malformed base64, missing fields, unknown reserved markers, and unsupported binary type names fail at protocol validation before the agent accesses JDBC.

## Why a Reserved Envelope

A separate parallel type array would allow values and types to drift out of alignment. Wrapping every parameter would replace a stable protocol with a broader type system that JDBC drivers cannot implement portably. Reusing an ordinary object without a reserved marker would also conflict with the existing rule that arrays and objects are serialized as JSON text.

The reserved marker makes the exceptional path explicit and collision-resistant. Maps without `__clutch_jdbc_param` retain their existing JSON-text behavior, and all other ordinary parameters continue through `setObject`.

## Byte and Encoding Ownership

The envelope is byte-oriented. The agent decodes base64 but does not guess a character encoding. Clutch preserves the encoding reported when a text-like BLOB was decoded and uses it when the edited text is converted back to bytes; new text without source encoding uses UTF-8. This keeps driver binding separate from UI and edit-state policy.

## Compatibility and Scope

This is an additive protocol shape for ordinary parameter execution, but a binary envelope requires an agent version that understands the reserved marker. Clutch therefore updates its pinned agent version and published-jar checksum with the release.

The change does not add full BLOB download or streaming, arbitrary JDBC type tags, SQL literal rendering, or automatic binary-to-text detection during writes. It only carries exact bytes that the client already owns through the prepared-statement boundary.

## Superseded Decisions

This decision supersedes the values-only conclusion in [011](011-prepared-dml-protocol.md) for binary parameters and the deferred client-side BLOB mutation limitation in [009](009-text-like-blob-values.md). Their remaining decisions still apply to ordinary values, preview SQL, result conversion, and the absence of full BLOB streaming.
