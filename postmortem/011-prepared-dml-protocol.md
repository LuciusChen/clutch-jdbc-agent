# 011 - Bind staged DML values in JDBC

> Superseded in part by [019](019-typed-binary-prepared-parameters.md):
> `execute-params` now also accepts a reserved typed binary envelope; ordinary
> values retain the values-only binding path described here.

## Context

Clutch already represented staged mutations as SQL templates plus positional
values, but the JDBC adapter rendered those values back into SQL literals. That
discarded the parameter boundary before the request reached JDBC.

## Decision

Add `execute-params` with a positional `values` array. The agent prepares the SQL
and binds each JSON value with `PreparedStatement.setObject`. Object and array
values are serialized as JSON text because JDBC has no portable structured-value
binding contract.

The protocol intentionally carries values only. Clutch's optional parameter type
metadata is backend-specific and JDBC result metadata does not provide a portable
binding type for every driver. Sending an ignored type field would imply a
guarantee the agent cannot provide.

## Consequences

Mutation execution no longer depends on literal escaping. Preview remains a
rendered SQL string because it serves a different, user-facing purpose. The
agent integration test covers JSON decoding, false booleans, Unicode text, SQL
NULL, prepared binding, execution, and result serialization through a real H2
JDBC connection.
