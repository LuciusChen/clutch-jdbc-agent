# 016 — Signal primary connection invalidation explicitly

## Context

The agent removes a logical connection after a fatal foreground JDBC failure or an uncooperative execute/fetch timeout. The error response previously described the exception but did not tell the client that the referenced `conn-id` had been removed, so a live shared JVM could make the client reuse an id that the agent no longer owned.

## Decision

An error response now includes `diag.connection-invalidated=true` when the request semantically references a local logical connection that is absent from the connection manager after handling. This covers both the first poison response and later requests for the removed id if the first response was ignored. An unrelated or unknown operation does not become connection-scoped merely because its params contain a `conn-id` key.

Fatal exceptions from foreground execute, fetch, either cancellation path, and uncooperative foreground timeouts poison the logical connection. Classification follows JDBC cause and next-exception chains, so a plain SQLState `08` failure remains fatal even when a driver wraps it in a runtime exception; cursor fetch normalizes that wrapper to an `SQLException` with its cause intact while the cursor's connection id and lane are still available. A metadata cursor instead abandons unsafe cursor resources and atomically detaches its isolated metadata session. Both invalidation and successful replacement publish the new metadata state before the old JDBC connection is closed on the existing network executor, so a broken close cannot delay the response or hold the session lock. The next metadata request can replace the session without discarding the primary transaction. Metadata cursor fetches also stay out of the foreground running-statement slot, so an explicit cancel continues to target foreground SQL. Metadata-only failures therefore do not set the lifecycle marker.

## Rationale

The agent owns JDBC failure classification and the logical connection map. Inferring lifecycle from exception class, SQLState, vendor code, or error text in every client would duplicate policy and would make metadata-only failures indistinguishable from primary-session loss. Consulting the authoritative map also keeps the signal correct after a prior response is lost and avoids request-thread state leaking into a later response.

## Consequences

Clients can retire the stale id immediately while preserving the original failure for diagnostics. Other logical connections in the shared process remain usable. Foreground SQL is not replayed because the server may have executed it before the transport failure became visible; a later user command may establish a new session and execute once.

## Deliberate limit

The marker reports that the request's local logical connection is no longer usable. It is not a general retry hint, connection pool signal, or invitation to classify JDBC errors in the client.
