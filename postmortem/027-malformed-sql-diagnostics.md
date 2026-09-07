# 027 — Preserve SQL validation errors while building diagnostics

## Context

Both execute operations reject missing or non-string SQL before JDBC access. However, the error boundary subsequently cast the same malformed value to String when calculating diagnostic sql-length. A ClassCastException then escaped Dispatcher and replaced the useful validation error at the Agent fallback. It was not a JVM crash, but it lost the protocol category and structured diagnostics.

## Decision

Use the diagnostic class's existing non-throwing optionalStringParam accessor. Only a valid string contributes sql-length; an invalid value is omitted, not coerced or echoed. Validation still rejects the request, and valid SQL/error redaction behavior is unchanged. Ordinary diagnostics and opt-in debug already share requestContext, so one correction covers both without an additional catch, fallback response, or validation layer.

## Verification

Regression tests first failed against the unfixed implementation. They cover both execute operations, null/numeric/boolean/list/object SQL, debug on and off, preservation of other context fields, and valid SQL length. The production line loop must return exactly one structured error for every malformed request without leaking the invalid value, and continue serving requests. A separate Podman H2 TCP test verifies the same paths through Clutch's public query APIs and a successful query on the same connection after each rejection.
