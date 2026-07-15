# 015 — Recover Oracle bad-packet connection failures

## Context

[Oracle defines `TNS-12592`](https://docs.oracle.com/en/error-help/db/tns-12592/) as a malformed packet. The affected Oracle JDBC driver reports it as a plain `SQLException` with SQLState `66000` and vendor code `12592`. The existing recovery rule recognized `SQLRecoverableException` and SQLState class `08`, so this real connection failure could pass a subsequent liveness check and leave Clutch's metadata session unusable even while foreground SQL on the primary session still succeeded.

The row-identity preflight exposed the mismatch most clearly: the user's `SELECT` returned rows, but metadata discovery failed and result editing was disabled. Treating that as a primary-key problem or broadly retrying metadata would hide the actual connection lifecycle defect.

## Decision

The connection-failure rule now recognizes the exact Oracle vendor code `12592` in addition to the existing recoverable exception and SQLState `08` cases. It follows both ordinary causes and JDBC `nextException` links with identity-based cycle protection, and both metadata recovery and foreground connection poisoning use that one rule.

Metadata requests still receive at most one retry. If that retry also loses its connection, the agent prepares a fresh metadata session only for the next request. If restoring the remembered schema fails after any replacement, that metadata session is invalidated so it cannot be reused under the default or an uncertain schema. The primary session remains untouched unless its own foreground operation reports a fatal connection failure.

## Rationale

Vendor code `12592` is the narrow stable signal available from the affected Oracle driver. Matching message text would depend on localization and formatting, while treating every Oracle `125xx` or every SQLState `66000` as recoverable would misclassify unrelated database errors. A retry loop was also rejected: it could multiply load and latency without repairing a persistent network fault.

## Consequences

A transient bad packet can self-heal without discarding a healthy primary transaction. Repeated failures remain visible through the normal structured error response, and a failed retry never triggers a third dispatch for the same request. Recovery may add a new JDBC connection only after a concrete failure; it does not add pooling, background retries, or a general exception policy layer.

## Limitations

The agent cannot prevent Oracle Net or the network from producing a malformed packet. It can only classify the resulting JDBC exception, discard unsafe session state, and bound recovery. Persistent transport or schema-restore failures still require the underlying Oracle/network configuration to be corrected.
