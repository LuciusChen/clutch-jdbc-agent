# Java 17 Baseline

## Background

`clutch-jdbc-agent` had drifted to a Java 21 build baseline even though the
runtime code did not meaningfully depend on Java 21 APIs. Lowering all the way
to Java 11 was considered, but that path required mechanical rewrites of
records, switch expressions, pattern matching, and tests that did not improve
the agent's design or behavior.

## Decision

Adopt Java 17 as the supported build and runtime baseline.

## Rationale

- Java 17 has much wider availability than Java 21 in typical JDBC desktop and
  server environments.
- The codebase can stay close to the current straightforward style:
  records, `switch ->`, `var`, and `instanceof` pattern matching remain valid.
- Only Java 21-only constructs need to be removed. In practice this was the
  pattern-matching `switch` in `TypeConverter`.
- This keeps the agent boring and easy to debug while reducing the user's JVM
  burden.

## Alternatives Considered

### Stay on Java 21

Rejected because it raises the installation bar for a very small sidecar
without providing meaningful benefits to the protocol or JDBC behavior.

### Lower all the way to Java 11

Rejected because it forces broad syntax downgrades across the codebase with
little user benefit compared to Java 17. The resulting diff would be large,
mechanical, and harder to maintain.

## Known Limitations

- The project still assumes a modern enough toolchain to build with Maven on
  Java 17.
- If future changes introduce a true Java 21 dependency, that decision should be
  documented explicitly instead of silently drifting upward again.
