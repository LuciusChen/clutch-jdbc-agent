# JDBC agent working guide

Maintain a thin, debuggable Java 17+ JDBC-to-JSON bridge. Prefer direct implementation and clear resource ownership over a general-purpose database framework.

## Implementation quality

- Correctness and passing tests are the baseline. A cleanup must also simplify state, control flow, ownership or reading the code; moving code or reducing line count alone is not enough.
- Helpers should own meaningful operations or shared rules. Remove pure forwarding, one-use accessor ladders and redundant validation when an existing owner already supplies the contract. Do not extract helpers to meet a line-count target or merely hide nesting.
- Keep genuinely different semantics separate. Primary and metadata sessions, transaction completion, savepoint recovery and diagnostic inspection are not interchangeable just because their code looks similar.
- Prefer straightforward Java 17, existing conventions, local data shapes and try-with-resources. Use package-private visibility where appropriate; keep Javadoc for public contracts. Split classes only at distinct responsibilities; do not add Service/Factory/Helper layers without a current need.
- Remove unused internal code and speculative compatibility scaffolding. Preserve the documented baseline and supported driver capabilities; do not delete necessary compatibility solely to reduce branches.
- Treat tests as implementation too: prove public behavior, lifecycle invariants and meaningful boundaries. Avoid tests that only lock in private helper structure; deterministic expected values are normal and random inputs are not a default requirement.

## Scope and completion

- An implementation request includes the change, relevant tests, diff review and necessary documentation. Continue through that work within the task and active permissions. A review or diagnosis request does not authorize implementation.
- Inspect the affected path and its callers; broaden when dependencies or evidence require it. Ask only for choices that materially affect scope, public behavior, compatibility or external effects.
- Preserve unrelated changes. Isolated local checks can be iterated within an authorized task; existing databases and user runtime directories are not disposable. Commit, push, publish and runtime replacement require task authorization.
- After a failed fix, revise the hypothesis before editing again. Resume when evidence supports a change; do not stack fallback paths or stop automatically after an arbitrary attempt count.
- Stop when the requested outcome and applicable verification are complete, or explain a concrete blocker. Report uncovered drivers or fault-injection limits accurately; passing a proxy-driver test does not establish behavior for every real JDBC driver.

## Ownership and protocol

- Java owns driver loading, connections, execution, cursors, metadata and value conversion. Pagination UI, SQL rewriting, schema caches, profiles and mutation orchestration belong to Clutch.
- `ConnectionManager` owns logical sessions and savepoints; `CursorManager` owns streaming resources; `Dispatcher` owns request routing, locks and execution lifecycle. Preserve primary/metadata session isolation. Metadata recovery must not discard or commit the primary transaction.
- Serialize operations on each JDBC session. Operations acquiring both locks use foreground-then-metadata order; `cancel` and `force-disconnect` bypass those locks for their existing lifecycle roles.
- stdout is only for one JSON response per request line, plus the startup ready message. Keep the dedicated protocol output and Java stdout quarantine in `Agent`; use structured stderr logging. Unparseable requests use id -1.
- Use `Request`'s typed accessors for their existing contracts. Invalid request fields must fail before JDBC work or cursor advancement; diagnostic inspection must not throw while describing the original error.
- Drivers remain external jars in `drivers/`. Preserve URLClassLoader/ServiceLoader loading and `DriverShim` registration; an empty driver directory is not itself a fatal startup error.
- Preserve value-conversion contracts, including decimal precision, local temporal values, Unicode-safe CLOB previews, original lengths, and complete text/encoding for supported small BLOBs. Read the relevant converter and tests before changing a representation.
- Do not add connection pooling, reactive orchestration, a new protocol/configuration framework, SQL parsing, multiple-result-set support or full LOB streaming without an explicitly scoped requirement and design rationale.

## Error and recovery boundaries

- `Dispatcher` converts dispatch/JDBC failures into structured error responses. Handlers normally propagate failures. `Agent` owns framing, parse errors, output failures and the outer unexpected-failure boundary; it is not a substitute for Dispatcher diagnostics.
- Catch expected exceptions at the owner that can validate, recover or clean up. Internal errors must not become success, empty results or guessed defaults. Preserve the primary exception and causal/suppressed diagnostics if cleanup also fails.
- Local resources use try-with-resources; cursor/session owners handle longer lifetimes and shutdown. Cleanup and timeout changes must preserve logical invalidation and must not introduce blocking work into a path that promises to return independently of driver cleanup.
- Normalize an unsupported optional JDBC operation only when the logical contract still holds. Catch the specific unsupported capability, keep broader SQL failures visible and verify both supported and unsupported paths. Preserve established legacy-driver handling only where that contract requires it.
- Recovery needs evidence, explicit ownership and a bounded policy. Do not replay user SQL with unknown execution/transaction outcome, introduce heuristic cancellation deadlines or share execution/cancellation capacity without proving the recovery and saturation semantics.

## Read when relevant

- Build commands, current module map and driver setup: [README](README.md).
- Protocol fields and value representations: [protocol](README.md#protocol), [type conversion](README.md#type-conversion) and the linked canonical Clutch protocol contract.
- Concurrency, recovery and session transitions: [architecture](README.md#architecture), the owning implementation and relevant tests.
- Non-obvious design or known limitations: search [postmortem/](postmortem/) for the affected topic. Do not read every historical record before a local edit.

## Verification

- Documentation/instruction-only changes: review consistency, links and the diff. No Java build, live database run or new product test is required.
- Code/tests: start with affected tests; before committing code, run `mvn package` with passing tests and no compiler warnings. Reuse a passing run for unchanged code and environment; repeat or broaden only for new changes, failures or unresolved concerns.
- Bug fixes need a failing regression before the fix. Reuse existing tests for mechanical or behavior-preserving cleanup. Protocol, routing and recovery tests should exercise Dispatcher or Agent as appropriate, not bypass the behavior being changed.
- Changes to drivers, execution, metadata, transactions, cancellation or conversion require the affected real database workflow via Clutch's test runner. Use Podman/disposable fixtures and the exact jar under test; identify which cases are unit tests, fault injection, live passes or skips. The client runner is `test/run-ci.sh native-live` in the Clutch checkout, with `CLUTCH_TEST_JDBC_AGENT_JAR` and `CLUTCH_TEST_JDBC_AGENT_DIR` selecting the isolated JDBC runtime.
- Before release, also smoke-test startup and ping against the exact artifact and review stdout isolation. Readiness is a separate id-0 response; the ping request must receive its own successful response.
- Review the complete intended diff and preserve unrelated work. Tests must not hide failures by changing fixtures or weakening assertions without a contract-based reason.

## Documentation and release

- Keep README and the canonical protocol document consistent with changed behavior. Coordinate wire field/semantic changes with `clutch-db-jdbc.el`; distinguish compatible additions from actual contract breaks.
- Java 17 is the published baseline. An intentional baseline change must update pom.xml, README, Clutch's requirements and release metadata, with a rationale.
- Published jar bytes are what Clutch consumes. Prefer a version bump for a changed release artifact; update Clutch's version/checksum pair against the published bytes. Do not substitute the checksum of an arbitrary local build.
- Keep a concise postmortem for non-obvious protocol, lifecycle, driver or compatibility decisions, abandoned designs and deliberately deferred limitations. Routine cleanup, wording or instruction maintenance does not require a new record; preserve historical records as history.
