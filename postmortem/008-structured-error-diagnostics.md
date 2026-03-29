# 008 — Structured Error Diagnostics

## Context

`clutch-jdbc-agent` originally returned request failures as a single string:

```json
{"ok":false,"error":"..."}
```

That kept the protocol simple, but it collapsed three different things into one
channel:

- the short message suitable for normal user display
- the structured details needed for debugging
- stderr/runtime logging

This became a real problem in practice for JDBC troubleshooting:

- connect failures needed SQLState, cause chain, and redacted connection context
- lazy-connect drivers often failed on first `execute`, not on `connect`
- users and tests needed something more stable than vendor-specific prose

## Decision

Keep the short `error` string, but add an optional `diag` payload on request
failures.

`diag` is for structured troubleshooting data:

- category
- op
- request id
- connection id when known
- exception class
- SQLState
- vendor code
- cause chain
- redacted request context
- generated/internal SQL in `diag.context.generated-sql` when the failing path
  was not user-authored SQL

stderr remains for process/runtime logging, not as the only request-level debug
channel.

Later follow-up extended this model with one more strictly opt-in layer:

- `debug` is only returned when the client explicitly asks for it
- it carries verbose but still redacted backend capture (currently request
  context + stack trace)
- it is not a replacement for `diag`; it is a deeper troubleshooting tool for
  reproductions, not the default UI contract

## Why not just append more text to `error`

Appending more prose to `error` makes both UX and testing worse:

- users get noisier default messages
- tests end up locking vendor wording instead of stable fields
- we still cannot safely carry redacted context in a structured way

Keeping `error` short while moving richer data into `diag` preserves a clean
default UX and gives Elisp a stable diagnostics contract.

## Redaction

Diagnostics must be safe to surface in Emacs and safe to copy into bug reports.

Never include:

- passwords
- cookie values
- auth tokens
- raw secret-bearing JDBC URL query parameter values

Allowed:

- redacted URL forms
- property names
- exception class / SQLState / vendor code
- request ids and connection ids

## Scope

This change does not introduce a full logging framework or UI by itself.

It only establishes the protocol foundation so the Elisp side can build a
proper single-buffer debug workflow without overloading stderr or the default
error string.  The follow-up UI now consumes this foundation through
`*clutch-debug*` and surfaces generated/internal SQL from the same
diagnostics payload.
