# 013 - Bound client-controlled fetch batches

## Context

The protocol accepted any integer `fetch-size` and passed it directly to
`CursorManager`. Zero produced an empty, unfinished batch forever; negative
values failed inside `ArrayList`; very large values requested an equally large
backing array before any row was read.

## Decision

Validate `fetch-size` once in `Dispatcher` for `execute`, `execute-params`, and
`fetch`. It must be an integer from 1 through 10,000; omitted values still use
the existing default of 500. Invalid values fail before JDBC access or cursor
advancement.

Ten thousand rows leaves ample room above Clutch's normal 500-row page and the
agent's 1,000-row Oracle metadata batch while placing a finite bound on the
client-controlled row-list allocation. The Elisp client enforces the same
range so custom settings fail before an RPC is sent.

## Scope

This is deliberately not a general resource-management framework. Cell values
and encoded response bytes can vary independently of row count. A byte budget
should be added only with measurements that define useful limits for real JDBC
drivers and wide result shapes.
