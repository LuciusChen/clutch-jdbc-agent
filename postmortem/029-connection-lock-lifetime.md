# 029 — Release idle connection locks

## Evidence

The dispatcher kept foreground and metadata locks in two maps keyed by a logical connection id, even after `disconnect`. Through the agent's JSON protocol with H2, 100 and 1000 completed connect/disconnect cycles left 251 and 2051 live `ReentrantLock` objects after full GC. The extra 900 cycles accounted for exactly 1800 extra locks.

## Decision

One map now holds a foreground/metadata lock pair only while requests are using or waiting for it. A request reserves the pair before attempting either lock and releases its reservation after unlocking. Both reservation and release update the per-id reference count through atomic map operations, so a new request cannot get a different lock while an earlier request still owns or waits for the old one. Requests acquiring both locks retain foreground-then-metadata order. `cancel` and `force-disconnect` still bypass these locks; a stuck request retains its reservation until it exits.

## Verification

The locally rebuilt agent retained 51 live `ReentrantLock` objects after 10, 100 and 1000 completed connect/disconnect cycles and full GC. Dispatcher concurrency, forced disconnect, and the full Java test suite passed. The change does not alter JDBC connection, cursor, transaction or wire protocol ownership.
