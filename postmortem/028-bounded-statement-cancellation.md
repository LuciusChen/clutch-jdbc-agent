# 028 — Withdraw the bounded-cancellation experiment

## Verified limitation

Timeout recovery calls Statement.cancel synchronously before checking the worker's termination latch. A driver blocked inside cancel can prevent both that check and the agent response. Explicit cancel has the same limitation. A blocking Statement proxy reproduced this through Dispatcher; it was not an observed Oracle or other production-driver incident.

## Attempted design

The experiment submitted cancellation to the existing 16-slot JDBC execution pool and waited at most one second. Blocking or rejected cancellation retired the affected logical connection; metadata-fetch timeout retired only its metadata session. Four new blocking cases and the normal Java/database suites passed with that implementation.

## Why it was withdrawn before release

- The fixed one-second deadline was not justified by slow-but-successful cancellation measurements. Clutch already allows five seconds for a cancel acknowledgement, so the new deadline could force a reconnect that the existing client would have avoided.
- Cancellation competed with normal JDBC execution for the same 16 slots. With all slots busy, cancellation could be rejected without calling the driver at all, yet the experiment treated that rejection as grounds for invalidating the connection.
- Passing blocking-driver tests established the original limitation and one recovery outcome, not the acceptability of premature invalidation, transaction loss or the new scheduling dependency.

The experiment was withdrawn before commit or release. Dispatcher and its regular tests retain the previous implementation, including cooperative cancellation, worker-termination checks and metadata-session isolation. No replacement executor, timeout option or fallback was introduced.

## Retained evidence and open debt

The original CancelProbe.java and its before/after logs remain in the local audit artifacts. The withdrawn source/test patch and local jar were archived separately, so the failed design can be reproduced without keeping failing tests in the regular suite.

Clutch's outer RPC and cancel timeouts bound client waits, but do not guarantee physical driver termination or reclamation of every stuck agent thread. That limitation remains deliberately unresolved. A future proposal must cover slow successful cancellation, saturation of the execution pool, connection reuse and transaction preservation as well as permanent blocking before choosing a deadline or scheduling strategy.

The published 0.2.21 jar and Clutch's matching version/checksum are unchanged.
