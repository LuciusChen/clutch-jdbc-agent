# 026 — One owner for exact request field access

## Context

Dispatcher and MetadataOps independently implemented the same required integer and string checks. Their behavior agreed, so this was maintenance duplication, not a demonstrated protocol bug. Moving MetadataOps to Dispatcher utility methods would introduce a reverse handler dependency; a new validator class would add another abstraction for a small amount of code.

## Decision

Keep exact typed field access on the existing Request class. Both handlers use the same required accessors, and Dispatcher uses its optional integer accessor for timeouts. The non-throwing integer accessor also serves diagnostic context, where a malformed identifier must not mask the original failure. Missing, null, fractional and out-of-range values retain their existing acceptance and error messages.

Request remains independent of JDBC operations. Fetch-size bounds, non-negative idle intervals, optional metadata strings, and other operation-specific rules stay with their handlers. There are no new request subclasses, validation schemas, coercions, defaults, or protocol fields. This change does not publish a new jar or change the checksum pinned by Clutch.

## Verification and limits

Expanded dispatch tests exercise signed 32-bit boundaries, the accepted Java number representations, rejected numeric and non-numeric values, optional nulls, and required strings before and after the move. This consolidation claims less duplicated code, not a performance improvement or complete validation of every malformed request field.
