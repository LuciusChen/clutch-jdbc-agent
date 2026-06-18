# 009 - Text-like BLOB Values

## Context

JDBC exposes Oracle `BLOB`, generic `Blob`, and `byte[]` values as binary
payloads. The original protocol intentionally returned only a small placeholder:

```json
{"__type":"blob","length":123}
```

That was safe and cheap, but it hid a common production shape: applications often
store UTF-8 or legacy-encoded JSON/XML text in binary columns. In those cases
Clutch could only render `<BLOB>`, while database clients such as DataGrip show a
compact textual preview when the bytes clearly decode to structured text.

## Decision

Keep binary BLOBs as placeholders, but enrich small text-like BLOBs with decoded
text:

```json
{"__type":"blob","length":123,"text":"...","encoding":"UTF-8"}
```

The detection is deliberately narrow:

- try UTF-8 first, then GB18030
- use strict decoders that reject malformed bytes
- accept decoded text only when it parses as JSON or has an XML-like shape
- keep all other BLOBs as the original placeholder

The protocol remains one response object per request. There is no BLOB streaming
or large binary download path.

## Why not decode every BLOB

Blind decoding is a fallback in the wrong layer. It would misrepresent arbitrary
binary values, add UI noise, and risk expensive conversions for data that Clutch
cannot safely display as text.

The narrow structured-text gate gives the Elisp side enough information to reuse
the normal JSON/XML display path without pretending every binary column is text.

## Compatibility

Existing clients that only understand `__type` and `length` can keep treating the
value as a BLOB placeholder. Newer Clutch versions may use `text` and `encoding`
when present.

This is still a release-level protocol change because the response payload can
carry additional fields, so Clutch must pin the agent version and checksum
together.

## Scope

This does not add:

- full BLOB content streaming
- binary export
- automatic character-set detection beyond UTF-8 and GB18030
- XML parsing or validation
- client-side mutation support for BLOB values

Those are separate product decisions and should not be hidden inside type
conversion.
