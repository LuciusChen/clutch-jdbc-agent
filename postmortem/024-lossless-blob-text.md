# 024 - Preserve Structured BLOB Content

The released converter decoded structured binary payloads and called `strip()` before returning the text. A 15-byte JSON payload with surrounding whitespace therefore returned only 11 bytes of content. Since the caller uses the reported encoding for parameter binding, an unchanged round trip could change stored bytes.

Only structure detection now strips a temporary string. The returned text remains the exact decoded content. Regression tests exercise JSON and XML, leading/trailing whitespace, UTF-8 and GB18030, and verify the re-encoded bytes rather than only parse-equivalent JSON.

This changes no protocol fields and adds no large-object streaming. CLOBs remain explicit previews; callers must preserve incomplete-value state and reject lossy mutation or export. The fixed local build must be released under a new version before updating Clutch's published version/checksum pair; the existing 0.2.20 release asset is not replaced.
