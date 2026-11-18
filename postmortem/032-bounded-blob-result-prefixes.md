# 032 - Bound JDBC BLOB result prefixes

The result envelope can include the first 256 bytes for hexadecimal inspection. The prefix is available when the JDBC driver has already returned bytes, or when the converter already reads a small locator to identify structured text. Complete JSON/XML text and its encoding continue to use the existing fields.

Fetching even 256 bytes from each large Oracle BLOB locator added a database round trip per row when LOB prefetch was disabled. A 40-row test went from 3 to 43 round trips. The converter keeps those locators length-only, which restored the original 3 round trips. This keeps preview generation out of the viewer and avoids retaining locators for later requests.
